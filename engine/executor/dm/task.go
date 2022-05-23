// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package dm

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/dm/config"
	dmconfig "github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/jobmaster/dm"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	"github.com/pingcap/tiflow/engine/lib"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/lib/registry"
	"github.com/pingcap/tiflow/engine/model"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/broker"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RegisterWorker is used to register dm task to global registry
func RegisterWorker() {
	registry.GlobalWorkerRegistry().MustRegisterWorkerType(lib.WorkerDMDump, newTaskFactory(lib.WorkerDMDump))
	registry.GlobalWorkerRegistry().MustRegisterWorkerType(lib.WorkerDMLoad, newTaskFactory(lib.WorkerDMLoad))
	registry.GlobalWorkerRegistry().MustRegisterWorkerType(lib.WorkerDMSync, newTaskFactory(lib.WorkerDMSync))
}

// taskFactory create dm task
type taskFactory struct {
	workerType libModel.WorkerType
}

// newTaskFactory creates abstractFactory
func newTaskFactory(workerType libModel.WorkerType) *taskFactory {
	return &taskFactory{workerType: workerType}
}

// DeserializeConfig implements WorkerFactory.DeserializeConfig
func (f taskFactory) DeserializeConfig(configBytes []byte) (registry.WorkerConfig, error) {
	cfg := &dmconfig.SubTaskConfig{}
	err := cfg.Decode(string(configBytes), true)
	return cfg, err
}

// NewWorkerImpl implements WorkerFactory.NewWorkerImpl
func (f taskFactory) NewWorkerImpl(ctx *dcontext.Context, workerID libModel.WorkerID, masterID libModel.MasterID, conf lib.WorkerConfig) (lib.WorkerImpl, error) {
	baseDMTask := NewBaseDMTask(ctx, masterID, f.workerType, conf)
	switch f.workerType {
	case lib.WorkerDMDump:
		return newDumpTask(baseDMTask), nil
	case lib.WorkerDMLoad:
		return newLoadTask(baseDMTask), nil
	default:
		return newSyncTask(baseDMTask), nil
	}
}

// Task defines the interface for dump/load/sync
type Task interface {
	onInit(ctx context.Context) error
	onFinished(ctx context.Context) error
	createUnitHolder(cfg *config.SubTaskConfig) *unitHolder
}

// BaseTask implements some default methods for dm task
type BaseTask struct {
	Task
	lib.BaseWorker
	unitHolder   *unitHolder
	messageAgent *dmpkg.MessageAgentImpl

	ctx                context.Context
	cancel             context.CancelFunc
	cfg                *dmconfig.SubTaskConfig
	storageWriteHandle broker.Handle
	stage              metadata.TaskStage
	workerType         libModel.WorkerType
	taskID             string
	masterID           libModel.MasterID
}

// NewBaseDMTask creates BaseDMTask instances
func NewBaseDMTask(dCtx *dcontext.Context, masterID libModel.MasterID, workerType libModel.WorkerType, conf lib.WorkerConfig) BaseTask {
	ctx, cancel := context.WithCancel(context.Background())
	return BaseTask{
		ctx:        ctx,
		cancel:     cancel,
		cfg:        conf.(*config.SubTaskConfig),
		stage:      metadata.StageInit,
		workerType: workerType,
		taskID:     conf.(*config.SubTaskConfig).SourceID,
		masterID:   masterID,
	}
}

func (t *BaseTask) createComponents(ctx context.Context) error {
	log.L().Debug("create components")
	t.messageAgent = dmpkg.NewMessageAgent(t.ctx, map[string]dmpkg.Sender{t.masterID: t}, t)
	return nil
}

// InitImpl implements lib.BaseWorker.InitImpl
func (t *BaseTask) InitImpl(ctx context.Context) error {
	log.L().Info("init task")
	if err := t.createComponents(ctx); err != nil {
		return err
	}
	if err := t.onInit(ctx); err != nil {
		return err
	}
	t.unitHolder = t.createUnitHolder(t.cfg)
	return t.unitHolder.init(ctx)
}

// Tick implements lib.WorkerImpl.Tick
func (t *BaseTask) Tick(ctx context.Context) error {
	t.unitHolder.lazyProcess(ctx)
	if err := t.unitHolder.tick(ctx); err != nil {
		return err
	}
	return t.tryUpdateStatus(ctx)
}

// Workload implements lib.WorkerImpl.Worload
func (t *BaseTask) Workload() model.RescUnit {
	log.L().Info("dmtask.Workload")
	return 0
}

// OnMasterFailover implements lib.WorkerImpl.OnMasterFailover
func (t *BaseTask) OnMasterFailover(reason lib.MasterFailoverReason) error {
	log.L().Info("dmtask.OnMasterFailover")
	return nil
}

// OnMasterMessage implements lib.WorkerImpl.OnMasterMessage
func (t *BaseTask) OnMasterMessage(topic p2p.Topic, message p2p.MessageValue) error {
	log.L().Info("dmtask.OnMasterMessage", zap.String("topic", topic), zap.Any("message", message))
	return t.messageAgent.OnMessage(t.masterID, topic, message)
}

// CloseImpl implements lib.WorkerImpl.CloseImpl
func (t *BaseTask) CloseImpl(ctx context.Context) error {
	t.cancel()
	t.unitHolder.close()
	return nil
}

// setupStorge Open and configs external storage
func (t *BaseTask) setupStorge(ctx context.Context) error {
	rid := dm.NewDMResourceID(t.cfg.Name, t.cfg.SourceID)
	h, err := t.OpenStorage(ctx, rid)
	for status.Code(err) == codes.Unavailable {
		log.L().Info("simple retry", zap.Error(err))
		time.Sleep(time.Second)
		h, err = t.OpenStorage(ctx, rid)
	}
	if err != nil {
		return errors.Trace(err)
	}
	t.storageWriteHandle = h
	t.cfg.ExtStorage = h.BrExternalStorage()
	return nil
}

// persistStorge persist storge.
func (t *BaseTask) persistStorge(ctx context.Context) error {
	return t.storageWriteHandle.Persist(ctx)
}

func (t *BaseTask) tryUpdateStatus(ctx context.Context) error {
	stage := t.unitHolder.Stage()
	if stage == t.stage {
		return nil
	}
	log.L().Info("task stage changed", zap.Int("from", int(stage)), zap.Int("to", int(t.stage)))
	t.stage = stage

	status := t.workerStatus(stage)
	if stage != metadata.StageFinished {
		return t.UpdateStatus(ctx, status)
	}

	if err := t.onFinished(ctx); err != nil {
		log.L().Error("failed to handle finished status", zap.Error(err))
		// retry next tick
		return nil
	}
	return t.Exit(ctx, status, nil)
}

func (t *BaseTask) workerStatus(stage metadata.TaskStage) libModel.WorkerStatus {
	code := libModel.WorkerStatusNormal
	if stage == metadata.StageFinished {
		code = libModel.WorkerStatusFinished
	}
	status := runtime.NewTaskStatus(t.workerType, t.taskID, stage)
	// nolint:errcheck
	statusBytes, _ := json.Marshal(status)
	return libModel.WorkerStatus{
		Code:     code,
		ExtBytes: statusBytes,
	}
}
