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
	"sync"
	"time"

	"github.com/pingcap/errors"
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
	registry.GlobalWorkerRegistry().MustRegisterWorkerType(lib.WorkerDMDump, newWorkerFactory(lib.WorkerDMDump))
	registry.GlobalWorkerRegistry().MustRegisterWorkerType(lib.WorkerDMLoad, newWorkerFactory(lib.WorkerDMLoad))
	registry.GlobalWorkerRegistry().MustRegisterWorkerType(lib.WorkerDMSync, newWorkerFactory(lib.WorkerDMSync))
}

// workerFactory create dm task
type workerFactory struct {
	workerType libModel.WorkerType
}

// newWorkerFactory creates abstractFactory
func newWorkerFactory(workerType libModel.WorkerType) *workerFactory {
	return &workerFactory{workerType: workerType}
}

// DeserializeConfig implements WorkerFactory.DeserializeConfig
func (f workerFactory) DeserializeConfig(configBytes []byte) (registry.WorkerConfig, error) {
	cfg := &dmconfig.SubTaskConfig{}
	err := cfg.Decode(string(configBytes), true)
	return cfg, err
}

// NewWorkerImpl implements WorkerFactory.NewWorkerImpl
func (f workerFactory) NewWorkerImpl(ctx *dcontext.Context, workerID libModel.WorkerID, masterID libModel.MasterID, conf lib.WorkerConfig) (lib.WorkerImpl, error) {
	return newDMWorker(ctx, masterID, f.workerType, conf.(*dmconfig.SubTaskConfig)), nil
}

// dmWorker implements methods for lib.WorkerImpl
type dmWorker struct {
	lib.BaseWorker

	unitHolder   unitHolder
	messageAgent dmpkg.MessageAgent

	mu                 sync.RWMutex
	cfg                *dmconfig.SubTaskConfig
	storageWriteHandle broker.Handle
	stage              metadata.TaskStage
	workerType         libModel.WorkerType
	taskID             string
	masterID           libModel.MasterID
}

func newDMWorker(ctx *dcontext.Context, masterID libModel.MasterID, workerType lib.WorkerType, cfg *dmconfig.SubTaskConfig) *dmWorker {
	w := &dmWorker{
		cfg:        cfg,
		stage:      metadata.StageInit,
		workerType: workerType,
		taskID:     cfg.SourceID,
		masterID:   masterID,
		unitHolder: newUnitHolderImpl(workerType, cfg),
	}

	// nolint:errcheck
	ctx.Deps().Construct(func(m p2p.MessageHandlerManager) (p2p.MessageHandlerManager, error) {
		w.messageAgent = dmpkg.NewMessageAgentImpl(w.taskID, w, m)
		return m, nil
	})
	return w
}

// InitImpl implements lib.WorkerImpl.InitImpl
func (w *dmWorker) InitImpl(ctx context.Context) error {
	if err := w.messageAgent.Init(ctx); err != nil {
		return err
	}
	// register jobmaster sender
	if err := w.messageAgent.UpdateSender(w.masterID, w); err != nil {
		return err
	}
	if w.cfg.Mode != dmconfig.ModeIncrement {
		if err := w.setupstorage(ctx); err != nil {
			return err
		}
	}
	return w.unitHolder.Init(ctx)
}

// Tick implements lib.WorkerImpl.Tick
func (w *dmWorker) Tick(ctx context.Context) error {
	if err := w.unitHolder.Tick(ctx); err != nil {
		return err
	}
	if err := w.tryUpdateStatus(ctx); err != nil {
		return err
	}
	return w.messageAgent.Tick(ctx)
}

// Workload implements lib.WorkerImpl.Worload
func (w *dmWorker) Workload() model.RescUnit {
	log.L().Info("dmworker.Workload")
	return 0
}

// OnMasterFailover implements lib.WorkerImpl.OnMasterFailover
func (w *dmWorker) OnMasterFailover(reason lib.MasterFailoverReason) error {
	log.L().Info("dmworker.OnMasterFailover")
	return nil
}

// OnMasterMessage implements lib.WorkerImpl.OnMasterMessage
func (w *dmWorker) OnMasterMessage(topic p2p.Topic, message p2p.MessageValue) error {
	log.L().Info("dmworker.OnMasterMessage", zap.String("topic", topic), zap.Any("message", message))
	return nil
}

// CloseImpl implements lib.WorkerImpl.CloseImpl
func (w *dmWorker) CloseImpl(ctx context.Context) error {
	// unreigster jobmaster sender
	if err := w.messageAgent.UpdateSender(w.masterID, nil); err != nil {
		return err
	}
	w.unitHolder.Close(ctx)
	w.messageAgent.Close(ctx)
	return nil
}

// exit closes the task and exits.
func (w *dmWorker) exit(ctx context.Context, status libModel.WorkerStatus, err error) error {
	if err := w.CloseImpl(ctx); err != nil {
		log.L().Warn("fail to close task", log.ShortError(err))
	}
	return w.Exit(ctx, status, err)
}

// setupstorage opens and configs external storage
func (w *dmWorker) setupstorage(ctx context.Context) error {
	rid := dm.NewDMResourceID(w.cfg.Name, w.cfg.SourceID)
	h, err := w.OpenStorage(ctx, rid)
	for status.Code(err) == codes.Unavailable {
		log.L().Info("simple retry", zap.Error(err))
		time.Sleep(time.Second)
		h, err = w.OpenStorage(ctx, rid)
	}
	if err != nil {
		return errors.Trace(err)
	}
	w.storageWriteHandle = h
	w.cfg.ExtStorage = h.BrExternalStorage()
	return nil
}

// persistStorage persists storage.
func (w *dmWorker) persistStorage(ctx context.Context) error {
	return w.storageWriteHandle.Persist(ctx)
}

// tryUpdateStatus updates status when task stage changed.
func (w *dmWorker) tryUpdateStatus(ctx context.Context) error {
	stage := w.unitHolder.Stage()
	if stage == w.getStage() {
		return nil
	}
	log.L().Info("task stage changed", zap.Int("from", int(stage)), zap.Int("to", int(w.stage)))
	w.setStage(stage)

	status := w.workerStatus()
	if stage != metadata.StageFinished {
		return w.UpdateStatus(ctx, status)
	}

	if w.workerType == lib.WorkerDMDump {
		if err := w.persistStorage(ctx); err != nil {
			log.L().Error("failed to persist storage", zap.Error(err))
			return nil
		}
	}
	return w.exit(ctx, status, nil)
}

// workerStatus gets worker status.
func (w *dmWorker) workerStatus() libModel.WorkerStatus {
	stage := w.getStage()
	code := libModel.WorkerStatusNormal
	if stage == metadata.StageFinished {
		code = libModel.WorkerStatusFinished
	}
	status := runtime.TaskStatus{Unit: w.workerType, Task: w.taskID, Stage: stage}
	// nolint:errcheck
	statusBytes, _ := json.Marshal(status)
	return libModel.WorkerStatus{
		Code:     code,
		ExtBytes: statusBytes,
	}
}

// getStage gets stage.
func (w *dmWorker) getStage() metadata.TaskStage {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.stage
}

func (w *dmWorker) setStage(stage metadata.TaskStage) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stage = stage
}
