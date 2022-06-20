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
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/dm/worker"
	"github.com/pingcap/tiflow/dm/pkg/backoff"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/framework"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/framework/registry"
	"github.com/pingcap/tiflow/engine/jobmaster/dm"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
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
	registry.GlobalWorkerRegistry().MustRegisterWorkerType(framework.WorkerDMDump, newWorkerFactory(framework.WorkerDMDump))
	registry.GlobalWorkerRegistry().MustRegisterWorkerType(framework.WorkerDMLoad, newWorkerFactory(framework.WorkerDMLoad))
	registry.GlobalWorkerRegistry().MustRegisterWorkerType(framework.WorkerDMSync, newWorkerFactory(framework.WorkerDMSync))
}

// workerFactory create dm task
type workerFactory struct {
	workerType frameModel.WorkerType
}

// newWorkerFactory creates abstractFactory
func newWorkerFactory(workerType frameModel.WorkerType) *workerFactory {
	return &workerFactory{workerType: workerType}
}

// DeserializeConfig implements WorkerFactory.DeserializeConfig
func (f workerFactory) DeserializeConfig(configBytes []byte) (registry.WorkerConfig, error) {
	cfg := &dmconfig.SubTaskConfig{}
	err := cfg.Decode(string(configBytes), true)
	return cfg, err
}

// NewWorkerImpl implements WorkerFactory.NewWorkerImpl
func (f workerFactory) NewWorkerImpl(ctx *dcontext.Context, workerID frameModel.WorkerID, masterID frameModel.MasterID, conf framework.WorkerConfig) (framework.WorkerImpl, error) {
	log.L().Info("new dm worker", zap.String("id", workerID), zap.String("master-id", masterID))
	return newDMWorker(ctx, masterID, f.workerType, conf.(*dmconfig.SubTaskConfig)), nil
}

// dmWorker implements methods for framework.WorkerImpl
type dmWorker struct {
	framework.BaseWorker

	unitHolder   unitHolder
	messageAgent dmpkg.MessageAgent
	autoResume   *worker.AutoResumeInfo

	mu                 sync.RWMutex
	cfg                *dmconfig.SubTaskConfig
	storageWriteHandle broker.Handle
	stage              metadata.TaskStage
	workerType         frameModel.WorkerType
	taskID             string
	masterID           frameModel.MasterID
}

func newDMWorker(ctx *dcontext.Context, masterID frameModel.MasterID, workerType framework.WorkerType, cfg *dmconfig.SubTaskConfig) *dmWorker {
	// TODO: support config later
	// nolint:errcheck
	bf, _ := backoff.NewBackoff(dmconfig.DefaultBackoffFactor, dmconfig.DefaultBackoffJitter, dmconfig.DefaultBackoffMin, dmconfig.DefaultBackoffMax)
	autoResume := &worker.AutoResumeInfo{Backoff: bf, LatestPausedTime: time.Now(), LatestResumeTime: time.Now()}
	w := &dmWorker{
		cfg:        cfg,
		stage:      metadata.StageInit,
		workerType: workerType,
		taskID:     cfg.SourceID,
		masterID:   masterID,
		unitHolder: newUnitHolderImpl(workerType, cfg),
		autoResume: autoResume,
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
	log.L().Info("initializing the dm worker", zap.String("task-id", w.taskID))
	if err := w.messageAgent.Init(ctx); err != nil {
		return err
	}
	// register jobmaster client
	if err := w.messageAgent.UpdateClient(w.masterID, w); err != nil {
		return err
	}
	if w.cfg.Mode != dmconfig.ModeIncrement {
		if err := w.setupStorage(ctx); err != nil {
			return err
		}
	}
	w.cfg.MetricsFactory = w.MetricFactory()
	return w.unitHolder.Init(ctx)
}

// Tick implements lib.WorkerImpl.Tick
func (w *dmWorker) Tick(ctx context.Context) error {
	if err := w.checkAndAutoResume(ctx); err != nil {
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
func (w *dmWorker) OnMasterFailover(reason framework.MasterFailoverReason) error {
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
	log.L().Info("close the dm worker", zap.String("task-id", w.taskID))
	var recordErr error
	// unregister jobmaster client
	if err := w.messageAgent.UpdateClient(w.masterID, nil); err != nil {
		log.L().Error("failed to update message client", log.ShortError(err))
		recordErr = err
	}
	w.unitHolder.Close(ctx)
	if err := w.messageAgent.Close(ctx); err != nil {
		log.L().Error("failed to close message client", log.ShortError(err))
		recordErr = err
	}
	return recordErr
}

// setupStorage opens and configs external storage
func (w *dmWorker) setupStorage(ctx context.Context) error {
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
	currentStage, _ := w.unitHolder.Stage()
	previousStage := w.getStage()
	if currentStage == previousStage {
		return nil
	}
	log.L().Info("task stage changed", zap.String("task-id", w.taskID), zap.Int("from", int(previousStage)), zap.Int("to", int(currentStage)))
	w.setStage(currentStage)

	status := w.workerStatus()
	if currentStage != metadata.StageFinished {
		log.L().Info("update status", zap.String("task-id", w.taskID), zap.String("status", string(status.ExtBytes)))
		return w.UpdateStatus(ctx, status)
	}

	if w.workerType == framework.WorkerDMDump {
		if err := w.persistStorage(ctx); err != nil {
			log.L().Error("failed to persist storage", zap.Error(err))
			// persist in next tick
			return nil
		}
	}
	return w.Exit(ctx, status, nil)
}

// workerStatus gets worker status.
func (w *dmWorker) workerStatus() frameModel.WorkerStatus {
	stage := w.getStage()
	code := frameModel.WorkerStatusNormal
	if stage == metadata.StageFinished {
		code = frameModel.WorkerStatusFinished
	}
	status := &runtime.TaskStatus{Unit: w.workerType, Task: w.taskID, Stage: stage}
	// nolint:errcheck
	statusBytes, _ := json.Marshal(status)
	return frameModel.WorkerStatus{
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

func (w *dmWorker) checkAndAutoResume(ctx context.Context) error {
	stage, result := w.unitHolder.Stage()
	if stage != metadata.StageError {
		return nil
	}

	log.L().Error("task runs with error", zap.String("task-id", w.taskID), zap.Any("error msg", result.Errors))
	subtaskStage := &pb.SubTaskStatus{
		Stage:  pb.Stage_Paused,
		Result: result,
	}
	strategy := w.autoResume.CheckResumeSubtask(subtaskStage, dmconfig.DefaultBackoffRollback)
	log.L().Info("got auto resume strategy", zap.String("task-id", w.taskID), zap.Stringer("strategy", strategy))

	if strategy == worker.ResumeDispatch {
		log.L().Info("dispatch auto resume task", zap.String("task-id", w.taskID))
		return w.unitHolder.Resume(ctx)
	}
	return nil
}
