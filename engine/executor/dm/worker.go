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

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	dmconfig "github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/backoff"
	"github.com/pingcap/tiflow/dm/worker"
	"github.com/pingcap/tiflow/engine/framework"
	"github.com/pingcap/tiflow/engine/framework/logutil"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/framework/registry"
	"github.com/pingcap/tiflow/engine/jobmaster/dm"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	"github.com/pingcap/tiflow/engine/model"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/broker"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	derror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RegisterWorker is used to register dm task to global registry
func RegisterWorker() {
	registry.GlobalWorkerRegistry().MustRegisterWorkerType(frameModel.WorkerDMDump, newWorkerFactory(frameModel.WorkerDMDump))
	registry.GlobalWorkerRegistry().MustRegisterWorkerType(frameModel.WorkerDMLoad, newWorkerFactory(frameModel.WorkerDMLoad))
	registry.GlobalWorkerRegistry().MustRegisterWorkerType(frameModel.WorkerDMSync, newWorkerFactory(frameModel.WorkerDMSync))
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
	cfg := &config.TaskCfg{}
	_, err := toml.Decode(string(configBytes), cfg)
	return cfg, err
}

// NewWorkerImpl implements WorkerFactory.NewWorkerImpl
func (f workerFactory) NewWorkerImpl(ctx *dcontext.Context, workerID frameModel.WorkerID, masterID frameModel.MasterID, conf framework.WorkerConfig) (framework.WorkerImpl, error) {
	cfg := conf.(*config.TaskCfg)
	log.Info("new dm worker", zap.String(logutil.ConstFieldJobKey, masterID), zap.Stringer("worker_type", f.workerType), zap.String(logutil.ConstFieldWorkerKey, workerID), zap.Any("task_config", cfg))
	return newDMWorker(ctx, masterID, f.workerType, cfg), nil
}

// IsRetryableError implements WorkerFactory.IsRetryableError
func (f workerFactory) IsRetryableError(err error) bool {
	return true
}

// dmWorker implements methods for framework.WorkerImpl
type dmWorker struct {
	framework.BaseWorker

	unitHolder   unitHolder
	messageAgent dmpkg.MessageAgent
	autoResume   *worker.AutoResumeInfo

	mu                    sync.RWMutex
	cfg                   *dmconfig.SubTaskConfig
	storageWriteHandle    broker.Handle
	stage                 metadata.TaskStage
	workerType            frameModel.WorkerType
	taskID                string
	masterID              frameModel.MasterID
	messageHandlerManager p2p.MessageHandlerManager

	cfgModRevision uint64
	needExtStorage bool
}

func newDMWorker(ctx *dcontext.Context, masterID frameModel.MasterID, workerType framework.WorkerType, cfg *config.TaskCfg) *dmWorker {
	// TODO: support config later
	// nolint:errcheck
	bf, _ := backoff.NewBackoff(dmconfig.DefaultBackoffFactor, dmconfig.DefaultBackoffJitter, dmconfig.DefaultBackoffMin, dmconfig.DefaultBackoffMax)
	autoResume := &worker.AutoResumeInfo{Backoff: bf, LatestPausedTime: time.Now(), LatestResumeTime: time.Now()}
	dmSubtaskCfg := cfg.ToDMSubTaskCfg(masterID)
	w := &dmWorker{
		cfg:            dmSubtaskCfg,
		stage:          metadata.StageInit,
		workerType:     workerType,
		taskID:         dmSubtaskCfg.SourceID,
		masterID:       masterID,
		unitHolder:     newUnitHolderImpl(workerType, dmSubtaskCfg),
		autoResume:     autoResume,
		cfgModRevision: cfg.ModRevision,
		needExtStorage: cfg.NeedExtStorage,
	}

	// nolint:errcheck
	ctx.Deps().Construct(func(m p2p.MessageHandlerManager) (p2p.MessageHandlerManager, error) {
		w.messageHandlerManager = m
		return m, nil
	})
	return w
}

// InitImpl implements lib.WorkerImpl.InitImpl
func (w *dmWorker) InitImpl(ctx context.Context) error {
	w.Logger().Info("initializing the dm worker", zap.String("task-id", w.taskID))
	w.messageAgent = dmpkg.NewMessageAgentImpl(w.taskID, w, w.messageHandlerManager, w.Logger())
	// register jobmaster client
	if err := w.messageAgent.UpdateClient(w.masterID, w); err != nil {
		return err
	}
	if w.cfg.Mode != dmconfig.ModeIncrement && w.needExtStorage {
		if err := w.setupStorage(ctx); err != nil {
			return err
		}
	}
	w.cfg.MetricsFactory = w.MetricFactory()
	w.cfg.FrameworkLogger = w.Logger()
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
	// update unit status periodically to update metrics
	w.unitHolder.CheckAndUpdateStatus(ctx)
	return w.messageAgent.Tick(ctx)
}

// Workload implements lib.WorkerImpl.Worload
func (w *dmWorker) Workload() model.RescUnit {
	w.Logger().Info("dmworker.Workload")
	return 0
}

// OnMasterMessage implements lib.WorkerImpl.OnMasterMessage
func (w *dmWorker) OnMasterMessage(ctx context.Context, topic p2p.Topic, message p2p.MessageValue) error {
	w.Logger().Info("dmworker.OnMasterMessage", zap.String("topic", topic), zap.Any("message", message))
	return nil
}

// CloseImpl implements lib.WorkerImpl.CloseImpl
func (w *dmWorker) CloseImpl(ctx context.Context) {
	w.Logger().Info("close the dm worker", zap.String("task-id", w.taskID))

	if err := w.unitHolder.Close(ctx); err != nil {
		w.Logger().Error("fail to close unit holder", zap.Error(err))
	}

	if w.messageAgent == nil {
		return
	}
	if err := w.messageAgent.UpdateClient(w.masterID, nil); err != nil {
		w.Logger().Error("failed to update message client", zap.Error(err))
	}
	if err := w.messageAgent.Close(ctx); err != nil {
		w.Logger().Error("failed to close message client", zap.Error(err))
	}
}

// setupStorage opens and configs external storage
func (w *dmWorker) setupStorage(ctx context.Context) error {
	rid := dm.NewDMResourceID(w.cfg.Name, w.cfg.SourceID)
	opts := []broker.OpenStorageOption{}
	if w.workerType == frameModel.WorkerDMDump {
		// always use an empty storage for dumpling task
		opts = append(opts, broker.WithCleanBeforeOpen())
	}

	h, err := w.OpenStorage(ctx, rid, opts...)
	for status.Code(err) == codes.Unavailable {
		w.Logger().Info("simple retry", zap.Error(err))
		time.Sleep(time.Second)
		h, err = w.OpenStorage(ctx, rid, opts...)
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
	w.Logger().Info("task stage changed", zap.String("task-id", w.taskID), zap.Stringer("from", previousStage), zap.Stringer("to", currentStage))
	w.setStage(currentStage)

	status := w.workerStatus(ctx)
	if currentStage != metadata.StageFinished {
		w.Logger().Info("update status", zap.String("task-id", w.taskID), zap.String("status", string(status.ExtBytes)))
		return w.UpdateStatus(ctx, status)
	}

	if w.workerType == frameModel.WorkerDMDump {
		if err := w.persistStorage(ctx); err != nil {
			w.Logger().Error("failed to persist storage", zap.Error(err))
			// persist in next tick
			return nil
		}
	}

	if err := w.Exit(ctx, framework.ExitReasonFinished, nil, status.ExtBytes); err != nil {
		return err
	}

	return derror.ErrWorkerFinish.FastGenByArgs()
}

// workerStatus gets worker status.
func (w *dmWorker) workerStatus(ctx context.Context) frameModel.WorkerStatus {
	var (
		stage       = w.getStage()
		code        frameModel.WorkerState
		taskStatus  = &runtime.TaskStatus{Unit: w.workerType, Task: w.taskID, Stage: stage, CfgModRevision: w.cfgModRevision}
		finalStatus any
	)
	if stage == metadata.StageFinished {
		code = frameModel.WorkerStateFinished
		_, result := w.unitHolder.Stage()
		status := w.unitHolder.Status(ctx)
		// nolint:errcheck
		statusBytes, _ := json.Marshal(status)
		finalStatus = &runtime.FinishedTaskStatus{
			TaskStatus: *taskStatus,
			Result:     result,
			Status:     statusBytes,
		}
	} else {
		code = frameModel.WorkerStateNormal
		finalStatus = taskStatus
	}
	// nolint:errcheck
	statusBytes, _ := json.Marshal(finalStatus)
	return frameModel.WorkerStatus{
		State:    code,
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

	w.Logger().Error("task runs with error", zap.String("task-id", w.taskID), zap.Any("error msg", result.Errors))
	subtaskStage := &pb.SubTaskStatus{
		Stage:  pb.Stage_Paused,
		Result: result,
	}
	strategy := w.autoResume.CheckResumeSubtask(subtaskStage, dmconfig.DefaultBackoffRollback)
	w.Logger().Info("got auto resume strategy", zap.String("task-id", w.taskID), zap.Stringer("strategy", strategy))

	if strategy == worker.ResumeDispatch {
		w.Logger().Info("dispatch auto resume task", zap.String("task-id", w.taskID))
		err := w.unitHolder.Resume(ctx)
		if err == nil {
			w.autoResume.LatestResumeTime = time.Now()
			w.autoResume.Backoff.BoundaryForward()
		}
		return err
	}
	return nil
}
