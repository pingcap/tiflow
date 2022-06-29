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

	"github.com/gin-gonic/gin"
	"github.com/pingcap/errors"
	"go.uber.org/zap"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/dm/checker"
	dmconfig "github.com/pingcap/tiflow/dm/dm/config"
	ctlcommon "github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/engine/executor/worker"
	"github.com/pingcap/tiflow/engine/framework"
	libMetadata "github.com/pingcap/tiflow/engine/framework/metadata"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/framework/registry"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/checkpoint"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	"github.com/pingcap/tiflow/engine/model"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/logutil"
)

// JobMaster defines job master of dm job
type JobMaster struct {
	framework.BaseJobMaster

	workerID frameModel.WorkerID
	jobCfg   *config.JobCfg

	metadata        *metadata.MetaData
	workerManager   *WorkerManager
	taskManager     *TaskManager
	messageAgent    dmpkg.MessageAgent
	checkpointAgent checkpoint.Agent
}

var _ framework.JobMasterImpl = (*JobMaster)(nil)

type dmJobMasterFactory struct{}

// RegisterWorker is used to register dm job master to global registry
func RegisterWorker() {
	registry.GlobalWorkerRegistry().MustRegisterWorkerType(framework.DMJobMaster, dmJobMasterFactory{})
}

// DeserializeConfig implements WorkerFactory.DeserializeConfig
func (j dmJobMasterFactory) DeserializeConfig(configBytes []byte) (registry.WorkerConfig, error) {
	cfg := &config.JobCfg{}
	err := cfg.Decode(configBytes)
	return cfg, err
}

// NewWorkerImpl implements WorkerFactory.NewWorkerImpl
func (j dmJobMasterFactory) NewWorkerImpl(dCtx *dcontext.Context, workerID frameModel.WorkerID, masterID frameModel.MasterID, conf framework.WorkerConfig) (framework.WorkerImpl, error) {
	log.L().Info("new dm jobmaster", zap.String("id", workerID))
	jm := &JobMaster{
		workerID:        workerID,
		jobCfg:          conf.(*config.JobCfg),
		checkpointAgent: checkpoint.NewAgentImpl(conf.(*config.JobCfg)),
	}
	// nolint:errcheck
	dCtx.Deps().Construct(func(m p2p.MessageHandlerManager) (p2p.MessageHandlerManager, error) {
		jm.messageAgent = dmpkg.NewMessageAgentImpl(workerID, jm, m)
		return m, nil
	})
	return jm, nil
}

func (jm *JobMaster) initComponents(ctx context.Context) error {
	log.L().Debug("init components", zap.String("id", jm.workerID))
	if err := jm.messageAgent.Init(ctx); err != nil {
		return err
	}
	taskStatus, workerStatus, err := jm.getInitStatus()
	if err != nil {
		return err
	}
	jm.metadata = metadata.NewMetaData(jm.ID(), jm.MetaKVClient())
	jm.taskManager = NewTaskManager(taskStatus, jm.metadata.JobStore(), jm.messageAgent)
	jm.workerManager = NewWorkerManager(workerStatus, jm.metadata.JobStore(), jm, jm.messageAgent, jm.checkpointAgent)
	// register jobmanager client
	return jm.messageAgent.UpdateClient(libMetadata.JobManagerUUID, jm)
}

// InitImpl implements JobMasterImpl.InitImpl
func (jm *JobMaster) InitImpl(ctx context.Context) error {
	log.L().Info("initializing the dm jobmaster", zap.String("id", jm.workerID), zap.String("jobmaster_id", jm.JobMasterID()))
	if err := jm.initComponents(ctx); err != nil {
		return err
	}
	if err := jm.preCheck(ctx); err != nil {
		return err
	}
	if err := jm.checkpointAgent.Init(ctx); err != nil {
		return err
	}
	return jm.taskManager.OperateTask(ctx, dmpkg.Create, jm.jobCfg, nil)
}

// Tick implements JobMasterImpl.Tick
func (jm *JobMaster) Tick(ctx context.Context) error {
	jm.workerManager.Tick(ctx)
	jm.taskManager.Tick(ctx)
	return jm.messageAgent.Tick(ctx)
}

// OnMasterRecovered implements JobMasterImpl.OnMasterRecovered
func (jm *JobMaster) OnMasterRecovered(ctx context.Context) error {
	log.L().Info("recovering the dm jobmaster", zap.String("id", jm.workerID))
	return jm.initComponents(ctx)
}

// OnWorkerDispatched implements JobMasterImpl.OnWorkerDispatched
func (jm *JobMaster) OnWorkerDispatched(worker framework.WorkerHandle, result error) error {
	log.L().Info("on worker dispatched", zap.String("id", jm.workerID), zap.String("worker_id", worker.ID()))
	if result != nil {
		log.L().Error("failed to create worker", zap.String("worker_id", worker.ID()), zap.Error(result))
		jm.workerManager.removeWorkerStatusByWorkerID(worker.ID())
		jm.workerManager.SetNextCheckTime(time.Now())
	}
	return nil
}

// OnWorkerOnline implements JobMasterImpl.OnWorkerOnline
func (jm *JobMaster) OnWorkerOnline(worker framework.WorkerHandle) error {
	log.L().Debug("on worker online", zap.String("id", jm.workerID), zap.String("worker_id", worker.ID()))
	return jm.handleOnlineStatus(worker)
}

func (jm *JobMaster) handleOnlineStatus(worker framework.WorkerHandle) error {
	var taskStatus runtime.TaskStatus
	if err := json.Unmarshal(worker.Status().ExtBytes, &taskStatus); err != nil {
		return err
	}

	jm.taskManager.UpdateTaskStatus(taskStatus)
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus(taskStatus.Task, taskStatus.Unit, worker.ID(), runtime.WorkerOnline))
	return jm.messageAgent.UpdateClient(taskStatus.Task, worker.Unwrap())
}

// OnWorkerOffline implements JobMasterImpl.OnWorkerOffline
func (jm *JobMaster) OnWorkerOffline(worker framework.WorkerHandle, reason error) error {
	log.L().Info("on worker offline", zap.String("id", jm.workerID), zap.String("worker_id", worker.ID()))
	var taskStatus runtime.TaskStatus
	if err := json.Unmarshal(worker.Status().ExtBytes, &taskStatus); err != nil {
		return err
	}

	if taskStatus.Stage == metadata.StageFinished {
		return jm.onWorkerFinished(taskStatus, worker)
	}
	jm.taskManager.UpdateTaskStatus(runtime.NewOfflineStatus(taskStatus.Task))
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus(taskStatus.Task, taskStatus.Unit, worker.ID(), runtime.WorkerOffline))
	if err := jm.messageAgent.UpdateClient(taskStatus.Task, nil); err != nil {
		return err
	}
	jm.workerManager.SetNextCheckTime(time.Now())
	return nil
}

func (jm *JobMaster) onWorkerFinished(taskStatus runtime.TaskStatus, worker framework.WorkerHandle) error {
	log.L().Info("on worker finished", zap.String("id", jm.workerID), zap.String("worker_id", worker.ID()))
	jm.taskManager.UpdateTaskStatus(taskStatus)
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus(taskStatus.Task, taskStatus.Unit, worker.ID(), runtime.WorkerFinished))
	if err := jm.messageAgent.UpdateClient(taskStatus.Task, nil); err != nil {
		return err
	}
	jm.workerManager.SetNextCheckTime(time.Now())
	return nil
}

// OnWorkerStatusUpdated implements JobMasterImpl.OnWorkerStatusUpdated
func (jm *JobMaster) OnWorkerStatusUpdated(worker framework.WorkerHandle, newStatus *frameModel.WorkerStatus) error {
	// we alreay update finished status in OnWorkerOffline
	if newStatus.Code == frameModel.WorkerStatusFinished || len(newStatus.ExtBytes) == 0 {
		return nil
	}
	log.L().Debug("on worker status updated", zap.String("extra bytes", string(newStatus.ExtBytes)), zap.String("id", jm.workerID), zap.String("worker_id", worker.ID()))
	return jm.handleOnlineStatus(worker)
}

// OnJobManagerMessage implements JobMasterImpl.OnJobManagerMessage
func (jm *JobMaster) OnJobManagerMessage(topic p2p.Topic, message interface{}) error {
	// TODO: receive user request
	return nil
}

// OnOpenAPIInitialized implements JobMasterImpl.OnOpenAPIInitialized.
func (jm *JobMaster) OnOpenAPIInitialized(apiGroup *gin.RouterGroup) {
	// TODO: register openapi handlers to apiGroup.
}

// OnWorkerMessage implements JobMasterImpl.OnWorkerMessage
func (jm *JobMaster) OnWorkerMessage(worker framework.WorkerHandle, topic p2p.Topic, message interface{}) error {
	log.L().Debug("on worker message", zap.String("id", jm.workerID), zap.String("worker_id", worker.ID()))
	return nil
}

// OnMasterMessage implements JobMasterImpl.OnMasterMessage
func (jm *JobMaster) OnMasterMessage(topic p2p.Topic, message interface{}) error {
	return nil
}

// CloseImpl implements JobMasterImpl.CloseImpl
func (jm *JobMaster) CloseImpl(ctx context.Context) error {
	log.L().Info("close the dm jobmaster", zap.String("id", jm.workerID))
	if err := jm.taskManager.OperateTask(ctx, dmpkg.Delete, nil, nil); err != nil {
		return err
	}

outer:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			// wait all worker offline
			jm.workerManager.SetNextCheckTime(time.Now())
			// manually call Tick since outer event loop is closed.
			jm.workerManager.Tick(ctx)
			if len(jm.workerManager.WorkerStatus()) == 0 {
				if err := jm.checkpointAgent.Remove(ctx); err != nil {
					log.L().Error("failed to remove checkpoint", zap.Error(err))
				}
				break outer
			}
		}
	}

	// unregister jobmanager client
	if err := jm.messageAgent.UpdateClient(libMetadata.JobManagerUUID, nil); err != nil {
		return err
	}
	return jm.messageAgent.Close(ctx)
}

// ID implements JobMasterImpl.ID
func (jm *JobMaster) ID() worker.RunnableID {
	return jm.workerID
}

// Workload implements JobMasterImpl.Workload
func (jm *JobMaster) Workload() model.RescUnit {
	// TODO: implement workload
	return 2
}

// IsJobMasterImpl implements JobMasterImpl.IsJobMasterImpl
func (jm *JobMaster) IsJobMasterImpl() {
	panic("unreachable")
}

func (jm *JobMaster) getInitStatus() ([]runtime.TaskStatus, []runtime.WorkerStatus, error) {
	log.L().Debug("get init status", zap.String("id", jm.workerID))
	// NOTE: GetWorkers should return all online workers,
	// and no further OnWorkerOnline will be received if JobMaster doesn't CreateWorker.
	workerHandles := jm.GetWorkers()
	taskStatusList := make([]runtime.TaskStatus, 0, len(workerHandles))
	workerStatusList := make([]runtime.WorkerStatus, 0, len(workerHandles))
	for _, workerHandle := range workerHandles {
		if workerHandle.GetTombstone() != nil {
			continue
		}
		var taskStatus runtime.TaskStatus
		err := json.Unmarshal(workerHandle.Status().ExtBytes, &taskStatus)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		taskStatusList = append(taskStatusList, taskStatus)
		workerStatusList = append(workerStatusList, runtime.NewWorkerStatus(taskStatus.Task, taskStatus.Unit, workerHandle.ID(), runtime.WorkerOnline))
	}

	return taskStatusList, workerStatusList, nil
}

func (jm *JobMaster) preCheck(ctx context.Context) error {
	log.L().Info("start pre-checking job config", zap.String("id", jm.workerID), zap.String("jobmaster_id", jm.JobMasterID()))

	taskCfgs := jm.jobCfg.ToTaskCfgs()
	dmSubtaskCfgs := make([]*dmconfig.SubTaskConfig, 0, len(taskCfgs))
	for _, taskCfg := range taskCfgs {
		dmSubtaskCfgs = append(dmSubtaskCfgs, taskCfg.ToDMSubTaskCfg())
	}

	msg, err := checker.CheckSyncConfigFunc(ctx, dmSubtaskCfgs, ctlcommon.DefaultErrorCnt, ctlcommon.DefaultWarnCnt)
	if err != nil {
		log.L().Error("error when pre-checking", zap.String("id", jm.workerID), zap.String("jobmaster_id", jm.JobMasterID()), logutil.ShortError(err))
		return err
	}
	log.L().Info("finish pre-checking job config", zap.String("id", jm.workerID), zap.String("jobmaster_id", jm.JobMasterID()), zap.String("result", msg))
	return nil
}
