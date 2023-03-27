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
	"sync/atomic"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/dm/checker"
	dmconfig "github.com/pingcap/tiflow/dm/config"
	ctlcommon "github.com/pingcap/tiflow/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/master"
	"github.com/pingcap/tiflow/engine/framework"
	"github.com/pingcap/tiflow/engine/framework/logutil"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/framework/registry"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/checkpoint"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// JobMaster defines job master of dm job
type JobMaster struct {
	framework.BaseJobMaster

	// only use when init
	// it will be outdated if user update job cfg.
	initJobCfg *config.JobCfg

	initialized atomic.Bool

	metadata              *metadata.MetaData
	workerManager         *WorkerManager
	taskManager           *TaskManager
	ddlCoordinator        *DDLCoordinator
	messageAgent          dmpkg.MessageAgent
	checkpointAgent       checkpoint.Agent
	messageHandlerManager p2p.MessageHandlerManager
}

var (
	_               framework.JobMasterImpl = (*JobMaster)(nil)
	internalVersion                         = semver.New("6.1.0")
)

type dmJobMasterFactory struct{}

// RegisterWorker is used to register dm job master to global registry
func RegisterWorker() {
	registry.GlobalWorkerRegistry().MustRegisterWorkerType(frameModel.DMJobMaster, dmJobMasterFactory{})
}

// DeserializeConfig implements WorkerFactory.DeserializeConfig
func (j dmJobMasterFactory) DeserializeConfig(configBytes []byte) (registry.WorkerConfig, error) {
	cfg := &config.JobCfg{}
	err := cfg.Decode(configBytes)
	return cfg, err
}

// NewWorkerImpl implements WorkerFactory.NewWorkerImpl
func (j dmJobMasterFactory) NewWorkerImpl(
	dCtx *dcontext.Context,
	workerID frameModel.WorkerID,
	masterID frameModel.MasterID,
	conf framework.WorkerConfig,
) (framework.WorkerImpl, error) {
	log.L().Info("new dm jobmaster", zap.String(logutil.ConstFieldJobKey, workerID))
	jm := &JobMaster{
		initJobCfg: conf.(*config.JobCfg),
	}
	// nolint:errcheck
	dCtx.Deps().Construct(func(m p2p.MessageHandlerManager) (p2p.MessageHandlerManager, error) {
		jm.messageHandlerManager = m
		return m, nil
	})
	return jm, nil
}

// IsRetryableError implements WorkerFactory.IsRetryableError
func (j dmJobMasterFactory) IsRetryableError(err error) bool {
	// TODO: business logic implements this, err is a *terror.Error if it is
	// generated from DM code.
	return true
}

// initComponents initializes components of dm job master
// it need to be called firstly in InitImpl and OnMasterRecovered
// we should create all components if there is any error
// CloseImpl/StopImpl will be called later to close components
func (jm *JobMaster) initComponents() error {
	jm.Logger().Info("initializing the dm jobmaster components")
	taskStatus, workerStatus, err := jm.getInitStatus()
	jm.metadata = metadata.NewMetaData(jm.MetaKVClient(), jm.Logger())
	jm.messageAgent = dmpkg.NewMessageAgent(jm.ID(), jm, jm.messageHandlerManager, jm.Logger())
	jm.checkpointAgent = checkpoint.NewCheckpointAgent(jm.ID(), jm.Logger())
	jm.taskManager = NewTaskManager(jm.ID(), taskStatus, jm.metadata.JobStore(), jm.messageAgent, jm.Logger(), jm.MetricFactory())
	jm.workerManager = NewWorkerManager(jm.ID(), workerStatus, jm.metadata.JobStore(), jm.metadata.UnitStateStore(),
		jm, jm.messageAgent, jm.checkpointAgent, jm.Logger(), GetDMStorageType(jm.GetEnabledBucketStorage()))
	jm.ddlCoordinator = NewDDLCoordinator(jm.ID(), jm.MetaKVClient(), jm.checkpointAgent, jm.metadata.JobStore(), jm.Logger())
	return errors.Trace(err)
}

// InitImpl implements JobMasterImpl.InitImpl
func (jm *JobMaster) InitImpl(ctx context.Context) error {
	jm.Logger().Info("initializing the dm jobmaster")
	if err := jm.initComponents(); err != nil {
		return errors.Trace(err)
	}
	if err := jm.preCheck(ctx, jm.initJobCfg); err != nil {
		return jm.Exit(ctx, framework.ExitReasonFailed, err, nil)
	}
	if err := jm.bootstrap(ctx); err != nil {
		return errors.Trace(err)
	}
	if err := jm.checkpointAgent.Create(ctx, jm.initJobCfg); err != nil {
		return errors.Trace(err)
	}
	if err := jm.taskManager.OperateTask(ctx, dmpkg.Create, jm.initJobCfg, nil); err != nil {
		return errors.Trace(err)
	}
	if err := jm.ddlCoordinator.Reset(ctx); err != nil {
		return err
	}
	jm.initialized.Store(true)
	return nil
}

// OnMasterRecovered implements JobMasterImpl.OnMasterRecovered
// When it is called, the jobCfg may not be in the metadata, and we should not report an error
func (jm *JobMaster) OnMasterRecovered(ctx context.Context) error {
	jm.Logger().Info("recovering the dm jobmaster")
	if err := jm.initComponents(); err != nil {
		return errors.Trace(err)
	}
	if err := jm.bootstrap(ctx); err != nil {
		return errors.Trace(err)
	}
	if err := jm.ddlCoordinator.Reset(ctx); err != nil {
		return err
	}
	jm.initialized.Store(true)
	return nil
}

// Tick implements JobMasterImpl.Tick
// Do not do heavy work in Tick, it will block the message processing.
func (jm *JobMaster) Tick(ctx context.Context) error {
	jm.workerManager.DoTick(ctx)
	jm.taskManager.DoTick(ctx)
	if err := jm.messageAgent.Tick(ctx); err != nil {
		return errors.Trace(err)
	}
	if jm.isFinished(ctx) {
		return jm.cancel(ctx, frameModel.WorkerStateFinished)
	}
	return nil
}

// OnWorkerDispatched implements JobMasterImpl.OnWorkerDispatched
func (jm *JobMaster) OnWorkerDispatched(worker framework.WorkerHandle, result error) error {
	jm.Logger().Info("on worker dispatched", zap.String(logutil.ConstFieldWorkerKey, worker.ID()))
	if result != nil {
		jm.Logger().Error("failed to create worker", zap.String(logutil.ConstFieldWorkerKey, worker.ID()), zap.Error(result))
		jm.workerManager.removeWorkerStatusByWorkerID(worker.ID())
		jm.workerManager.SetNextCheckTime(time.Now())
	}
	return nil
}

// OnWorkerOnline implements JobMasterImpl.OnWorkerOnline
func (jm *JobMaster) OnWorkerOnline(worker framework.WorkerHandle) error {
	// because DM needs business online message with worker bound information, plain framework online is no use
	jm.Logger().Info("on worker online", zap.String(logutil.ConstFieldWorkerKey, worker.ID()))
	return nil
}

// handleOnlineStatus is used by OnWorkerStatusUpdated.
func (jm *JobMaster) handleOnlineStatus(worker framework.WorkerHandle) error {
	extBytes := worker.Status().ExtBytes
	if len(extBytes) == 0 {
		jm.Logger().Warn("worker status extBytes is empty", zap.String(logutil.ConstFieldWorkerKey, worker.ID()))
		return nil
	}
	var taskStatus runtime.TaskStatus
	if err := json.Unmarshal(extBytes, &taskStatus); err != nil {
		return errors.Trace(err)
	}

	jm.taskManager.UpdateTaskStatus(taskStatus)
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus(taskStatus.Task, taskStatus.Unit, worker.ID(), runtime.WorkerOnline, taskStatus.CfgModRevision))
	return jm.messageAgent.UpdateClient(taskStatus.Task, worker.Unwrap())
}

// OnWorkerOffline implements JobMasterImpl.OnWorkerOffline
func (jm *JobMaster) OnWorkerOffline(worker framework.WorkerHandle, reason error) error {
	extBytes := worker.Status().ExtBytes
	if len(extBytes) == 0 {
		jm.Logger().Warn("worker status extBytes is empty", zap.String(logutil.ConstFieldWorkerKey, worker.ID()))
		return nil
	}
	jm.Logger().Info("on worker offline", zap.String(logutil.ConstFieldWorkerKey, worker.ID()))
	var taskStatus runtime.TaskStatus
	if err := json.Unmarshal(extBytes, &taskStatus); err != nil {
		return errors.Trace(err)
	}

	if taskStatus.Stage == metadata.StageFinished {
		var finishedTaskStatus runtime.FinishedTaskStatus
		if err := json.Unmarshal(extBytes, &finishedTaskStatus); err != nil {
			return errors.Trace(err)
		}
		return jm.onWorkerFinished(finishedTaskStatus, worker)
	}
	jm.taskManager.UpdateTaskStatus(runtime.NewOfflineStatus(taskStatus.Task))
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus(taskStatus.Task, taskStatus.Unit, worker.ID(), runtime.WorkerOffline, taskStatus.CfgModRevision))
	if err := jm.messageAgent.UpdateClient(taskStatus.Task, nil); err != nil {
		return errors.Trace(err)
	}
	jm.workerManager.SetNextCheckTime(time.Now())
	return nil
}

func (jm *JobMaster) onWorkerFinished(finishedTaskStatus runtime.FinishedTaskStatus, worker framework.WorkerHandle) error {
	jm.Logger().Info("on worker finished", zap.String(logutil.ConstFieldWorkerKey, worker.ID()))
	taskStatus := finishedTaskStatus.TaskStatus

	unitStateStore := jm.metadata.UnitStateStore()
	err := unitStateStore.ReadModifyWrite(context.TODO(), func(state *metadata.UnitState) error {
		finishedTaskStatus.StageUpdatedTime = time.Now()
		finishedTaskStatus.Duration = finishedTaskStatus.StageUpdatedTime.Sub(state.CurrentUnitStatus[taskStatus.Task].CreatedTime)
		for i, status := range state.FinishedUnitStatus[taskStatus.Task] {
			// when the unit is restarted by update-cfg or something, overwrite the old status and truncate
			if status.Unit == taskStatus.Unit {
				state.FinishedUnitStatus[taskStatus.Task][i] = &finishedTaskStatus
				state.FinishedUnitStatus[taskStatus.Task] = state.FinishedUnitStatus[taskStatus.Task][:i+1]
				return nil
			}
		}
		state.FinishedUnitStatus[taskStatus.Task] = append(
			state.FinishedUnitStatus[taskStatus.Task], &finishedTaskStatus,
		)
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	jm.taskManager.UpdateTaskStatus(taskStatus)
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus(taskStatus.Task, taskStatus.Unit, worker.ID(), runtime.WorkerFinished, taskStatus.CfgModRevision))

	// we should call this after we set "state.FinishedUnitStatus" to make sure finished-unit-status is persisted
	// before client is removed. else in status of get-job-api, status of current unit might be missing, since
	// query-status might not get status of current unit status and the status is not in "state.FinishedUnitStatus"
	if err := jm.messageAgent.RemoveClient(taskStatus.Task); err != nil {
		return errors.Trace(err)
	}
	jm.workerManager.SetNextCheckTime(time.Now())
	return nil
}

// OnWorkerStatusUpdated implements JobMasterImpl.OnWorkerStatusUpdated
func (jm *JobMaster) OnWorkerStatusUpdated(worker framework.WorkerHandle, newStatus *frameModel.WorkerStatus) error {
	// we already update finished status in OnWorkerOffline
	if newStatus.State == frameModel.WorkerStateFinished || len(newStatus.ExtBytes) == 0 {
		return nil
	}
	jm.Logger().Info("on worker status updated", zap.String(logutil.ConstFieldWorkerKey, worker.ID()), zap.String("extra bytes", string(newStatus.ExtBytes)))
	if err := jm.handleOnlineStatus(worker); err != nil {
		return errors.Trace(err)
	}
	// run task manager tick when worker status changed to operate task.
	jm.taskManager.SetNextCheckTime(time.Now())
	return nil
}

// OnJobManagerMessage implements JobMasterImpl.OnJobManagerMessage
func (jm *JobMaster) OnJobManagerMessage(topic p2p.Topic, message interface{}) error {
	// TODO: receive user request
	return nil
}

// OnOpenAPIInitialized implements JobMasterImpl.OnOpenAPIInitialized.
func (jm *JobMaster) OnOpenAPIInitialized(router *gin.RouterGroup) {
	jm.initOpenAPI(router)
}

// OnWorkerMessage implements JobMasterImpl.OnWorkerMessage
func (jm *JobMaster) OnWorkerMessage(worker framework.WorkerHandle, topic p2p.Topic, message interface{}) error {
	return nil
}

// OnMasterMessage implements JobMasterImpl.OnMasterMessage
func (jm *JobMaster) OnMasterMessage(ctx context.Context, topic p2p.Topic, message interface{}) error {
	return nil
}

// CloseImpl implements JobMasterImpl.CloseImpl
func (jm *JobMaster) CloseImpl(ctx context.Context) {
	if jm.messageAgent != nil {
		if err := jm.messageAgent.Close(ctx); err != nil {
			jm.Logger().Error("failed to close message agent", zap.Error(err))
		}
	}
}

// OnCancel implements JobMasterImpl.OnCancel
func (jm *JobMaster) OnCancel(ctx context.Context) error {
	jm.Logger().Info("on cancel job master")
	return jm.cancel(ctx, frameModel.WorkerStateStopped)
}

// StopImpl implements JobMasterImpl.StopImpl
// checkpoint is removed when job is stopped, this is different with OP DM where
// `--remove-meta` is specified at start-task.
func (jm *JobMaster) StopImpl(ctx context.Context) {
	jm.Logger().Info("stoping the dm jobmaster")

	// close component
	jm.CloseImpl(ctx)

	// remove other resources
	if err := jm.removeCheckpoint(ctx); err != nil {
		jm.Logger().Error("failed to remove checkpoint", zap.Error(err))
	}
	if err := jm.ddlCoordinator.ClearMetadata(ctx); err != nil {
		jm.Logger().Error("failed to clear ddl metadata", zap.Error(err))
	}
	if err := jm.taskManager.OperateTask(ctx, dmpkg.Delete, nil, nil); err != nil {
		jm.Logger().Error("failed to delete task", zap.Error(err))
	}
}

// IsJobMasterImpl implements JobMasterImpl.IsJobMasterImpl
func (jm *JobMaster) IsJobMasterImpl() {
	panic("unreachable")
}

func (jm *JobMaster) getInitStatus() ([]runtime.TaskStatus, []runtime.WorkerStatus, error) {
	jm.Logger().Info("get init status")
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
		extBytes := workerHandle.Status().ExtBytes
		if len(extBytes) == 0 {
			return nil, nil, errors.Errorf("extBytes of %d is empty", workerHandle.ID())
		}
		err := json.Unmarshal(extBytes, &taskStatus)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		taskStatusList = append(taskStatusList, taskStatus)
		workerStatusList = append(workerStatusList, runtime.NewWorkerStatus(taskStatus.Task, taskStatus.Unit, workerHandle.ID(), runtime.WorkerOnline, taskStatus.CfgModRevision))
	}

	return taskStatusList, workerStatusList, nil
}

func (jm *JobMaster) preCheck(ctx context.Context, cfg *config.JobCfg) error {
	jm.Logger().Info("start pre-checking job config")

	// TODO: refactor this, e.g. move this check to checkpoint agent
	// lightning create checkpoint table with name `$jobID_lightning_checkpoint_list`
	// max table of TiDB is 64, so length of jobID should be less or equal than 64-26=38
	if len(jm.ID()) > 38 {
		return errors.New("job id is too long, max length is 38")
	}

	if err := master.AdjustTargetDB(ctx, cfg.TargetDB); err != nil {
		return errors.Trace(err)
	}

	if cfg.TaskMode == dmconfig.ModeIncrement {
		for _, inst := range cfg.Upstreams {
			if inst.Meta == nil {
				meta, err2 := master.GetLatestMeta(ctx, inst.Flavor, inst.DBCfg)
				if err2 != nil {
					return errors.Trace(err2)
				}
				inst.Meta = meta
			}
		}
	}

	taskCfgs := cfg.ToTaskCfgs()
	dmSubtaskCfgs := make([]*dmconfig.SubTaskConfig, 0, len(taskCfgs))
	for _, taskCfg := range taskCfgs {
		dmSubtaskCfgs = append(dmSubtaskCfgs, taskCfg.ToDMSubTaskCfg(jm.ID()))
	}

	msg, err := checker.CheckSyncConfigFunc(ctx, dmSubtaskCfgs, ctlcommon.DefaultErrorCnt, ctlcommon.DefaultWarnCnt)
	if err != nil {
		jm.Logger().Error("error when pre-checking", zap.Error(err))
		return errors.Trace(err)
	}
	jm.Logger().Info("finish pre-checking job config", zap.String("result", msg))
	return nil
}

// all task finished and all worker tombstone
func (jm *JobMaster) isFinished(ctx context.Context) bool {
	return jm.taskManager.allFinished(ctx) && jm.workerManager.allTombStone()
}

func (jm *JobMaster) status(ctx context.Context, code frameModel.WorkerState) (frameModel.WorkerStatus, error) {
	status := frameModel.WorkerStatus{
		State: code,
	}
	if jobStatus, err := jm.QueryJobStatus(ctx, nil); err != nil {
		return status, err
	} else if bs, err := json.Marshal(jobStatus); err != nil {
		return status, err
	} else {
		status.ExtBytes = bs
		return status, nil
	}
}

// cancel remove jobCfg in metadata, and wait all workers offline.
func (jm *JobMaster) cancel(ctx context.Context, code frameModel.WorkerState) error {
	var detail []byte
	status, err := jm.status(ctx, code)
	if err != nil {
		jm.Logger().Error("failed to get status", zap.Error(err))
	} else {
		detail = status.ExtBytes
	}

	if err := jm.taskManager.OperateTask(ctx, dmpkg.Deleting, nil, nil); err != nil {
		// would not recover again
		jm.Logger().Warn("failed to mark task deleting", zap.Error(err))
		return jm.Exit(ctx, framework.ExitReasonCanceled, err, detail)
	}
	newCtx, cancel := context.WithTimeout(ctx, 10*runtime.HeartbeatInterval)
	defer cancel()
	// wait all worker exit
	jm.workerManager.SetNextCheckTime(time.Now())
	for {
		select {
		case <-newCtx.Done():
			jm.Logger().Warn("cancel context is timeout", zap.Error(ctx.Err()))
			return jm.Exit(ctx, framework.ExitReasonCanceled, newCtx.Err(), detail)
		case <-time.After(time.Second):
			if jm.workerManager.allTombStone() {
				jm.Logger().Info("all worker are offline, will exit")
				return jm.Exit(ctx, framework.WorkerStateToExitReason(status.State), err, detail)
			}
			jm.workerManager.SetNextCheckTime(time.Now())
		}
	}
}

func (jm *JobMaster) removeCheckpoint(ctx context.Context) error {
	state, err := jm.metadata.JobStore().Get(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	job := state.(*metadata.Job)
	for _, task := range job.Tasks {
		cfg := task.Cfg.ToJobCfg()
		return jm.checkpointAgent.Remove(ctx, cfg)
	}
	return errors.New("no task found in job")
}

// bootstrap should be invoked after initComponents.
func (jm *JobMaster) bootstrap(ctx context.Context) error {
	jm.Logger().Info("start bootstraping")
	// get old version
	clusterInfoStore := jm.metadata.ClusterInfoStore()
	state, err := clusterInfoStore.Get(ctx)
	if err != nil {
		// put cluster info for new job
		// TODO: better error handling by error code.
		if errors.Cause(err) == metadata.ErrStateNotFound {
			jm.Logger().Info("put cluster info for new job", zap.Stringer("internal version", internalVersion))
			return clusterInfoStore.Put(ctx, metadata.NewClusterInfo(*internalVersion))
		}
		jm.Logger().Info("get cluster info error", zap.Error(err))
		return errors.Trace(err)
	}
	clusterInfo := state.(*metadata.ClusterInfo)
	jm.Logger().Info("get cluster info for job", zap.Any("cluster_info", clusterInfo))

	if err := jm.metadata.Upgrade(ctx, clusterInfo.Version); err != nil {
		return errors.Trace(err)
	}
	if err := jm.checkpointAgent.Upgrade(ctx, clusterInfo.Version); err != nil {
		return errors.Trace(err)
	}

	// only update for new version
	if clusterInfo.Version.LessThan(*internalVersion) {
		return clusterInfoStore.UpdateVersion(ctx, *internalVersion)
	}
	return nil
}
