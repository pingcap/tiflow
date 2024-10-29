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
//
// MVC for dm-master's openapi server
// Model(data in etcd): source of truth
// View(openapi_view): do some inner work such as validate, filter, prepare parameters/response and call controller to update model.
// Controller(openapi_controller): call model func to update data.

package master

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/dm/checker"
	dmcommon "github.com/pingcap/tiflow/dm/common"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/master/scheduler"
	"github.com/pingcap/tiflow/dm/master/workerrpc"
	"github.com/pingcap/tiflow/dm/openapi"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// nolint:unparam
func (s *Server) getClusterInfo(ctx context.Context) (*openapi.GetClusterInfoResponse, error) {
	info := &openapi.GetClusterInfoResponse{}
	info.ClusterId = s.ClusterID()

	resp, err := s.etcdClient.Get(ctx, dmcommon.ClusterTopologyKey)
	if err != nil {
		return nil, err
	}

	// already set by tiup, load to info
	if len(resp.Kvs) == 1 {
		topo := &openapi.ClusterTopology{}
		if err := json.Unmarshal(resp.Kvs[0].Value, topo); err != nil {
			return nil, err
		}
		info.Topology = topo
	}
	return info, nil
}

func (s *Server) updateClusterInfo(ctx context.Context, topo *openapi.ClusterTopology) (*openapi.GetClusterInfoResponse, error) {
	if val, err := json.Marshal(topo); err != nil {
		return nil, err
	} else if _, err := s.etcdClient.Put(ctx, dmcommon.ClusterTopologyKey, string(val)); err != nil {
		return nil, err
	}
	info := &openapi.GetClusterInfoResponse{}
	info.ClusterId = s.ClusterID()
	info.Topology = topo
	return info, nil
}

func (s *Server) getSourceStatusListFromWorker(ctx context.Context, sourceName string, specifiedSource bool) ([]openapi.SourceStatus, error) {
	workerStatusList := s.getStatusFromWorkers(ctx, []string{sourceName}, "", specifiedSource)
	sourceStatusList := make([]openapi.SourceStatus, len(workerStatusList))
	for i, workerStatus := range workerStatusList {
		if workerStatus == nil {
			// this should not happen unless the rpc in the worker server has been modified
			return nil, terror.ErrOpenAPICommonError.New("worker's query-status response is nil")
		}
		sourceStatus := openapi.SourceStatus{SourceName: sourceName, WorkerName: workerStatus.SourceStatus.Worker}
		if !workerStatus.Result {
			sourceStatus.ErrorMsg = &workerStatus.Msg
			sourceStatusList[i] = sourceStatus
			continue
		}

		if relayStatus := workerStatus.SourceStatus.GetRelayStatus(); relayStatus != nil {
			sourceStatus.RelayStatus = &openapi.RelayStatus{
				MasterBinlog:       relayStatus.MasterBinlog,
				MasterBinlogGtid:   relayStatus.MasterBinlogGtid,
				RelayBinlogGtid:    relayStatus.RelayBinlogGtid,
				RelayCatchUpMaster: relayStatus.RelayCatchUpMaster,
				RelayDir:           relayStatus.RelaySubDir,
				Stage:              relayStatus.Stage.String(),
			}
		}
		// add error if some error happen
		if workerStatus.SourceStatus.Result != nil && len(workerStatus.SourceStatus.Result.Errors) > 0 {
			var errorMsgs string
			for _, err := range workerStatus.SourceStatus.Result.Errors {
				errorMsgs += fmt.Sprintf("%s\n", err.Message)
			}
			sourceStatus.ErrorMsg = &errorMsgs
		}
		sourceStatusList[i] = sourceStatus
	}
	return sourceStatusList, nil
}

func (s *Server) createSource(ctx context.Context, req openapi.CreateSourceRequest) (*openapi.Source, error) {
	cfg := config.OpenAPISourceToSourceCfg(req.Source)
	if err := CheckAndAdjustSourceConfigFunc(ctx, cfg); err != nil {
		return nil, err
	}

	var err error
	if req.WorkerName == nil {
		err = s.scheduler.AddSourceCfg(cfg)
	} else {
		err = s.scheduler.AddSourceCfgWithWorker(cfg, *req.WorkerName)
	}
	if err != nil {
		return nil, err
	}
	// TODO: refine relay logic https://github.com/pingcap/tiflow/issues/4985
	if cfg.EnableRelay {
		return &req.Source, s.enableRelay(ctx, req.Source.SourceName, openapi.EnableRelayRequest{})
	}
	return &req.Source, nil
}

func (s *Server) updateSource(ctx context.Context, sourceName string, req openapi.UpdateSourceRequest) (*openapi.Source, error) {
	// TODO: support dynamic updates
	oldCfg := s.scheduler.GetSourceCfgByID(sourceName)
	if oldCfg == nil {
		return nil, terror.ErrSchedulerSourceCfgNotExist.Generate(sourceName)
	}
	newCfg := config.OpenAPISourceToSourceCfg(req.Source)

	// update's request will be no password when user doesn't input password and wants to use old password.
	if req.Source.Password == nil {
		newCfg.From.Password = oldCfg.From.Password
	}

	if err := CheckAndAdjustSourceConfigFunc(ctx, newCfg); err != nil {
		return nil, err
	}
	if err := s.scheduler.UpdateSourceCfg(newCfg); err != nil {
		return nil, err
	}
	// when enable filed updated, we need operate task on this source
	if worker := s.scheduler.GetWorkerBySource(sourceName); worker != nil && newCfg.Enable != oldCfg.Enable {
		stage := pb.Stage_Running
		taskNameList := s.scheduler.GetTaskNameListBySourceName(sourceName, nil)
		if !newCfg.Enable {
			stage = pb.Stage_Stopped
		}
		if err := s.scheduler.BatchOperateTaskOnWorker(ctx, worker, taskNameList, sourceName, stage, false); err != nil {
			return nil, err
		}
	}
	return &req.Source, nil
}

// nolint:unparam
func (s *Server) deleteSource(ctx context.Context, sourceName string, force bool) error {
	if force {
		for _, taskName := range s.scheduler.GetTaskNameListBySourceName(sourceName, nil) {
			if err := s.scheduler.RemoveSubTasks(taskName, sourceName); err != nil {
				return err
			}
		}
	}
	return s.scheduler.RemoveSourceCfg(sourceName)
}

func (s *Server) getSource(ctx context.Context, sourceName string, req openapi.DMAPIGetSourceParams) (*openapi.Source, error) {
	sourceCfg := s.scheduler.GetSourceCfgByID(sourceName)
	if sourceCfg == nil {
		return nil, terror.ErrSchedulerSourceCfgNotExist.Generate(sourceName)
	}
	source := config.SourceCfgToOpenAPISource(sourceCfg)
	if req.WithStatus != nil && *req.WithStatus {
		var statusList []openapi.SourceStatus
		statusList, err := s.getSourceStatus(ctx, sourceName)
		if err != nil {
			return nil, err
		}
		source.StatusList = &statusList
		// add task name list
		taskNameList := openapi.TaskNameList{}
		taskNameList = append(taskNameList, s.scheduler.GetTaskNameListBySourceName(sourceName, nil)...)
		source.TaskNameList = &taskNameList
	}
	return &source, nil
}

func (s *Server) getSourceStatus(ctx context.Context, sourceName string) ([]openapi.SourceStatus, error) {
	return s.getSourceStatusListFromWorker(ctx, sourceName, true)
}

func (s *Server) listSource(ctx context.Context, req openapi.DMAPIGetSourceListParams) ([]openapi.Source, error) {
	sourceCfgM := s.scheduler.GetSourceCfgs()
	openapiSourceList := make([]openapi.Source, 0, len(sourceCfgM))
	// fill status and filter
	for _, sourceCfg := range sourceCfgM {
		// filter by enable_relay
		// TODO(ehco),maybe worker should use sourceConfig.EnableRelay to determine whether start relay
		if req.EnableRelay != nil {
			relayWorkers, err := s.scheduler.GetRelayWorkers(sourceCfg.SourceID)
			if err != nil {
				return nil, err
			}
			if (*req.EnableRelay && len(relayWorkers) == 0) || (!*req.EnableRelay && len(relayWorkers) > 0) {
				continue
			}
		}
		source := config.SourceCfgToOpenAPISource(sourceCfg)
		if req.WithStatus != nil && *req.WithStatus {
			sourceStatusList, err := s.getSourceStatus(ctx, source.SourceName)
			if err != nil {
				return nil, err
			}
			source.StatusList = &sourceStatusList
			// add task name list
			taskNameList := openapi.TaskNameList{}
			taskNameList = append(taskNameList, s.scheduler.GetTaskNameListBySourceName(sourceCfg.SourceID, nil)...)
			source.TaskNameList = &taskNameList
		}
		openapiSourceList = append(openapiSourceList, source)
	}
	return openapiSourceList, nil
}

// nolint:unparam
func (s *Server) enableRelay(ctx context.Context, sourceName string, req openapi.EnableRelayRequest) error {
	sourceCfg := s.scheduler.GetSourceCfgByID(sourceName)
	if sourceCfg == nil {
		return terror.ErrSchedulerSourceCfgNotExist.Generate(sourceName)
	}
	if req.WorkerNameList == nil {
		worker := s.scheduler.GetWorkerBySource(sourceName)
		if worker == nil {
			return terror.ErrWorkerNoStart.Generate()
		}
		req.WorkerNameList = &openapi.WorkerNameList{worker.BaseInfo().Name}
	}

	needUpdate := false
	// update relay related in source cfg
	if req.RelayBinlogName != nil && sourceCfg.RelayBinLogName != *req.RelayBinlogName {
		sourceCfg.RelayBinLogName = *req.RelayBinlogName
		needUpdate = true
	}
	if req.RelayBinlogGtid != nil && sourceCfg.RelayBinlogGTID != *req.RelayBinlogGtid {
		sourceCfg.RelayBinlogGTID = *req.RelayBinlogGtid
		needUpdate = true
	}
	if req.RelayDir != nil && sourceCfg.RelayDir != *req.RelayDir {
		sourceCfg.RelayDir = *req.RelayDir
		needUpdate = true
	}
	if needUpdate {
		// update current source relay config before start relay
		if err := s.scheduler.UpdateSourceCfg(sourceCfg); err != nil {
			return err
		}
	}
	return s.scheduler.StartRelay(sourceName, *req.WorkerNameList)
}

// nolint:unparam
func (s *Server) disableRelay(ctx context.Context, sourceName string, req openapi.DisableRelayRequest) error {
	sourceCfg := s.scheduler.GetSourceCfgByID(sourceName)
	if sourceCfg == nil {
		return terror.ErrSchedulerSourceCfgNotExist.Generate(sourceName)
	}
	if req.WorkerNameList == nil {
		worker := s.scheduler.GetWorkerBySource(sourceName)
		if worker == nil {
			return terror.ErrWorkerNoStart.Generate()
		}
		req.WorkerNameList = &openapi.WorkerNameList{worker.BaseInfo().Name}
	}
	return s.scheduler.StopRelay(sourceName, *req.WorkerNameList)
}

func (s *Server) purgeRelay(ctx context.Context, sourceName string, req openapi.PurgeRelayRequest) error {
	purgeReq := &workerrpc.Request{
		Type:       workerrpc.CmdPurgeRelay,
		PurgeRelay: &pb.PurgeRelayRequest{Filename: req.RelayBinlogName},
	}
	if req.RelayDir != nil {
		purgeReq.PurgeRelay.SubDir = *req.RelayDir
	}
	// NOTE not all worker that enabled relay is recorded in scheduler, we need refine this later
	workers, err := s.scheduler.GetRelayWorkers(sourceName)
	if err != nil {
		return err
	}
	if len(workers) == 0 {
		return terror.ErrOpenAPICommonError.Generatef("relay worker for source %s not found, please `enable-relay` first", sourceName)
	}
	for _, w := range workers {
		resp, err := w.SendRequest(ctx, purgeReq, s.cfg.RPCTimeout)
		if err != nil {
			return err
		}
		if resp.PurgeRelay.Msg != "" {
			return terror.ErrOpenAPICommonError.Generate(resp.PurgeRelay.Msg)
		}
	}
	return nil
}

func (s *Server) enableSource(ctx context.Context, sourceName string) error {
	cfg := s.scheduler.GetSourceCfgByID(sourceName)
	if cfg == nil {
		return terror.ErrSchedulerSourceCfgNotExist.Generate(sourceName)
	}
	worker := s.scheduler.GetWorkerBySource(sourceName)
	if worker == nil {
		return terror.ErrWorkerNoStart.Generate()
	}
	if cfg.Enable {
		return nil
	}
	cfg.Enable = true
	if err := s.scheduler.UpdateSourceCfg(cfg); err != nil {
		return err
	}
	taskNameList := s.scheduler.GetTaskNameListBySourceName(sourceName, nil)
	return s.scheduler.BatchOperateTaskOnWorker(ctx, worker, taskNameList, sourceName, pb.Stage_Running, false)
}

func (s *Server) disableSource(ctx context.Context, sourceName string) error {
	cfg := s.scheduler.GetSourceCfgByID(sourceName)
	if cfg == nil {
		return terror.ErrSchedulerSourceCfgNotExist.Generate(sourceName)
	}
	if !cfg.Enable {
		return nil
	}
	worker := s.scheduler.GetWorkerBySource(sourceName)
	if worker == nil {
		// no need to stop task if the source is not running
		cfg.Enable = false
		return s.scheduler.UpdateSourceCfg(cfg)
	}
	taskNameList := s.scheduler.GetTaskNameListBySourceName(sourceName, nil)
	if err := s.scheduler.BatchOperateTaskOnWorker(ctx, worker, taskNameList, sourceName, pb.Stage_Stopped, false); err != nil {
		return err
	}
	cfg.Enable = false
	return s.scheduler.UpdateSourceCfg(cfg)
}

func (s *Server) transferSource(ctx context.Context, sourceName, workerName string) error {
	return s.scheduler.TransferSource(ctx, sourceName, workerName)
}

func (s *Server) checkTask(ctx context.Context, subtaskCfgList []*config.SubTaskConfig, errCnt, warnCnt int64) (string, error) {
	// TODO(ehco) no api for this task now
	return checker.CheckSyncConfigFunc(ctx, subtaskCfgList, errCnt, warnCnt)
}

func (s *Server) checkOpenAPITaskBeforeOperate(ctx context.Context, task *openapi.Task) ([]*config.SubTaskConfig, string, error) {
	// prepare target db config
	toDBCfg := config.GetTargetDBCfgFromOpenAPITask(task)
	if err := AdjustTargetDBSessionCfg(ctx, toDBCfg); err != nil {
		return nil, "", err
	}
	// prepare source db config source name -> source config
	sourceCfgMap := make(map[string]*config.SourceConfig)
	for _, cfg := range task.SourceConfig.SourceConf {
		if sourceCfg := s.scheduler.GetSourceCfgByID(cfg.SourceName); sourceCfg != nil {
			sourceCfgMap[cfg.SourceName] = sourceCfg
		} else {
			return nil, "", terror.ErrSchedulerSourceCfgNotExist.Generate(cfg.SourceName)
		}
	}
	// generate sub task configs
	subTaskConfigList, err := config.OpenAPITaskToSubTaskConfigs(task, toDBCfg, sourceCfgMap)
	if err != nil {
		return nil, "", err
	}
	stCfgsForCheck, err := s.generateSubTasksForCheck(subTaskConfigList)
	if err != nil {
		return nil, "", err
	}
	// check subtask config
	msg, err := s.checkTask(ctx, stCfgsForCheck, common.DefaultErrorCnt, common.DefaultWarnCnt)
	if err != nil {
		return nil, "", terror.WithClass(err, terror.ClassDMMaster)
	}
	return subTaskConfigList, msg, nil
}

func (s *Server) createTask(ctx context.Context, req openapi.CreateTaskRequest) (*openapi.OperateTaskResponse, error) {
	task := &req.Task
	if err := task.Adjust(); err != nil {
		return nil, err
	}
	subTaskConfigList, msg, err := s.checkOpenAPITaskBeforeOperate(ctx, task)
	if err != nil {
		return nil, err
	}
	res := &openapi.OperateTaskResponse{
		Task:        *task,
		CheckResult: msg,
	}
	return res, s.scheduler.AddSubTasks(false, pb.Stage_Stopped, subtaskCfgPointersToInstances(subTaskConfigList...)...)
}

func (s *Server) updateTask(ctx context.Context, req openapi.UpdateTaskRequest) (*openapi.OperateTaskResponse, error) {
	task := &req.Task
	if err := task.Adjust(); err != nil {
		return nil, err
	}
	subTaskConfigList, msg, err := s.checkOpenAPITaskBeforeOperate(ctx, task)
	if err != nil {
		return nil, err
	}
	res := &openapi.OperateTaskResponse{
		Task:        *task,
		CheckResult: msg,
	}
	return res, s.scheduler.UpdateSubTasks(ctx, subtaskCfgPointersToInstances(subTaskConfigList...)...)
}

func (s *Server) deleteTask(ctx context.Context, taskName string, force bool) error {
	// check if there is running task
	var task *openapi.Task
	var err error
	if !force {
		task, err = s.getTask(ctx, taskName, openapi.DMAPIGetTaskParams{})
		if err != nil {
			return err
		}
		for _, sourceConf := range task.SourceConfig.SourceConf {
			stage := s.scheduler.GetExpectSubTaskStage(taskName, sourceConf.SourceName)
			// TODO delete  openapi.TaskStagePasused when use openapi to impl dmctl
			if stage.Expect != pb.Stage_Paused && stage.Expect != pb.Stage_Stopped {
				return terror.ErrOpenAPICommonError.Generatef("task %s have running subtasks, please stop them or delete task with force.", taskName)
			}
		}
	} else {
		task, err = s.getTask(ctx, taskName, openapi.DMAPIGetTaskParams{})
		if err != nil {
			return err
		}
	}

	// remove meta
	release, err := s.scheduler.AcquireSubtaskLatch(taskName)
	if err != nil {
		return terror.ErrSchedulerLatchInUse.Generate("RemoveMeta", taskName)
	}
	defer release()

	ignoreCannotConnectError := func(err error) bool {
		if err == nil {
			return true
		}
		if force && strings.Contains(err.Error(), "connect: connection refused") {
			log.L().Warn("connect downstream error when fore delete task", zap.Error(err))
			return true
		}
		return false
	}

	toDBCfg := config.GetTargetDBCfgFromOpenAPITask(task)
	if adjustErr := AdjustTargetDBSessionCfg(ctx, toDBCfg); adjustErr != nil {
		if !ignoreCannotConnectError(adjustErr) {
			return adjustErr
		}
	}
	metaSchema := *task.MetaSchema
	err = s.removeMetaData(ctx, taskName, metaSchema, toDBCfg)
	if err != nil {
		if !ignoreCannotConnectError(err) {
			return terror.Annotate(err, "while removing metadata")
		}
	}
	release()
	sourceNameList := s.getTaskSourceNameList(taskName)
	// delete subtask on worker
	return s.scheduler.RemoveSubTasks(taskName, sourceNameList...)
}

func (s *Server) getTask(ctx context.Context, taskName string, req openapi.DMAPIGetTaskParams) (*openapi.Task, error) {
	subTaskConfigM := s.scheduler.GetSubTaskCfgsByTask(taskName)
	if subTaskConfigM == nil {
		return nil, terror.ErrSchedulerTaskNotExist.Generate(taskName)
	}
	subTaskConfigList := make([]*config.SubTaskConfig, 0, len(subTaskConfigM))
	for sourceName := range subTaskConfigM {
		subTaskConfigList = append(subTaskConfigList, subTaskConfigM[sourceName])
	}
	task := config.SubTaskConfigsToOpenAPITask(subTaskConfigList)
	if req.WithStatus != nil && *req.WithStatus {
		subTaskStatusList, err := s.getTaskStatus(ctx, task.Name)
		if err != nil {
			return nil, err
		}
		task.StatusList = &subTaskStatusList
	}
	return task, nil
}

func (s *Server) getTaskStatus(ctx context.Context, taskName string) ([]openapi.SubTaskStatus, error) {
	req := openapi.DMAPIGetTaskStatusParams{}
	if req.SourceNameList == nil || len(*req.SourceNameList) == 0 {
		sourceNameList := openapi.SourceNameList(s.getTaskSourceNameList(taskName))
		req.SourceNameList = &sourceNameList
	}
	workerStatusList := s.getStatusFromWorkers(ctx, *req.SourceNameList, taskName, true)
	subTaskStatusList := make([]openapi.SubTaskStatus, 0, len(workerStatusList))

	handleProcessError := func(err *pb.ProcessError) string {
		errorMsg := fmt.Sprintf("[code=%d:class=%s:scope=%s:level=%s], Message: %s", err.ErrCode, err.ErrClass, err.ErrScope, err.ErrLevel, err.Message)
		if err.RawCause != "" {
			errorMsg = fmt.Sprintf("%s, RawCause: %s", errorMsg, err.RawCause)
		}
		if err.Workaround != "" {
			errorMsg = fmt.Sprintf("%s, Workaround: %s", errorMsg, err.Workaround)
		}
		errorMsg = fmt.Sprintf("%s.", errorMsg)
		return errorMsg
	}

	for _, workerStatus := range workerStatusList {
		if workerStatus == nil || workerStatus.SourceStatus == nil {
			// this should not happen unless the rpc in the worker server has been modified
			return nil, terror.ErrOpenAPICommonError.New("worker's query-status response is nil")
		}
		sourceStatus := workerStatus.SourceStatus
		openapiSubTaskStatus := openapi.SubTaskStatus{
			Name:       taskName,
			SourceName: sourceStatus.GetSource(),
			WorkerName: sourceStatus.GetWorker(),
		}
		if !workerStatus.Result {
			openapiSubTaskStatus.ErrorMsg = &workerStatus.Msg
			subTaskStatusList = append(subTaskStatusList, openapiSubTaskStatus)
			continue
		}
		if len(workerStatus.SubTaskStatus) == 0 {
			// this should not happen unless the rpc in the worker server has been modified
			return nil, terror.ErrOpenAPICommonError.New("worker's query-status response is nil")
		}
		subTaskStatus := workerStatus.SubTaskStatus[0]
		if subTaskStatus == nil {
			// this should not happen unless the rpc in the worker server has been modified
			return nil, terror.ErrOpenAPICommonError.New("worker's query-status response is nil")
		}
		openapiSubTaskStatus.Stage = openapi.TaskStage(subTaskStatus.GetStage().String())
		openapiSubTaskStatus.Unit = subTaskStatus.GetUnit().String()
		openapiSubTaskStatus.UnresolvedDdlLockId = &subTaskStatus.UnresolvedDDLLockID
		// add load status
		if loadS := subTaskStatus.GetLoad(); loadS != nil {
			openapiSubTaskStatus.LoadStatus = &openapi.LoadStatus{
				FinishedBytes:  loadS.FinishedBytes,
				MetaBinlog:     loadS.MetaBinlog,
				MetaBinlogGtid: loadS.MetaBinlogGTID,
				Progress:       loadS.Progress,
				TotalBytes:     loadS.TotalBytes,
			}
		}
		// add sync status
		if syncerS := subTaskStatus.GetSync(); syncerS != nil {
			openapiSubTaskStatus.SyncStatus = &openapi.SyncStatus{
				BinlogType:          syncerS.GetBinlogType(),
				BlockingDdls:        syncerS.GetBlockingDDLs(),
				MasterBinlog:        syncerS.GetMasterBinlog(),
				MasterBinlogGtid:    syncerS.GetMasterBinlogGtid(),
				RecentTps:           syncerS.RecentTps,
				SecondsBehindMaster: syncerS.SecondsBehindMaster,
				Synced:              syncerS.Synced,
				SyncerBinlog:        syncerS.SyncerBinlog,
				SyncerBinlogGtid:    syncerS.SyncerBinlogGtid,
				TotalEvents:         syncerS.TotalEvents,
				TotalTps:            syncerS.TotalTps,
			}
			if unResolvedGroups := syncerS.GetUnresolvedGroups(); len(unResolvedGroups) > 0 {
				openapiSubTaskStatus.SyncStatus.UnresolvedGroups = make([]openapi.ShardingGroup, len(unResolvedGroups))
				for i, unResolvedGroup := range unResolvedGroups {
					openapiSubTaskStatus.SyncStatus.UnresolvedGroups[i] = openapi.ShardingGroup{
						DdlList:       unResolvedGroup.DDLs,
						FirstLocation: unResolvedGroup.FirstLocation,
						Synced:        unResolvedGroup.Synced,
						Target:        unResolvedGroup.Target,
						Unsynced:      unResolvedGroup.Unsynced,
					}
				}
			}
		}
		// add dump status
		if dumpS := subTaskStatus.GetDump(); dumpS != nil {
			openapiSubTaskStatus.DumpStatus = &openapi.DumpStatus{
				CompletedTables:   dumpS.CompletedTables,
				EstimateTotalRows: dumpS.EstimateTotalRows,
				FinishedBytes:     dumpS.FinishedBytes,
				FinishedRows:      dumpS.FinishedRows,
				TotalTables:       dumpS.TotalTables,
			}
		}
		// add error if some error happens
		if subTaskStatus.Result != nil && len(subTaskStatus.Result.Errors) > 0 {
			var errorMsgs string
			for _, err := range subTaskStatus.Result.Errors {
				errorMsgs += fmt.Sprintf("%s\n", handleProcessError(err))
			}
			openapiSubTaskStatus.ErrorMsg = &errorMsgs
		}
		subTaskStatusList = append(subTaskStatusList, openapiSubTaskStatus)
	}
	return subTaskStatusList, nil
}

func (s *Server) listTask(ctx context.Context, req openapi.DMAPIGetTaskListParams) ([]openapi.Task, error) {
	subTaskConfigMap := s.scheduler.GetALlSubTaskCfgs()
	taskList := config.SubTaskConfigsToOpenAPITaskList(subTaskConfigMap)
	taskArray := make([]openapi.Task, 0, len(taskList))

	if req.Stage != nil || (req.WithStatus != nil && *req.WithStatus) {
		for idx := range taskList {
			subTaskStatusList, err := s.getTaskStatus(ctx, taskList[idx].Name)
			if err != nil {
				return taskArray, err
			}
			taskList[idx].StatusList = &subTaskStatusList
		}
	}
	for idx, task := range taskList {
		filtered := false
		// filter by stage
		if task.StatusList != nil && req.Stage != nil {
			for _, status := range *task.StatusList {
				if status.Stage != *req.Stage {
					filtered = true
					break
				}
			}
		}
		// filter by source
		if req.SourceNameList != nil {
			if len(task.SourceConfig.SourceConf) != len(*req.SourceNameList) {
				filtered = true
			}
			sourceNameMap := make(map[string]struct{})
			for _, sourceName := range *req.SourceNameList {
				sourceNameMap[sourceName] = struct{}{}
			}
			for _, sourceConf := range task.SourceConfig.SourceConf {
				if _, ok := sourceNameMap[sourceConf.SourceName]; !ok {
					filtered = true
					break
				}
			}
		}
		if !filtered {
			// remove status
			if req.WithStatus == nil || (req.WithStatus != nil && !*req.WithStatus) {
				taskList[idx].StatusList = nil
			}
			taskArray = append(taskArray, *taskList[idx])
		}
	}
	return taskArray, nil
}

func (s *Server) startTask(ctx context.Context, taskName string, req openapi.StartTaskRequest) error {
	// start all subtasks for this task
	if req.SourceNameList == nil || len(*req.SourceNameList) == 0 {
		sourceNameList := openapi.SourceNameList(s.getTaskSourceNameList(taskName))
		req.SourceNameList = &sourceNameList
	}
	subTaskConfigM := s.scheduler.GetSubTaskCfgsByTask(taskName)
	needStartSubTaskList := make([]*config.SubTaskConfig, 0, len(*req.SourceNameList))
	for _, sourceName := range *req.SourceNameList {
		subTaskCfg, ok := subTaskConfigM[sourceName]
		if !ok {
			return terror.ErrSchedulerSourceCfgNotExist.Generate(sourceName)
		}
		// start task check. incremental task need to specify meta or start time
		if subTaskCfg.Meta == nil && subTaskCfg.Mode == config.ModeIncrement && req.StartTime == nil {
			return terror.ErrConfigMetadataNotSet.Generate(sourceName, config.ModeIncrement)
		}
		cfg := s.scheduler.GetSourceCfgByID(sourceName)
		if cfg == nil {
			return terror.ErrSchedulerSourceCfgNotExist.Generate(sourceName)
		}
		if !cfg.Enable {
			return terror.ErrMasterStartTask.Generate(taskName, fmt.Sprintf("source: %s is not enabled", sourceName))
		}
		needStartSubTaskList = append(needStartSubTaskList, subTaskCfg)
	}
	if len(needStartSubTaskList) == 0 {
		return nil
	}

	var (
		release scheduler.ReleaseFunc
		err     error
	)
	// removeMeta
	if req.RemoveMeta != nil && *req.RemoveMeta {
		// use same latch for remove-meta and start-task
		release, err = s.scheduler.AcquireSubtaskLatch(taskName)
		if err != nil {
			return terror.ErrSchedulerLatchInUse.Generate("RemoveMeta", taskName)
		}
		defer release()
		metaSchema := needStartSubTaskList[0].MetaSchema
		targetDB := needStartSubTaskList[0].To
		err = s.removeMetaData(ctx, taskName, metaSchema, &targetDB)
		if err != nil {
			return terror.Annotate(err, "while removing metadata")
		}
	}

	// handle task cli args
	cliArgs, err := config.OpenAPIStartTaskReqToTaskCliArgs(req)
	if err != nil {
		return terror.Annotate(err, "while converting task command line arguments")
	}

	if err = handleCliArgs(s.etcdClient, taskName, *req.SourceNameList, cliArgs); err != nil {
		return err
	}
	if release != nil {
		release()
	}

	return s.scheduler.UpdateExpectSubTaskStage(pb.Stage_Running, taskName, *req.SourceNameList...)
}

// nolint:unparam
func (s *Server) stopTask(ctx context.Context, taskName string, req openapi.StopTaskRequest) error {
	// all subtasks for this task
	if req.SourceNameList == nil || len(*req.SourceNameList) == 0 {
		sourceNameList := openapi.SourceNameList(s.getTaskSourceNameList(taskName))
		req.SourceNameList = &sourceNameList
	}
	// handle task cli args
	cliArgs, err := config.OpenAPIStopTaskReqToTaskCliArgs(req)
	if err != nil {
		return terror.Annotate(err, "while converting task command line arguments")
	}
	if err = handleCliArgs(s.etcdClient, taskName, *req.SourceNameList, cliArgs); err != nil {
		return err
	}
	return s.scheduler.UpdateExpectSubTaskStage(pb.Stage_Stopped, taskName, *req.SourceNameList...)
}

// handleCliArgs handles cli args.
// it will try to delete args if cli args is nil.
func handleCliArgs(cli *clientv3.Client, taskName string, sources []string, cliArgs *config.TaskCliArgs) error {
	if cliArgs == nil {
		err := ha.DeleteTaskCliArgs(cli, taskName, sources)
		if err != nil {
			return terror.Annotate(err, "while removing task command line arguments")
		}
	} else {
		err := ha.PutTaskCliArgs(cli, taskName, sources, *cliArgs)
		if err != nil {
			return terror.Annotate(err, "while putting task command line arguments")
		}
	}
	return nil
}

// nolint:unparam
func (s *Server) convertTaskConfig(ctx context.Context, req openapi.ConverterTaskRequest) (*openapi.Task, *config.TaskConfig, error) {
	if req.TaskConfigFile != nil {
		taskCfg := config.NewTaskConfig()
		if err := taskCfg.RawDecode(*req.TaskConfigFile); err != nil {
			return nil, nil, err
		}
		// clear extra config in MySQLInstance, use cfg.xxConfigName instead otherwise adjust will fail
		for _, cfg := range taskCfg.MySQLInstances {
			cfg.Mydumper = nil
			cfg.Loader = nil
			cfg.Syncer = nil
		}
		if adjustErr := taskCfg.Adjust(); adjustErr != nil {
			return nil, nil, adjustErr
		}
		sourceCfgMap := make(map[string]*config.SourceConfig, len(taskCfg.MySQLInstances))
		for _, source := range taskCfg.MySQLInstances {
			sourceCfg := s.scheduler.GetSourceCfgByID(source.SourceID)
			if sourceCfg == nil {
				return nil, nil, terror.ErrConfigSourceIDNotFound.Generate(source.SourceID)
			}
			sourceCfgMap[source.SourceID] = sourceCfg
		}
		task, err := config.TaskConfigToOpenAPITask(taskCfg, sourceCfgMap)
		if err != nil {
			return nil, nil, err
		}
		return task, taskCfg, nil
	}
	task := req.Task
	if adjustErr := task.Adjust(); adjustErr != nil {
		return nil, nil, adjustErr
	}
	sourceCfgMap := make(map[string]*config.SourceConfig, len(task.SourceConfig.SourceConf))
	for _, cfg := range task.SourceConfig.SourceConf {
		sourceCfg := s.scheduler.GetSourceCfgByID(cfg.SourceName)
		if sourceCfg == nil {
			return nil, nil, terror.ErrConfigSourceIDNotFound.Generate(cfg.SourceName)
		}
		sourceCfgMap[sourceCfg.SourceID] = sourceCfg
	}
	taskCfg, err := config.OpenAPITaskToTaskConfig(task, sourceCfgMap)
	if err != nil {
		return nil, nil, err
	}
	return task, taskCfg, nil
}
