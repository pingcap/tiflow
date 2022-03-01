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

	"github.com/pingcap/tiflow/dm/checker"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/openapi"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

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
		} else if relayStatus := workerStatus.SourceStatus.GetRelayStatus(); relayStatus != nil {
			sourceStatus.RelayStatus = &openapi.RelayStatus{
				MasterBinlog:       relayStatus.MasterBinlog,
				MasterBinlogGtid:   relayStatus.MasterBinlogGtid,
				RelayBinlogGtid:    relayStatus.RelayBinlogGtid,
				RelayCatchUpMaster: relayStatus.RelayCatchUpMaster,
				RelayDir:           relayStatus.RelaySubDir,
				Stage:              relayStatus.Stage.String(),
			}
		}
		sourceStatusList[i] = sourceStatus
	}
	return sourceStatusList, nil
}

// nolint:unparam
func (s *Server) createSource(ctx context.Context, cfg *config.SourceConfig) error {
	return s.scheduler.AddSourceCfg(cfg)
}

// nolint:unparam,unused
func (s *Server) updateSource(ctx context.Context, cfg *config.SourceConfig) error {
	// TODO(ehco) no caller now , will implement later
	return nil
}

// nolint:unparam
func (s *Server) deleteSource(ctx context.Context, sourceName string, force bool) error {
	if force {
		for _, taskName := range s.scheduler.GetTaskNameListBySourceName(sourceName) {
			if err := s.scheduler.RemoveSubTasks(taskName, sourceName); err != nil {
				return err
			}
		}
	}
	return s.scheduler.RemoveSourceCfg(sourceName)
}

// nolint:unparam,unused
func (s *Server) getSource(ctx context.Context, sourceName string) (openapiSource openapi.Source, err error) {
	sourceCfg := s.scheduler.GetSourceCfgByID(sourceName)
	if sourceCfg == nil {
		return openapiSource, terror.ErrSchedulerSourceCfgNotExist.Generate(sourceName)
	}
	openapiSource = config.SourceCfgToOpenAPISource(sourceCfg)
	return openapiSource, nil
}

func (s *Server) getSourceStatus(ctx context.Context, sourceName string) ([]openapi.SourceStatus, error) {
	return s.getSourceStatusListFromWorker(ctx, sourceName, true)
}

// nolint:unparam
func (s *Server) listSource(ctx context.Context, req interface{}) []openapi.Source {
	// TODO(ehco) implement filter later
	sourceM := s.scheduler.GetSourceCfgs()
	openapiSourceList := make([]openapi.Source, 0, len(sourceM))
	for _, source := range sourceM {
		openapiSourceList = append(openapiSourceList, config.SourceCfgToOpenAPISource(source))
	}
	return openapiSourceList
}

// nolint:unparam,unused
func (s *Server) enableSource(ctx context.Context, sourceName, workerName string) error {
	worker := s.scheduler.GetWorkerBySource(sourceName)
	if worker == nil {
		return terror.ErrWorkerNoStart
	}
	taskNameList := s.scheduler.GetTaskNameListBySourceName(sourceName)
	return s.scheduler.BatchOperateTaskOnWorker(ctx, worker, taskNameList, sourceName, pb.Stage_Running, true)
}

// nolint:unused
func (s *Server) disableSource(ctx context.Context, sourceName string) error {
	worker := s.scheduler.GetWorkerBySource(sourceName)
	if worker == nil {
		// no need to stop task if the source is not running
		return nil
	}
	taskNameList := s.scheduler.GetTaskNameListBySourceName(sourceName)
	return s.scheduler.BatchOperateTaskOnWorker(ctx, worker, taskNameList, sourceName, pb.Stage_Stopped, true)
}

func (s *Server) transferSource(ctx context.Context, sourceName, workerName string) error {
	return s.scheduler.TransferSource(ctx, sourceName, workerName)
}

func (s *Server) checkTask(ctx context.Context, subtaskCfgList []*config.SubTaskConfig, errCnt, warnCnt int64) (string, error) {
	return checker.CheckSyncConfigFunc(ctx, subtaskCfgList, errCnt, warnCnt)
}

// nolint:unparam,unused
func (s *Server) createTask(ctx context.Context, subtaskCfgList []*config.SubTaskConfig) error {
	return s.scheduler.AddSubTasks(false, pb.Stage_Stopped, subtaskCfgPointersToInstances(subtaskCfgList...)...)
}

// nolint:unused
func (s *Server) updateTask(ctx context.Context, taskCfg *config.TaskConfig) error {
	// TODO(ehco) no caller now , will implement later
	return nil
}

// nolint:unparam,unused
func (s *Server) deleteTask(ctx context.Context, taskName string) error {
	sourceNameList := s.getTaskSourceNameList(taskName)
	return s.scheduler.RemoveSubTasks(taskName, sourceNameList...)
}

// nolint:unused
func (s *Server) getTask(ctx context.Context, taskName string) error {
	// TODO(ehco) no caller now , will implement later
	return nil
}

func (s *Server) getTaskStatus(ctx context.Context, taskName string, sourceNameList []string) ([]openapi.SubTaskStatus, error) {
	workerStatusList := s.getStatusFromWorkers(ctx, sourceNameList, taskName, true)
	subTaskStatusList := make([]openapi.SubTaskStatus, 0, len(workerStatusList))
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
		openapiSubTaskStatus.Stage = subTaskStatus.GetStage().String()
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
		subTaskStatusList = append(subTaskStatusList, openapiSubTaskStatus)
	}
	return subTaskStatusList, nil
}

// nolint:unparam,unused
func (s *Server) listTask(ctx context.Context, req interface{}) []openapi.Task {
	// TODO(ehco) implement filter later
	subTaskConfigMap := s.scheduler.GetSubTaskCfgs()
	return config.SubTaskConfigsToOpenAPITask(subTaskConfigMap)
}

// nolint:unparam,unused
func (s *Server) startTask(ctx context.Context, taskName string, sourceNameList []string, removeMeta bool, req interface{}) error {
	// TODO(ehco) merge start-task req
	subTaskConfigM := s.scheduler.GetSubTaskCfgsByTask(taskName)
	needStartSubTaskList := make([]*config.SubTaskConfig, 0, len(subTaskConfigM))
	for _, sourceName := range sourceNameList {
		subTaskCfg, ok := subTaskConfigM[sourceName]
		if !ok {
			return terror.ErrSchedulerSourceCfgNotExist.Generate(sourceName)
		}
		needStartSubTaskList = append(needStartSubTaskList, subTaskCfg)
	}
	if len(needStartSubTaskList) == 0 {
		return nil
	}
	if removeMeta {
		// use same latch for remove-meta and start-task
		release, err := s.scheduler.AcquireSubtaskLatch(taskName)
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
		release()
	}
	return s.scheduler.UpdateExpectSubTaskStage(pb.Stage_Running, taskName, sourceNameList...)
}

// nolint:unparam,unused
func (s *Server) stopTask(ctx context.Context, taskName string, sourceNameList []string) error {
	return s.scheduler.UpdateExpectSubTaskStage(pb.Stage_Stopped, taskName, sourceNameList...)
}
