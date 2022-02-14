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

package master

import (
	"context"

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
	return s.scheduler.AddSourceConfig(cfg)
}

// nolint:unparam
func (s *Server) updateSource(ctx context.Context, cfg *config.SourceConfig) error {
	// TODO(ehco) no caller now , will implement later
	return nil
}

// nolint:unparam
func (s *Server) deleteSource(ctx context.Context, sourceName string) error {
	return s.scheduler.RemoveSourceCfg(sourceName)
}

// nolint:unparam
// nolint:unused
func (s *Server) getSource(ctx context.Context, sourceName string) *config.SourceConfig {
	return s.scheduler.GetSourceCfgByID(sourceName)
}

func (s *Server) getSourceStatus(ctx context.Context, sourceName string) ([]openapi.SourceStatus, error) {
	return s.getSourceStatusListFromWorker(ctx, sourceName, true)
}

// nolint:unparam
func (s *Server) listSource(ctx context.Context, req interface{}) []*config.SourceConfig {
	// TODO(ehco) implement filter later
	sourceM := s.scheduler.GetSourceCfgs()
	sourceList := make([]*config.SourceConfig, 0, len(sourceM))
	for _, source := range sourceM {
		sourceList = append(sourceList, source)
	}
	return sourceList
}

func (s *Server) enableSource(ctx context.Context, sourceName, workerName string) error {
	return s.transferSource(ctx, sourceName, workerName)
}

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
