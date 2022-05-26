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
	"testing"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/engine/executor/dm/unit"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/lib"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/deps"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
	"github.com/pingcap/tiflow/engine/pkg/errors"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/stretchr/testify/require"
)

func TestQueryStatusAPI(t *testing.T) {
	var (
		dumpStatus = &pb.DumpStatus{
			TotalTables:       10,
			CompletedTables:   1,
			FinishedBytes:     100,
			FinishedRows:      10,
			EstimateTotalRows: 1000,
		}
		loadStatus = &pb.LoadStatus{
			FinishedBytes:  4,
			TotalBytes:     100,
			Progress:       "4%",
			MetaBinlog:     "mysql-bin.000002, 8",
			MetaBinlogGTID: "1-2-3",
		}
		syncStatus = &pb.SyncStatus{
			TotalEvents:         10,
			TotalTps:            10,
			RecentTps:           10,
			MasterBinlog:        "mysql-bin.000002, 4",
			MasterBinlogGtid:    "1-2-20",
			SyncerBinlog:        "mysql-bin.000001, 432",
			SyncerBinlogGtid:    "1-2-10",
			BlockingDDLs:        []string{"ALTER TABLE `db`.`tb` ADD COLUMN a INT"},
			Synced:              false,
			BinlogType:          "remote",
			SecondsBehindMaster: 10,
			BlockDDLOwner:       "",
			ConflictMsg:         "",
		}
		dumpStatusBytes, _ = json.Marshal(dumpStatus)
		loadStatusBytes, _ = json.Marshal(loadStatus)
		syncStatusBytes, _ = json.Marshal(syncStatus)
		dumpStatusResp     = &dmpkg.QueryStatusResponse{
			Unit:   lib.WorkerDMDump,
			Stage:  metadata.StageRunning,
			Status: dumpStatusBytes,
		}
		loadStatusResp = &dmpkg.QueryStatusResponse{
			Unit:   lib.WorkerDMLoad,
			Stage:  metadata.StageFinished,
			Status: loadStatusBytes,
		}
		syncStatusResp = &dmpkg.QueryStatusResponse{
			Unit:   lib.WorkerDMSync,
			Stage:  metadata.StagePaused,
			Status: syncStatusBytes,
		}
	)

	dctx := dcontext.Background()
	dp := deps.NewDeps()
	require.NoError(t, dp.Provide(func() p2p.MessageHandlerManager {
		return p2p.NewMockMessageHandlerManager()
	}))
	dctx = dctx.WithDeps(dp)

	task := newBaseTask(dctx, "master-id", lib.WorkerDMDump, &config.SubTaskConfig{SourceID: "task-id"})
	unitHolder := &unit.MockHolder{}
	task.unitHolder = unitHolder

	unitHolder.On("Status").Return(dumpStatus).Once()
	task.setStage(metadata.StageRunning)
	resp := task.QueryStatus(context.Background(), &dmpkg.QueryStatusRequest{Task: "task-id"})
	require.Equal(t, "", resp.ErrorMsg)
	require.Equal(t, dumpStatusResp, resp)

	unitHolder.On("Status").Return(loadStatus).Once()
	task.workerType = lib.WorkerDMLoad
	task.setStage(metadata.StageFinished)
	resp = task.QueryStatus(context.Background(), &dmpkg.QueryStatusRequest{Task: "task-id"})
	require.Equal(t, "", resp.ErrorMsg)
	require.Equal(t, loadStatusResp, resp)

	unitHolder.On("Status").Return(syncStatus).Once()
	task.workerType = lib.WorkerDMSync
	task.setStage(metadata.StagePaused)
	resp = task.QueryStatus(context.Background(), &dmpkg.QueryStatusRequest{Task: "task-id"})
	require.Equal(t, "", resp.ErrorMsg)
	require.Equal(t, syncStatusResp, resp)
}

func TestStopWorker(t *testing.T) {
	dctx := dcontext.Background()
	dp := deps.NewDeps()
	require.NoError(t, dp.Provide(func() p2p.MessageHandlerManager {
		return p2p.NewMockMessageHandlerManager()
	}))
	dctx = dctx.WithDeps(dp)

	baseTask := newBaseTask(dctx, "master-id", lib.WorkerDMDump, &config.SubTaskConfig{SourceID: "task-id"})
	baseTask.BaseWorker = lib.MockBaseWorker("worker-id", "master-id", baseTask)
	baseTask.BaseWorker.Init(context.Background())
	baseTask.unitHolder = &unit.MockHolder{}

	require.EqualError(t, baseTask.StopWorker(context.Background(), &dmpkg.StopWorkerMessage{Task: "wrong-task-id"}), "task id mismatch, get wrong-task-id, actually task-id")
	err := baseTask.StopWorker(context.Background(), &dmpkg.StopWorkerMessage{Task: "task-id"})
	require.True(t, errors.ErrWorkerFinish.Equal(err))
}

func (t *baseTask) onInit(ctx context.Context) error {
	return nil
}

func (t *baseTask) createUnitHolder(*config.SubTaskConfig) unit.Holder {
	return &unit.MockHolder{}
}
