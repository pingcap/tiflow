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
	"errors"
	"fmt"
	"testing"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/engine/framework"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/deps"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
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
		processError = &pb.ProcessError{
			ErrCode:    1,
			ErrClass:   "class",
			ErrScope:   "scope",
			ErrLevel:   "low",
			Message:    "msg",
			RawCause:   "raw cause",
			Workaround: "workaround",
		}
		dumpStatusBytes, _ = json.Marshal(dumpStatus)
		loadStatusBytes, _ = json.Marshal(loadStatus)
		syncStatusBytes, _ = json.Marshal(syncStatus)
		dumpStatusResp     = &dmpkg.QueryStatusResponse{
			Unit:   framework.WorkerDMDump,
			Stage:  metadata.StageRunning,
			Status: dumpStatusBytes,
		}
		loadStatusResp = &dmpkg.QueryStatusResponse{
			Unit:   framework.WorkerDMLoad,
			Stage:  metadata.StageFinished,
			Result: &pb.ProcessResult{IsCanceled: false},
			Status: loadStatusBytes,
		}
		syncStatusResp = &dmpkg.QueryStatusResponse{
			Unit:   framework.WorkerDMSync,
			Stage:  metadata.StagePaused,
			Result: &pb.ProcessResult{Errors: []*pb.ProcessError{processError}},
			Status: syncStatusBytes,
		}
	)

	dctx := dcontext.Background()
	dp := deps.NewDeps()
	require.NoError(t, dp.Provide(func() p2p.MessageHandlerManager {
		return p2p.NewMockMessageHandlerManager()
	}))
	dctx = dctx.WithDeps(dp)

	dmWorker := newDMWorker(dctx, "", framework.WorkerDMDump, &config.SubTaskConfig{SourceID: "task-id"})
	unitHolder := &mockUnitHolder{}
	dmWorker.unitHolder = unitHolder

	require.Equal(t, dmWorker.QueryStatus(context.Background(), &dmpkg.QueryStatusRequest{Task: "wrong-task-id"}).ErrorMsg,
		fmt.Sprintf("task id mismatch, get %s, actually %s", "wrong-task-id", "task-id"))

	unitHolder.On("Status").Return(dumpStatus).Once()
	unitHolder.On("Stage").Return(metadata.StageRunning, nil).Once()
	resp := dmWorker.QueryStatus(context.Background(), &dmpkg.QueryStatusRequest{Task: "task-id"})
	require.Equal(t, "", resp.ErrorMsg)
	require.Equal(t, dumpStatusResp, resp)

	unitHolder.On("Status").Return(loadStatus).Once()
	unitHolder.On("Stage").Return(metadata.StageFinished, &pb.ProcessResult{IsCanceled: false}).Once()
	dmWorker.workerType = framework.WorkerDMLoad
	resp = dmWorker.QueryStatus(context.Background(), &dmpkg.QueryStatusRequest{Task: "task-id"})
	require.Equal(t, "", resp.ErrorMsg)
	require.Equal(t, loadStatusResp, resp)

	unitHolder.On("Status").Return(syncStatus).Once()
	unitHolder.On("Stage").Return(metadata.StagePaused, &pb.ProcessResult{Errors: []*pb.ProcessError{processError}}).Once()
	dmWorker.workerType = framework.WorkerDMSync
	resp = dmWorker.QueryStatus(context.Background(), &dmpkg.QueryStatusRequest{Task: "task-id"})
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

	dmWorker := newDMWorker(dctx, "master-id", framework.WorkerDMDump, &config.SubTaskConfig{SourceID: "task-id"})
	dmWorker.BaseWorker = framework.MockBaseWorker("worker-id", "master-id", dmWorker)
	dmWorker.BaseWorker.Init(context.Background())
	dmWorker.unitHolder = &mockUnitHolder{}

	require.EqualError(t, dmWorker.StopWorker(context.Background(), &dmpkg.StopWorkerMessage{Task: "wrong-task-id"}), "task id mismatch, get wrong-task-id, actually task-id")
	err := dmWorker.StopWorker(context.Background(), &dmpkg.StopWorkerMessage{Task: "task-id"})
	require.True(t, cerrors.ErrWorkerFinish.Equal(err))

	// mock close by framework
	require.NoError(t, dmWorker.CloseImpl(context.Background()))
}

func TestOperateTask(t *testing.T) {
	dctx := dcontext.Background()
	dp := deps.NewDeps()
	require.NoError(t, dp.Provide(func() p2p.MessageHandlerManager {
		return p2p.NewMockMessageHandlerManager()
	}))
	dctx = dctx.WithDeps(dp)

	dmWorker := newDMWorker(dctx, "master-id", framework.WorkerDMDump, &config.SubTaskConfig{SourceID: "task-id"})
	dmWorker.BaseWorker = framework.MockBaseWorker("worker-id", "master-id", dmWorker)
	dmWorker.BaseWorker.Init(context.Background())
	mockUnitHolder := &mockUnitHolder{}
	dmWorker.unitHolder = mockUnitHolder

	require.EqualError(t, dmWorker.OperateTask(context.Background(), &dmpkg.OperateTaskMessage{Task: "wrong-task-id"}), "task id mismatch, get wrong-task-id, actually task-id")
	mockUnitHolder.On("Pause").Return(nil).Once()
	require.Nil(t, dmWorker.OperateTask(context.Background(), &dmpkg.OperateTaskMessage{Task: "task-id", Op: dmpkg.Pause}))
	mockUnitHolder.On("Resume").Return(nil).Once()
	require.Nil(t, dmWorker.OperateTask(context.Background(), &dmpkg.OperateTaskMessage{Task: "task-id", Op: dmpkg.Resume}))
	require.EqualError(t, dmWorker.OperateTask(context.Background(), &dmpkg.OperateTaskMessage{Task: "task-id", Op: dmpkg.Update}),
		fmt.Sprintf("unsupported op type %d for task %s", dmpkg.Update, "task-id"))
}

func TestBinlog(t *testing.T) {
	mockUnitHolder := &mockUnitHolder{}
	dmWorker := &dmWorker{taskID: "task-id", unitHolder: mockUnitHolder}

	mockUnitHolder.On("Binlog").Return("", errors.New("error")).Once()
	require.Equal(t, "error", dmWorker.BinlogTask(context.Background(), &dmpkg.BinlogTaskRequest{Task: "task-id"}).ErrorMsg)
	mockUnitHolder.On("Binlog").Return("msg", nil).Once()
	require.Equal(t, "msg", dmWorker.BinlogTask(context.Background(), &dmpkg.BinlogTaskRequest{Task: "task-id"}).Msg)
}

func TestBinlogSchema(t *testing.T) {
	mockUnitHolder := &mockUnitHolder{}
	dmWorker := &dmWorker{taskID: "task-id", unitHolder: mockUnitHolder}

	require.Equal(t, "task id mismatch, get wrong-task-id, actually task-id", dmWorker.BinlogSchemaTask(context.Background(), &dmpkg.BinlogSchemaTaskRequest{Source: "wrong-task-id"}).ErrorMsg)
	mockUnitHolder.On("BinlogSchema").Return("", errors.New("error")).Once()
	require.Equal(t, "error", dmWorker.BinlogSchemaTask(context.Background(), &dmpkg.BinlogSchemaTaskRequest{Source: "task-id"}).ErrorMsg)
	mockUnitHolder.On("BinlogSchema").Return("msg", nil).Once()
	require.Equal(t, "msg", dmWorker.BinlogSchemaTask(context.Background(), &dmpkg.BinlogSchemaTaskRequest{Source: "task-id"}).Msg)
}
