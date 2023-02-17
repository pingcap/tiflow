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
	"fmt"
	"testing"

	"github.com/gogo/protobuf/jsonpb"
	dmconfig "github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/config/dbconfig"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/engine/framework"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/deps"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/errors"
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
			Bps:               1000,
			Progress:          "20.00 %",
		}
		loadStatus = &pb.LoadStatus{
			FinishedBytes:  4,
			TotalBytes:     100,
			Progress:       "4%",
			MetaBinlog:     "mysql-bin.000002, 8",
			MetaBinlogGTID: "1-2-3",
			Bps:            1000,
		}
		syncStatus = &pb.SyncStatus{
			TotalRows:           10,
			TotalRps:            10,
			RecentRps:           10,
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
		processError = &dmpkg.ProcessError{
			ErrCode:    1,
			ErrClass:   "class",
			ErrScope:   "scope",
			ErrLevel:   "low",
			Message:    "msg",
			RawCause:   "raw cause",
			Workaround: "workaround",
		}
		pbProcessError = &pb.ProcessError{
			ErrCode:    1,
			ErrClass:   "class",
			ErrScope:   "scope",
			ErrLevel:   "low",
			Message:    "msg",
			RawCause:   "raw cause",
			Workaround: "workaround",
		}
		mar                = jsonpb.Marshaler{EmitDefaults: true}
		dumpStatusBytes, _ = mar.MarshalToString(dumpStatus)
		loadStatusBytes, _ = mar.MarshalToString(loadStatus)
		syncStatusBytes, _ = mar.MarshalToString(syncStatus)
		dumpStatusResp     = &dmpkg.QueryStatusResponse{
			Unit:             frameModel.WorkerDMDump,
			Stage:            metadata.StageRunning,
			Status:           []byte(dumpStatusBytes),
			IoTotalBytes:     0,
			DumpIoTotalBytes: 0,
		}
		loadStatusResp = &dmpkg.QueryStatusResponse{
			Unit:             frameModel.WorkerDMLoad,
			Stage:            metadata.StageFinished,
			Result:           &dmpkg.ProcessResult{IsCanceled: false},
			Status:           []byte(loadStatusBytes),
			IoTotalBytes:     0,
			DumpIoTotalBytes: 0,
		}
		syncStatusResp = &dmpkg.QueryStatusResponse{
			Unit:             frameModel.WorkerDMSync,
			Stage:            metadata.StagePaused,
			Result:           &dmpkg.ProcessResult{Errors: []*dmpkg.ProcessError{processError}},
			Status:           []byte(syncStatusBytes),
			IoTotalBytes:     0,
			DumpIoTotalBytes: 0,
		}
		taskCfg = &config.TaskCfg{
			JobCfg: config.JobCfg{
				TargetDB: &dbconfig.DBConfig{},
				Upstreams: []*config.UpstreamCfg{
					{
						MySQLInstance: dmconfig.MySQLInstance{
							Mydumper: &dmconfig.MydumperConfig{},
							Loader:   &dmconfig.LoaderConfig{},
							Syncer:   &dmconfig.SyncerConfig{},
							SourceID: "task-id",
						},
						DBCfg: &dbconfig.DBConfig{},
					},
				},
			},
		}
	)

	dctx := dcontext.Background()
	dp := deps.NewDeps()
	require.NoError(t, dp.Provide(func() p2p.MessageHandlerManager {
		return p2p.NewMockMessageHandlerManager()
	}))
	dctx = dctx.WithDeps(dp)

	dmWorker, err := newDMWorker(dctx, "master-id", frameModel.WorkerDMDump, taskCfg)
	require.NoError(t, err)
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
	dmWorker.workerType = frameModel.WorkerDMLoad
	resp = dmWorker.QueryStatus(context.Background(), &dmpkg.QueryStatusRequest{Task: "task-id"})
	require.Equal(t, "", resp.ErrorMsg)
	require.Equal(t, loadStatusResp, resp)

	unitHolder.On("Status").Return(syncStatus).Once()
	unitHolder.On("Stage").Return(metadata.StagePaused, &pb.ProcessResult{Errors: []*pb.ProcessError{pbProcessError}}).Once()
	dmWorker.workerType = frameModel.WorkerDMSync
	resp = dmWorker.QueryStatus(context.Background(), &dmpkg.QueryStatusRequest{Task: "task-id"})
	require.Equal(t, "", resp.ErrorMsg)
	require.Equal(t, syncStatusResp, resp)
	require.Contains(t, string(syncStatusResp.Status), "secondsBehindMaster")
}

func TestStopWorker(t *testing.T) {
	dctx := dcontext.Background()
	dp := deps.NewDeps()
	require.NoError(t, dp.Provide(func() p2p.MessageHandlerManager {
		return p2p.NewMockMessageHandlerManager()
	}))
	dctx = dctx.WithDeps(dp)

	taskCfg := &config.TaskCfg{
		JobCfg: config.JobCfg{
			TargetDB: &dbconfig.DBConfig{},
			Upstreams: []*config.UpstreamCfg{
				{
					MySQLInstance: dmconfig.MySQLInstance{
						Mydumper: &dmconfig.MydumperConfig{},
						Loader:   &dmconfig.LoaderConfig{},
						Syncer:   &dmconfig.SyncerConfig{},
						SourceID: "task-id",
					},
					DBCfg: &dbconfig.DBConfig{},
				},
			},
		},
	}
	dmWorker, err := newDMWorker(dctx, "master-id", frameModel.WorkerDMDump, taskCfg)
	require.NoError(t, err)
	dmWorker.BaseWorker = framework.MockBaseWorker("worker-id", "master-id", dmWorker)
	dmWorker.BaseWorker.Init(context.Background())
	dmWorker.unitHolder = &mockUnitHolder{}

	require.EqualError(t, dmWorker.StopWorker(context.Background(), &dmpkg.StopWorkerMessage{Task: "wrong-task-id"}), "task id mismatch, get wrong-task-id, actually task-id")
	err = dmWorker.StopWorker(context.Background(), &dmpkg.StopWorkerMessage{Task: "task-id"})
	require.NoError(t, err)

	// mock close by framework
	dmWorker.CloseImpl(context.Background())
}

func TestOperateTask(t *testing.T) {
	dctx := dcontext.Background()
	dp := deps.NewDeps()
	require.NoError(t, dp.Provide(func() p2p.MessageHandlerManager {
		return p2p.NewMockMessageHandlerManager()
	}))
	dctx = dctx.WithDeps(dp)

	taskCfg := &config.TaskCfg{
		JobCfg: config.JobCfg{
			TargetDB: &dbconfig.DBConfig{},
			Upstreams: []*config.UpstreamCfg{
				{
					MySQLInstance: dmconfig.MySQLInstance{
						Mydumper: &dmconfig.MydumperConfig{},
						Loader:   &dmconfig.LoaderConfig{},
						Syncer:   &dmconfig.SyncerConfig{},
						SourceID: "task-id",
					},
					DBCfg: &dbconfig.DBConfig{},
				},
			},
		},
	}
	dmWorker, err := newDMWorker(dctx, "master-id", frameModel.WorkerDMDump, taskCfg)
	require.NoError(t, err)
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
