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
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/dm/checker"
	dmconfig "github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/master"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	kvmock "github.com/pingcap/tiflow/engine/pkg/meta/mock"
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestQueryStatusAPI(t *testing.T) {
	var (
		ctx               = context.Background()
		metaKVClient      = kvmock.NewMetaMock()
		mockBaseJobmaster = &MockBaseJobmaster{t: t}
		jm                = &JobMaster{
			BaseJobMaster: mockBaseJobmaster,
			metadata:      metadata.NewMetaData(metaKVClient, log.L()),
		}
		jobCfg  = &config.JobCfg{ModRevision: 4}
		taskCfg = jobCfg.ToTaskCfg()
		job     = &metadata.Job{
			Tasks: map[string]*metadata.Task{
				"task1": {Stage: metadata.StagePaused, Cfg: taskCfg},
				"task2": {Stage: metadata.StageFinished, Cfg: taskCfg},
				"task3": {Stage: metadata.StageFinished, Cfg: taskCfg},
				"task4": {Stage: metadata.StageRunning, Cfg: taskCfg},
				"task5": {Stage: metadata.StageRunning, Cfg: taskCfg},
				"task6": {Stage: metadata.StageRunning, Cfg: taskCfg},
				"task7": {Stage: metadata.StageFinished, Cfg: taskCfg},
			},
		}
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
			BlockingDDLs:        []string{"ALTER TABLE db.tb ADD COLUMN a INT"},
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
		dumpStatusBytes, _ = json.Marshal(dumpStatus)
		loadStatusBytes, _ = json.Marshal(loadStatus)
		syncStatusBytes, _ = json.Marshal(syncStatus)
		dumpStatusResp     = &dmpkg.QueryStatusResponse{Unit: frameModel.WorkerDMDump, Stage: metadata.StageRunning, Status: dumpStatusBytes, IoTotalBytes: 0, DumpIoTotalBytes: 0}
		loadStatusResp     = &dmpkg.QueryStatusResponse{Unit: frameModel.WorkerDMLoad, Stage: metadata.StagePaused, Result: &dmpkg.ProcessResult{IsCanceled: true}, Status: loadStatusBytes, IoTotalBytes: 0, DumpIoTotalBytes: 0}
		syncStatusResp     = &dmpkg.QueryStatusResponse{Unit: frameModel.WorkerDMSync, Stage: metadata.StageError, Result: &dmpkg.ProcessResult{Errors: []*dmpkg.ProcessError{processError}}, Status: syncStatusBytes, IoTotalBytes: 0, DumpIoTotalBytes: 0}
		dumpTime, _        = time.Parse(time.RFC3339Nano, "2020-11-04T18:47:57.43382274+08:00")
		loadTime, _        = time.Parse(time.RFC3339Nano, "2020-11-04T19:47:57.43382274+08:00")
		syncTime, _        = time.Parse(time.RFC3339Nano, "2020-11-04T20:47:57.43382274+08:00")
		dumpDuration       = time.Hour
		loadDuration       = time.Minute
		unitState          = &metadata.UnitState{
			CurrentUnitStatus: map[string]*metadata.UnitStatus{
				// task1's worker not found, and current unit status is not stored
				// task2's worker not found
				"task2": {CreatedTime: syncTime},
				"task3": {CreatedTime: dumpTime},
				"task4": {CreatedTime: dumpTime},
				"task5": {CreatedTime: loadTime},
				"task6": {CreatedTime: syncTime},
				// task7's worker not found
				"task7": {CreatedTime: syncTime},
			},
			FinishedUnitStatus: map[string][]*metadata.FinishedTaskStatus{
				"task2": {
					&metadata.FinishedTaskStatus{
						TaskStatus: metadata.TaskStatus{
							Unit:             frameModel.WorkerDMDump,
							Task:             "task2",
							Stage:            metadata.StageFinished,
							CfgModRevision:   3,
							StageUpdatedTime: loadTime,
						},
						Status:   dumpStatusBytes,
						Duration: dumpDuration,
					},
					&metadata.FinishedTaskStatus{
						TaskStatus: metadata.TaskStatus{
							Unit:             frameModel.WorkerDMLoad,
							Task:             "task2",
							Stage:            metadata.StageFinished,
							CfgModRevision:   3,
							StageUpdatedTime: syncTime,
						},
						Status:   loadStatusBytes,
						Duration: loadDuration,
					},
				},
				"task7": {
					&metadata.FinishedTaskStatus{
						TaskStatus: metadata.TaskStatus{
							Unit:             frameModel.WorkerDMDump,
							Task:             "task7",
							Stage:            metadata.StageFinished,
							CfgModRevision:   4,
							StageUpdatedTime: loadTime,
						},
						Status:   dumpStatusBytes,
						Duration: dumpDuration,
					},
					&metadata.FinishedTaskStatus{
						TaskStatus: metadata.TaskStatus{
							Unit:             frameModel.WorkerDMLoad,
							Task:             "task7",
							Stage:            metadata.StageFinished,
							CfgModRevision:   4,
							StageUpdatedTime: syncTime,
						},
						Status:   loadStatusBytes,
						Duration: loadDuration,
					},
				},
			},
		}
	)
	messageAgent := &dmpkg.MockMessageAgent{}
	jm.messageAgent = messageAgent
	jm.workerManager = NewWorkerManager(mockBaseJobmaster.ID(), nil, jm.metadata.JobStore(), jm.metadata.UnitStateStore(), nil, nil, nil, jm.Logger(),
		resModel.ResourceTypeLocalFile)
	jm.taskManager = NewTaskManager("test-job", nil, nil, nil, jm.Logger(), promutil.NewFactory4Test(t.TempDir()))
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus("task3", frameModel.WorkerDMDump, "worker3", runtime.WorkerOnline, 4))
	messageAgent.On("SendRequest", mock.Anything, "task3", mock.Anything, mock.Anything).Return(nil, context.DeadlineExceeded).Once()
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus("task4", frameModel.WorkerDMDump, "worker4", runtime.WorkerOnline, 3))
	messageAgent.On("SendRequest", mock.Anything, "task4", mock.Anything, mock.Anything).Return(dumpStatusResp, nil).Once()
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus("task5", frameModel.WorkerDMLoad, "worker5", runtime.WorkerOnline, 4))
	messageAgent.On("SendRequest", mock.Anything, "task5", mock.Anything, mock.Anything).Return(loadStatusResp, nil).Once()
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus("task6", frameModel.WorkerDMSync, "worker6", runtime.WorkerOnline, 3))
	messageAgent.On("SendRequest", mock.Anything, "task6", mock.Anything, mock.Anything).Return(syncStatusResp, nil).Once()

	err := jm.metadata.UnitStateStore().Put(ctx, unitState)
	require.NoError(t, err)

	// no job
	jobStatus, err := jm.QueryJobStatus(context.Background(), nil)
	require.Error(t, err)
	require.Nil(t, jobStatus)

	require.NoError(t, jm.metadata.JobStore().Put(context.Background(), job))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	jobStatus, err = jm.QueryJobStatus(ctx, []string{"task8"})
	require.NoError(t, err)
	taskStatus := jobStatus.TaskStatus["task8"]
	require.Equal(t, "", taskStatus.WorkerID)
	require.Equal(t, "", taskStatus.ExpectedStage.String())
	require.Equal(t, &dmpkg.QueryStatusResponse{ErrorMsg: "task task8 for job not found"}, taskStatus.Status)

	jobStatus, err = jm.QueryJobStatus(ctx, nil)
	require.NoError(t, err)
	require.Len(t, jobStatus.TaskStatus, 7)

	for task, currentStatus := range jobStatus.TaskStatus {
		// start-time is fixed at 2020-11-04 except task1 which is paused and don't have current status,
		// we just check that it's > 24h （now it's 2022-12-16)
		if task != "task1" {
			require.Greater(t, currentStatus.Duration, 24*time.Hour)
		}
		// this is for passing follow test, because we can't offer the precise duration in advance
		currentStatus.Duration = time.Second
		jobStatus.TaskStatus[task] = currentStatus
	}

	expectedStatus := `{
	"job_id": "dm-jobmaster-id",
	"task_status": {
		"task1": {
			"expected_stage": "Paused",
			"worker_id": "",
			"config_outdated": true,
			"status": {
				"error_message": "worker for task task1 not found",
				"unit": "",
				"stage": "",
				"result": null,
				"status": null,
				"io_total_bytes": 0,
				"dump_io_total_bytes": 0
			},
			"duration": 1000000000
		},
		"task2": {
			"expected_stage": "Finished",
			"worker_id": "",
			"config_outdated": true,
			"status": {
				"error_message": "worker for task task2 not found",
				"unit": "",
				"stage": "",
				"result": null,
				"status": null,
				"io_total_bytes": 0,
				"dump_io_total_bytes": 0
			},
			"duration": 1000000000
		},
		"task3": {
			"expected_stage": "Finished",
			"worker_id": "worker3",
			"config_outdated": false,
			"status": {
				"error_message": "context deadline exceeded",
				"unit": "",
				"stage": "",
				"result": null,
				"status": null,
				"io_total_bytes": 0,
				"dump_io_total_bytes": 0
			},
			"duration": 1000000000
		},
		"task4": {
			"expected_stage": "Running",
			"worker_id": "worker4",
			"config_outdated": true,
			"status": {
				"error_message": "",
				"unit": "DMDumpTask",
				"stage": "Running",
				"result": null,
				"status": {
					"totalTables": 10,
					"completedTables": 1,
					"finishedBytes": 100,
					"finishedRows": 10,
					"estimateTotalRows": 1000,
					"bps": 1000,
					"progress": "20.00 %"
				},
				"io_total_bytes": 0,
				"dump_io_total_bytes": 0
			},
			"duration": 1000000000
		},
		"task5": {
			"expected_stage": "Running",
			"worker_id": "worker5",
			"config_outdated": false,
			"status": {
				"error_message": "",
				"unit": "DMLoadTask",
				"stage": "Paused",
				"result": {
					"is_canceled": true
				},
				"status": {
					"finishedBytes": 4,
					"totalBytes": 100,
					"progress": "4%",
					"metaBinlog": "mysql-bin.000002, 8",
					"metaBinlogGTID": "1-2-3",
					"bps": 1000
				},
				"io_total_bytes": 0,
				"dump_io_total_bytes": 0
			},
			"duration": 1000000000
		},
		"task6": {
			"expected_stage": "Running",
			"worker_id": "worker6",
			"config_outdated": true,
			"status": {
				"error_message": "",
				"unit": "DMSyncTask",
				"stage": "Error",
				"result": {
					"errors": [
						{
							"error_code": 1,
							"error_class": "class",
							"error_scope": "scope",
							"error_level": "low",
							"message": "msg",
							"raw_cause": "raw cause",
							"workaround": "workaround"
						}
					]
				},
				"status": {
					"masterBinlog": "mysql-bin.000002, 4",
					"masterBinlogGtid": "1-2-20",
					"syncerBinlog": "mysql-bin.000001, 432",
					"syncerBinlogGtid": "1-2-10",
					"blockingDDLs": [
						"ALTER TABLE db.tb ADD COLUMN a INT"
					],
					"binlogType": "remote",
					"secondsBehindMaster": 10,
					"totalRows": 10,
					"totalRps": 10,
					"recentRps": 10
				},
				"io_total_bytes": 0,
				"dump_io_total_bytes": 0
			},
			"duration": 1000000000
		},
		"task7": {
			"expected_stage": "Finished",
			"worker_id": "",
			"config_outdated": true,
			"status": {
				"error_message": "worker for task task7 not found",
				"unit": "",
				"stage": "",
				"result": null,
				"status": null,
				"io_total_bytes": 0,
				"dump_io_total_bytes": 0
			},
			"duration": 1000000000
		}
	},
	"finished_unit_status": {
		"task2": [
			{
				"Unit": "DMDumpTask",
				"Task": "task2",
				"Stage": "Finished",
				"CfgModRevision": 3,
				"StageUpdatedTime": "2020-11-04T19:47:57.43382274+08:00",
				"Result": null,
				"Status": {
					"totalTables": 10,
					"completedTables": 1,
					"finishedBytes": 100,
					"finishedRows": 10,
					"estimateTotalRows": 1000,
					"bps": 1000,
					"progress": "20.00 %"
				},
				"Duration": 3600000000000
			},
			{
				"Unit": "DMLoadTask",
				"Task": "task2",
				"Stage": "Finished",
				"CfgModRevision": 3,
				"StageUpdatedTime": "2020-11-04T20:47:57.43382274+08:00",
				"Result": null,
				"Status": {
					"finishedBytes": 4,
					"totalBytes": 100,
					"progress": "4%",
					"metaBinlog": "mysql-bin.000002, 8",
					"metaBinlogGTID": "1-2-3",
					"bps": 1000
				},
				"Duration": 60000000000
			}
		],
		"task7": [
			{
				"Unit": "DMDumpTask",
				"Task": "task7",
				"Stage": "Finished",
				"CfgModRevision": 4,
				"StageUpdatedTime": "2020-11-04T19:47:57.43382274+08:00",
				"Result": null,
				"Status": {
					"totalTables": 10,
					"completedTables": 1,
					"finishedBytes": 100,
					"finishedRows": 10,
					"estimateTotalRows": 1000,
					"bps": 1000,
					"progress": "20.00 %"
				},
				"Duration": 3600000000000
			},
			{
				"Unit": "DMLoadTask",
				"Task": "task7",
				"Stage": "Finished",
				"CfgModRevision": 4,
				"StageUpdatedTime": "2020-11-04T20:47:57.43382274+08:00",
				"Result": null,
				"Status": {
					"finishedBytes": 4,
					"totalBytes": 100,
					"progress": "4%",
					"metaBinlog": "mysql-bin.000002, 8",
					"metaBinlogGTID": "1-2-3",
					"bps": 1000
				},
				"Duration": 60000000000
			}
		]
	}
}`
	status, err := json.MarshalIndent(jobStatus, "", "\t")
	require.NoError(t, err)
	require.Equal(t, expectedStatus, string(status))
}

func TestOperateTask(t *testing.T) {
	jm := &JobMaster{
		taskManager: NewTaskManager("test-job", nil, metadata.NewJobStore(kvmock.NewMetaMock(), log.L()), nil, log.L(), promutil.NewFactory4Test(t.TempDir())),
	}
	require.EqualError(t, jm.operateTask(context.Background(), dmpkg.Delete, nil, nil), fmt.Sprintf("unsupported op type %d for operate task", dmpkg.Delete))
	require.EqualError(t, jm.operateTask(context.Background(), dmpkg.Pause, nil, nil), "state not found")
}

func TestGetJobCfg(t *testing.T) {
	kvClient := kvmock.NewMetaMock()
	jm := &JobMaster{
		metadata: metadata.NewMetaData(kvClient, log.L()),
	}
	jobCfg, err := jm.GetJobCfg(context.Background())
	require.EqualError(t, err, "state not found")
	require.Nil(t, jobCfg)

	jobCfg = &config.JobCfg{TaskMode: dmconfig.ModeFull, Upstreams: []*config.UpstreamCfg{{}}}
	job := metadata.NewJob(jobCfg)
	jm.metadata.JobStore().Put(context.Background(), job)

	jobCfg, err = jm.GetJobCfg(context.Background())
	require.NoError(t, err)
	require.Equal(t, dmconfig.ModeFull, jobCfg.TaskMode)
}

func TestUpdateJobCfg(t *testing.T) {
	var (
		mockBaseJobmaster   = &MockBaseJobmaster{t: t}
		metaKVClient        = kvmock.NewMetaMock()
		mockCheckpointAgent = &MockCheckpointAgent{}
		messageAgent        = &dmpkg.MockMessageAgent{}
		jobCfg              = &config.JobCfg{}
		jm                  = &JobMaster{
			BaseJobMaster:   mockBaseJobmaster,
			metadata:        metadata.NewMetaData(metaKVClient, log.L()),
			checkpointAgent: mockCheckpointAgent,
		}
	)
	jm.taskManager = NewTaskManager("test-job", nil, jm.metadata.JobStore(), messageAgent, jm.Logger(), promutil.NewFactory4Test(t.TempDir()))
	jm.workerManager = NewWorkerManager(mockBaseJobmaster.ID(), nil, jm.metadata.JobStore(), jm.metadata.UnitStateStore(), jm, messageAgent, mockCheckpointAgent, jm.Logger(),
		resModel.ResourceTypeLocalFile)
	funcBackup := master.CheckAndAdjustSourceConfigFunc
	master.CheckAndAdjustSourceConfigFunc = func(ctx context.Context, cfg *dmconfig.SourceConfig) error { return nil }
	defer func() {
		master.CheckAndAdjustSourceConfigFunc = funcBackup
	}()

	precheckError := errors.New("precheck error")
	checker.CheckSyncConfigFunc = func(_ context.Context, _ []*dmconfig.SubTaskConfig, _, _ int64) (string, error) {
		return "", precheckError
	}
	require.NoError(t, jobCfg.DecodeFile(jobTemplatePath))
	verDB := conn.InitVersionDB()
	verDB.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnError(errors.New("database error"))
	require.EqualError(t, jm.UpdateJobCfg(context.Background(), jobCfg), "database error")

	verDB = conn.InitVersionDB()
	verDB.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v6.1.0"))
	checker.CheckSyncConfigFunc = func(_ context.Context, _ []*dmconfig.SubTaskConfig, _, _ int64) (string, error) {
		return "check pass", nil
	}
	require.EqualError(t, jm.UpdateJobCfg(context.Background(), jobCfg), "state not found")

	err := jm.taskManager.OperateTask(context.Background(), dmpkg.Create, jobCfg, nil)
	require.NoError(t, err)
	verDB = conn.InitVersionDB()
	verDB.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v6.1.0"))
	checker.CheckSyncConfigFunc = func(_ context.Context, _ []*dmconfig.SubTaskConfig, _, _ int64) (string, error) {
		return "check pass", nil
	}
}

func TestBinlog(t *testing.T) {
	kvClient := kvmock.NewMetaMock()
	messageAgent := &dmpkg.MockMessageAgent{}
	jm := &JobMaster{
		metadata:     metadata.NewMetaData(kvClient, log.L()),
		messageAgent: messageAgent,
	}
	resp, err := jm.Binlog(context.Background(), &dmpkg.BinlogRequest{})
	require.EqualError(t, err, "state not found")
	require.Nil(t, resp)

	messageAgent.On("SendRequest", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&dmpkg.CommonTaskResponse{Msg: "msg"}, nil).Once()
	messageAgent.On("SendRequest", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error")).Once()
	job := metadata.NewJob(&config.JobCfg{Upstreams: []*config.UpstreamCfg{
		{MySQLInstance: dmconfig.MySQLInstance{SourceID: "task1"}},
		{MySQLInstance: dmconfig.MySQLInstance{SourceID: "task2"}},
	}})
	jm.metadata.JobStore().Put(context.Background(), job)
	resp, err = jm.Binlog(context.Background(), &dmpkg.BinlogRequest{})
	require.Nil(t, err)
	require.Equal(t, "", resp.ErrorMsg)
	errMsg := resp.Results["task1"].ErrorMsg + resp.Results["task2"].ErrorMsg
	msg := resp.Results["task1"].Msg + resp.Results["task2"].Msg
	require.Equal(t, "error", errMsg)
	require.Equal(t, "msg", msg)
}

func TestBinlogSchema(t *testing.T) {
	messageAgent := &dmpkg.MockMessageAgent{}
	jm := &JobMaster{
		messageAgent: messageAgent,
	}
	resp := jm.BinlogSchema(context.Background(), &dmpkg.BinlogSchemaRequest{})
	require.Equal(t, "must specify at least one source", resp.ErrorMsg)

	messageAgent.On("SendRequest", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&dmpkg.CommonTaskResponse{Msg: "msg"}, nil).Once()
	messageAgent.On("SendRequest", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error")).Once()
	resp = jm.BinlogSchema(context.Background(), &dmpkg.BinlogSchemaRequest{Sources: []string{"task1", "task2"}})
	require.Equal(t, "", resp.ErrorMsg)
	errMsg := resp.Results["task1"].ErrorMsg + resp.Results["task2"].ErrorMsg
	msg := resp.Results["task1"].Msg + resp.Results["task2"].Msg
	require.Equal(t, "error", errMsg)
	require.Equal(t, "msg", msg)
}

func sortString(w string) string {
	s := strings.Split(w, "")
	sort.Strings(s)
	return strings.Join(s, "")
}
