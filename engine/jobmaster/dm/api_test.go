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

	"github.com/pingcap/errors"
	dmconfig "github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/framework"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
	kvmock "github.com/pingcap/tiflow/engine/pkg/meta/kvclient/mock"
	"github.com/stretchr/testify/require"
)

func TestQueryStatusAPI(t *testing.T) {
	var (
		metaKVClient      = kvmock.NewMetaMock()
		mockBaseJobmaster = &MockBaseJobmaster{}
		jm                = &JobMaster{
			workerID:      "jobmaster-worker-id",
			BaseJobMaster: mockBaseJobmaster,
			metadata:      metadata.NewMetaData(mockBaseJobmaster.JobMasterID(), metaKVClient),
		}
		job = &metadata.Job{
			Tasks: map[string]*metadata.Task{
				"task1": {Stage: metadata.StagePaused},
				"task2": {Stage: metadata.StageFinished},
				"task3": {Stage: metadata.StageFinished},
				"task4": {Stage: metadata.StageRunning},
				"task5": {Stage: metadata.StageRunning},
				"task6": {Stage: metadata.StageRunning},
			},
		}
		finishedTaskStatus = runtime.TaskStatus{
			Unit:  framework.WorkerDMLoad,
			Task:  "task2",
			Stage: metadata.StageFinished,
		}
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
			BlockingDDLs:        []string{"ALTER TABLE db.tb ADD COLUMN a INT"},
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
		dumpStatusResp     = &dmpkg.QueryStatusResponse{Unit: framework.WorkerDMDump, Stage: metadata.StageRunning, Status: dumpStatusBytes}
		loadStatusResp     = &dmpkg.QueryStatusResponse{Unit: framework.WorkerDMLoad, Stage: metadata.StagePaused, Result: &pb.ProcessResult{IsCanceled: true}, Status: loadStatusBytes}
		syncStatusResp     = &dmpkg.QueryStatusResponse{Unit: framework.WorkerDMSync, Stage: metadata.StageError, Result: &pb.ProcessResult{Errors: []*pb.ProcessError{processError}}, Status: syncStatusBytes}
	)
	messageAgent := &dmpkg.MockMessageAgent{}
	jm.messageAgent = messageAgent
	jm.workerManager = NewWorkerManager(nil, jm.metadata.JobStore(), nil, nil, nil)
	jm.taskManager = NewTaskManager(nil, nil, nil)
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus("task2", framework.WorkerDMLoad, "worker2", runtime.WorkerFinished))
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus("task3", framework.WorkerDMDump, "worker3", runtime.WorkerOnline))
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus("task4", framework.WorkerDMDump, "worker4", runtime.WorkerOnline))
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus("task5", framework.WorkerDMLoad, "worker5", runtime.WorkerOnline))
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus("task6", framework.WorkerDMSync, "worker6", runtime.WorkerOnline))
	jm.taskManager.UpdateTaskStatus(finishedTaskStatus)

	// no job
	jobStatus, err := jm.QueryJobStatus(context.Background(), nil)
	require.Error(t, err)
	require.Nil(t, jobStatus)

	require.NoError(t, jm.metadata.JobStore().Put(context.Background(), job))

	messageAgent.On("SendRequest").Return(nil, context.DeadlineExceeded).Once()
	messageAgent.On("SendRequest").Return(dumpStatusResp, nil).Once()
	messageAgent.On("SendRequest").Return(loadStatusResp, nil).Once()
	messageAgent.On("SendRequest").Return(syncStatusResp, nil).Once()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	jobStatus, err = jm.QueryJobStatus(ctx, []string{"task7"})
	require.NoError(t, err)
	taskStatus := jobStatus.TaskStatus["task7"]
	require.Equal(t, "", taskStatus.WorkerID)
	require.Equal(t, 0, int(taskStatus.ExpectedStage))
	require.Equal(t, &dmpkg.QueryStatusResponse{ErrorMsg: "task task7 for job not found"}, taskStatus.Status)

	jobStatus, err = jm.QueryJobStatus(ctx, nil)
	require.NoError(t, err)

	expectedStatus := `{
	"JobMasterID": "dm-jobmaster-id",
	"WorkerID": "jobmaster-worker-id",
	"TaskStatus": {
		"task1": {
			"ExpectedStage": 3,
			"WorkerID": "",
			"Status": {
				"ErrorMsg": "worker for task task1 not found",
				"Unit": 0,
				"Stage": 0,
				"Result": null,
				"Status": null
			}
		},
		"task2": {
			"ExpectedStage": 4,
			"WorkerID": "worker2",
			"Status": {
				"ErrorMsg": "",
				"Unit": 11,
				"Stage": 4,
				"Result": null,
				"Status": null
			}
		},
		"task3": {
			"ExpectedStage": 4,
			"WorkerID": "worker3",
			"Status": {
				"ErrorMsg": "context deadline exceeded",
				"Unit": 0,
				"Stage": 0,
				"Result": null,
				"Status": null
			}
		},
		"task4": {
			"ExpectedStage": 2,
			"WorkerID": "worker4",
			"Status": {
				"ErrorMsg": "",
				"Unit": 10,
				"Stage": 2,
				"Result": null,
				"Status": {
					"totalTables": 10,
					"completedTables": 1,
					"finishedBytes": 100,
					"finishedRows": 10,
					"estimateTotalRows": 1000
				}
			}
		},
		"task5": {
			"ExpectedStage": 2,
			"WorkerID": "worker5",
			"Status": {
				"ErrorMsg": "",
				"Unit": 11,
				"Stage": 3,
				"Result": {
					"isCanceled": true
				},
				"Status": {
					"finishedBytes": 4,
					"totalBytes": 100,
					"progress": "4%",
					"metaBinlog": "mysql-bin.000002, 8",
					"metaBinlogGTID": "1-2-3"
				}
			}
		},
		"task6": {
			"ExpectedStage": 2,
			"WorkerID": "worker6",
			"Status": {
				"ErrorMsg": "",
				"Unit": 12,
				"Stage": 5,
				"Result": {
					"errors": [
						{
							"ErrCode": 1,
							"ErrClass": "class",
							"ErrScope": "scope",
							"ErrLevel": "low",
							"Message": "msg",
							"RawCause": "raw cause",
							"Workaround": "workaround"
						}
					]
				},
				"Status": {
					"totalEvents": 10,
					"totalTps": 10,
					"recentTps": 10,
					"masterBinlog": "mysql-bin.000002, 4",
					"masterBinlogGtid": "1-2-20",
					"syncerBinlog": "mysql-bin.000001, 432",
					"syncerBinlogGtid": "1-2-10",
					"blockingDDLs": [
						"ALTER TABLE db.tb ADD COLUMN a INT"
					],
					"binlogType": "remote",
					"secondsBehindMaster": 10
				}
			}
		}
	}
}`
	status, err := json.MarshalIndent(jobStatus, "", "\t")
	require.NoError(t, err)
	require.Equal(t, sortString(expectedStatus), sortString(string(status)))

	// test with DebugJob
	var args struct {
		Tasks []string
	}
	args.Tasks = []string{"task1", "task2", "task3", "task4", "task5", "task6"}
	jsonArg, err := json.Marshal(args)
	require.NoError(t, err)
	messageAgent.On("SendRequest").Return(nil, context.DeadlineExceeded).Once()
	messageAgent.On("SendRequest").Return(dumpStatusResp, nil).Once()
	messageAgent.On("SendRequest").Return(loadStatusResp, nil).Once()
	messageAgent.On("SendRequest").Return(syncStatusResp, nil).Once()
	resp2 := jm.DebugJob(ctx, &enginepb.DebugJobRequest{Command: dmpkg.QueryStatus, JsonArg: string(jsonArg)})
	var jobStatus2 JobStatus
	require.NoError(t, json.Unmarshal([]byte(resp2.JsonRet), &jobStatus2))
	status2, err := json.MarshalIndent(jobStatus, "", "\t")
	require.NoError(t, err)
	require.Equal(t, sortString(string(status)), sortString(string(status2)))
}

func TestOperateTask(t *testing.T) {
	jm := &JobMaster{
		taskManager: NewTaskManager(nil, metadata.NewJobStore("master-id", kvmock.NewMetaMock()), nil),
	}
	require.EqualError(t, jm.OperateTask(context.Background(), dmpkg.Delete, nil, nil), fmt.Sprintf("unsupport op type %d for operate task", dmpkg.Delete))
	require.EqualError(t, jm.OperateTask(context.Background(), dmpkg.Pause, nil, nil), "state not found")

	// test with DebugJob
	var args struct {
		Tasks []string
		Op    dmpkg.OperateType
	}
	args.Tasks = []string{"task1"}
	args.Op = dmpkg.Pause
	jsonArg, err := json.Marshal(args)
	require.NoError(t, err)
	resp := jm.DebugJob(context.Background(), &enginepb.DebugJobRequest{Command: dmpkg.OperateTask, JsonArg: string(jsonArg)})
	require.Equal(t, resp.Err.Message, "state not found")
}

func TestGetJobCfg(t *testing.T) {
	kvClient := kvmock.NewMetaMock()
	jm := &JobMaster{
		metadata: metadata.NewMetaData("master-id", kvClient),
	}
	jobCfg, err := jm.GetJobCfg(context.Background())
	require.EqualError(t, err, "state not found")
	require.Nil(t, jobCfg)

	jobCfg = &config.JobCfg{Name: "job-id", Upstreams: []*config.UpstreamCfg{{}}}
	job := metadata.NewJob(jobCfg)
	jm.metadata.JobStore().Put(context.Background(), job)

	jobCfg, err = jm.GetJobCfg(context.Background())
	require.NoError(t, err)
	require.Equal(t, "job-id", jobCfg.Name)

	// test with DebugJob
	resp := jm.DebugJob(context.Background(), &enginepb.DebugJobRequest{Command: dmpkg.GetJobCfg})
	require.NoError(t, err)
	require.Contains(t, resp.JsonRet, jobCfg.Name)
}

func TestBinlog(t *testing.T) {
	kvClient := kvmock.NewMetaMock()
	messageAgent := &dmpkg.MockMessageAgent{}
	jm := &JobMaster{
		metadata:     metadata.NewMetaData("master-id", kvClient),
		messageAgent: messageAgent,
	}
	resp, err := jm.Binlog(context.Background(), &dmpkg.BinlogRequest{})
	require.EqualError(t, err, "state not found")
	require.Nil(t, resp)

	messageAgent.On("SendRequest").Return(&dmpkg.CommonTaskResponse{Msg: "msg"}, nil).Once()
	messageAgent.On("SendRequest").Return(nil, errors.New("error")).Once()
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

	// test with DebugJob
	req := dmpkg.BinlogRequest{Sources: []string{"task1"}}
	jsonArg, err := json.Marshal(req)
	require.NoError(t, err)
	messageAgent.On("SendRequest").Return(&dmpkg.CommonTaskResponse{Msg: "msg"}, nil).Once()
	resp2 := jm.DebugJob(context.Background(), &enginepb.DebugJobRequest{Command: dmpkg.Binlog, JsonArg: string(jsonArg)})
	require.NoError(t, err)
	var binlogResp dmpkg.BinlogResponse
	require.NoError(t, json.Unmarshal([]byte(resp2.JsonRet), &binlogResp))
	require.Equal(t, "", binlogResp.ErrorMsg)
	require.Len(t, binlogResp.Results, 1)
	require.Equal(t, "", binlogResp.Results["task1"].ErrorMsg)
	require.Equal(t, "msg", binlogResp.Results["task1"].Msg)
}

func TestBinlogSchema(t *testing.T) {
	messageAgent := &dmpkg.MockMessageAgent{}
	jm := &JobMaster{
		messageAgent: messageAgent,
	}
	resp := jm.BinlogSchema(context.Background(), &dmpkg.BinlogSchemaRequest{})
	require.Equal(t, "must specify at least one source", resp.ErrorMsg)

	messageAgent.On("SendRequest").Return(&dmpkg.CommonTaskResponse{Msg: "msg"}, nil).Once()
	messageAgent.On("SendRequest").Return(nil, errors.New("error")).Once()
	resp = jm.BinlogSchema(context.Background(), &dmpkg.BinlogSchemaRequest{Sources: []string{"task1", "task2"}})
	require.Equal(t, "", resp.ErrorMsg)
	errMsg := resp.Results["task1"].ErrorMsg + resp.Results["task2"].ErrorMsg
	msg := resp.Results["task1"].Msg + resp.Results["task2"].Msg
	require.Equal(t, "error", errMsg)
	require.Equal(t, "msg", msg)

	// test with DebugJob
	req := dmpkg.BinlogSchemaRequest{Sources: []string{"task1"}}
	jsonArg, err := json.Marshal(req)
	require.NoError(t, err)
	messageAgent.On("SendRequest").Return(&dmpkg.CommonTaskResponse{Msg: "msg"}, nil).Once()
	resp2 := jm.DebugJob(context.Background(), &enginepb.DebugJobRequest{Command: dmpkg.BinlogSchema, JsonArg: string(jsonArg)})
	require.NoError(t, err)
	var binlogSchemaResp dmpkg.BinlogSchemaResponse
	require.NoError(t, json.Unmarshal([]byte(resp2.JsonRet), &binlogSchemaResp))
	require.Equal(t, "", binlogSchemaResp.ErrorMsg)
	require.Len(t, binlogSchemaResp.Results, 1)
	require.Equal(t, "", binlogSchemaResp.Results["task1"].ErrorMsg)
	require.Equal(t, "msg", binlogSchemaResp.Results["task1"].Msg)
}

func sortString(w string) string {
	s := strings.Split(w, "")
	sort.Strings(s)
	return strings.Join(s, "")
}
