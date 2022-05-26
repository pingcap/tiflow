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

	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	"github.com/pingcap/tiflow/engine/lib"
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
		dump = &pb.DumpStatus{
			TotalTables:       10,
			CompletedTables:   1,
			FinishedBytes:     100,
			FinishedRows:      10,
			EstimateTotalRows: 1000,
		}
		load = &pb.LoadStatus{
			FinishedBytes:  4,
			TotalBytes:     100,
			Progress:       "4%",
			MetaBinlog:     "mysql-bin.000002, 8",
			MetaBinlogGTID: "1-2-3",
		}
		sync = &pb.SyncStatus{
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
		dumpStatusBytes, _ = json.Marshal(dump)
		loadStatusBytes, _ = json.Marshal(load)
		syncStatusBytes, _ = json.Marshal(sync)
		dumpStatus         = runtime.TaskStatus{
			Unit:   lib.WorkerDMDump,
			Task:   "task5",
			Stage:  metadata.StageRunning,
			Status: dumpStatusBytes,
		}
		loadStatus = runtime.TaskStatus{
			Unit:   lib.WorkerDMLoad,
			Task:   "task3",
			Stage:  metadata.StageFinished,
			Status: loadStatusBytes,
		}
		syncStatus = runtime.TaskStatus{
			Unit:   lib.WorkerDMSync,
			Task:   "task6",
			Stage:  metadata.StagePaused,
			Status: syncStatusBytes,
		}
	)
	messageAgent := &dmpkg.MockMessageAgent{}
	jm.messageAgent = messageAgent
	jm.workerManager = NewWorkerManager(nil, jm.metadata.JobStore(), nil, nil, nil)
	jm.taskManager = NewTaskManager(nil, nil, nil)
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus("task2", lib.WorkerDMLoad, "worker2", runtime.WorkerFinished))
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus("task3", lib.WorkerDMLoad, "worker3", runtime.WorkerFinished))
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus("task4", lib.WorkerDMDump, "worker4", runtime.WorkerOnline))
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus("task5", lib.WorkerDMDump, "worker5", runtime.WorkerOnline))
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus("task6", lib.WorkerDMSync, "worker6", runtime.WorkerOnline))
	jm.taskManager.UpdateTaskStatus(loadStatus)

	// no job
	jobStatus, err := jm.QueryJobStatus(context.Background(), nil)
	require.Error(t, err)
	require.Nil(t, jobStatus)

	require.NoError(t, jm.metadata.JobStore().Put(context.Background(), job))

	messageAgent.On("SendRequest").Return(nil, context.DeadlineExceeded).Once()
	messageAgent.On("SendRequest").Return(&dmpkg.QueryStatusResponse{TaskStatus: dumpStatus}, nil).Once()
	messageAgent.On("SendRequest").Return(&dmpkg.QueryStatusResponse{TaskStatus: syncStatus}, nil).Once()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
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
				"TaskStatus": {
					"Unit": 0,
					"Task": "",
					"Stage": 0,
					"Status": null
				}
			}
		},
		"task2": {
			"ExpectedStage": 4,
			"WorkerID": "worker2",
			"Status": {
				"ErrorMsg": "finished task status for task task2 not found",
				"TaskStatus": {
					"Unit": 0,
					"Task": "",
					"Stage": 0,
					"Status": null
				}
			}
		},
		"task3": {
			"ExpectedStage": 4,
			"WorkerID": "worker3",
			"Status": {
				"ErrorMsg": "",
				"TaskStatus": {
					"Unit": 11,
					"Task": "task3",
					"Stage": 4,
					"Status": {
						"finishedBytes": 4,
						"totalBytes": 100,
						"progress": "4%",
						"metaBinlog": "mysql-bin.000002, 8",
						"metaBinlogGTID": "1-2-3"
					}
				}
			}
		},
		"task4": {
			"ExpectedStage": 2,
			"WorkerID": "worker4",
			"Status": {
				"ErrorMsg": "",
				"TaskStatus": {
					"Unit": 10,
					"Task": "task5",
					"Stage": 2,
					"Status": {
						"totalTables": 10,
						"completedTables": 1,
						"finishedBytes": 100,
						"finishedRows": 10,
						"estimateTotalRows": 1000
					}
				}
			}
		},
		"task5": {
			"ExpectedStage": 2,
			"WorkerID": "worker5",
			"Status": {
				"ErrorMsg": "",
				"TaskStatus": {
					"Unit": 12,
					"Task": "task6",
					"Stage": 3,
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
		},
		"task6": {
			"ExpectedStage": 2,
			"WorkerID": "worker6",
			"Status": {
				"ErrorMsg": "context deadline exceeded",
				"TaskStatus": {
					"Unit": 0,
					"Task": "",
					"Stage": 0,
					"Status": null
				}
			}
		}
	}
}`
	status, err := json.MarshalIndent(jobStatus, "", "\t")
	fmt.Println(string(status))
	require.NoError(t, err)
	require.Equal(t, sortString(expectedStatus), sortString(string(status)))
}

func sortString(w string) string {
	s := strings.Split(w, "")
	sort.Strings(s)
	return strings.Join(s, "")
}
