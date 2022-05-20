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
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

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
		dumpStatus = &runtime.DumpStatus{
			DefaultTaskStatus: runtime.DefaultTaskStatus{
				Unit:  lib.WorkerDMDump,
				Task:  "task5",
				Stage: metadata.StageRunning,
			},
			TotalTables:       10,
			CompletedTables:   1,
			FinishedBytes:     100,
			FinishedRows:      10,
			EstimateTotalRows: 1000,
		}
		loadStatus = &runtime.LoadStatus{
			DefaultTaskStatus: runtime.DefaultTaskStatus{
				Unit:  lib.WorkerDMLoad,
				Task:  "task3",
				Stage: metadata.StageFinished,
			},
			FinishedBytes:  100,
			TotalBytes:     100,
			Progress:       "100%",
			MetaBinlog:     "mysql-bin.000002,154",
			MetaBinlogGTID: "0-1-2",
		}
		syncStatus = &runtime.SyncStatus{
			DefaultTaskStatus: runtime.DefaultTaskStatus{
				Unit:  lib.WorkerDMSync,
				Task:  "task6",
				Stage: metadata.StagePaused,
			},
			TotalEvents:         100,
			TotalTps:            10,
			RecentTps:           5,
			MasterBinlog:        "mysql-bin.000004,1000",
			MasterBinlogGtid:    "0-1-100",
			SyncerBinlog:        "mysql-bin.000004,4",
			SyncerBinlogGtid:    "0-1-10",
			Synced:              false,
			BinlogType:          "remote",
			SecondsBehindMaster: 2,
		}
	)
	jm.taskManager = NewTaskManager(nil, jm.metadata.JobStore(), nil)
	jm.workerManager = NewWorkerManager(nil, jm.metadata.JobStore(), nil, nil)
	jm.messageAgent = NewMessageAgent(nil, jm.ID(), jm)
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus("task2", lib.WorkerDMLoad, "worker2", runtime.WorkerFinished))
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus("task3", lib.WorkerDMLoad, "worker3", runtime.WorkerFinished))
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus("task4", lib.WorkerDMDump, "worker4", runtime.WorkerOnline))
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus("task5", lib.WorkerDMDump, "worker5", runtime.WorkerOnline))
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus("task6", lib.WorkerDMSync, "worker6", runtime.WorkerOnline))
	jm.messageAgent.UpdateWorkerHandle("task4", &MockSender{})
	jm.messageAgent.UpdateWorkerHandle("task5", &MockSender{})
	jm.messageAgent.UpdateWorkerHandle("task6", &MockSender{})
	jm.taskManager.UpdateTaskStatus(loadStatus)

	// no job
	jobStatus, err := jm.QueryStatus(context.Background(), nil)
	require.Error(t, err)
	require.Nil(t, jobStatus)

	require.NoError(t, jm.metadata.JobStore().Put(context.Background(), job))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		jobStatus, err = jm.QueryStatus(ctx, nil)
		require.NoError(t, err)
	}()
	// receive query status response
	time.Sleep(50 * time.Millisecond)

	queryStatus1 := dmpkg.QueryStatusResponse{TaskStatus: dumpStatus}
	queryStatus2 := dmpkg.QueryStatusResponse{TaskStatus: syncStatus}
	jm.messageAgent.OnWorkerMessage(dmpkg.MessageWithID{ID: 2, Message: queryStatus1})
	jm.messageAgent.OnWorkerMessage(dmpkg.MessageWithID{ID: 3, Message: queryStatus2})
	wg.Wait()

	expectedStatus := `{
	"JobMasterID": "dm-jobmaster-id",
	"WorkerID": "jobmaster-worker-id",
	"TaskStatus": {
		"task1": {
			"ExpectedStage": 2,
			"WorkerID": "",
			"Status": {
				"ErrorMsg": "worker for task task1 not found",
				"TaskStatus": null
			}
		},
		"task2": {
			"ExpectedStage": 3,
			"WorkerID": "worker2",
			"Status": {
				"ErrorMsg": "finished task status for task task2 not found",
				"TaskStatus": null
			}
		},
		"task3": {
			"ExpectedStage": 3,
			"WorkerID": "worker3",
			"Status": {
				"ErrorMsg": "",
				"TaskStatus": {
					"Unit": 11,
					"Task": "task3",
					"Stage": 3,
					"FinishedBytes": 100,
					"TotalBytes": 100,
					"Progress": "100%",
					"MetaBinlog": "mysql-bin.000002,154",
					"MetaBinlogGTID": "0-1-2"
				}
			}
		},
		"task4": {
			"ExpectedStage": 1,
			"WorkerID": "worker4",
			"Status": {
				"ErrorMsg": "context deadline exceeded",
				"TaskStatus": null
			}
		},
		"task5": {
			"ExpectedStage": 1,
			"WorkerID": "worker5",
			"Status": {
				"ErrorMsg": "",
				"TaskStatus": {
					"Unit": 10,
					"Task": "task5",
					"Stage": 1,
					"TotalTables": 10,
					"CompletedTables": 1,
					"FinishedBytes": 100,
					"FinishedRows": 10,
					"EstimateTotalRows": 1000
				}
			}
		},
		"task6": {
			"ExpectedStage": 1,
			"WorkerID": "worker6",
			"Status": {
				"ErrorMsg": "",
				"TaskStatus": {
					"Unit": 12,
					"Task": "task6",
					"Stage": 2,
					"TotalEvents": 100,
					"TotalTps": 10,
					"RecentTps": 5,
					"MasterBinlog": "mysql-bin.000004,1000",
					"MasterBinlogGtid": "0-1-100",
					"SyncerBinlog": "mysql-bin.000004,4",
					"SyncerBinlogGtid": "0-1-10",
					"BlockingDDLs": null,
					"Synced": false,
					"BinlogType": "remote",
					"SecondsBehindMaster": 2,
					"BlockDDLOwner": "",
					"ConflictMsg": ""
				}
			}
		}
	}
}`
	status, err := json.MarshalIndent(jobStatus, "", "\t")
	require.NoError(t, err)
	require.Equal(t, sortString(expectedStatus), sortString(string(status)))
}

func sortString(w string) string {
	s := strings.Split(w, "")
	sort.Strings(s)
	return strings.Join(s, "")
}
