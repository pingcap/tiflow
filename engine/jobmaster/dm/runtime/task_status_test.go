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

package runtime

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/lib"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
)

func TestTaskStatus(t *testing.T) {
	t.Parallel()

	offlineStatus := NewOfflineStatus("task_status_test")
	require.Equal(t, offlineStatus.GetUnit(), libModel.WorkerType(0))
	require.Equal(t, offlineStatus.GetTask(), "task_status_test")
	require.Equal(t, offlineStatus.GetStage(), metadata.StageUnscheduled)
	bytes, err := MarshalTaskStatus(offlineStatus)
	require.NoError(t, err)
	newOfflineStatus, err := UnmarshalTaskStatus(bytes)
	require.EqualError(t, err, "unknown unit: 0")
	require.Nil(t, newOfflineStatus)

	dumpStatus := &DumpStatus{
		DefaultTaskStatus: DefaultTaskStatus{
			Unit:  lib.WorkerDMDump,
			Task:  "dump-task",
			Stage: metadata.StageRunning,
		},
		DumpStatus: &pb.DumpStatus{
			TotalTables:       10,
			CompletedTables:   1,
			FinishedBytes:     2.0,
			FinishedRows:      3.0,
			EstimateTotalRows: 100.0,
		},
	}
	bytes, err = MarshalTaskStatus(dumpStatus)
	require.Nil(t, err)
	newDumpStatus, err := UnmarshalTaskStatus(bytes)
	require.Nil(t, err)
	require.Equal(t, newDumpStatus, dumpStatus)

	loadStatus := &LoadStatus{
		DefaultTaskStatus: DefaultTaskStatus{
			Unit:  lib.WorkerDMLoad,
			Task:  "load-task",
			Stage: metadata.StageFinished,
		},
		LoadStatus: &pb.LoadStatus{
			FinishedBytes:  4,
			TotalBytes:     100,
			Progress:       "4%",
			MetaBinlog:     "mysql-bin.000002, 8",
			MetaBinlogGTID: "1-2-3",
		},
	}
	bytes, err = MarshalTaskStatus(loadStatus)
	require.Nil(t, err)
	newLoadStatus, err := UnmarshalTaskStatus(bytes)
	require.Nil(t, err)
	require.Equal(t, newLoadStatus, loadStatus)

	syncStatus := &SyncStatus{
		DefaultTaskStatus: DefaultTaskStatus{
			Unit:  lib.WorkerDMSync,
			Task:  "sync-task",
			Stage: metadata.StagePaused,
		},
		SyncStatus: &pb.SyncStatus{
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
		},
	}
	bytes, err = MarshalTaskStatus(syncStatus)
	require.Nil(t, err)
	newSyncStatus, err := UnmarshalTaskStatus(bytes)
	require.Nil(t, err)
	require.Equal(t, newSyncStatus, syncStatus)
}
