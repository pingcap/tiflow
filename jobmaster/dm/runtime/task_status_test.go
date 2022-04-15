package runtime

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/jobmaster/dm/metadata"
	"github.com/hanfei1991/microcosm/lib"
	libModel "github.com/hanfei1991/microcosm/lib/model"
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
		TotalTables:       10,
		CompletedTables:   1,
		FinishedBytes:     2.0,
		FinishedRows:      3.0,
		EstimateTotalRows: 100.0,
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
		FinishedBytes:  4,
		TotalBytes:     100,
		Progress:       "4%",
		MetaBinlog:     "mysql-bin.000002, 8",
		MetaBinlogGTID: "1-2-3",
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
	bytes, err = MarshalTaskStatus(syncStatus)
	require.Nil(t, err)
	newSyncStatus, err := UnmarshalTaskStatus(bytes)
	require.Nil(t, err)
	require.Equal(t, newSyncStatus, syncStatus)
}
