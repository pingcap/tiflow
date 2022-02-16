package servermaster

import (
	"testing"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/stretchr/testify/require"
)

func TestJobFsmStateTrans(t *testing.T) {
	t.Parallel()

	fsm := NewJobFsm()

	id := "fsm-test-job-master-1"
	job := &lib.MasterMetaExt{
		ID:     id,
		Config: []byte("simple config"),
	}
	worker := lib.NewTombstoneWorkerHandle(id, lib.WorkerStatus{Code: lib.WorkerStatusNormal})

	// create new job, enter into WaitAckack job queue
	fsm.JobDispatched(job)
	require.Equal(t, 1, fsm.WaitAckJobCount())

	// OnWorkerOnline, WaitAck -> Online
	err := fsm.JobOnline(worker)
	require.Nil(t, err)
	require.Equal(t, 0, fsm.WaitAckJobCount())
	require.Equal(t, 1, fsm.OnlineJobCount())

	// OnWorkerOffline, Online -> Pending
	fsm.JobOffline(worker)
	require.Equal(t, 0, fsm.OnlineJobCount())
	require.Equal(t, 1, fsm.PendingJobCount())

	// Tick, process pending jobs, Pending -> WaitAck
	dispatchedJobs := make([]*lib.MasterMetaExt, 0)
	err = fsm.IterPendingJobs(func(job *lib.MasterMetaExt) (string, error) {
		dispatchedJobs = append(dispatchedJobs, job)
		return id, nil
	})
	require.Nil(t, err)
	require.Equal(t, 0, fsm.PendingJobCount())
	require.Equal(t, 1, fsm.WaitAckJobCount())

	// Dispatch job meets error, WaitAck -> Pending
	err = fsm.JobDispatchFailed(worker)
	require.Nil(t, err)
	require.Equal(t, 1, fsm.PendingJobCount())
	require.Equal(t, 0, fsm.WaitAckJobCount())
}
