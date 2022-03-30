package servermaster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/lib"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/pb"
)

func TestJobFsmStateTrans(t *testing.T) {
	t.Parallel()

	fsm := NewJobFsm()

	id := "fsm-test-job-master-1"
	job := &lib.MasterMetaKVData{
		ID:     id,
		Config: []byte("simple config"),
	}
	worker := lib.NewTombstoneWorkerHandle(id, libModel.WorkerStatus{Code: libModel.WorkerStatusNormal}, nil)

	// create new job, enter into WaitAckack job queue
	fsm.JobDispatched(job, false)
	require.Equal(t, 1, fsm.JobCount(pb.QueryJobResponse_dispatched))

	// OnWorkerOnline, WaitAck -> Online
	err := fsm.JobOnline(worker)
	require.Nil(t, err)
	require.Equal(t, 0, fsm.JobCount(pb.QueryJobResponse_dispatched))
	require.Equal(t, 1, fsm.JobCount(pb.QueryJobResponse_online))

	// OnWorkerOffline, Online -> Pending
	fsm.JobOffline(worker, true /* needFailover */)
	require.Equal(t, 0, fsm.JobCount(pb.QueryJobResponse_online))
	require.Equal(t, 1, fsm.JobCount(pb.QueryJobResponse_pending))

	// Tick, process pending jobs, Pending -> WaitAck
	dispatchedJobs := make([]*lib.MasterMetaKVData, 0)
	err = fsm.IterPendingJobs(func(job *lib.MasterMetaKVData) (string, error) {
		dispatchedJobs = append(dispatchedJobs, job)
		return id, nil
	})
	require.Nil(t, err)
	require.Equal(t, 0, fsm.JobCount(pb.QueryJobResponse_pending))
	require.Equal(t, 1, fsm.JobCount(pb.QueryJobResponse_dispatched))

	// Dispatch job meets error, WaitAck -> Pending
	err = fsm.JobDispatchFailed(worker)
	require.Nil(t, err)
	require.Equal(t, 1, fsm.JobCount(pb.QueryJobResponse_pending))
	require.Equal(t, 0, fsm.JobCount(pb.QueryJobResponse_dispatched))

	// Tick, Pending -> WaitAck
	err = fsm.IterPendingJobs(func(job *lib.MasterMetaKVData) (string, error) {
		return id, nil
	})
	require.Nil(t, err)
	require.Equal(t, 1, fsm.JobCount(pb.QueryJobResponse_dispatched))
	// job finished
	fsm.JobOffline(worker, false /*needFailover*/)
	require.Equal(t, 0, fsm.JobCount(pb.QueryJobResponse_dispatched))

	// offline invalid job, will do nothing
	invalidWorker := lib.NewTombstoneWorkerHandle(id+"invalid", libModel.WorkerStatus{}, nil)
	fsm.JobOffline(invalidWorker, true)
}
