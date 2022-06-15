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

package servermaster

import (
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/framework/master"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
)

func TestJobFsmStateTrans(t *testing.T) {
	t.Parallel()

	fsm := NewJobFsm()

	id := "fsm-test-job-master-1"
	job := &frameModel.MasterMetaKVData{
		ID:     id,
		Config: []byte("simple config"),
	}

	createWorkerCount := 0

	// Failover, job fsm loads tombstone job master
	fsm.JobDispatched(job, true)
	err := fsm.IterWaitAckJobs(func(job *frameModel.MasterMetaKVData) (string, error) {
		createWorkerCount++
		return id, nil
	})
	require.Nil(t, err)
	require.Equal(t, 1, createWorkerCount)
	require.Equal(t, 1, fsm.JobCount(pb.QueryJobResponse_dispatched))

	// job that is not added from failover won't be processed
	err = fsm.IterWaitAckJobs(func(job *frameModel.MasterMetaKVData) (string, error) {
		createWorkerCount++
		return id, nil
	})
	require.Nil(t, err)
	require.Equal(t, 1, createWorkerCount)

	// OnWorkerOnline, WaitAck -> Online
	err = fsm.JobOnline(&master.MockHandle{
		WorkerID:     id,
		WorkerStatus: &frameModel.WorkerStatus{Code: frameModel.WorkerStatusNormal},
		ExecutorID:   "executor-1",
	})
	require.Nil(t, err)
	require.Equal(t, 0, fsm.JobCount(pb.QueryJobResponse_dispatched))
	require.Equal(t, 1, fsm.JobCount(pb.QueryJobResponse_online))

	// OnWorkerOffline, Online -> Pending
	fsm.JobOffline(&master.MockHandle{
		WorkerID:     id,
		WorkerStatus: &frameModel.WorkerStatus{Code: frameModel.WorkerStatusNormal},
		IsTombstone:  true,
	}, true /* needFailover */)
	require.Equal(t, 0, fsm.JobCount(pb.QueryJobResponse_online))
	require.Equal(t, 1, fsm.JobCount(pb.QueryJobResponse_pending))

	// Tick, process pending jobs, Pending -> WaitAck
	dispatchedJobs := make([]*frameModel.MasterMetaKVData, 0)
	err = fsm.IterPendingJobs(func(job *frameModel.MasterMetaKVData) (string, error) {
		dispatchedJobs = append(dispatchedJobs, job)
		return id, nil
	})
	require.Nil(t, err)
	require.Equal(t, 0, fsm.JobCount(pb.QueryJobResponse_pending))
	require.Equal(t, 1, fsm.JobCount(pb.QueryJobResponse_dispatched))

	// Dispatch job meets error, WaitAck -> Pending
	err = fsm.JobDispatchFailed(&master.MockHandle{
		WorkerID:     id,
		WorkerStatus: &frameModel.WorkerStatus{Code: frameModel.WorkerStatusNormal},
		IsTombstone:  true,
	})
	require.Nil(t, err)
	require.Equal(t, 1, fsm.JobCount(pb.QueryJobResponse_pending))
	require.Equal(t, 0, fsm.JobCount(pb.QueryJobResponse_dispatched))

	// Tick, Pending -> WaitAck
	err = fsm.IterPendingJobs(func(job *frameModel.MasterMetaKVData) (string, error) {
		return id, nil
	})
	require.Nil(t, err)
	require.Equal(t, 1, fsm.JobCount(pb.QueryJobResponse_dispatched))
	// job finished
	fsm.JobOffline(&master.MockHandle{
		WorkerID:     id,
		WorkerStatus: &frameModel.WorkerStatus{Code: frameModel.WorkerStatusNormal},
		IsTombstone:  true,
	}, false /*needFailover*/)
	require.Equal(t, 0, fsm.JobCount(pb.QueryJobResponse_dispatched))

	// offline invalid job, will do nothing
	invalidWorker := &master.MockHandle{
		WorkerID:     id + "invalid",
		WorkerStatus: &frameModel.WorkerStatus{Code: frameModel.WorkerStatusNormal},
		ExecutorID:   "executor-1",
	}

	fsm.JobOffline(invalidWorker, true)
}
