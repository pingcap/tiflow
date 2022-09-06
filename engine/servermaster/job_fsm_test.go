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

	"github.com/pingcap/tiflow/engine/framework"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/stretchr/testify/require"
)

func TestJobFsmStateTrans(t *testing.T) {
	t.Parallel()

	fsm := NewJobFsm()

	id := "fsm-test-job-master-1"
	job := &frameModel.MasterMeta{
		ID:     id,
		Config: []byte("simple config"),
	}

	createWorkerCount := 0

	// Failover, job fsm loads tombstone job master
	fsm.JobDispatched(job, true)
	err := fsm.IterWaitAckJobs(func(job *frameModel.MasterMeta) (string, error) {
		createWorkerCount++
		return id, nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, createWorkerCount)
	require.Len(t, fsm.waitAckJobs, 1)

	// job that is not added from failover won't be processed
	err = fsm.IterWaitAckJobs(func(job *frameModel.MasterMeta) (string, error) {
		createWorkerCount++
		return id, nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, createWorkerCount)

	// OnWorkerOnline, WaitAck -> Online
	err = fsm.JobOnline(&framework.MockHandle{
		WorkerID:     id,
		WorkerStatus: &frameModel.WorkerStatus{State: frameModel.WorkerStateNormal},
		ExecutorID:   "executor-1",
	})
	require.NoError(t, err)
	require.Empty(t, fsm.waitAckJobs)
	require.Len(t, fsm.onlineJobs, 1)

	// OnWorkerOffline, Online -> Pending
	fsm.JobOffline(&framework.MockHandle{
		WorkerID:     id,
		WorkerStatus: &frameModel.WorkerStatus{State: frameModel.WorkerStateNormal},
		IsTombstone:  true,
	}, true /* needFailover */)
	require.Empty(t, fsm.onlineJobs)
	require.Len(t, fsm.pendingJobs, 1)

	// Tick, process pending jobs, Pending -> WaitAck
	dispatchedJobs := make([]*frameModel.MasterMeta, 0)
	err = fsm.IterPendingJobs(func(job *frameModel.MasterMeta) (string, error) {
		dispatchedJobs = append(dispatchedJobs, job)
		return id, nil
	})
	require.NoError(t, err)
	require.Empty(t, fsm.pendingJobs)
	require.Len(t, fsm.waitAckJobs, 1)

	// Dispatch job meets error, WaitAck -> Pending
	err = fsm.JobDispatchFailed(&framework.MockHandle{
		WorkerID:     id,
		WorkerStatus: &frameModel.WorkerStatus{State: frameModel.WorkerStateNormal},
		IsTombstone:  true,
	})
	require.NoError(t, err)
	require.Len(t, fsm.pendingJobs, 1)
	require.Empty(t, fsm.waitAckJobs)

	// Tick, Pending -> WaitAck
	err = fsm.IterPendingJobs(func(job *frameModel.MasterMeta) (string, error) {
		return id, nil
	})
	require.NoError(t, err)
	require.Len(t, fsm.waitAckJobs, 1)
	// job finished
	fsm.JobOffline(&framework.MockHandle{
		WorkerID:     id,
		WorkerStatus: &frameModel.WorkerStatus{State: frameModel.WorkerStateNormal},
		IsTombstone:  true,
	}, false /*needFailover*/)
	require.Empty(t, fsm.waitAckJobs)

	// offline invalid job, will do nothing
	invalidWorker := &framework.MockHandle{
		WorkerID:     id + "invalid",
		WorkerStatus: &frameModel.WorkerStatus{State: frameModel.WorkerStateNormal},
		ExecutorID:   "executor-1",
	}

	fsm.JobOffline(invalidWorker, true)
}
