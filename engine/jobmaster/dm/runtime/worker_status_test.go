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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/engine/lib"
)

func TestWorkerStatus(t *testing.T) {
	t.Parallel()

	task := "worker_status_test"
	workerID := "worker-id"
	workerStatus := NewWorkerStatus(task, lib.WorkerDMDump, workerID, WorkerOffline)
	require.Equal(t, workerStatus.TaskID, task)
	require.Equal(t, workerStatus.ID, workerID)
	require.Equal(t, workerStatus.Unit, lib.WorkerDMDump)
	require.Equal(t, workerStatus.Stage, WorkerOffline)
	require.True(t, workerStatus.IsOffline())
	require.False(t, workerStatus.RunAsExpected())

	workerStatus = InitWorkerStatus(task, lib.WorkerDMLoad, workerID)
	require.Equal(t, workerStatus.Unit, lib.WorkerDMLoad)
	require.Equal(t, workerStatus.Stage, WorkerCreating)
	require.False(t, workerStatus.IsOffline())
	require.True(t, workerStatus.RunAsExpected())

	workerStatus = NewWorkerStatus(task, lib.WorkerDMSync, workerID, WorkerOnline)
	require.Equal(t, workerStatus.Unit, lib.WorkerDMSync)
	require.Equal(t, workerStatus.Stage, WorkerOnline)
	require.False(t, workerStatus.IsOffline())
	require.True(t, workerStatus.RunAsExpected())

	workerStatus = NewWorkerStatus(task, lib.WorkerDMLoad, workerID, WorkerFinished)
	require.Equal(t, workerStatus.Unit, lib.WorkerDMLoad)
	require.Equal(t, workerStatus.Stage, WorkerFinished)
	require.False(t, workerStatus.IsOffline())
	require.True(t, workerStatus.RunAsExpected())

	workerStatus = InitWorkerStatus(task, lib.WorkerDMLoad, workerID)
	require.False(t, workerStatus.CreateFailed())
	workerStatus.createdTime = time.Now().Add(-2*HeartbeatInterval - 1)
	require.True(t, workerStatus.CreateFailed())
}
