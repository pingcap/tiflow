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
	"encoding/json"
	"testing"
	"time"

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/stretchr/testify/require"
)

func TestWorkerStatus(t *testing.T) {
	t.Parallel()

	task := "worker_status_test"
	workerID := "worker-id"
	workerStatus := NewWorkerStatus(task, frameModel.WorkerDMDump, workerID, WorkerOffline, 1)
	require.Equal(t, workerStatus.TaskID, task)
	require.Equal(t, workerStatus.ID, workerID)
	require.Equal(t, workerStatus.Unit, frameModel.WorkerDMDump)
	require.Equal(t, workerStatus.Stage, WorkerOffline)
	require.Equal(t, workerStatus.CfgModRevision, uint64(1))
	require.True(t, workerStatus.IsOffline())
	require.True(t, workerStatus.IsTombStone())
	require.False(t, workerStatus.RunAsExpected())

	workerStatus = InitWorkerStatus(task, frameModel.WorkerDMLoad, workerID)
	require.Equal(t, workerStatus.Unit, frameModel.WorkerDMLoad)
	require.Equal(t, workerStatus.Stage, WorkerCreating)
	require.False(t, workerStatus.IsOffline())
	require.False(t, workerStatus.IsTombStone())
	require.True(t, workerStatus.RunAsExpected())

	workerStatus = NewWorkerStatus(task, frameModel.WorkerDMSync, workerID, WorkerOnline, 0)
	require.Equal(t, workerStatus.Unit, frameModel.WorkerDMSync)
	require.Equal(t, workerStatus.Stage, WorkerOnline)
	require.False(t, workerStatus.IsOffline())
	require.False(t, workerStatus.IsTombStone())
	require.True(t, workerStatus.RunAsExpected())

	workerStatus = NewWorkerStatus(task, frameModel.WorkerDMLoad, workerID, WorkerFinished, 0)
	require.Equal(t, workerStatus.Unit, frameModel.WorkerDMLoad)
	require.Equal(t, workerStatus.Stage, WorkerFinished)
	require.False(t, workerStatus.IsOffline())
	require.True(t, workerStatus.IsTombStone())
	require.True(t, workerStatus.RunAsExpected())

	workerStatus = InitWorkerStatus(task, frameModel.WorkerDMLoad, workerID)
	require.False(t, workerStatus.CreateFailed())
	require.False(t, workerStatus.IsTombStone())
	workerStatus.createdTime = time.Now().Add(-2*HeartbeatInterval - 1)
	require.True(t, workerStatus.CreateFailed())
	require.True(t, workerStatus.IsTombStone())

	for i, s := range typesStringify {
		ws, ok := toWorkerStage[s]
		require.True(t, ok)
		bs, err := json.Marshal(ws)
		require.NoError(t, err)
		var ws2 WorkerStage
		require.NoError(t, json.Unmarshal(bs, &ws2))
		require.Equal(t, ws, ws2)
		require.Equal(t, ws, WorkerStage(i))
	}

	ws := WorkerStage(-1)
	require.Equal(t, "Unknown WorkerStage -1", ws.String())
	ws = WorkerStage(1000)
	require.Equal(t, "Unknown WorkerStage 1000", ws.String())
	bs, err := json.Marshal(ws)
	require.NoError(t, err)
	var ws2 WorkerStage
	require.EqualError(t, json.Unmarshal(bs, &ws2), "Unknown WorkerStage Unknown WorkerStage 1000")
	require.Equal(t, WorkerStage(0), ws2)
}
