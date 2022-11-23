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

package example

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/engine/framework"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/stretchr/testify/require"
)

const (
	masterID = "master"

	executorNodeID = "node-exec"

	workerID = "worker"
)

var initLogger sync.Once

func newExampleMaster(t *testing.T) *exampleMaster {
	self := &exampleMaster{}
	self.DefaultBaseMaster = framework.MockBaseMaster(t, masterID, self)
	return self
}

func TestExampleMaster(t *testing.T) {
	t.Parallel()

	initLogger.Do(func() {
		_ = logutil.InitLogger(&logutil.Config{
			Level: "debug",
		})
	})

	master := newExampleMaster(t)
	// master.Init will call CreateWorker, so we mock it first

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	epoch, err := master.MetaKVClient().GenEpoch(ctx)
	require.NoError(t, err)

	framework.MockBaseMasterCreateWorker(
		t,
		master.DefaultBaseMaster,
		exampleWorkerType,
		exampleWorkerCfg,
		masterID,
		workerID,
		executorNodeID,
		nil,
		// call GenEpoch three times, including create master meta, master init
		// refresh meta, create worker.
		epoch+3,
	)

	err = master.Init(ctx)
	require.NoError(t, err)

	// worker is online
	require.Eventually(t, func() bool {
		err = master.Poll(ctx)
		require.NoError(t, err)

		err = framework.MockBaseMasterWorkerHeartbeat(t,
			master.DefaultBaseMaster, masterID, workerID, executorNodeID)
		if err != nil {
			t.Logf("mock worker heartbeat failed: %s", err)
			return false
		}

		master.worker.mu.Lock()
		online := master.worker.online
		require.NoError(t, master.worker.receivedErr)
		handle := master.worker.handle
		master.worker.mu.Unlock()
		return online && handle != nil
	}, 2*time.Second, 100*time.Millisecond)

	// GetWorkers and master.CreateWorker should be consistent
	handle, ok := master.GetWorkers()[master.worker.id]
	require.True(t, ok)
	require.Equal(t, master.worker.handle, handle)

	framework.MockBaseMasterWorkerUpdateStatus(ctx, t, master.DefaultBaseMaster, masterID, workerID, executorNodeID, &frameModel.WorkerStatus{
		State: frameModel.WorkerStateInit,
	})

	require.Eventually(t, func() bool {
		err := master.Poll(ctx)
		require.NoError(t, err)

		master.worker.mu.Lock()
		code := master.worker.statusCode
		master.worker.mu.Unlock()

		return code == frameModel.WorkerStateInit
	}, time.Second, time.Millisecond*10)

	err = master.Close(ctx)
	require.NoError(t, err)
}
