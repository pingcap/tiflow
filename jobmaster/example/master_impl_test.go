package example

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/lib"
)

const (
	masterID = "master"

	executorNodeID = "node-exec"

	workerID = "worker"
)

var initLogger sync.Once

func newExampleMaster() *exampleMaster {
	self := &exampleMaster{}
	self.DefaultBaseMaster = lib.MockBaseMaster(masterID, self)
	return self
}

func TestExampleMaster(t *testing.T) {
	t.Parallel()

	initLogger.Do(func() {
		_ = log.InitLogger(&log.Config{
			Level: "debug",
		})
	})

	master := newExampleMaster()
	// master.Init will call CreateWorker, so we mock it first
	lib.MockBaseMasterCreateWorker(
		t,
		master.DefaultBaseMaster,
		exampleWorkerType,
		exampleWorkerCfg,
		exampleWorkerCost,
		masterID,
		workerID,
		executorNodeID,
	)

	ctx := context.Background()
	err := master.Init(ctx)
	require.NoError(t, err)

	// master.Init will asynchronously create a worker
	require.Eventually(t, func() bool {
		master.worker.mu.Lock()
		require.NoError(t, master.worker.receivedErr)
		handle := master.worker.handle
		master.worker.mu.Unlock()
		return handle != nil
	}, time.Second, 100*time.Millisecond)

	// GetWorkers and master.CreateWorker should be consistent
	handle, ok := master.GetWorkers()[master.worker.id]
	require.True(t, ok)
	require.Equal(t, master.worker.handle, handle)

	// before worker's first heartbeat, its status is WorkerStatusCreated
	err = master.Tick(ctx)
	require.NoError(t, err)
	master.worker.mu.Lock()
	code := master.worker.statusCode
	master.worker.mu.Unlock()
	require.Equal(t, lib.WorkerStatusCreated, code)

	lib.MockBaseMasterWorkerHeartbeat(t, master.DefaultBaseMaster, masterID, workerID, executorNodeID)

	// worker is online after one heartbeat
	require.Eventually(t, func() bool {
		master.worker.mu.Lock()
		online := master.worker.online
		master.worker.mu.Unlock()
		return online
	}, 2*time.Second, 100*time.Millisecond)

	lib.MockBaseMasterWorkerUpdateStatus(ctx, t, master.DefaultBaseMaster, masterID, workerID, executorNodeID, &lib.WorkerStatus{
		Code: lib.WorkerStatusInit,
	})

	require.Eventually(t, func() bool {
		err := master.Poll(ctx)
		require.NoError(t, err)

		master.worker.mu.Lock()
		code = master.worker.statusCode
		master.worker.mu.Unlock()

		return code == lib.WorkerStatusInit
	}, time.Second, time.Millisecond*10)

	err = master.Close(ctx)
	require.NoError(t, err)
}
