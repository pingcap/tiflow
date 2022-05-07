package example

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/lib"
	libModel "github.com/hanfei1991/microcosm/lib/model"
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
		nil,
	)

	ctx := context.Background()
	err := master.Init(ctx)
	require.NoError(t, err)

	// worker is online
	require.Eventually(t, func() bool {
		err = master.Poll(ctx)
		require.NoError(t, err)

		lib.MockBaseMasterWorkerHeartbeat(t, master.DefaultBaseMaster, masterID, workerID, executorNodeID)

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

	lib.MockBaseMasterWorkerUpdateStatus(ctx, t, master.DefaultBaseMaster, masterID, workerID, executorNodeID, &libModel.WorkerStatus{
		Code: libModel.WorkerStatusInit,
	})

	require.Eventually(t, func() bool {
		err := master.Poll(ctx)
		require.NoError(t, err)

		master.worker.mu.Lock()
		code := master.worker.statusCode
		master.worker.mu.Unlock()

		return code == libModel.WorkerStatusInit
	}, time.Second, time.Millisecond*10)

	err = master.Close(ctx)
	require.NoError(t, err)
}
