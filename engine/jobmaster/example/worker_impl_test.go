package example

import (
	"context"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/lib"
	libModel "github.com/hanfei1991/microcosm/lib/model"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
)

func newExampleWorker() *exampleWorker {
	self := &exampleWorker{}
	self.BaseWorker = lib.MockBaseWorker(workerID, masterID, self)
	return self
}

func TestExampleWorker(t *testing.T) {
	t.Parallel()

	initLogger.Do(func() {
		_ = log.InitLogger(&log.Config{
			Level: "debug",
		})
	})

	worker := newExampleWorker()
	ctx := context.Background()
	err := worker.Init(ctx)
	require.NoError(t, err)

	// tick twice
	err = worker.Tick(ctx)
	require.NoError(t, err)
	err = worker.Tick(ctx)
	require.NoError(t, err)

	broker := worker.BaseWorker.(*lib.BaseWorkerForTesting).Broker
	broker.AssertPersisted(t, "/local/example")
	broker.AssertFileExists(t, workerID, "/local/example", "1.txt")
	broker.AssertFileExists(t, workerID, "/local/example", "2.txt")

	time.Sleep(time.Second)
	require.Eventually(t, func() bool {
		return worker.Status().Code == libModel.WorkerStatusFinished
	}, time.Second, time.Millisecond*100)

	resp, err := worker.BaseWorker.MetaKVClient().Get(ctx, tickKey)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, "2", string(resp.Kvs[0].Value))

	lib.MockBaseWorkerCheckSendMessage(t, worker.BaseWorker.(*lib.BaseWorkerForTesting).DefaultBaseWorker, testTopic, testMsg)
	err = worker.Close(ctx)
	require.NoError(t, err)
}
