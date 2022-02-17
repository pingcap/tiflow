package dm

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/lib/registry"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
)

func TestLoadWorker(t *testing.T) {
	// This test requires a TiDB running on port 4000 and dump files placed in
	// /tmp/dftest.db_ut . TestDumpWorker can generate the dump files.
	t.SkipNow()
	t.Parallel()

	require.NoError(t, log.InitLogger(&log.Config{Level: "debug"}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workerWrapped, err := registry.GlobalWorkerRegistry().CreateWorker(
		dcontext.Background(), WorkerDMLoad, workerID, masterID, mockWorkerConfig())
	require.NoError(t, err)

	worker := workerWrapped.(*loadWorker)
	worker.DefaultBaseWorker = lib.MockBaseWorker(workerID, masterID, worker)

	putMasterMeta(context.Background(), t, worker.MetaKVClient(), &lib.MasterMetaKVData{
		ID:          masterID,
		NodeID:      nodeID,
		Epoch:       1,
		Initialized: true,
	})

	err = worker.Init(ctx)
	require.NoError(t, err)
	err = worker.Tick(ctx)
	require.NoError(t, err)
	utils.WaitSomething(10, 100*time.Millisecond, func() bool {
		status := worker.Status()
		return status.Code == lib.WorkerStatusError || status.Code == lib.WorkerStatusFinished
	})
	cancel()
	status := worker.Status()
	require.Equal(t, lib.WorkerStatusFinished, status.Code)
	err = worker.Close(context.Background())
	require.NoError(t, err)
}
