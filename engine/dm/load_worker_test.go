package dm

import (
	"context"
	"testing"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/lib"
	libModel "github.com/hanfei1991/microcosm/lib/model"
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
		dcontext.Background(), lib.WorkerDMLoad, workerID, masterID, mockWorkerConfig())
	require.NoError(t, err)

	worker := workerWrapped.(*loadWorker)
	worker.BaseWorker = lib.MockBaseWorker(workerID, masterID, worker)

	putMasterMeta(context.Background(), t, worker.MetaKVClient(), &libModel.MasterMetaKVData{
		ID:         masterID,
		NodeID:     nodeID,
		Epoch:      1,
		StatusCode: libModel.MasterStatusInit,
	})

	err = worker.Init(ctx)
	require.NoError(t, err)
	err = worker.Tick(ctx)
	require.NoError(t, err)
	lib.MockBaseWorkerWaitUpdateStatus(t, worker.BaseWorker.(*lib.DefaultBaseWorker))

	cancel()
	err = worker.Close(context.Background())
	require.NoError(t, err)
}
