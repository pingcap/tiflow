package dm

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/lib/registry"
	"github.com/hanfei1991/microcosm/pkg/adapter"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/metadata"
)

// nolint: unused
var (
	masterID = "master-id"
	workerID = "worker-id"
	nodeID   = "node-id"
)

// nolint: unused
func mockWorkerConfig() []byte {
	cfg := &config.SubTaskConfig{
		SourceID: "source-id",
		From: config.DBConfig{
			Host:     "127.0.0.1",
			Port:     3306,
			User:     "root",
			Password: "123456",
		},
		To: config.DBConfig{
			Host:     "127.0.0.1",
			Port:     4000,
			User:     "root",
			Password: "",
		},
		ServerID:   102,
		MetaSchema: "db_test",
		Name:       "db_ut",
		Mode:       config.ModeAll,
		Flavor:     "mysql",
		LoaderConfig: config.LoaderConfig{
			Dir: "/tmp/dftest.db_ut",
		},
		BAList: &filter.Rules{
			DoDBs: []string{"test"},
		},
	}
	cfg.From.Adjust()
	cfg.To.Adjust()

	value, _ := cfg.Toml()
	return []byte(value)
}

// nolint: unused
func putMasterMeta(
	ctx context.Context,
	t *testing.T,
	metaClient metadata.MetaKV,
	metaData *lib.MasterMetaKVData,
) {
	masterKey := adapter.MasterInfoKey.Encode(masterID)
	masterInfoBytes, err := json.Marshal(metaData)
	require.NoError(t, err)
	_, err = metaClient.Put(ctx, masterKey, string(masterInfoBytes))
	require.NoError(t, err)
}

func TestDumpWorker(t *testing.T) {
	// This test requires a MySQL running on port 3306. The "test" database on
	// the MySQL instance should contain some data to be dumped.
	t.SkipNow()
	t.Parallel()

	require.NoError(t, log.InitLogger(&log.Config{Level: "debug"}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workerWrapped, err := registry.GlobalWorkerRegistry().CreateWorker(
		dcontext.Background(), WorkerDMDump, workerID, masterID, mockWorkerConfig())
	require.NoError(t, err)

	worker := workerWrapped.(*dumpWorker)
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
