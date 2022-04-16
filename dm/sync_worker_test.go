package dm

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/util/filter"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/lib"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/lib/registry"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
)

// nolint: unused
func mockWorkerConfigIncremental() []byte {
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
		Mode:       config.ModeIncrement,
		Flavor:     "mysql",
		BAList: &filter.Rules{
			DoDBs: []string{"test"},
		},
		Meta: &config.Meta{
			BinLogName: "mysql-bin.000003",
			BinLogPos:  194,
		},
		SyncerConfig: config.SyncerConfig{
			WorkerCount: 16,
			Batch:       100,
		},
	}
	cfg.From.Adjust()
	cfg.To.Adjust()

	value, _ := cfg.Toml()
	return []byte(value)
}

func TestSyncWorker(t *testing.T) {
	// This test requires a TiDB running on port 4000 and a MySQL on 3306. Also
	// the binlog information should be corresponded to the mockWorkerConfigIncremental
	t.SkipNow()
	t.Parallel()

	require.NoError(t, log.InitLogger(&log.Config{Level: "debug"}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workerWrapped, err := registry.GlobalWorkerRegistry().CreateWorker(
		dcontext.Background(), lib.WorkerDMSync, workerID, masterID, mockWorkerConfigIncremental())
	require.NoError(t, err)

	worker := workerWrapped.(*syncWorker)
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

	time.Sleep(3 * time.Second)

	cancel()
	err = worker.Close(context.Background())
	require.NoError(t, err)
}
