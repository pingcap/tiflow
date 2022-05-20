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

package dmtask

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/util/filter"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/engine/lib"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/lib/registry"
	"github.com/pingcap/tiflow/engine/pkg/adapter"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
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
	metaClient metaclient.KVClient,
	metaData *libModel.MasterMetaKVData,
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
		dcontext.Background(), lib.WorkerDMDump, workerID, masterID, mockWorkerConfig())
	require.NoError(t, err)

	worker := workerWrapped.(*DumpTask)
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
