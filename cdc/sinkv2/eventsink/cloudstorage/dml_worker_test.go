// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package cloudstorage

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/stretchr/testify/require"
)

func testDMLWorker(ctx context.Context, t *testing.T, dir string) *dmlWorker {
	uri := fmt.Sprintf("file:///%s?flush-interval=2s", dir)
	bs, err := storage.ParseBackend(uri, &storage.BackendOptions{})
	require.Nil(t, err)
	storage, err := storage.New(ctx, bs, nil)
	require.Nil(t, err)
	errCh := make(chan error, 1)
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	cfg := cloudstorage.NewConfig()
	err = cfg.Apply(context.TODO(), sinkURI, config.GetDefaultReplicaConfig())
	require.Nil(t, err)

	d := newDMLWorker(1, model.DefaultChangeFeedID("dml-worker-test"), storage,
		cfg, ".json", errCh)
	return d
}

func TestGenerateCloudStoragePath(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := testDMLWorker(ctx, t, t.TempDir())
	table := versionedTable{
		TableName: model.TableName{
			Schema: "test",
			Table:  "table1",
		},
		version: 5,
	}
	path := w.generateDataFilePath(table)
	require.Equal(t, "test/table1/5/CDC000001.json", path)
	path = w.generateDataFilePath(table)
	require.Equal(t, "test/table1/5/CDC000002.json", path)
	w.close()
}

func TestDMLWorkerRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	parentDir := t.TempDir()
	d := testDMLWorker(ctx, t, parentDir)
	fragCh := chann.New[eventFragment]()
	table1Dir := path.Join(parentDir, "test/table1/99")
	os.MkdirAll(table1Dir, 0o755)
	d.run(ctx, fragCh)
	// assume table1 and table2 are dispatched to the same DML worker
	table1 := model.TableName{
		Schema:  "test",
		Table:   "table1",
		TableID: 100,
	}
	tableInfo := &model.TableInfo{
		TableName: model.TableName{
			Schema:  "test",
			Table:   "table1",
			TableID: 100,
		},
		Version: 99,
		TableInfo: &timodel.TableInfo{
			Columns: []*timodel.ColumnInfo{
				{ID: 1, Name: timodel.NewCIStr("name"), FieldType: *types.NewFieldType(mysql.TypeLong)},
			},
		},
	}
	for i := 0; i < 5; i++ {
		frag := eventFragment{
			seqNumber: uint64(i),
			versionedTable: versionedTable{
				TableName: table1,
				version:   99,
			},
			event: &eventsink.TxnCallbackableEvent{
				Event: &model.SingleTableTxn{
					TableInfo: tableInfo,
					Rows: []*model.RowChangedEvent{
						{
							Table: &model.TableName{
								Schema:  "test",
								Table:   "table1",
								TableID: 100,
							},
							Columns: []*model.Column{
								{Name: "c1", Value: 100},
								{Name: "c2", Value: "hello world"},
							},
						},
					},
				},
			},
			encodedMsgs: []*common.Message{
				{
					Value: []byte(fmt.Sprintf(`{"id":%d,"database":"test","table":"table1","pkNames":[],"isDdl":false,`+
						`"type":"INSERT","es":0,"ts":1663572946034,"sql":"","sqlType":{"c1":12,"c2":12},`+
						`"data":[{"c1":"100","c2":"hello world"}],"old":null}`, i)),
				},
			},
		}
		fragCh.In() <- frag
	}

	time.Sleep(4 * time.Second)
	// check whether files for table1 has been generated
	files, err := os.ReadDir(table1Dir)
	require.Nil(t, err)
	require.Len(t, files, 2)
	var fileNames []string
	for _, f := range files {
		fileNames = append(fileNames, f.Name())
	}
	require.ElementsMatch(t, []string{"CDC000001.json", "schema.json"}, fileNames)
	cancel()
	d.close()
	fragCh.Close()
	for range fragCh.Out() {
		// drain the fragCh
	}
}
