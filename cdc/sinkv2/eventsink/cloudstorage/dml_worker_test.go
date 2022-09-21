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
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/stretchr/testify/require"
)

func testDMLWorker(ctx context.Context, t *testing.T, dir string) *dmlWorker {
	uri := fmt.Sprintf("file:///%s", dir)
	bs, err := storage.ParseBackend(uri, &storage.BackendOptions{})
	require.Nil(t, err)
	storage, err := storage.New(ctx, bs, nil)
	require.Nil(t, err)
	errCh := make(chan error, 1)
	d := newDMLWorker(1, model.DefaultChangeFeedID("dml-worker-test"), storage,
		2*time.Second, ".json", errCh)
	return d
}

func TestGenerateCloudStoragePath(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := testDMLWorker(ctx, t, t.TempDir())
	table := versionedTable{
		TableName: model.TableName{
			Schema: "test",
			Table:  "table1",
		},
		TableVersion: 5,
	}
	path := d.generateCloudStoragePath(table)
	require.Equal(t, "test/table1/5/CDC000001.json", path)
	path = d.generateCloudStoragePath(table)
	require.Equal(t, "test/table1/5/CDC000002.json", path)
}

func TestDMLWorkerRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	parentDir := t.TempDir()
	d := testDMLWorker(ctx, t, parentDir)
	fragCh := chann.New[eventFragment]()
	table1Dir := path.Join(parentDir, "test/table1/99")
	table2Dir := path.Join(parentDir, "test/table2/199")
	os.MkdirAll(table1Dir, 0o755)
	os.MkdirAll(table2Dir, 0o755)
	d.run(ctx, fragCh)
	// assume table1 and table2 are dispatched to the same DML worker
	table1 := model.TableName{
		Schema:  "test",
		Table:   "table1",
		TableID: 100,
	}
	// assume 5 event fragments of table 1 are arrived in reverse order
	for i := 5; i > 0; i-- {
		frag := eventFragment{
			seqNumber:    uint64(i),
			tableName:    table1,
			tableVersion: 99,
			event: &eventsink.TxnCallbackableEvent{
				Event: &model.SingleTableTxn{
					Table: &model.TableName{
						Schema:  "test",
						Table:   "table1",
						TableID: 100,
					},
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
	fragCh.In() <- eventFragment{
		seqNumber:    5,
		tableName:    table1,
		tableVersion: 99,
	}

	table2 := model.TableName{
		Schema:  "test",
		Table:   "table2",
		TableID: 200,
	}
	// assume 3 event fragments of table 2 are arrived sequentially
	for i := 1; i <= 3; i++ {
		frag := eventFragment{
			seqNumber:    uint64(i),
			tableName:    table2,
			tableVersion: 199,
			event: &eventsink.TxnCallbackableEvent{
				Event: &model.SingleTableTxn{
					Table: &model.TableName{
						Schema:  "test",
						Table:   "table1",
						TableID: 200,
					},
					Rows: []*model.RowChangedEvent{
						{
							Table: &model.TableName{
								Schema:  "test",
								Table:   "table1",
								TableID: 200,
							},
							Columns: []*model.Column{
								{Name: "c1", Value: 100},
								{Name: "c2", Value: "你好，世界"},
							},
						},
					},
				},
			},
			encodedMsgs: []*common.Message{
				{
					Value: []byte(fmt.Sprintf(`{"id":%d,"database":"test","table":"table2","pkNames":[],"isDdl":false,`+
						`"type":"INSERT","es":0,"ts":1663572946034,"sql":"","sqlType":{"c1":12,"c2":12},`+
						`"data":[{"c1":"200","c2":"你好，世界"}],"old":null}`, i)),
				},
			},
		}
		fragCh.In() <- frag
	}
	fragCh.In() <- eventFragment{
		seqNumber:    3,
		tableName:    table2,
		tableVersion: 199,
	}

	time.Sleep(4 * time.Second)
	// check whether files for table1 has been generated
	files, err := ioutil.ReadDir(table1Dir)
	require.Nil(t, err)
	require.Len(t, files, 1)
	require.Equal(t, "CDC000001.json", files[0].Name())
	// check whether files for table2 has been generated
	files, err = ioutil.ReadDir(table2Dir)
	require.Nil(t, err)
	require.Len(t, files, 1)
	require.Equal(t, "CDC000001.json", files[0].Name())
	cancel()
	d.close()
}
