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
	"path"
	"sync"
	"testing"
	"time"

	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
)

func testDMLWorker(ctx context.Context, t *testing.T, dir string) *dmlWorker {
	uri := fmt.Sprintf("file:///%s?flush-interval=2s", dir)
	storage, err := util.GetExternalStorageFromURI(ctx, uri)
	require.Nil(t, err)
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	cfg := cloudstorage.NewConfig()
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.DateSeparator = config.DateSeparatorNone.String()
	err = cfg.Apply(context.TODO(), sinkURI, replicaConfig)
	cfg.FileIndexWidth = 6
	require.Nil(t, err)

	statistics := metrics.NewStatistics(ctx, sink.TxnSink)
	d := newDMLWorker(1, model.DefaultChangeFeedID("dml-worker-test"), storage,
		cfg, ".json", chann.NewAutoDrainChann[eventFragment](), clock.New(), statistics)
	return d
}

func TestDMLWorkerRun(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	parentDir := t.TempDir()
	d := testDMLWorker(ctx, t, parentDir)
	fragCh := d.inputCh
	table1Dir := path.Join(parentDir, "test/table1/99")
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
			versionedTable: cloudstorage.VersionedTableName{
				TableNameWithPhysicTableID: table1,
				TableInfoVersion:           99,
			},
			event: &dmlsink.TxnCallbackableEvent{
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

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = d.run(ctx)
	}()

	time.Sleep(4 * time.Second)
	// check whether files for table1 has been generated
	fileNames := getTableFiles(t, table1Dir)
	require.Len(t, fileNames, 2)
	require.ElementsMatch(t, []string{"CDC000001.json", "CDC.index"}, fileNames)
	cancel()
	d.close()
	wg.Wait()
	fragCh.CloseAndDrain()
}
