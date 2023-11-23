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
	"sync"
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/builder"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	dmlsink "github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/util"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func testEncodingWorker(
	t *testing.T,
) (*encodingWorker, chan eventFragment, chan eventFragment) {
	uri := fmt.Sprintf("file:///%s", t.TempDir())
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	encoderConfig, err := util.GetEncoderConfig(sinkURI, config.ProtocolOpen,
		config.GetDefaultReplicaConfig(), config.DefaultMaxMessageBytes)
	require.Nil(t, err)
	encoderBuilder, err := builder.NewEventBatchEncoderBuilder(context.TODO(), encoderConfig)
	require.Nil(t, err)
	encoder := encoderBuilder.Build()
	changefeedID := model.DefaultChangeFeedID("test-encode")

	encodedCh := make(chan eventFragment)
	msgCh := make(chan eventFragment, 1024)
	return newEncodingWorker(1, changefeedID, encoder, msgCh, encodedCh), msgCh, encodedCh
}

func TestEncodeEvents(t *testing.T) {
	t.Parallel()

	encodingWorker, _, encodedCh := testEncodingWorker(t)
	ctx, cancel := context.WithCancel(context.Background())
	eg, egCtx := errgroup.WithContext(ctx)
	outputChs := []*chann.DrainableChann[eventFragment]{chann.NewDrainableChann[eventFragment]()}
	defragmenter := newDefragmenter(encodedCh, outputChs)
	eg.Go(func() error {
		return defragmenter.run(egCtx)
	})

	colInfos := []rowcodec.ColInfo{
		{
			ID:            1,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeLong),
		},
		{
			ID:            2,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeString),
		},
	}
	err := encodingWorker.encodeEvents(ctx, eventFragment{
		versionedTable: cloudstorage.VersionedTableName{
			TableNameWithPhysicTableID: model.TableName{
				Schema:  "test",
				Table:   "table1",
				TableID: 100,
			},
		},
		seqNumber: 1,
		event: &dmlsink.TxnCallbackableEvent{
			Event: &model.SingleTableTxn{
				TableInfo: &model.TableInfo{
					TableName: model.TableName{
						Schema:  "test",
						Table:   "table1",
						TableID: 100,
					},
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
						ColInfos: colInfos,
					},
					{
						Table: &model.TableName{
							Schema:  "test",
							Table:   "table1",
							TableID: 100,
						},
						Columns: []*model.Column{
							{Name: "c1", Value: 200},
							{Name: "c2", Value: "你好，世界"},
						},
						ColInfos: colInfos,
					},
				},
			},
		},
	})
	require.Nil(t, err)
	cancel()
	require.ErrorIs(t, eg.Wait(), context.Canceled)
}

func TestEncodingWorkerRun(t *testing.T) {
	encodingWorker, msgCh, encodedCh := testEncodingWorker(t)
	ctx, cancel := context.WithCancel(context.Background())
	eg, egCtx := errgroup.WithContext(ctx)
	outputChs := []*chann.DrainableChann[eventFragment]{chann.NewDrainableChann[eventFragment]()}
	defragmenter := newDefragmenter(encodedCh, outputChs)
	eg.Go(func() error {
		return defragmenter.run(egCtx)
	})

	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.table1(c1 int primary key, c2 varchar(255))`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	table := model.TableName{
		Schema:  "test",
		Table:   "table1",
		TableID: 100,
	}
	event := &model.SingleTableTxn{
		Table: &model.TableName{
			Schema:  "test",
			Table:   "table1",
			TableID: 100,
		},
		TableInfo: tableInfo,
		Rows: []*model.RowChangedEvent{
			{
				Table: &model.TableName{
					Schema:  "test",
					Table:   "table1",
					TableID: 100,
				},
				TableInfo: tableInfo,
				Columns: []*model.Column{
					{Name: "c1", Value: 100},
					{Name: "c2", Value: "hello world"},
				},
				ColInfos: colInfos,
			},
		},
	}

	for i := 0; i < 3; i++ {
		frag := eventFragment{
			versionedTable: cloudstorage.VersionedTableName{
				TableNameWithPhysicTableID: table,
			},
			seqNumber: uint64(i + 1),
			event: &eventsink.TxnCallbackableEvent{
				Event: event,
			},
		}
		msgCh <- frag
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = encodingWorker.run(ctx)
	}()

	cancel()
	encodingWorker.close()
	wg.Wait()
}
