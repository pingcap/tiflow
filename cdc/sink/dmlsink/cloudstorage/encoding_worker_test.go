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

	timodel "github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	"github.com/pingcap/tiflow/cdc/sink/util"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/pingcap/tiflow/pkg/sink/codec/builder"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func testEncodingWorker(
	t *testing.T,
) (*encodingWorker, chan eventFragment, chan eventFragment) {
	uri := fmt.Sprintf("file:///%s", t.TempDir())
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)

	changefeedID := model.DefaultChangeFeedID("changefeed-test")
	encoderConfig, err := util.GetEncoderConfig(changefeedID, sinkURI, config.ProtocolCsv,
		config.GetDefaultReplicaConfig(), config.DefaultMaxMessageBytes)
	require.Nil(t, err)
	encoderBuilder, err := builder.NewTxnEventEncoderBuilder(encoderConfig)
	require.Nil(t, err)
	encoder := encoderBuilder.Build()

	encodedCh := make(chan eventFragment)
	msgCh := make(chan eventFragment, 1024)
	return newEncodingWorker(1, changefeedID, encoder, msgCh, encodedCh), msgCh, encodedCh
}

func TestEncodeEvents(t *testing.T) {
	t.Parallel()

	encodingWorker, _, encodedCh := testEncodingWorker(t)
	ctx, cancel := context.WithCancel(context.Background())
	eg, egCtx := errgroup.WithContext(ctx)
	outputChs := []*chann.DrainableChann[eventFragment]{chann.NewAutoDrainChann[eventFragment]()}
	defragmenter := newDefragmenter(encodedCh, outputChs)
	eg.Go(func() error {
		return defragmenter.run(egCtx)
	})

	tidbTableInfo := &timodel.TableInfo{
		ID:   100,
		Name: pmodel.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
			{ID: 1, Name: pmodel.NewCIStr("c1"), FieldType: *types.NewFieldType(mysql.TypeLong)},
			{ID: 2, Name: pmodel.NewCIStr("c2"), FieldType: *types.NewFieldType(mysql.TypeString)},
		},
	}
	tableInfo := model.WrapTableInfo(100, "test", 33, tidbTableInfo)

	err := encodingWorker.encodeEvents(eventFragment{
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
				TableInfo: tableInfo,
				Rows: []*model.RowChangedEvent{
					{
						PhysicalTableID: 100,
						TableInfo:       tableInfo,
						Columns: []*model.ColumnData{
							{ColumnID: 1, Value: 100},
							{ColumnID: 2, Value: "hello world"},
						},
					},
					{
						PhysicalTableID: 100,
						TableInfo:       tableInfo,
						Columns: []*model.ColumnData{
							{ColumnID: 1, Value: 200},
							{ColumnID: 2, Value: "你好，世界"},
						},
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
	t.Parallel()

	encodingWorker, msgCh, encodedCh := testEncodingWorker(t)
	ctx, cancel := context.WithCancel(context.Background())
	eg, egCtx := errgroup.WithContext(ctx)
	outputChs := []*chann.DrainableChann[eventFragment]{chann.NewAutoDrainChann[eventFragment]()}
	defragmenter := newDefragmenter(encodedCh, outputChs)
	eg.Go(func() error {
		return defragmenter.run(egCtx)
	})

	table := model.TableName{
		Schema:  "test",
		Table:   "table1",
		TableID: 100,
	}
	tidbTableInfo := &timodel.TableInfo{
		ID:   100,
		Name: pmodel.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
			{ID: 1, Name: pmodel.NewCIStr("c1"), FieldType: *types.NewFieldType(mysql.TypeLong)},
			{ID: 2, Name: pmodel.NewCIStr("c2"), FieldType: *types.NewFieldType(mysql.TypeVarchar)},
		},
	}
	tableInfo := model.WrapTableInfo(100, "test", 33, tidbTableInfo)
	event := &model.SingleTableTxn{
		TableInfo: tableInfo,
		Rows: []*model.RowChangedEvent{
			{
				PhysicalTableID: 100,
				TableInfo:       tableInfo,
				Columns: []*model.ColumnData{
					{ColumnID: 1, Value: 100},
					{ColumnID: 2, Value: "hello world"},
				},
			},
		},
	}

	for i := 0; i < 3; i++ {
		frag := eventFragment{
			versionedTable: cloudstorage.VersionedTableName{
				TableNameWithPhysicTableID: table,
			},
			seqNumber: uint64(i + 1),
			event: &dmlsink.TxnCallbackableEvent{
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
