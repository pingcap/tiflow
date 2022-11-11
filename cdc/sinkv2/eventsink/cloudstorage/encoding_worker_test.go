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
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/builder"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/util"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func testEncodingWorker(ctx context.Context, t *testing.T) (*encodingWorker, func()) {
	uri := fmt.Sprintf("file:///%s", t.TempDir())
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	encoderConfig, err := util.GetEncoderConfig(sinkURI, config.ProtocolOpen,
		config.GetDefaultReplicaConfig(), config.DefaultMaxMessageBytes)
	require.Nil(t, err)
	encoderBuilder, err := builder.NewEventBatchEncoderBuilder(context.TODO(), encoderConfig)
	require.Nil(t, err)
	encoder := encoderBuilder.Build()
	errCh := make(chan error, 10)
	changefeedID := model.DefaultChangeFeedID("test-encode")

	msgCh := make(chan eventFragment, 1024)
	defragmenter := newDefragmenter(ctx)
	worker := newEncodingWorker(1, changefeedID, encoder, msgCh, defragmenter, errCh)
	return worker, func() {
		defragmenter.close()
	}
}

func TestEncodeEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	worker, fn := testEncodingWorker(ctx, t)
	defer fn()
	err := worker.encodeEvents(ctx, eventFragment{
		versionedTable: versionedTable{
			TableName: model.TableName{
				Schema:  "test",
				Table:   "table1",
				TableID: 100,
			},
		},
		seqNumber: 1,
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
					},
				},
			},
		},
	})
	require.Nil(t, err)
	cancel()
}

func TestEncodingWorkerRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	worker, fn := testEncodingWorker(ctx, t)
	defer fn()
	worker.run(ctx)
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
	}

	for i := 0; i < 3; i++ {
		frag := eventFragment{
			versionedTable: versionedTable{
				TableName: table,
			},
			seqNumber: uint64(i + 1),
			event: &eventsink.TxnCallbackableEvent{
				Event: event,
			},
		}
		worker.inputCh <- frag
	}
	cancel()
	worker.close()
}
