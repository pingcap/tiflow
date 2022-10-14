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

package mq

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/builder"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	mqv1 "github.com/pingcap/tiflow/cdc/sink/mq"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink/mq/dmlproducer"
	"github.com/pingcap/tiflow/cdc/sinkv2/metrics"
	"github.com/pingcap/tiflow/cdc/sinkv2/tablesink/state"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/stretchr/testify/require"
)

func newBatchEncodeWorker(ctx context.Context, t *testing.T) (*worker, dmlproducer.DMLProducer) {
	// 200 is about the size of a rowEvent change.
	encoderConfig := common.NewConfig(config.ProtocolOpen).WithMaxMessageBytes(200)
	builder, err := builder.NewEventBatchEncoderBuilder(context.Background(), encoderConfig)
	require.Nil(t, err)
	encoder := builder.Build()
	require.Nil(t, err)
	p, err := dmlproducer.NewDMLMockProducer(context.Background(), nil, nil, nil)
	require.Nil(t, err)
	id := model.DefaultChangeFeedID("test")
	return newWorker(id, config.ProtocolOpen, encoder, p, metrics.NewStatistics(ctx, sink.RowSink)), p
}

func newNonBatchEncodeWorker(ctx context.Context, t *testing.T) (*worker, dmlproducer.DMLProducer) {
	// 200 is about the size of a rowEvent change.
	encoderConfig := common.NewConfig(config.ProtocolCanalJSON).WithMaxMessageBytes(200)
	builder, err := builder.NewEventBatchEncoderBuilder(context.Background(), encoderConfig)
	require.Nil(t, err)
	encoder := builder.Build()
	require.Nil(t, err)
	p, err := dmlproducer.NewDMLMockProducer(context.Background(), nil, nil, nil)
	require.Nil(t, err)
	id := model.DefaultChangeFeedID("test")
	return newWorker(id, config.ProtocolCanalJSON, encoder, p, metrics.NewStatistics(ctx, sink.RowSink)), p
}

func TestBatchEncode_Batch(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker, _ := newBatchEncodeWorker(ctx, t)
	defer worker.close()
	key := mqv1.TopicPartitionKey{
		Topic:     "test",
		Partition: 1,
	}
	tableStatus := state.TableSinkSinking
	row := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
	}

	events := make([]mqEvent, 0, 512)
	for i := 0; i < 512; i++ {
		events = append(events, mqEvent{
			key: key,
			rowEvent: &eventsink.RowChangeCallbackableEvent{
				Event:     row,
				Callback:  func() {},
				SinkState: &tableStatus,
			},
		})
	}

	// Test batching returns when the events count is equal to the batch size.
	var wg sync.WaitGroup
	batch := make([]mqEvent, 512)
	wg.Add(1)
	go func() {
		defer wg.Done()
		endIndex, err := worker.batch(ctx, batch)
		require.NoError(t, err)
		require.Equal(t, 512, endIndex)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, event := range events {
			worker.msgChan.In() <- event
		}
	}()
	wg.Wait()
}

func TestBatchEncode_Group(t *testing.T) {
	t.Parallel()

	key1 := mqv1.TopicPartitionKey{
		Topic:     "test",
		Partition: 1,
	}
	key2 := mqv1.TopicPartitionKey{
		Topic:     "test",
		Partition: 2,
	}
	key3 := mqv1.TopicPartitionKey{
		Topic:     "test1",
		Partition: 2,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker, _ := newBatchEncodeWorker(ctx, t)
	defer worker.close()

	tableStatus := state.TableSinkSinking

	events := []mqEvent{
		{
			rowEvent: &eventsink.RowChangeCallbackableEvent{
				Event: &model.RowChangedEvent{
					CommitTs: 1,
					Table:    &model.TableName{Schema: "a", Table: "b"},
					Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
				},
				Callback:  func() {},
				SinkState: &tableStatus,
			},
			key: key1,
		},
		{
			rowEvent: &eventsink.RowChangeCallbackableEvent{
				Event: &model.RowChangedEvent{
					CommitTs: 2,
					Table:    &model.TableName{Schema: "a", Table: "b"},
					Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
				},
				Callback:  func() {},
				SinkState: &tableStatus,
			},
			key: key1,
		},
		{
			rowEvent: &eventsink.RowChangeCallbackableEvent{
				Event: &model.RowChangedEvent{
					CommitTs: 3,
					Table:    &model.TableName{Schema: "a", Table: "b"},
					Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "cc"}},
				},
				Callback:  func() {},
				SinkState: &tableStatus,
			},
			key: key1,
		},
		{
			rowEvent: &eventsink.RowChangeCallbackableEvent{
				Event: &model.RowChangedEvent{
					CommitTs: 2,
					Table:    &model.TableName{Schema: "aa", Table: "bb"},
					Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
				},
				Callback:  func() {},
				SinkState: &tableStatus,
			},
			key: key2,
		},
		{
			rowEvent: &eventsink.RowChangeCallbackableEvent{
				Event: &model.RowChangedEvent{
					CommitTs: 2,
					Table:    &model.TableName{Schema: "aaa", Table: "bbb"},
					Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
				},
				Callback:  func() {},
				SinkState: &tableStatus,
			},
			key: key3,
		},
	}

	paritionedRows := worker.group(events)
	require.Len(t, paritionedRows, 3)
	require.Len(t, paritionedRows[key1], 3)
	// We must ensure that the sequence is not broken.
	require.LessOrEqual(
		t,
		paritionedRows[key1][0].Event.GetCommitTs(), paritionedRows[key1][1].Event.GetCommitTs(),
		paritionedRows[key1][2].Event.GetCommitTs(),
	)
	require.Len(t, paritionedRows[key2], 1)
	require.Len(t, paritionedRows[key3], 1)
}

func TestBatchEncode_AsyncSend(t *testing.T) {
	t.Parallel()

	key1 := mqv1.TopicPartitionKey{
		Topic:     "test",
		Partition: 1,
	}
	key2 := mqv1.TopicPartitionKey{
		Topic:     "test",
		Partition: 2,
	}
	key3 := mqv1.TopicPartitionKey{
		Topic:     "test1",
		Partition: 2,
	}

	tableStatus := state.TableSinkSinking
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker, p := newBatchEncodeWorker(ctx, t)
	defer worker.close()
	events := []mqEvent{
		{
			rowEvent: &eventsink.RowChangeCallbackableEvent{
				Event: &model.RowChangedEvent{
					CommitTs: 1,
					Table:    &model.TableName{Schema: "a", Table: "b"},
					Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
				},
				Callback:  func() {},
				SinkState: &tableStatus,
			},
			key: key1,
		},
		{
			rowEvent: &eventsink.RowChangeCallbackableEvent{
				Event: &model.RowChangedEvent{
					CommitTs: 2,
					Table:    &model.TableName{Schema: "a", Table: "b"},
					Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
				},
				Callback:  func() {},
				SinkState: &tableStatus,
			},
			key: key1,
		},
		{
			rowEvent: &eventsink.RowChangeCallbackableEvent{
				Event: &model.RowChangedEvent{
					CommitTs: 3,
					Table:    &model.TableName{Schema: "a", Table: "b"},
					Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "cc"}},
				},
				Callback:  func() {},
				SinkState: &tableStatus,
			},
			key: key1,
		},
		{
			rowEvent: &eventsink.RowChangeCallbackableEvent{
				Event: &model.RowChangedEvent{
					CommitTs: 2,
					Table:    &model.TableName{Schema: "aa", Table: "bb"},
					Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
				},
				Callback:  func() {},
				SinkState: &tableStatus,
			},
			key: key2,
		},
		{
			rowEvent: &eventsink.RowChangeCallbackableEvent{
				Event: &model.RowChangedEvent{
					CommitTs: 2,
					Table:    &model.TableName{Schema: "aaa", Table: "bbb"},
					Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
				},
				Callback:  func() {},
				SinkState: &tableStatus,
			},
			key: key3,
		},
		{
			rowEvent: &eventsink.RowChangeCallbackableEvent{
				Event: &model.RowChangedEvent{
					CommitTs: 3,
					Table:    &model.TableName{Schema: "aaa", Table: "bbb"},
					Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
				},
				Callback:  func() {},
				SinkState: &tableStatus,
			},
			key: key3,
		},
	}

	paritionedRows := worker.group(events)
	err := worker.asyncSend(context.Background(), paritionedRows)
	require.NoError(t, err)
	mp := p.(*dmlproducer.MockDMLProducer)
	require.Len(t, mp.GetAllEvents(), 6)
	require.Len(t, mp.GetEvents(key1), 3)
	require.Len(t, mp.GetEvents(key2), 1)
	require.Len(t, mp.GetEvents(key3), 2)
}

func TestBatchEncode_AsyncSendWhenTableStopping(t *testing.T) {
	t.Parallel()

	key1 := mqv1.TopicPartitionKey{
		Topic:     "test",
		Partition: 1,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker, p := newBatchEncodeWorker(ctx, t)
	defer worker.close()
	replicatingStatus := state.TableSinkSinking
	stoopedStatus := state.TableSinkStopping
	events := []mqEvent{
		{
			rowEvent: &eventsink.RowChangeCallbackableEvent{
				Event: &model.RowChangedEvent{
					CommitTs: 1,
					Table:    &model.TableName{Schema: "a", Table: "b"},
					Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
				},
				Callback:  func() {},
				SinkState: &replicatingStatus,
			},
			key: key1,
		},
		{
			rowEvent: &eventsink.RowChangeCallbackableEvent{
				Event: &model.RowChangedEvent{
					CommitTs: 2,
					Table:    &model.TableName{Schema: "a", Table: "b"},
					Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
				},
				Callback:  func() {},
				SinkState: &replicatingStatus,
			},
			key: key1,
		},
		{
			rowEvent: &eventsink.RowChangeCallbackableEvent{
				Event: &model.RowChangedEvent{
					CommitTs: 3,
					Table:    &model.TableName{Schema: "a", Table: "b"},
					Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "cc"}},
				},
				Callback:  func() {},
				SinkState: &stoopedStatus,
			},
			key: key1,
		},
	}

	paritionedRows := worker.group(events)
	err := worker.asyncSend(context.Background(), paritionedRows)
	require.NoError(t, err)
	mp := p.(*dmlproducer.MockDMLProducer)
	require.Len(t, mp.GetAllEvents(), 2)
}

func TestBatchEncodeWorker_Abort(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	worker, _ := newBatchEncodeWorker(ctx, t)
	defer worker.close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := worker.run(ctx)
		require.Error(t, context.Canceled, err)
	}()

	cancel()
	wg.Wait()
}

func TestNonBatchEncode_Send(t *testing.T) {
	t.Parallel()

	key1 := mqv1.TopicPartitionKey{
		Topic:     "test",
		Partition: 1,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker, p := newNonBatchEncodeWorker(ctx, t)
	defer worker.close()
	replicatingStatus := state.TableSinkSinking
	stoopedStatus := state.TableSinkStopping
	events := []mqEvent{
		{
			rowEvent: &eventsink.RowChangeCallbackableEvent{
				Event: &model.RowChangedEvent{
					CommitTs: 1,
					Table:    &model.TableName{Schema: "a", Table: "b"},
					Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
				},
				Callback:  func() {},
				SinkState: &replicatingStatus,
			},
			key: key1,
		},
		{
			rowEvent: &eventsink.RowChangeCallbackableEvent{
				Event: &model.RowChangedEvent{
					CommitTs: 2,
					Table:    &model.TableName{Schema: "a", Table: "b"},
					Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
				},
				Callback:  func() {},
				SinkState: &replicatingStatus,
			},
			key: key1,
		},
		{
			rowEvent: &eventsink.RowChangeCallbackableEvent{
				Event: &model.RowChangedEvent{
					CommitTs: 3,
					Table:    &model.TableName{Schema: "a", Table: "b"},
					Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "cc"}},
				},
				Callback:  func() {},
				SinkState: &stoopedStatus,
			},
			key: key1,
		},
	}
	for _, e := range events {
		worker.msgChan.In() <- e
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = worker.run(ctx)
	}()
	mp := p.(*dmlproducer.MockDMLProducer)
	require.Eventually(t, func() bool {
		return len(mp.GetAllEvents()) == 2
	}, 3*time.Second, 100*time.Millisecond)
	cancel()
	wg.Wait()
}

func TestNonBatchEncodeWorker_Abort(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	worker, _ := newBatchEncodeWorker(ctx, t)
	defer worker.close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := worker.run(ctx)
		require.Error(t, context.Canceled, err)
	}()

	cancel()
	wg.Wait()
}
