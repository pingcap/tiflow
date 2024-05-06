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
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dmlproducer"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/cdc/sink/tablesink/state"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/builder"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func newBatchEncodeWorker(ctx context.Context, t *testing.T) (*worker, dmlproducer.DMLProducer) {
	id := model.DefaultChangeFeedID("test")
	// 200 is about the size of a rowEvent change.
	encoderConfig := common.NewConfig(config.ProtocolOpen).WithMaxMessageBytes(200).WithChangefeedID(id)
	encoderBuilder, err := builder.NewRowEventEncoderBuilder(context.Background(), encoderConfig)
	require.NoError(t, err)
	p := dmlproducer.NewDMLMockProducer(context.Background(), id, nil, nil, nil, nil)
	require.NoError(t, err)
	encoderConcurrency := 4
	statistics := metrics.NewStatistics(ctx, id, sink.RowSink)

	cfg := config.GetDefaultReplicaConfig()
	cfg.Sink.EncoderConcurrency = &encoderConcurrency
	encoderGroup := codec.NewEncoderGroup(cfg.Sink, encoderBuilder, id)
	return newWorker(id, config.ProtocolOpen, p, encoderGroup, statistics), p
}

func newNonBatchEncodeWorker(ctx context.Context, t *testing.T) (*worker, dmlproducer.DMLProducer) {
	id := model.DefaultChangeFeedID("test")
	// 300 is about the size of a rowEvent change.
	encoderConfig := common.NewConfig(config.ProtocolCanalJSON).WithMaxMessageBytes(300).WithChangefeedID(id)
	encoderBuilder, err := builder.NewRowEventEncoderBuilder(context.Background(), encoderConfig)
	require.NoError(t, err)
	p := dmlproducer.NewDMLMockProducer(context.Background(), id, nil, nil, nil, nil)
	require.NoError(t, err)
	encoderConcurrency := 4
	statistics := metrics.NewStatistics(ctx, id, sink.RowSink)

	cfg := config.GetDefaultReplicaConfig()
	cfg.Sink.EncoderConcurrency = &encoderConcurrency
	encoderGroup := codec.NewEncoderGroup(cfg.Sink, encoderBuilder, id)

	return newWorker(id, config.ProtocolOpen, p, encoderGroup, statistics), p
}

func TestNonBatchEncode_SendMessages(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker, p := newNonBatchEncodeWorker(ctx, t)
	defer worker.close()

	key := model.TopicPartitionKey{
		Topic:     "test",
		Partition: 1,
	}
	row := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
	}
	tableStatus := state.TableSinkSinking

	count := 512
	total := 0
	expected := 0
	for i := 0; i < count; i++ {
		expected += i

		bit := i
		worker.msgChan.In() <- mqEvent{
			key: key,
			rowEvent: &dmlsink.RowChangeCallbackableEvent{
				Event: row,
				Callback: func() {
					total += bit
				},
				SinkState: &tableStatus,
			},
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = worker.run(ctx)
	}()

	mp := p.(*dmlproducer.MockDMLProducer)
	require.Eventually(t, func() bool {
		return len(mp.GetAllEvents()) == count
	}, 3*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		return total == expected
	}, 3*time.Second, 10*time.Millisecond)
	cancel()

	wg.Wait()
}

func TestBatchEncode_Batch(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker, _ := newBatchEncodeWorker(ctx, t)
	defer worker.close()
	key := model.TopicPartitionKey{
		Topic:     "test",
		Partition: 1,
	}
	tableStatus := state.TableSinkSinking
	row := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
	}

	for i := 0; i < 512; i++ {
		worker.msgChan.In() <- mqEvent{
			key: key,
			rowEvent: &dmlsink.RowChangeCallbackableEvent{
				Event:     row,
				Callback:  func() {},
				SinkState: &tableStatus,
			},
		}
	}

	// Test batching returns when the events count is equal to the batch size.
	batch := make([]mqEvent, 512)
	endIndex, err := worker.batch(ctx, batch, time.Minute)
	require.NoError(t, err)
	require.Equal(t, 512, endIndex)
}

func TestBatchEncode_Group(t *testing.T) {
	t.Parallel()

	key1 := model.TopicPartitionKey{
		Topic:     "test",
		Partition: 1,
	}
	key2 := model.TopicPartitionKey{
		Topic:     "test",
		Partition: 2,
	}
	key3 := model.TopicPartitionKey{
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
			rowEvent: &dmlsink.RowChangeCallbackableEvent{
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
			rowEvent: &dmlsink.RowChangeCallbackableEvent{
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
			rowEvent: &dmlsink.RowChangeCallbackableEvent{
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
			rowEvent: &dmlsink.RowChangeCallbackableEvent{
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
			rowEvent: &dmlsink.RowChangeCallbackableEvent{
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

	partitionedRows := worker.group(events)
	require.Len(t, partitionedRows, 3)
	require.Len(t, partitionedRows[key1], 3)
	// We must ensure that the sequence is not broken.
	require.LessOrEqual(
		t,
		partitionedRows[key1][0].Event.GetCommitTs(), partitionedRows[key1][1].Event.GetCommitTs(),
		partitionedRows[key1][2].Event.GetCommitTs(),
	)
	require.Len(t, partitionedRows[key2], 1)
	require.Len(t, partitionedRows[key3], 1)
}

func TestBatchEncode_GroupWhenTableStopping(t *testing.T) {
	t.Parallel()

	key1 := model.TopicPartitionKey{
		Topic:     "test",
		Partition: 1,
	}
	key2 := model.TopicPartitionKey{
		Topic:     "test",
		Partition: 2,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker, _ := newBatchEncodeWorker(ctx, t)
	defer worker.close()
	replicatingStatus := state.TableSinkSinking
	stoppedStatus := state.TableSinkStopping
	events := []mqEvent{
		{
			rowEvent: &dmlsink.RowChangeCallbackableEvent{
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
			rowEvent: &dmlsink.RowChangeCallbackableEvent{
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
			rowEvent: &dmlsink.RowChangeCallbackableEvent{
				Event: &model.RowChangedEvent{
					CommitTs: 3,
					Table:    &model.TableName{Schema: "a", Table: "b"},
					Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "cc"}},
				},
				Callback:  func() {},
				SinkState: &stoppedStatus,
			},
			key: key2,
		},
	}

	partitionedRows := worker.group(events)
	require.Len(t, partitionedRows, 1)
	require.Len(t, partitionedRows[key1], 2)
	// We must ensure that the sequence is not broken.
	require.LessOrEqual(
		t,
		partitionedRows[key1][0].Event.GetCommitTs(),
		partitionedRows[key1][1].Event.GetCommitTs(),
	)
}

func TestBatchEncode_SendMessages(t *testing.T) {
	t.Parallel()

	key1 := model.TopicPartitionKey{
		Topic:     "test",
		Partition: 1,
	}
	key2 := model.TopicPartitionKey{
		Topic:     "test",
		Partition: 2,
	}
	key3 := model.TopicPartitionKey{
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
			rowEvent: &dmlsink.RowChangeCallbackableEvent{
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
			rowEvent: &dmlsink.RowChangeCallbackableEvent{
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
			rowEvent: &dmlsink.RowChangeCallbackableEvent{
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
			rowEvent: &dmlsink.RowChangeCallbackableEvent{
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
			rowEvent: &dmlsink.RowChangeCallbackableEvent{
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
			rowEvent: &dmlsink.RowChangeCallbackableEvent{
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

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = worker.run(ctx)
	}()

	for _, event := range events {
		worker.msgChan.In() <- event
	}

	mp := p.(*dmlproducer.MockDMLProducer)
	require.Eventually(t, func() bool {
		return len(mp.GetAllEvents()) == len(events)
	}, 3*time.Second, 100*time.Millisecond)
	require.Eventually(t, func() bool {
		return len(mp.GetEvents(key1.Topic, key1.Partition)) == 3
	}, 3*time.Second, 100*time.Millisecond)
	require.Eventually(t, func() bool {
		return len(mp.GetEvents(key2.Topic, key2.Partition)) == 1
	}, 3*time.Second, 100*time.Millisecond)
	require.Eventually(t, func() bool {
		return len(mp.GetEvents(key3.Topic, key3.Partition)) == 2
	}, 3*time.Second, 100*time.Millisecond)

	cancel()
	wg.Wait()
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

func TestNonBatchEncode_SendMessagesWhenTableStopping(t *testing.T) {
	t.Parallel()

	key1 := model.TopicPartitionKey{
		Topic:     "test",
		Partition: 1,
	}
	key2 := model.TopicPartitionKey{
		Topic:     "test",
		Partition: 2,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker, p := newNonBatchEncodeWorker(ctx, t)
	defer worker.close()
	replicatingStatus := state.TableSinkSinking
	stoppedStatus := state.TableSinkStopping
	events := []mqEvent{
		{
			rowEvent: &dmlsink.RowChangeCallbackableEvent{
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
			rowEvent: &dmlsink.RowChangeCallbackableEvent{
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
			rowEvent: &dmlsink.RowChangeCallbackableEvent{
				Event: &model.RowChangedEvent{
					CommitTs: 3,
					Table:    &model.TableName{Schema: "a", Table: "b"},
					Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "cc"}},
				},
				Callback:  func() {},
				SinkState: &stoppedStatus,
			},
			key: key2,
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
