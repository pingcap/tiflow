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

package sink

import (
	"context"
	"sync"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"
)

type mockProducer struct {
	mqEvent map[int32][]*codec.MQMessage
	flushed bool

	mockErr chan error
}

func (m *mockProducer) AsyncSendMessage(ctx context.Context, message *codec.MQMessage, partition int32) error {
	select {
	case err := <-m.mockErr:
		return err
	default:
	}

	if _, ok := m.mqEvent[partition]; !ok {
		m.mqEvent[partition] = make([]*codec.MQMessage, 0)
	}
	m.mqEvent[partition] = append(m.mqEvent[partition], message)
	return nil
}

func (m *mockProducer) SyncBroadcastMessage(ctx context.Context, message *codec.MQMessage) error {
	panic("Not used")
}

func (m *mockProducer) Flush(ctx context.Context) error {
	m.flushed = true
	return nil
}

func (m *mockProducer) GetPartitionNum() int32 {
	panic("Not used")
}

func (m *mockProducer) Close() error {
	panic("Not used")
}

func (m *mockProducer) InjectError(err error) {
	m.mockErr <- err
}

func NewMockProducer() *mockProducer {
	return &mockProducer{
		mqEvent: make(map[int32][]*codec.MQMessage),
		mockErr: make(chan error, 1),
	}
}

func newTestWorker() (*flushWorker, *mockProducer) {
	// 200 is about the size of a row change.
	encoderConfig := codec.NewConfig(config.ProtocolOpen, timeutil.SystemLocation()).
		WithMaxMessageBytes(200)
	builder, err := codec.NewEventBatchEncoderBuilder(encoderConfig, &security.Credential{})
	if err != nil {
		panic(err)
	}
	encoder := builder.Build()
	if err != nil {
		panic(err)
	}
	producer := NewMockProducer()
	return newFlushWorker(encoder, producer, NewStatistics(context.Background(), "Test")), producer
}

func TestBatch(t *testing.T) {
	t.Parallel()

<<<<<<< HEAD:cdc/sink/mq_flush_worker_test.go
	worker, _ := newTestWorker()
=======
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker, _ := newTestWorker(ctx)
	defer worker.close()
	key := topicPartitionKey{
		topic:     "test",
		partition: 1,
	}

>>>>>>> 2b758b1a3 (sink/mq(ticdc): make EmitCheckpointTs and FlushRowChangedEvents non-blocking (#5675)):cdc/sink/mq/mq_flush_worker_test.go
	tests := []struct {
		name      string
		events    []mqEvent
		expectedN int
	}{
		{
			name: "Normal batching",
			events: []mqEvent{
				{
					resolvedTs: 0,
				},
				{
					row: &model.RowChangedEvent{
						CommitTs: 1,
						Table:    &model.TableName{Schema: "a", Table: "b"},
						Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
					},
					partition: 1,
				},
				{
					row: &model.RowChangedEvent{
						CommitTs: 2,
						Table:    &model.TableName{Schema: "a", Table: "b"},
						Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
					},
					partition: 1,
				},
			},
			expectedN: 2,
		},
		{
			name: "No row change events",
			events: []mqEvent{
				{
					resolvedTs: 1,
				},
			},
			expectedN: 0,
		},
		{
			name: "The resolved ts event appears in the middle",
			events: []mqEvent{
				{
					row: &model.RowChangedEvent{
						CommitTs: 1,
						Table:    &model.TableName{Schema: "a", Table: "b"},
						Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
					},
					partition: 1,
				},
				{
					resolvedTs: 1,
				},
				{
					row: &model.RowChangedEvent{
						CommitTs: 2,
						Table:    &model.TableName{Schema: "a", Table: "b"},
						Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
					},
					partition: 1,
				},
			},
			expectedN: 1,
		},
	}

	var wg sync.WaitGroup
	ctx := context.Background()
	batch := make([]mqEvent, 3)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				endIndex, err := worker.batch(ctx, batch)
				require.NoError(t, err)
				require.Equal(t, test.expectedN, endIndex)
			}()
			wg.Add(1)
			go func() {
				defer wg.Done()
				for _, event := range test.events {
<<<<<<< HEAD:cdc/sink/mq_flush_worker_test.go
					err := worker.addEvent(context.Background(), event)
=======
					err := worker.addEvent(ctx, event)
>>>>>>> 2b758b1a3 (sink/mq(ticdc): make EmitCheckpointTs and FlushRowChangedEvents non-blocking (#5675)):cdc/sink/mq/mq_flush_worker_test.go
					require.NoError(t, err)
				}
			}()
			wg.Wait()
		})
	}
}

func TestGroup(t *testing.T) {
	t.Parallel()

<<<<<<< HEAD:cdc/sink/mq_flush_worker_test.go
	worker, _ := newTestWorker()
=======
	key1 := topicPartitionKey{
		topic:     "test",
		partition: 1,
	}
	key2 := topicPartitionKey{
		topic:     "test",
		partition: 2,
	}
	key3 := topicPartitionKey{
		topic:     "test1",
		partition: 2,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker, _ := newTestWorker(ctx)
	defer worker.close()
>>>>>>> 2b758b1a3 (sink/mq(ticdc): make EmitCheckpointTs and FlushRowChangedEvents non-blocking (#5675)):cdc/sink/mq/mq_flush_worker_test.go
	events := []mqEvent{
		{
			row: &model.RowChangedEvent{
				CommitTs: 1,
				Table:    &model.TableName{Schema: "a", Table: "b"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
			},
			partition: 1,
		},
		{
			row: &model.RowChangedEvent{
				CommitTs: 2,
				Table:    &model.TableName{Schema: "a", Table: "b"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
			},
			partition: 1,
		},
		{
			row: &model.RowChangedEvent{
				CommitTs: 3,
				Table:    &model.TableName{Schema: "a", Table: "b"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "cc"}},
			},
			partition: 1,
		},
		{
			row: &model.RowChangedEvent{
				CommitTs: 2,
				Table:    &model.TableName{Schema: "aa", Table: "bb"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
			},
			partition: 2,
		},
	}

	paritionedRows := worker.group(events)
	require.Len(t, paritionedRows, 2)
	require.Len(t, paritionedRows[1], 3)
	// We must ensure that the sequence is not broken.
	require.LessOrEqual(
		t,
		paritionedRows[1][0].CommitTs, paritionedRows[1][1].CommitTs,
		paritionedRows[1][2].CommitTs,
	)
	require.Len(t, paritionedRows[2], 1)
}

func TestAsyncSend(t *testing.T) {
	t.Parallel()

<<<<<<< HEAD:cdc/sink/mq_flush_worker_test.go
	worker, producer := newTestWorker()
=======
	key1 := topicPartitionKey{
		topic:     "test",
		partition: 1,
	}

	key2 := topicPartitionKey{
		topic:     "test",
		partition: 2,
	}

	key3 := topicPartitionKey{
		topic:     "test",
		partition: 3,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker, producer := newTestWorker(ctx)
	defer worker.close()
>>>>>>> 2b758b1a3 (sink/mq(ticdc): make EmitCheckpointTs and FlushRowChangedEvents non-blocking (#5675)):cdc/sink/mq/mq_flush_worker_test.go
	events := []mqEvent{
		{
			row: &model.RowChangedEvent{
				CommitTs: 1,
				Table:    &model.TableName{Schema: "a", Table: "b"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
			},
			partition: 1,
		},
		{
			row: &model.RowChangedEvent{
				CommitTs: 2,
				Table:    &model.TableName{Schema: "a", Table: "b"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
			},
			partition: 1,
		},
		{
			row: &model.RowChangedEvent{
				CommitTs: 3,
				Table:    &model.TableName{Schema: "a", Table: "b"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "cc"}},
			},
			partition: 1,
		},
		{
			row: &model.RowChangedEvent{
				CommitTs: 2,
				Table:    &model.TableName{Schema: "aa", Table: "bb"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
			},
			partition: 2,
		},
		{
			row: &model.RowChangedEvent{
				CommitTs: 2,
				Table:    &model.TableName{Schema: "aaa", Table: "bbb"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
			},
			partition: 3,
		},
		{
			row: &model.RowChangedEvent{
				CommitTs: 2,
				Table:    &model.TableName{Schema: "aaa", Table: "bbb"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
			},
			partition: 3,
		},
	}

	paritionedRows := worker.group(events)
	err := worker.asyncSend(context.Background(), paritionedRows)
	require.NoError(t, err)
	require.Len(t, producer.mqEvent, 3)
	require.Len(t, producer.mqEvent[1], 3)
	require.Len(t, producer.mqEvent[2], 1)
	require.Len(t, producer.mqEvent[3], 2)
}

func TestFlush(t *testing.T) {
	t.Parallel()

<<<<<<< HEAD:cdc/sink/mq_flush_worker_test.go
	worker, producer := newTestWorker()
=======
	key1 := topicPartitionKey{
		topic:     "test",
		partition: 1,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker, producer := newTestWorker(ctx)
	defer worker.close()
	flushedChan := make(chan struct{})
	flushed := atomic.NewBool(false)
>>>>>>> 2b758b1a3 (sink/mq(ticdc): make EmitCheckpointTs and FlushRowChangedEvents non-blocking (#5675)):cdc/sink/mq/mq_flush_worker_test.go
	events := []mqEvent{
		{
			row: &model.RowChangedEvent{
				CommitTs: 1,
				Table:    &model.TableName{Schema: "a", Table: "b"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
			},
			partition: 1,
		},
		{
			row: &model.RowChangedEvent{
				CommitTs: 2,
				Table:    &model.TableName{Schema: "a", Table: "b"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
			},
			partition: 1,
		},
		{
			row: &model.RowChangedEvent{
				CommitTs: 3,
				Table:    &model.TableName{Schema: "a", Table: "b"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "cc"}},
			},
			partition: 1,
		},
		{
			resolvedTs: 1,
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		batchBuf := make([]mqEvent, 4)
		ctx := context.Background()
		endIndex, err := worker.batch(ctx, batchBuf)
		require.NoError(t, err)
		require.Equal(t, 3, endIndex)
		require.True(t, worker.needSyncFlush)
		msgs := batchBuf[:endIndex]
		paritionedRows := worker.group(msgs)
		err = worker.asyncSend(ctx, paritionedRows)
		require.NoError(t, err)
		require.True(t, producer.flushed)
		require.False(t, worker.needSyncFlush)
	}()

	for _, event := range events {
		err := worker.addEvent(context.Background(), event)
		require.NoError(t, err)
	}

	wg.Wait()
}

func TestAbort(t *testing.T) {
	t.Parallel()

	worker, _ := newTestWorker()
	ctx, cancel := context.WithCancel(context.Background())
<<<<<<< HEAD:cdc/sink/mq_flush_worker_test.go
=======
	worker, _ := newTestWorker(ctx)
	defer worker.close()
>>>>>>> 2b758b1a3 (sink/mq(ticdc): make EmitCheckpointTs and FlushRowChangedEvents non-blocking (#5675)):cdc/sink/mq/mq_flush_worker_test.go

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

func TestProducerError(t *testing.T) {
	t.Parallel()

	worker, prod := newTestWorker()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
<<<<<<< HEAD:cdc/sink/mq_flush_worker_test.go
=======
	worker, prod := newTestWorker(ctx)
	defer worker.close()
>>>>>>> 2b758b1a3 (sink/mq(ticdc): make EmitCheckpointTs and FlushRowChangedEvents non-blocking (#5675)):cdc/sink/mq/mq_flush_worker_test.go

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := worker.run(ctx)
		require.Error(t, err)
		require.Regexp(t, ".*fake.*", err.Error())
	}()

	err := worker.addEvent(ctx, mqEvent{
		row: &model.RowChangedEvent{
			CommitTs: 1,
			Table:    &model.TableName{Schema: "a", Table: "b"},
			Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
		},
		partition: 1,
	})
	require.NoError(t, err)
	err = worker.addEvent(ctx, mqEvent{resolvedTs: 100})
	require.NoError(t, err)
	prod.InjectError(errors.New("fake"))
	wg.Wait()
<<<<<<< HEAD:cdc/sink/mq_flush_worker_test.go

	err = worker.addEvent(ctx, mqEvent{resolvedTs: 200})
	require.Error(t, err)
	require.Regexp(t, ".*fake.*", err.Error())

	err = worker.addEvent(ctx, mqEvent{resolvedTs: 300})
	require.Error(t, err)
	require.Regexp(t, ".*ErrMQWorkerClosed.*", err.Error())
}
=======
}

func TestWorker(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker, producer := newTestWorker(ctx)
	defer worker.close()
	go func() {
		_ = worker.run(ctx)
	}()

	err := worker.addEvent(ctx, mqEvent{
		row: &model.RowChangedEvent{
			CommitTs: 1,
			Table:    &model.TableName{Schema: "a", Table: "b"},
			Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
		},
		key: topicPartitionKey{
			topic:     "test",
			partition: 1,
		},
	})
	require.NoError(t, err)
	err = worker.addEvent(ctx, mqEvent{
		row: &model.RowChangedEvent{
			CommitTs: 300,
			Table:    &model.TableName{Schema: "a", Table: "b"},
			Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
		},
		key: topicPartitionKey{
			topic:     "test",
			partition: 1,
		},
	})
	require.NoError(t, err)
	var wg sync.WaitGroup

	flushedChan1 := make(chan struct{})
	flushed1 := atomic.NewBool(false)
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-flushedChan1:
			flushed1.Store(true)
		}
	}()
	err = worker.addEvent(ctx, mqEvent{flush: &flushEvent{
		resolvedTs: model.NewResolvedTs(100),
		flushed:    flushedChan1,
	}})
	require.NoError(t, err)

	flushedChan2 := make(chan struct{})
	flushed2 := atomic.NewBool(false)
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-flushedChan2:
			flushed2.Store(true)
		}
	}()
	err = worker.addEvent(ctx, mqEvent{flush: &flushEvent{
		resolvedTs: model.NewResolvedTs(200),
		flushed:    flushedChan2,
	}})
	require.NoError(t, err)

	wg.Wait()

	// Make sure we don't get a block even if we flush multiple times.
	require.Equal(t, 2, producer.flushedTimes)
	require.True(t, flushed1.Load())
	require.True(t, flushed2.Load())
}
>>>>>>> 2b758b1a3 (sink/mq(ticdc): make EmitCheckpointTs and FlushRowChangedEvents non-blocking (#5675)):cdc/sink/mq/mq_flush_worker_test.go
