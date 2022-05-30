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
	"math"
	"sync"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/cdc/sink/mq/codec"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

type mockProducer struct {
	mqEvent      map[topicPartitionKey][]*codec.MQMessage
	flushedTimes int

	mockErr chan error
}

func (m *mockProducer) AsyncSendMessage(
	ctx context.Context, topic string, partition int32, message *codec.MQMessage,
) error {
	select {
	case err := <-m.mockErr:
		return err
	default:
	}

	key := topicPartitionKey{
		topic:     topic,
		partition: partition,
	}
	if _, ok := m.mqEvent[key]; !ok {
		m.mqEvent[key] = make([]*codec.MQMessage, 0)
	}
	m.mqEvent[key] = append(m.mqEvent[key], message)
	return nil
}

func (m *mockProducer) SyncBroadcastMessage(
	ctx context.Context, topic string, partitionsNum int32, message *codec.MQMessage,
) error {
	panic("Not used")
}

func (m *mockProducer) Flush(ctx context.Context) error {
	m.flushedTimes += 1
	return nil
}

func (m *mockProducer) Close() error {
	panic("Not used")
}

func (m *mockProducer) InjectError(err error) {
	m.mockErr <- err
}

func NewMockProducer() *mockProducer {
	return &mockProducer{
		mqEvent: make(map[topicPartitionKey][]*codec.MQMessage),
		mockErr: make(chan error, 1),
	}
}

func newTestWorker(ctx context.Context) (*flushWorker, *mockProducer) {
	// 200 is about the size of a row change.
	encoderConfig := codec.NewConfig(config.ProtocolOpen).WithMaxMessageBytes(200)
	builder, err := codec.NewEventBatchEncoderBuilder(context.Background(), encoderConfig)
	if err != nil {
		panic(err)
	}
	encoder := builder.Build()
	if err != nil {
		panic(err)
	}
	producer := NewMockProducer()
	return newFlushWorker(encoder, producer,
		metrics.NewStatistics(ctx, metrics.SinkTypeMQ)), producer
}

//nolint:tparallel
func TestBatch(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker, _ := newTestWorker(ctx)
	key := topicPartitionKey{
		topic:     "test",
		partition: 1,
	}

	tests := []struct {
		name      string
		events    []mqEvent
		expectedN int
	}{
		{
			name: "Normal batching",
			events: []mqEvent{
				{
					flush: nil,
				},
				{
					row: &model.RowChangedEvent{
						CommitTs: 1,
						Table:    &model.TableName{Schema: "a", Table: "b"},
						Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
					},
					key: key,
				},
				{
					row: &model.RowChangedEvent{
						CommitTs: 2,
						Table:    &model.TableName{Schema: "a", Table: "b"},
						Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
					},
					key: key,
				},
			},
			expectedN: 2,
		},
		{
			name: "No row change events",
			events: []mqEvent{
				{
					flush: &flushEvent{
						resolvedTs: model.NewResolvedTs(1),
						flushed:    make(chan struct{}),
					},
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
					key: key,
				},
				{
					flush: &flushEvent{
						resolvedTs: model.NewResolvedTs(1),
						flushed:    make(chan struct{}),
					},
				},
				{
					row: &model.RowChangedEvent{
						// Indicates that this event is not expected to be processed
						CommitTs: math.MaxUint64,
						Table:    &model.TableName{Schema: "a", Table: "b"},
						Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
					},
					key: key,
				},
			},
			expectedN: 1,
		},
	}

	var wg sync.WaitGroup
	batch := make([]mqEvent, 3)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Can not be parallel, it tests reusing the same batch.
			wg.Add(1)
			go func() {
				defer wg.Done()
				endIndex, err := worker.batch(ctx, batch)
				require.NoError(t, err)
				require.Equal(t, test.expectedN, endIndex)
			}()

			go func() {
				for _, event := range test.events {
					err := worker.addEvent(ctx, event)
					if event.row != nil && event.row.CommitTs == math.MaxUint64 {
						// For unprocessed events, addEvent returns after ctx has been cancelled.
						require.Regexp(t, ".*context canceled.*", err)
					} else {
						require.NoError(t, err)
					}
				}
			}()
			wg.Wait()
		})
	}
}

func TestGroup(t *testing.T) {
	t.Parallel()

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

	events := []mqEvent{
		{
			row: &model.RowChangedEvent{
				CommitTs: 1,
				Table:    &model.TableName{Schema: "a", Table: "b"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
			},
			key: key1,
		},
		{
			row: &model.RowChangedEvent{
				CommitTs: 2,
				Table:    &model.TableName{Schema: "a", Table: "b"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
			},
			key: key1,
		},
		{
			row: &model.RowChangedEvent{
				CommitTs: 3,
				Table:    &model.TableName{Schema: "a", Table: "b"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "cc"}},
			},
			key: key1,
		},
		{
			row: &model.RowChangedEvent{
				CommitTs: 2,
				Table:    &model.TableName{Schema: "aa", Table: "bb"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
			},
			key: key2,
		},
		{
			row: &model.RowChangedEvent{
				CommitTs: 2,
				Table:    &model.TableName{Schema: "aaa", Table: "bbb"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
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
		paritionedRows[key1][0].CommitTs, paritionedRows[key1][1].CommitTs,
		paritionedRows[key1][2].CommitTs,
	)
	require.Len(t, paritionedRows[key2], 1)
	require.Len(t, paritionedRows[key3], 1)
}

func TestAsyncSend(t *testing.T) {
	t.Parallel()

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
	events := []mqEvent{
		{
			row: &model.RowChangedEvent{
				CommitTs: 1,
				Table:    &model.TableName{Schema: "a", Table: "b"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
			},
			key: key1,
		},
		{
			row: &model.RowChangedEvent{
				CommitTs: 2,
				Table:    &model.TableName{Schema: "a", Table: "b"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
			},
			key: key1,
		},
		{
			row: &model.RowChangedEvent{
				CommitTs: 3,
				Table:    &model.TableName{Schema: "a", Table: "b"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "cc"}},
			},
			key: key1,
		},
		{
			row: &model.RowChangedEvent{
				CommitTs: 2,
				Table:    &model.TableName{Schema: "aa", Table: "bb"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
			},
			key: key2,
		},
		{
			row: &model.RowChangedEvent{
				CommitTs: 2,
				Table:    &model.TableName{Schema: "aaa", Table: "bbb"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
			},
			key: key3,
		},
		{
			row: &model.RowChangedEvent{
				CommitTs: 2,
				Table:    &model.TableName{Schema: "aaa", Table: "bbb"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
			},
			key: key3,
		},
	}

	paritionedRows := worker.group(events)
	err := worker.asyncSend(context.Background(), paritionedRows)
	require.NoError(t, err)
	require.Len(t, producer.mqEvent, 3)
	require.Len(t, producer.mqEvent[key1], 3)
	require.Len(t, producer.mqEvent[key2], 1)
	require.Len(t, producer.mqEvent[key3], 2)
}

func TestFlush(t *testing.T) {
	t.Parallel()

	key1 := topicPartitionKey{
		topic:     "test",
		partition: 1,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker, producer := newTestWorker(ctx)
	flushedChan := make(chan struct{})
	flushed := atomic.NewBool(false)
	events := []mqEvent{
		{
			row: &model.RowChangedEvent{
				CommitTs: 1,
				Table:    &model.TableName{Schema: "a", Table: "b"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
			},
			key: key1,
		},
		{
			row: &model.RowChangedEvent{
				CommitTs: 2,
				Table:    &model.TableName{Schema: "a", Table: "b"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
			},
			key: key1,
		},
		{
			row: &model.RowChangedEvent{
				CommitTs: 3,
				Table:    &model.TableName{Schema: "a", Table: "b"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "cc"}},
			},
			key: key1,
		},
		{
			flush: &flushEvent{
				resolvedTs: model.NewResolvedTs(1),
				flushed:    flushedChan,
			},
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		select {
		case <-flushedChan:
			flushed.Store(true)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		batchBuf := make([]mqEvent, 4)
		ctx := context.Background()
		endIndex, err := worker.batch(ctx, batchBuf)
		require.NoError(t, err)
		require.Equal(t, 3, endIndex)
		require.NotNil(t, worker.needsFlush)
		msgs := batchBuf[:endIndex]
		paritionedRows := worker.group(msgs)
		err = worker.asyncSend(ctx, paritionedRows)
		require.NoError(t, err)
		require.Equal(t, 1, producer.flushedTimes)
		require.Nil(t, worker.needsFlush)
	}()

	for _, event := range events {
		err := worker.addEvent(context.Background(), event)
		require.NoError(t, err)
	}

	wg.Wait()
	// Make sure the flush event is processed and notify the flushedChan.
	require.True(t, flushed.Load())
}

func TestAbort(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	worker, _ := newTestWorker(ctx)

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker, prod := newTestWorker(ctx)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := worker.run(ctx)
		require.Error(t, err)
		require.Regexp(t, ".*fake.*", err.Error())
	}()

	prod.InjectError(errors.New("fake"))
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
	err = worker.addEvent(ctx, mqEvent{flush: &flushEvent{
		resolvedTs: model.NewResolvedTs(100),
		flushed:    make(chan struct{}),
	}})
	require.NoError(t, err)
	wg.Wait()

	err = worker.addEvent(ctx, mqEvent{flush: &flushEvent{
		resolvedTs: model.NewResolvedTs(200),
		flushed:    make(chan struct{}),
	}})
	require.Error(t, err)
	require.Regexp(t, ".*fake.*", err.Error())

	err = worker.addEvent(ctx, mqEvent{flush: &flushEvent{
		resolvedTs: model.NewResolvedTs(300),
		flushed:    make(chan struct{}),
	}})
	require.Error(t, err)
	require.Regexp(t, ".*ErrMQWorkerClosed.*", err.Error())
}

func TestWorker(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker, producer := newTestWorker(ctx)
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
