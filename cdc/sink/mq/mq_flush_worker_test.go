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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/cdc/sink/mq/codec"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

type mockProducer struct {
	mu           sync.RWMutex
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

	m.mu.Lock()
	defer m.mu.Unlock()
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
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushedTimes += 1
	return nil
}

func (m *mockProducer) Close() error {
	panic("Not used")
}

func (m *mockProducer) InjectError(err error) {
	m.mockErr <- err
}

func (m *mockProducer) getFlushedTimes() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.flushedTimes
}

func (m *mockProducer) getEventsByKey(key topicPartitionKey) []*codec.MQMessage {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.mqEvent[key]
}

func (m *mockProducer) getKeysCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.mqEvent)
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
	producer := NewMockProducer()
	return newFlushWorker(builder, producer,
		metrics.NewStatistics(ctx, metrics.SinkTypeMQ), config.ProtocolOpen, 4, model.DefaultChangeFeedID("changefeed-test")), producer
}

//nolint:tparallel
func TestBatch(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker, _ := newTestWorker(ctx)
	defer worker.close()
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
			wg.Add(1)
			go func() {
				defer wg.Done()
				for _, event := range test.events {
					err := worker.addEvent(ctx, event)
					require.NoError(t, err)
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
	defer worker.close()
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

func TestSendMessages(t *testing.T) {
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
	worker, producer := newTestWorker(ctx)
	defer worker.close()
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

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = worker.run(ctx)
	}()

	for _, event := range events {
		err := worker.addEvent(context.Background(), event)
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		return producer.getKeysCount() == 3
	}, 3*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return len(producer.getEventsByKey(key1)) == 3
	}, 3*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return len(producer.getEventsByKey(key2)) == 1
	}, 3*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return len(producer.getEventsByKey(key3)) == 2
	}, 3*time.Second, 10*time.Millisecond)

	cancel()
	wg.Wait()
}

func TestFlush(t *testing.T) {
	t.Parallel()

	key1 := topicPartitionKey{
		topic:     "test",
		partition: 1,
	}

	ctx, cancel := context.WithCancel(context.Background())
	worker, producer := newTestWorker(ctx)
	defer worker.close()
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
		_ = worker.run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-flushedChan
		flushed.Store(true)
	}()

	for _, event := range events {
		err := worker.addEvent(context.Background(), event)
		require.NoError(t, err)
	}

	// Make sure the flush event is processed and notify the flushedChan.
	require.Eventually(t, func() bool {
		return flushed.Load()
	}, 3*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return producer.getFlushedTimes() == 1
	}, 3*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return len(worker.flushChes) == 0
	}, 3*time.Second, 10*time.Millisecond)

	cancel()
	wg.Wait()
}

func TestAbort(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	worker, _ := newTestWorker(ctx)
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

func TestProducerError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker, prod := newTestWorker(ctx)
	defer worker.close()

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
	prod.InjectError(errors.New("fake"))
	wg.Wait()
}

func TestWorker(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	worker, producer := newTestWorker(ctx)
	defer worker.close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
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

	flushedChan1 := make(chan struct{})
	flushed1 := atomic.NewBool(false)
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-flushedChan1
		flushed1.Store(true)
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

		<-flushedChan2
		flushed2.Store(true)
	}()
	err = worker.addEvent(ctx, mqEvent{flush: &flushEvent{
		resolvedTs: model.NewResolvedTs(200),
		flushed:    flushedChan2,
	}})
	require.NoError(t, err)

	// Make sure we don't get a block even if we flush multiple times.

	require.Eventually(t, func() bool {
		return flushed1.Load()
	}, 3*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		return flushed2.Load()
	}, 3*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		return producer.getFlushedTimes() == 2
	}, 3*time.Second, 100*time.Millisecond)

	cancel()
	wg.Wait()
}
