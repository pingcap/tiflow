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

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/cdc/sink/mq/codec"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

type mockProducer struct {
	mqEvent map[topicPartitionKey][]*codec.MQMessage
	flushed bool

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
	m.flushed = true
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

func newTestWorker() (*flushWorker, *mockProducer) {
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
		metrics.NewStatistics(context.Background(), metrics.SinkTypeMQ)), producer
}

//nolint:tparallel
func TestBatch(t *testing.T) {
	t.Parallel()

	worker, _ := newTestWorker()
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
					resolved: model.NewResolvedTs(0),
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
					resolved: model.NewResolvedTs(1),
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
					resolved: model.NewResolvedTs(1),
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
			expectedN: 1,
		},
	}

	var wg sync.WaitGroup
	ctx := context.Background()
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
					err := worker.addEvent(context.Background(), event)
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
	worker, _ := newTestWorker()

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

	worker, producer := newTestWorker()
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
	worker, producer := newTestWorker()

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
			resolved: model.NewResolvedTs(1),
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
	err = worker.addEvent(ctx, mqEvent{resolved: model.NewResolvedTs(100)})
	require.NoError(t, err)
	wg.Wait()

	err = worker.addEvent(ctx, mqEvent{resolved: model.NewResolvedTs(200)})
	require.Error(t, err)
	require.Regexp(t, ".*fake.*", err.Error())

	err = worker.addEvent(ctx, mqEvent{resolved: model.NewResolvedTs(300)})
	require.Error(t, err)
	require.Regexp(t, ".*ErrMQWorkerClosed.*", err.Error())
}
