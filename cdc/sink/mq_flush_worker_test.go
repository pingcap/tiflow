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

	worker, _ := newTestWorker()
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

	worker, _ := newTestWorker()
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

	worker, producer := newTestWorker()
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

	worker, producer := newTestWorker()
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
		partition: 1,
	})
	require.NoError(t, err)
	err = worker.addEvent(ctx, mqEvent{resolvedTs: 100})
	require.NoError(t, err)
	wg.Wait()

	err = worker.addEvent(ctx, mqEvent{resolvedTs: 200})
	require.Error(t, err)
	require.Regexp(t, ".*fake.*", err.Error())

	err = worker.addEvent(ctx, mqEvent{resolvedTs: 300})
	require.Error(t, err)
	require.Regexp(t, ".*ErrMQWorkerClosed.*", err.Error())
}
