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

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/stretchr/testify/require"
)

type mockProducer struct {
	mqEvent map[int32][]*codec.MQMessage
}

func (m mockProducer) AsyncSendMessage(ctx context.Context, message *codec.MQMessage, partition int32) error {
	if _, ok := m.mqEvent[partition]; !ok {
		m.mqEvent[partition] = make([]*codec.MQMessage, 0)
	}
	m.mqEvent[partition] = append(m.mqEvent[partition], message)
	return nil
}

func (m mockProducer) SyncBroadcastMessage(ctx context.Context, message *codec.MQMessage) error {
	panic("Not used")
}

func (m mockProducer) Flush(ctx context.Context) error {
	panic("Not used")
}

func (m mockProducer) GetPartitionNum() int32 {
	panic("Not used")
}

func (m mockProducer) Close() error {
	panic("Not used")
}

func NewMockProducer() *mockProducer {
	return &mockProducer{
		mqEvent: make(map[int32][]*codec.MQMessage),
	}
}

func newTestWorker() (*flushWorker, *mockProducer) {
	encoder := codec.NewJSONEventBatchEncoder()
	// 200 is about the size of a row change.
	err := encoder.SetParams(map[string]string{"max-message-bytes": "200"})
	if err != nil {
		panic(err)
	}
	producer := NewMockProducer()
	return newFlushWorker(encoder, producer, NewStatistics(context.Background(), "Test")), producer
}

func TestBatch(t *testing.T) {
	worker, _ := newTestWorker()
	events := []mqEvent{
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
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		result, err := worker.batch(context.Background())
		require.NoError(t, err)
		require.Len(t, result, 2)
	}()

	for _, event := range events {
		worker.msgChan <- event
	}

	wg.Wait()
}

func TestGroup(t *testing.T) {
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

	paritionedEvents := worker.group(events)
	require.Len(t, paritionedEvents, 2)
	require.Len(t, paritionedEvents[1], 3)
	// We must ensure that the sequence is not broken.
	require.LessOrEqual(t, paritionedEvents[1][0].row.CommitTs, paritionedEvents[1][1].row.CommitTs, paritionedEvents[1][2].row.CommitTs)
	require.Len(t, paritionedEvents[2], 1)
}

func TestFlush(t *testing.T) {
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

	paritionedEvents := worker.group(events)
	err := worker.flush(context.Background(), paritionedEvents)
	require.NoError(t, err)
	require.Len(t, producer.mqEvent, 3)
	require.Len(t, producer.mqEvent[1], 3)
	require.Len(t, producer.mqEvent[2], 1)
	require.Len(t, producer.mqEvent[3], 2)
}

func TestAbort(t *testing.T) {
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
