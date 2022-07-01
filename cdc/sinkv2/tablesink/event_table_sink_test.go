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

package tablesink

import (
	"sort"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/pipeline"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/stretchr/testify/require"
)

// Assert TableSink implementation
var _ eventsink.EventSink[*model.SingleTableTxn] = (*mockEventSink)(nil)

type mockEventSink struct {
	events []*eventsink.TxnCallbackableEvent
}

func (m *mockEventSink) WriteEvents(rows ...*eventsink.TxnCallbackableEvent) {
	m.events = append(m.events, rows...)
}

func (m *mockEventSink) Close() error {
	// Do nothing.
	return nil
}

// acknowledge the txn events by call the callback function.
func (m *mockEventSink) acknowledge(commitTs uint64) {
	i := sort.Search(len(m.events), func(i int) bool {
		return m.events[i].Event.GetCommitTs() > commitTs
	})
	if i == 0 {
		return
	}
	ackedEvents := m.events[:i]

	for _, event := range ackedEvents {
		if event.TableStatus.Load() != pipeline.TableStateStopped {
			event.Callback()
		} else {
			// If the table is stopped, the event should be ignored.
			return
		}
	}

	// Remove the acked events from the event buffer.
	m.events = append(
		make([]*eventsink.TxnCallbackableEvent,
			0,
			len(m.events[i:])),
		m.events[i:]...,
	)
}

func getTestRows() []*model.RowChangedEvent {
	tableInfo := &model.TableName{
		Schema:      "test",
		Table:       "t1",
		TableID:     1,
		IsPartition: false,
	}

	return []*model.RowChangedEvent{
		{
			Table:    tableInfo,
			CommitTs: 101,
			StartTs:  98,
		},
		{
			Table:    tableInfo,
			CommitTs: 102,
			StartTs:  99,
		},
		{
			Table:    tableInfo,
			CommitTs: 102,
			StartTs:  100,
		},
		{
			Table:    tableInfo,
			CommitTs: 102,
			StartTs:  100,
		},
		{
			Table:    tableInfo,
			CommitTs: 103,
			StartTs:  101,
		},
		{
			Table:    tableInfo,
			CommitTs: 103,
			StartTs:  101,
		},
		{
			Table:    tableInfo,
			CommitTs: 104,
			StartTs:  102,
		},
		{
			Table:    tableInfo,
			CommitTs: 105,
			StartTs:  103,
			// Batch1
			SplitTxn: true,
		},
		{
			Table:    tableInfo,
			CommitTs: 105,
			StartTs:  103,
		},
		{
			Table:    tableInfo,
			CommitTs: 105,
			StartTs:  103,
		},
		{
			Table:    tableInfo,
			CommitTs: 105,
			StartTs:  103,
			// Batch2
			SplitTxn: true,
		},
		{
			Table:    tableInfo,
			CommitTs: 105,
			StartTs:  103,
		},
	}
}

func TestNewEventTableSink(t *testing.T) {
	t.Parallel()

	sink := &mockEventSink{}
	tb := New[*model.SingleTableTxn](sink, &eventsink.TxnEventAppender{})

	require.Equal(t, uint64(0), tb.eventID, "eventID should start from 0")
	require.Equal(t, model.NewResolvedTs(0), tb.maxResolvedTs, "maxResolvedTs should start from 0")
	require.NotNil(t, sink, tb.backendSink, "backendSink should be set")
	require.NotNil(t, tb.progressTracker, "progressTracker should be set")
	require.NotNil(t, tb.eventAppender, "eventAppender should be set")
	require.Equal(t, 0, len(tb.eventBuffer), "eventBuffer should be empty")
	require.Equal(t, pipeline.TableStatePreparing, tb.state, "tableState should be unknown")
}

func TestAppendRowChangedEvents(t *testing.T) {
	t.Parallel()

	sink := &mockEventSink{}
	tb := New[*model.SingleTableTxn](sink, &eventsink.TxnEventAppender{})

	tb.AppendRowChangedEvents(getTestRows()...)
	require.Len(t, tb.eventBuffer, 7, "txn event buffer should have 7 txns")
}

func TestUpdateResolvedTs(t *testing.T) {
	t.Parallel()

	sink := &mockEventSink{}
	tb := New[*model.SingleTableTxn](sink, &eventsink.TxnEventAppender{})

	tb.AppendRowChangedEvents(getTestRows()...)
	// No event will be flushed.
	tb.UpdateResolvedTs(model.NewResolvedTs(100))
	require.Equal(t, model.NewResolvedTs(100), tb.maxResolvedTs, "maxResolvedTs should be updated")
	require.Len(t, tb.eventBuffer, 7, "txn event buffer should have 7 txns")
	require.Len(t, sink.events, 0, "no event should not be flushed")

	// One event will be flushed.
	tb.UpdateResolvedTs(model.NewResolvedTs(101))
	require.Equal(t, model.NewResolvedTs(101), tb.maxResolvedTs, "maxResolvedTs should be updated")
	require.Len(t, tb.eventBuffer, 6, "txn event buffer should have 6 txns")
	require.Len(t, sink.events, 1, "one event should be flushed")

	// Two events will be flushed.
	tb.UpdateResolvedTs(model.NewResolvedTs(102))
	require.Equal(t, model.NewResolvedTs(102), tb.maxResolvedTs, "maxResolvedTs should be updated")
	require.Len(t, tb.eventBuffer, 4, "txn event buffer should have 4 txns")
	require.Len(t, sink.events, 3, "two events should be flushed")

	// Same resolved ts will not be flushed.
	tb.UpdateResolvedTs(model.NewResolvedTs(102))
	require.Equal(
		t,
		model.NewResolvedTs(102),
		tb.maxResolvedTs,
		"maxResolvedTs should not be updated",
	)
	require.Len(t, tb.eventBuffer, 4, "txn event buffer should still have 4 txns")
	require.Len(t, sink.events, 3, "no event should be flushed")

	// All events will be flushed.
	tb.UpdateResolvedTs(model.NewResolvedTs(105))
	require.Equal(t, model.NewResolvedTs(105), tb.maxResolvedTs, "maxResolvedTs should be updated")
	require.Len(t, tb.eventBuffer, 0, "txn event buffer should be empty")
	require.Len(t, sink.events, 7, "all events should be flushed")
}

func TestGetCheckpointTs(t *testing.T) {
	t.Parallel()

	sink := &mockEventSink{}
	tb := New[*model.SingleTableTxn](sink, &eventsink.TxnEventAppender{})

	tb.AppendRowChangedEvents(getTestRows()...)
	require.Equal(t, model.NewResolvedTs(0), tb.GetCheckpointTs(), "checkpointTs should be 0")

	// One event will be flushed.
	tb.UpdateResolvedTs(model.NewResolvedTs(101))
	require.Equal(t, model.NewResolvedTs(0), tb.GetCheckpointTs(), "checkpointTs should be 0")
	sink.acknowledge(101)
	require.Equal(t, model.NewResolvedTs(101), tb.GetCheckpointTs(), "checkpointTs should be 101")

	// Flush all events.
	tb.UpdateResolvedTs(model.NewResolvedTs(105))
	require.Equal(t, model.NewResolvedTs(101), tb.GetCheckpointTs(), "checkpointTs should be 101")

	// Only acknowledge some events.
	sink.acknowledge(102)
	require.Equal(
		t,
		model.NewResolvedTs(101),
		tb.GetCheckpointTs(),
		"checkpointTs should still be 101",
	)

	// Ack all events.
	sink.acknowledge(105)
	require.Equal(t, model.NewResolvedTs(105), tb.GetCheckpointTs(), "checkpointTs should be 105")
}

func TestClose(t *testing.T) {
	t.Parallel()

	sink := &mockEventSink{}
	tb := New[*model.SingleTableTxn](sink, &eventsink.TxnEventAppender{})

	tb.AppendRowChangedEvents(getTestRows()...)
	tb.UpdateResolvedTs(model.NewResolvedTs(105))
	require.Len(t, sink.events, 7, "all events should be flushed")
	tb.Close()
	require.Equal(t, pipeline.TableStateStopped, tb.state, "tableState should be closed")
	sink.acknowledge(105)
	require.Len(t, sink.events, 7, "no event should be acked")
}
