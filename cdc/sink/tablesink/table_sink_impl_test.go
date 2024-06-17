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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	"github.com/pingcap/tiflow/cdc/sink/tablesink/state"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// Assert EventSink implementation
var _ dmlsink.EventSink[*model.SingleTableTxn] = (*mockEventSink)(nil)

type mockEventSink struct {
	dead   chan struct{}
	events []*dmlsink.TxnCallbackableEvent
}

func (m *mockEventSink) WriteEvents(rows ...*dmlsink.TxnCallbackableEvent) error {
	m.events = append(m.events, rows...)
	return nil
}

func (m *mockEventSink) Close() {
	close(m.dead)
}

func (m *mockEventSink) Dead() <-chan struct{} {
	return m.dead
}

func (m *mockEventSink) SchemeOption() (string, bool) {
	return sink.BlackHoleScheme, false
}

// acknowledge the txn events by call the callback function.
func (m *mockEventSink) acknowledge(commitTs uint64) []*dmlsink.TxnCallbackableEvent {
	var droppedEvents []*dmlsink.TxnCallbackableEvent
	i := sort.Search(len(m.events), func(i int) bool {
		return m.events[i].Event.GetCommitTs() > commitTs
	})
	if i == 0 {
		return droppedEvents
	}
	ackedEvents := m.events[:i]
	for _, event := range ackedEvents {
		if event.GetTableSinkState() == state.TableSinkSinking {
			event.Callback()
		} else {
			event.Callback()
			droppedEvents = append(droppedEvents, event)
		}
	}

	// Remove the acked events from the event buffer.
	m.events = append(
		make([]*dmlsink.TxnCallbackableEvent,
			0,
			len(m.events[i:])),
		m.events[i:]...,
	)

	return droppedEvents
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

	sink := &mockEventSink{dead: make(chan struct{})}
	tb := New[*model.SingleTableTxn](
		model.DefaultChangeFeedID("1"), spanz.TableIDToComparableSpan(1), model.Ts(0),
		sink, &dmlsink.TxnEventAppender{},
		pdutil.NewClock4Test(),
		prometheus.NewCounter(prometheus.CounterOpts{}),
		prometheus.NewHistogram(prometheus.HistogramOpts{}))

	require.Equal(t, model.NewResolvedTs(0), tb.maxResolvedTs, "maxResolvedTs should start from 0")
	require.NotNil(t, sink, tb.backendSink, "backendSink should be set")
	require.NotNil(t, tb.progressTracker, "progressTracker should be set")
	require.NotNil(t, tb.eventAppender, "eventAppender should be set")
	require.Equal(t, 0, len(tb.eventBuffer), "eventBuffer should be empty")
	require.Equal(t, state.TableSinkSinking, tb.state, "table sink should be sinking")
}

func TestAppendRowChangedEvents(t *testing.T) {
	t.Parallel()

	sink := &mockEventSink{dead: make(chan struct{})}
	tb := New[*model.SingleTableTxn](
		model.DefaultChangeFeedID("1"), spanz.TableIDToComparableSpan(1), model.Ts(0),
		sink, &dmlsink.TxnEventAppender{},
		pdutil.NewClock4Test(),
		prometheus.NewCounter(prometheus.CounterOpts{}),
		prometheus.NewHistogram(prometheus.HistogramOpts{}))

	tb.AppendRowChangedEvents(getTestRows()...)
	require.Len(t, tb.eventBuffer, 7, "txn event buffer should have 7 txns")
}

func TestUpdateResolvedTs(t *testing.T) {
	t.Parallel()

	sink := &mockEventSink{dead: make(chan struct{})}
	tb := New[*model.SingleTableTxn](
		model.DefaultChangeFeedID("1"), spanz.TableIDToComparableSpan(1), model.Ts(0),
		sink, &dmlsink.TxnEventAppender{},
		pdutil.NewClock4Test(),
		prometheus.NewCounter(prometheus.CounterOpts{}),
		prometheus.NewHistogram(prometheus.HistogramOpts{}))

	tb.AppendRowChangedEvents(getTestRows()...)
	// No event will be flushed.
	err := tb.UpdateResolvedTs(model.NewResolvedTs(100))
	require.Nil(t, err)
	require.Equal(t, model.NewResolvedTs(100), tb.maxResolvedTs, "maxResolvedTs should be updated")
	require.Len(t, tb.eventBuffer, 7, "txn event buffer should have 7 txns")
	require.Len(t, sink.events, 0, "no event should not be flushed")

	// One event will be flushed.
	err = tb.UpdateResolvedTs(model.NewResolvedTs(101))
	require.Nil(t, err)
	require.Equal(t, model.NewResolvedTs(101), tb.maxResolvedTs, "maxResolvedTs should be updated")
	require.Len(t, tb.eventBuffer, 6, "txn event buffer should have 6 txns")
	require.Len(t, sink.events, 1, "one event should be flushed")

	// Two events will be flushed.
	err = tb.UpdateResolvedTs(model.NewResolvedTs(102))
	require.Nil(t, err)
	require.Equal(t, model.NewResolvedTs(102), tb.maxResolvedTs, "maxResolvedTs should be updated")
	require.Len(t, tb.eventBuffer, 4, "txn event buffer should have 4 txns")
	require.Len(t, sink.events, 3, "two events should be flushed")

	// Same resolved ts will not be flushed.
	err = tb.UpdateResolvedTs(model.NewResolvedTs(102))
	require.Nil(t, err)
	require.Equal(
		t,
		model.NewResolvedTs(102),
		tb.maxResolvedTs,
		"maxResolvedTs should not be updated",
	)
	require.Len(t, tb.eventBuffer, 4, "txn event buffer should still have 4 txns")
	require.Len(t, sink.events, 3, "no event should be flushed")

	// All events will be flushed.
	err = tb.UpdateResolvedTs(model.NewResolvedTs(105))
	require.Nil(t, err)
	require.Equal(t, model.NewResolvedTs(105), tb.maxResolvedTs, "maxResolvedTs should be updated")
	require.Len(t, tb.eventBuffer, 0, "txn event buffer should be empty")
	require.Len(t, sink.events, 7, "all events should be flushed")
}

func TestGetCheckpointTs(t *testing.T) {
	t.Parallel()

	sink := &mockEventSink{dead: make(chan struct{})}
	tb := New[*model.SingleTableTxn](
		model.DefaultChangeFeedID("1"), spanz.TableIDToComparableSpan(1), model.Ts(0),
		sink, &dmlsink.TxnEventAppender{},
		pdutil.NewClock4Test(),
		prometheus.NewCounter(prometheus.CounterOpts{}),
		prometheus.NewHistogram(prometheus.HistogramOpts{}))

	tb.AppendRowChangedEvents(getTestRows()...)
	require.Equal(t, model.NewResolvedTs(0), tb.GetCheckpointTs(), "checkpointTs should be 0")
	require.Equal(t, tb.lastSyncedTs.getLastSyncedTs(), uint64(0), "lastSyncedTs should be not updated")

	// One event will be flushed.
	err := tb.UpdateResolvedTs(model.NewResolvedTs(101))
	require.Nil(t, err)
	require.Equal(t, model.NewResolvedTs(0), tb.GetCheckpointTs(), "checkpointTs should be 0")
	sink.acknowledge(101)
	require.Equal(t, model.NewResolvedTs(101), tb.GetCheckpointTs(), "checkpointTs should be 101")
	require.Equal(t, tb.lastSyncedTs.getLastSyncedTs(), uint64(101), "lastSyncedTs should be the same as the flushed event")

	// Flush all events.
	err = tb.UpdateResolvedTs(model.NewResolvedTs(105))
	require.Nil(t, err)
	require.Equal(t, model.NewResolvedTs(101), tb.GetCheckpointTs(), "checkpointTs should be 101")
	require.Equal(t, tb.lastSyncedTs.getLastSyncedTs(), uint64(101), "lastSyncedTs should be not updated")

	// Only acknowledge some events.
	sink.acknowledge(102)
	require.Equal(
		t,
		model.NewResolvedTs(101),
		tb.GetCheckpointTs(),
		"checkpointTs should still be 101",
	)
	require.Equal(t, tb.lastSyncedTs.getLastSyncedTs(), uint64(102), "lastSyncedTs should be updated")

	// Ack all events.
	sink.acknowledge(105)
	require.Equal(t, model.NewResolvedTs(105), tb.GetCheckpointTs(), "checkpointTs should be 105")
	require.Equal(t, tb.lastSyncedTs.getLastSyncedTs(), uint64(105), "lastSyncedTs should be updated")
}

func TestClose(t *testing.T) {
	t.Parallel()

	sink := &mockEventSink{dead: make(chan struct{})}
	tb := New[*model.SingleTableTxn](
		model.DefaultChangeFeedID("1"), spanz.TableIDToComparableSpan(1), model.Ts(0),
		sink, &dmlsink.TxnEventAppender{},
		pdutil.NewClock4Test(),
		prometheus.NewCounter(prometheus.CounterOpts{}),
		prometheus.NewHistogram(prometheus.HistogramOpts{}))

	tb.AppendRowChangedEvents(getTestRows()...)
	err := tb.UpdateResolvedTs(model.NewResolvedTs(105))
	require.Nil(t, err)
	require.Len(t, sink.events, 7, "all events should be flushed")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		tb.Close()
		wg.Done()
	}()
	require.Eventually(t, func() bool {
		return state.TableSinkStopping == tb.state.Load()
	}, time.Second, time.Millisecond*10, "table should be stopping")
	droppedEvents := sink.acknowledge(105)
	require.Len(t, droppedEvents, 7, "all events should be dropped")
	wg.Wait()
	require.Eventually(t, func() bool {
		return state.TableSinkStopped == tb.state.Load()
	}, time.Second, time.Millisecond*10, "table should be stopped")
}

func TestOperationsAfterClose(t *testing.T) {
	t.Parallel()

	sink := &mockEventSink{dead: make(chan struct{})}
	tb := New[*model.SingleTableTxn](
		model.DefaultChangeFeedID("1"), spanz.TableIDToComparableSpan(1), model.Ts(0),
		sink, &dmlsink.TxnEventAppender{},
		pdutil.NewClock4Test(),
		prometheus.NewCounter(prometheus.CounterOpts{}),
		prometheus.NewHistogram(prometheus.HistogramOpts{}))

	require.True(t, tb.AsyncClose())

	tb.AppendRowChangedEvents(getTestRows()...)
	err := tb.UpdateResolvedTs(model.NewResolvedTs(105))
	require.Nil(t, err)
}

func TestCloseCancellable(t *testing.T) {
	t.Parallel()

	sink := &mockEventSink{dead: make(chan struct{})}
	tb := New[*model.SingleTableTxn](
		model.DefaultChangeFeedID("1"), spanz.TableIDToComparableSpan(1), model.Ts(0),
		sink, &dmlsink.TxnEventAppender{},
		pdutil.NewClock4Test(),
		prometheus.NewCounter(prometheus.CounterOpts{}),
		prometheus.NewHistogram(prometheus.HistogramOpts{}))

	tb.AppendRowChangedEvents(getTestRows()...)
	err := tb.UpdateResolvedTs(model.NewResolvedTs(105))
	require.Nil(t, err)
	require.Len(t, sink.events, 7, "all events should be flushed")

	go func() {
		time.Sleep(time.Millisecond * 10)
		sink.Close()
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		tb.Close()
		wg.Done()
	}()
	wg.Wait()
	require.Eventually(t, func() bool {
		return state.TableSinkStopped == tb.state.Load()
	}, time.Second, time.Millisecond*10, "table should be stopped")
}

func TestCloseReentrant(t *testing.T) {
	t.Parallel()

	sink := &mockEventSink{dead: make(chan struct{})}
	tb := New[*model.SingleTableTxn](
		model.DefaultChangeFeedID("1"), spanz.TableIDToComparableSpan(1), model.Ts(0),
		sink, &dmlsink.TxnEventAppender{},
		pdutil.NewClock4Test(),
		prometheus.NewCounter(prometheus.CounterOpts{}),
		prometheus.NewHistogram(prometheus.HistogramOpts{}))

	tb.AppendRowChangedEvents(getTestRows()...)
	err := tb.UpdateResolvedTs(model.NewResolvedTs(105))
	require.Nil(t, err)
	require.Len(t, sink.events, 7, "all events should be flushed")

	go func() {
		time.Sleep(time.Millisecond * 10)
		sink.Close()
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		tb.Close()
		wg.Done()
	}()
	wg.Wait()
	require.Eventually(t, func() bool {
		return state.TableSinkStopped == tb.state.Load()
	}, 5*time.Second, time.Millisecond*10, "table should be stopped")
	tb.Close()
}

// TestCheckpointTsFrozenWhenStopping make sure wo do not update checkpoint
// ts when it is stopping.
func TestCheckpointTsFrozenWhenStopping(t *testing.T) {
	t.Parallel()

	sink := &mockEventSink{dead: make(chan struct{})}
	tb := New[*model.SingleTableTxn](
		model.DefaultChangeFeedID("1"), spanz.TableIDToComparableSpan(1), model.Ts(0),
		sink, &dmlsink.TxnEventAppender{},
		pdutil.NewClock4Test(),
		prometheus.NewCounter(prometheus.CounterOpts{}),
		prometheus.NewHistogram(prometheus.HistogramOpts{}))

	tb.AppendRowChangedEvents(getTestRows()...)
	err := tb.UpdateResolvedTs(model.NewResolvedTs(105))
	require.Nil(t, err)
	require.Len(t, sink.events, 7, "all events should be flushed")

	// Table sink close should return even if callbacks are not called,
	// because the backend sink is closed.
	sink.Close()
	tb.Close()

	require.Equal(t, state.TableSinkStopped, tb.state.Load())

	currentTs := tb.GetCheckpointTs()
	sink.acknowledge(105)
	require.Equal(t, currentTs, tb.GetCheckpointTs(), "checkpointTs should not be updated")
	require.Equal(t, tb.lastSyncedTs.getLastSyncedTs(), uint64(105), "lastSyncedTs should not change")
}
