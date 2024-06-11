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

package sinkmanager

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/tablesink"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

type mockSink struct {
	mu         sync.Mutex
	events     []*eventsink.CallbackableEvent[*model.RowChangedEvent]
	writeTimes int
}

func newMockSink() *mockSink {
	return &mockSink{
		events: make([]*eventsink.CallbackableEvent[*model.RowChangedEvent], 0),
	}
}

func (m *mockSink) WriteEvents(events ...*eventsink.CallbackableEvent[*model.RowChangedEvent]) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writeTimes++
	m.events = append(m.events, events...)
	return nil
}

func (m *mockSink) SchemeOption() (string, bool) {
	return sink.BlackHoleScheme, false
}

func (m *mockSink) GetEvents() []*eventsink.CallbackableEvent[*model.RowChangedEvent] {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.events
}

func (m *mockSink) GetWriteTimes() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.writeTimes
}

func (m *mockSink) Scheme() string {
	return sink.BlackHoleScheme
}

func (m *mockSink) Close() {}

func (m *mockSink) Dead() <-chan struct{} {
	return make(chan struct{})
}

type mockDelayedTableSink struct {
	tablesink.TableSink

	closeCnt    int
	closeTarget int
}

func (t *mockDelayedTableSink) AsyncClose() bool {
	t.closeCnt++
	if t.closeCnt >= t.closeTarget {
		t.TableSink.Close()
		return true
	}
	return false
}

//nolint:unparam
func createTableSinkWrapper(changefeedID model.ChangeFeedID, tableID model.TableID) (*tableSinkWrapper, *mockSink) {
	tableState := tablepb.TableStatePreparing
	sink := newMockSink()
	innerTableSink := tablesink.New[*model.RowChangedEvent](
		changefeedID, tableID, model.Ts(0),
		sink, &eventsink.RowChangeEventAppender{},
		pdutil.NewClock4Test(),
		prometheus.NewCounter(prometheus.CounterOpts{}),
		prometheus.NewHistogram(prometheus.HistogramOpts{}))
	wrapper := newTableSinkWrapper(
		changefeedID,
		tableID,
		func() (tablesink.TableSink, uint64) { return innerTableSink, 1 },
		tableState,
		0,
		100,
		func(_ context.Context) (model.Ts, error) { return math.MaxUint64, nil },
	)
	wrapper.tableSink.s, wrapper.tableSink.version = wrapper.tableSinkCreater()
	return wrapper, sink
}

func TestTableSinkWrapperStop(t *testing.T) {
	t.Parallel()

	wrapper, _ := createTableSinkWrapper(model.DefaultChangeFeedID("1"), 1)
	wrapper.tableSink.s = &mockDelayedTableSink{
		TableSink:   wrapper.tableSink.s,
		closeCnt:    0,
		closeTarget: 10,
	}
	require.Equal(t, tablepb.TableStatePreparing, wrapper.getState())

	closeCnt := 0
	for {
		closeCnt++
		if wrapper.asyncStop() {
			break
		}
	}
	require.Equal(t, tablepb.TableStateStopped, wrapper.getState(), "table sink state should be stopped")
	require.Equal(t, 10, closeCnt, "table sink should be closed 10 times")
}

func TestUpdateReceivedSorterResolvedTs(t *testing.T) {
	t.Parallel()

	wrapper, _ := createTableSinkWrapper(model.DefaultChangeFeedID("1"), 1)
	wrapper.updateReceivedSorterResolvedTs(100)
	require.Equal(t, uint64(100), wrapper.getReceivedSorterResolvedTs())
	require.Equal(t, tablepb.TableStatePrepared, wrapper.getState())
}

func TestConvertNilRowChangedEvents(t *testing.T) {
	t.Parallel()

	events := []*model.PolymorphicEvent{nil}
	changefeedID := model.DefaultChangeFeedID("1")
	tableID := model.TableID(1)
	result, size := handleRowChangedEvents(changefeedID, tableID, events...)
	require.Equal(t, 0, len(result))
	require.Equal(t, uint64(0), size)
}

func TestConvertEmptyRowChangedEvents(t *testing.T) {
	t.Parallel()

	events := []*model.PolymorphicEvent{
		{
			StartTs: 1,
			CRTs:    2,
			Row: &model.RowChangedEvent{
				StartTs:  1,
				CommitTs: 2,
			},
		},
	}
	changefeedID := model.DefaultChangeFeedID("1")
	tableID := model.TableID(1)
	result, size := handleRowChangedEvents(changefeedID, tableID, events...)
	require.Equal(t, 0, len(result))
	require.Equal(t, uint64(0), size)
}

func TestConvertRowChangedEventsWhenEnableOldValue(t *testing.T) {
	t.Parallel()

	columns := []*model.Column{
		{
			Name:  "col1",
			Flag:  model.BinaryFlag,
			Value: "col1-value-updated",
		},
		{
			Name:  "col2",
			Flag:  model.HandleKeyFlag,
			Value: "col2-value-updated",
		},
	}
	preColumns := []*model.Column{
		{
			Name:  "col1",
			Flag:  model.BinaryFlag,
			Value: "col1-value",
		},
		{
			Name:  "col2",
			Flag:  model.HandleKeyFlag,
			Value: "col2-value",
		},
	}

	events := []*model.PolymorphicEvent{
		{
			CRTs:  1,
			RawKV: &model.RawKVEntry{OpType: model.OpTypePut},
			Row: &model.RowChangedEvent{
				CommitTs:   1,
				Columns:    columns,
				PreColumns: preColumns,
				Table: &model.TableName{
					Schema: "test",
					Table:  "test",
				},
			},
		},
	}
	changefeedID := model.DefaultChangeFeedID("1")
	tableID := model.TableID(1)
	result, size := handleRowChangedEvents(changefeedID, tableID, events...)
	require.Equal(t, 1, len(result))
	require.Equal(t, uint64(216), size)
}

func TestConvertRowChangedEventsWhenDisableOldValue(t *testing.T) {
	t.Parallel()

	// Update handle key.
	columns := []*model.Column{
		{
			Name:  "col1",
			Flag:  model.BinaryFlag,
			Value: "col1-value-updated",
		},
		{
			Name:  "col2",
			Flag:  model.HandleKeyFlag,
			Value: "col2-value-updated",
		},
	}
	preColumns := []*model.Column{
		{
			Name:  "col1",
			Flag:  model.BinaryFlag,
			Value: "col1-value",
		},
		{
			Name:  "col2",
			Flag:  model.HandleKeyFlag,
			Value: "col2-value",
		},
	}

	events := []*model.PolymorphicEvent{
		{
			CRTs:  1,
			RawKV: &model.RawKVEntry{OpType: model.OpTypePut},
			Row: &model.RowChangedEvent{
				CommitTs:   1,
				Columns:    columns,
				PreColumns: preColumns,
				Table: &model.TableName{
					Schema: "test",
					Table:  "test",
				},
			},
		},
	}
	changefeedID := model.DefaultChangeFeedID("1")
	tableID := model.TableID(1)
	result, size := handleRowChangedEvents(changefeedID, tableID, events...)
	require.Equal(t, 1, len(result))
	require.Equal(t, uint64(216), size)

	// Update non-handle key.
	columns = []*model.Column{
		{
			Name:  "col1",
			Flag:  model.BinaryFlag,
			Value: "col1-value-updated",
		},
		{
			Name:  "col2",
			Flag:  model.HandleKeyFlag,
			Value: "col2-value",
		},
	}
	preColumns = []*model.Column{
		{
			Name:  "col1",
			Flag:  model.BinaryFlag,
			Value: "col1-value",
		},
		{
			Name:  "col2",
			Flag:  model.HandleKeyFlag,
			Value: "col2-value",
		},
	}

	events = []*model.PolymorphicEvent{
		{
			CRTs:  1,
			RawKV: &model.RawKVEntry{OpType: model.OpTypePut},
			Row: &model.RowChangedEvent{
				CommitTs:   1,
				Columns:    columns,
				PreColumns: preColumns,
				Table: &model.TableName{
					Schema: "test",
					Table:  "test",
				},
			},
		},
	}
	result, size = handleRowChangedEvents(changefeedID, tableID, events...)
	require.Equal(t, 1, len(result))
	require.Equal(t, uint64(216), size)
}

func TestGetUpperBoundTs(t *testing.T) {
	t.Parallel()
	wrapper, _ := createTableSinkWrapper(
		model.DefaultChangeFeedID("1"), 1)
	// Test when there is no resolved ts.
	wrapper.barrierTs.Store(uint64(10))
	wrapper.receivedSorterResolvedTs.Store(uint64(11))
	require.Equal(t, uint64(10), wrapper.getUpperBoundTs())

	wrapper.barrierTs.Store(uint64(12))
	require.Equal(t, uint64(11), wrapper.getUpperBoundTs())
}

func TestTableSinkWrapperSinkVersion(t *testing.T) {
	t.Parallel()

	innerTableSink := tablesink.New[*model.RowChangedEvent](
		model.ChangeFeedID{}, 1, model.Ts(0),
		newMockSink(), &eventsink.RowChangeEventAppender{},
		pdutil.NewClock4Test(),
		prometheus.NewCounter(prometheus.CounterOpts{}),
		prometheus.NewHistogram(prometheus.HistogramOpts{}),
	)
	version := new(uint64)

	wrapper := newTableSinkWrapper(
		model.DefaultChangeFeedID("1"),
		1,
		func() (tablesink.TableSink, uint64) { return nil, 0 },
		tablepb.TableStatePrepared,
		model.Ts(10),
		model.Ts(20),
		func(_ context.Context) (model.Ts, error) { return math.MaxUint64, nil },
	)

	require.False(t, wrapper.initTableSink())

	wrapper.tableSinkCreater = func() (tablesink.TableSink, uint64) {
		*version += 1
		return innerTableSink, *version
	}

	require.True(t, wrapper.initTableSink())
	require.Equal(t, wrapper.tableSink.version, uint64(1))

	require.True(t, wrapper.asyncCloseTableSink())

	wrapper.doTableSinkClear()
	require.Nil(t, wrapper.tableSink.s)
	require.Equal(t, wrapper.tableSink.version, uint64(0))

	require.True(t, wrapper.initTableSink())
	require.Equal(t, wrapper.tableSink.version, uint64(2))

	wrapper.closeTableSink()

	wrapper.doTableSinkClear()
	require.Nil(t, wrapper.tableSink.s)
	require.Equal(t, wrapper.tableSink.version, uint64(0))
}

func TestTableSinkWrapperSinkInner(t *testing.T) {
	t.Parallel()

	innerTableSink := tablesink.New[*model.RowChangedEvent](
		model.ChangeFeedID{}, 1, model.Ts(0),
		newMockSink(), &eventsink.RowChangeEventAppender{},
		pdutil.NewClock4Test(),
		prometheus.NewCounter(prometheus.CounterOpts{}),
		prometheus.NewHistogram(prometheus.HistogramOpts{}),
	)
	version := new(uint64)

	wrapper := newTableSinkWrapper(
		model.DefaultChangeFeedID("1"),
		1,
		func() (tablesink.TableSink, uint64) {
			*version += 1
			return innerTableSink, *version
		},
		tablepb.TableStatePrepared,
		oracle.GoTimeToTS(time.Now()),
		oracle.GoTimeToTS(time.Now().Add(10000*time.Second)),
		func(_ context.Context) (model.Ts, error) { return math.MaxUint64, nil },
	)

	require.True(t, wrapper.initTableSink())

	wrapper.closeAndClearTableSink()

	// Shouldn't be stuck because version is 0.
	require.Equal(t, wrapper.tableSink.version, uint64(0))
	isStuck, _ := wrapper.sinkMaybeStuck(100 * time.Millisecond)
	require.False(t, isStuck)

	// Shouldn't be stuck because tableSink.advanced is just updated.
	require.True(t, wrapper.initTableSink())
	isStuck, _ = wrapper.sinkMaybeStuck(100 * time.Millisecond)
	require.False(t, isStuck)

	// Shouldn't be stuck because upperbound hasn't been advanced.
	time.Sleep(200 * time.Millisecond)
	isStuck, _ = wrapper.sinkMaybeStuck(100 * time.Millisecond)
	require.False(t, isStuck)

	// Shouldn't be stuck because `getCheckpointTs` will update tableSink.advanced.
	nowTs := oracle.GoTimeToTS(time.Now())
	wrapper.updateReceivedSorterResolvedTs(nowTs)
	wrapper.barrierTs.Store(nowTs)
	isStuck, _ = wrapper.sinkMaybeStuck(100 * time.Millisecond)
	require.False(t, isStuck)

	time.Sleep(200 * time.Millisecond)
	nowTs = oracle.GoTimeToTS(time.Now())
	wrapper.updateReceivedSorterResolvedTs(nowTs)
	wrapper.barrierTs.Store(nowTs)
	wrapper.updateResolvedTs(model.NewResolvedTs(nowTs))
	isStuck, _ = wrapper.sinkMaybeStuck(100 * time.Millisecond)
	require.True(t, isStuck)
}
