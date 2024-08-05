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
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	"github.com/pingcap/tiflow/cdc/sink/tablesink"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

type mockSink struct {
	mu         sync.Mutex
	events     []*dmlsink.CallbackableEvent[*model.RowChangedEvent]
	writeTimes int
}

func newMockSink() *mockSink {
	return &mockSink{
		events: make([]*dmlsink.CallbackableEvent[*model.RowChangedEvent], 0),
	}
}

func (m *mockSink) WriteEvents(events ...*dmlsink.CallbackableEvent[*model.RowChangedEvent]) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writeTimes++
	m.events = append(m.events, events...)
	return nil
}

func (m *mockSink) SchemeOption() (string, bool) {
	return sink.BlackHoleScheme, false
}

func (m *mockSink) GetEvents() []*dmlsink.CallbackableEvent[*model.RowChangedEvent] {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.events
}

func (m *mockSink) GetWriteTimes() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.writeTimes
}

func (m *mockSink) Close() {}

func (m *mockSink) Dead() <-chan struct{} {
	return make(chan struct{})
}

func (m *mockSink) AckAllEvents() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.events {
		e.Callback()
	}
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
func createTableSinkWrapper(
	changefeedID model.ChangeFeedID, span tablepb.Span,
) (*tableSinkWrapper, *mockSink) {
	tableState := tablepb.TableStatePreparing
	sink := newMockSink()
	innerTableSink := tablesink.New[*model.RowChangedEvent](
		changefeedID, span, model.Ts(0),
		sink, &dmlsink.RowChangeEventAppender{},
		pdutil.NewClock4Test(),
		prometheus.NewCounter(prometheus.CounterOpts{}),
		prometheus.NewHistogram(prometheus.HistogramOpts{}))
	wrapper := newTableSinkWrapper(
		changefeedID,
		span,
		func() (tablesink.TableSink, uint64) { return innerTableSink, 1 },
		tableState,
		0,
		100,
		func(_ context.Context) (model.Ts, error) { return math.MaxUint64, nil },
	)
	wrapper.tableSink.s, wrapper.tableSink.version = wrapper.tableSinkCreator()
	return wrapper, sink
}

func TestTableSinkWrapperStop(t *testing.T) {
	t.Parallel()

	wrapper, _ := createTableSinkWrapper(
		model.DefaultChangeFeedID("1"), spanz.TableIDToComparableSpan(1))
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

	wrapper, _ := createTableSinkWrapper(
		model.DefaultChangeFeedID("1"), spanz.TableIDToComparableSpan(1))
	wrapper.updateReceivedSorterResolvedTs(100)
	require.Equal(t, uint64(100), wrapper.getReceivedSorterResolvedTs())
	require.Equal(t, tablepb.TableStatePrepared, wrapper.getState())
}

func TestHandleNilRowChangedEvents(t *testing.T) {
	t.Parallel()

	events := []*model.PolymorphicEvent{nil}
	changefeedID := model.DefaultChangeFeedID("1")
	span := spanz.TableIDToComparableSpan(1)
	result, size := handleRowChangedEvents(changefeedID, span, events...)
	require.Equal(t, 0, len(result))
	require.Equal(t, uint64(0), size)
}

func TestHandleEmptyRowChangedEvents(t *testing.T) {
	t.Parallel()

	events := []*model.PolymorphicEvent{
		{
			StartTs: 1,
			CRTs:    2,
			// the row had no columns
			Row: &model.RowChangedEvent{
				StartTs:  1,
				CommitTs: 2,
			},
		},
	}
	changefeedID := model.DefaultChangeFeedID("1")
	span := spanz.TableIDToComparableSpan(1)

	result, size := handleRowChangedEvents(changefeedID, span, events...)
	require.Equal(t, 0, len(result))
	require.Equal(t, uint64(0), size)
}

func TestHandleRowChangedEventNormalEvent(t *testing.T) {
	t.Parallel()

	// Update non-unique key.
	columns := []*model.Column{
		{
			Name:  "col1",
			Flag:  model.BinaryFlag,
			Value: "col1-value",
		},
		{
			Name:  "col2",
			Flag:  model.HandleKeyFlag | model.UniqueKeyFlag,
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
			Flag:  model.HandleKeyFlag | model.UniqueKeyFlag,
			Value: "col2-value",
		},
	}
	tableInfo := model.BuildTableInfo("test", "test", columns, nil)
	events := []*model.PolymorphicEvent{
		{
			CRTs:  1,
			RawKV: &model.RawKVEntry{OpType: model.OpTypePut},
			Row: &model.RowChangedEvent{
				CommitTs:   1,
				TableInfo:  tableInfo,
				Columns:    model.Columns2ColumnDatas(columns, tableInfo),
				PreColumns: model.Columns2ColumnDatas(preColumns, tableInfo),
			},
		},
	}
	changefeedID := model.DefaultChangeFeedID("1")
	span := spanz.TableIDToComparableSpan(1)
	result, size := handleRowChangedEvents(changefeedID, span, events...)
	require.Equal(t, 1, len(result))
	require.Equal(t, uint64(testEventSize), size)
}

func TestGetUpperBoundTs(t *testing.T) {
	t.Parallel()
	wrapper, _ := createTableSinkWrapper(
		model.DefaultChangeFeedID("1"), spanz.TableIDToComparableSpan(1))
	// Test when there is no resolved ts.
	wrapper.barrierTs.Store(uint64(10))
	wrapper.receivedSorterResolvedTs.Store(uint64(11))
	require.Equal(t, uint64(10), wrapper.getUpperBoundTs())

	wrapper.barrierTs.Store(uint64(12))
	require.Equal(t, uint64(11), wrapper.getUpperBoundTs())
}

func TestNewTableSinkWrapper(t *testing.T) {
	t.Parallel()
	wrapper := newTableSinkWrapper(
		model.DefaultChangeFeedID("1"),
		spanz.TableIDToComparableSpan(1),
		nil,
		tablepb.TableStatePrepared,
		model.Ts(10),
		model.Ts(20),
		func(_ context.Context) (model.Ts, error) { return math.MaxUint64, nil },
	)
	require.NotNil(t, wrapper)
	require.Equal(t, uint64(10), wrapper.getUpperBoundTs())
	require.Equal(t, uint64(10), wrapper.getReceivedSorterResolvedTs())
	checkpointTs := wrapper.getCheckpointTs()
	require.Equal(t, uint64(10), checkpointTs.ResolvedMark())
}

func TestTableSinkWrapperSinkVersion(t *testing.T) {
	t.Parallel()

	innerTableSink := tablesink.New[*model.RowChangedEvent](
		model.ChangeFeedID{}, tablepb.Span{}, model.Ts(0),
		newMockSink(), &dmlsink.RowChangeEventAppender{},
		pdutil.NewClock4Test(),
		prometheus.NewCounter(prometheus.CounterOpts{}),
		prometheus.NewHistogram(prometheus.HistogramOpts{}),
	)
	version := new(uint64)

	wrapper := newTableSinkWrapper(
		model.DefaultChangeFeedID("1"),
		spanz.TableIDToComparableSpan(1),
		func() (tablesink.TableSink, uint64) { return nil, 0 },
		tablepb.TableStatePrepared,
		model.Ts(10),
		model.Ts(20),
		func(_ context.Context) (model.Ts, error) { return math.MaxUint64, nil },
	)

	require.False(t, wrapper.initTableSink())

	wrapper.tableSinkCreator = func() (tablesink.TableSink, uint64) {
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
		model.ChangeFeedID{}, tablepb.Span{}, model.Ts(0),
		newMockSink(), &dmlsink.RowChangeEventAppender{},
		pdutil.NewClock4Test(),
		prometheus.NewCounter(prometheus.CounterOpts{}),
		prometheus.NewHistogram(prometheus.HistogramOpts{}),
	)
	version := new(uint64)

	wrapper := newTableSinkWrapper(
		model.DefaultChangeFeedID("1"),
		spanz.TableIDToComparableSpan(1),
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
