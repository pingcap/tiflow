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

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	"github.com/pingcap/tiflow/cdc/sink/tablesink"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
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

//nolint:unparam
func createTableSinkWrapper(
	changefeedID model.ChangeFeedID, span tablepb.Span,
) (*tableSinkWrapper, *mockSink) {
	tableState := tablepb.TableStatePreparing
	sink := newMockSink()
	innerTableSink := tablesink.New[*model.RowChangedEvent](
		changefeedID, span, model.Ts(0),
		sink, &dmlsink.RowChangeEventAppender{}, prometheus.NewCounter(prometheus.CounterOpts{}))
	wrapper := newTableSinkWrapper(
		changefeedID,
		span,
		func() tablesink.TableSink { return innerTableSink },
		tableState,
		0,
		100,
		func(_ context.Context) (model.Ts, error) { return math.MaxUint64, nil },
	)
	wrapper.tableSink = wrapper.tableSinkCreater()
	return wrapper, sink
}

func TestTableSinkWrapperClose(t *testing.T) {
	t.Parallel()

	wrapper, _ := createTableSinkWrapper(
		model.DefaultChangeFeedID("1"), spanz.TableIDToComparableSpan(1))
	require.Equal(t, tablepb.TableStatePreparing, wrapper.getState())
	wrapper.stop()
	require.Equal(t, tablepb.TableStateStopped, wrapper.getState(), "table sink state should be stopped")
}

func TestUpdateReceivedSorterResolvedTs(t *testing.T) {
	t.Parallel()

	wrapper, _ := createTableSinkWrapper(
		model.DefaultChangeFeedID("1"), spanz.TableIDToComparableSpan(1))
	wrapper.updateReceivedSorterResolvedTs(100)
	require.Equal(t, uint64(100), wrapper.getReceivedSorterResolvedTs())
	require.Equal(t, tablepb.TableStatePrepared, wrapper.getState())
}

func TestConvertNilRowChangedEvents(t *testing.T) {
	t.Parallel()

	events := []*model.PolymorphicEvent{nil}
	changefeedID := model.DefaultChangeFeedID("1")
	span := spanz.TableIDToComparableSpan(1)
	enableOldVlaue := false
	result, size, err := convertRowChangedEvents(changefeedID, span, enableOldVlaue, events...)
	require.NoError(t, err)
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
	span := spanz.TableIDToComparableSpan(1)
	enableOldValue := false
	result, size, err := convertRowChangedEvents(changefeedID, span, enableOldValue, events...)
	require.NoError(t, err)
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
	span := spanz.TableIDToComparableSpan(1)
	enableOldValue := true
	result, size, err := convertRowChangedEvents(changefeedID, span, enableOldValue, events...)
	require.NoError(t, err)
	require.Equal(t, 1, len(result))
	require.Equal(t, uint64(224), size)
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
	span := spanz.TableIDToComparableSpan(1)
	enableOldValue := false
	result, size, err := convertRowChangedEvents(changefeedID, span, enableOldValue, events...)
	require.NoError(t, err)
	require.Equal(t, 2, len(result))
	require.Equal(t, uint64(224), size)

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
	result, size, err = convertRowChangedEvents(changefeedID, span, enableOldValue, events...)
	require.NoError(t, err)
	require.Equal(t, 1, len(result))
	require.Equal(t, uint64(224), size)
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
	require.Equal(t, uint64(10), wrapper.getCheckpointTs().ResolvedMark())
}
