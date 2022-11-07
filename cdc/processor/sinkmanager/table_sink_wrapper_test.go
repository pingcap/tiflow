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
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/tablesink"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type mockSink struct{}

func (m *mockSink) WriteEvents(_ ...*eventsink.CallbackableEvent[*model.RowChangedEvent]) error {
	return nil
}

func (m *mockSink) Close() error {
	return nil
}

func createTableSinkWrapper() *tableSinkWrapper {
	changefeedID := model.DefaultChangeFeedID("1")
	tableID := model.TableID(1)
	tableState := tablepb.TableStateReplicating
	innerTableSink := tablesink.New[*model.RowChangedEvent](changefeedID, tableID,
		&mockSink{}, &eventsink.RowChangeEventAppender{}, prometheus.NewCounter(prometheus.CounterOpts{}))
	wrapper := newTableSinkWrapper(
		changefeedID,
		tableID,
		innerTableSink,
		&tableState,
		100,
	)
	return wrapper
}

func TestTableSinkWrapperClose(t *testing.T) {
	t.Parallel()

	wrapper := createTableSinkWrapper()
	require.Equal(t, tablepb.TableStateReplicating, wrapper.getState())
	require.ErrorIs(t, cerror.ErrTableProcessorStoppedSafely, errors.Cause(wrapper.close(context.Background())))
	require.Equal(t, tablepb.TableStateStopped, wrapper.getState(), "table sink state should be stopped")
}

func TestUpdateReceivedSorterResolvedTs(t *testing.T) {
	t.Parallel()

	wrapper := createTableSinkWrapper()
	wrapper.updateReceivedSorterResolvedTs(100)
	require.Equal(t, uint64(100), wrapper.getReceivedSorterResolvedTs())
}

func TestConvertNilRowChangedEvents(t *testing.T) {
	t.Parallel()

	events := []*model.PolymorphicEvent{nil}
	changefeedID := model.DefaultChangeFeedID("1")
	tableID := model.TableID(1)
	enableOldVlaue := false
	result, size, err := convertRowChangedEvents(changefeedID, tableID, enableOldVlaue, events...)
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
	tableID := model.TableID(1)
	enableOldValue := false
	result, size, err := convertRowChangedEvents(changefeedID, tableID, enableOldValue, events...)
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
	tableID := model.TableID(1)
	enableOldValue := true
	result, size, err := convertRowChangedEvents(changefeedID, tableID, enableOldValue, events...)
	require.NoError(t, err)
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
	enableOldValue := false
	result, size, err := convertRowChangedEvents(changefeedID, tableID, enableOldValue, events...)
	require.NoError(t, err)
	require.Equal(t, 2, len(result))
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
	result, size, err = convertRowChangedEvents(changefeedID, tableID, enableOldValue, events...)
	require.NoError(t, err)
	require.Equal(t, 1, len(result))
	require.Equal(t, uint64(216), size)
}
