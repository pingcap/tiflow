// Copyright 2020 PingCAP, Inc.
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

package maxwell

import (
	"context"
	"testing"

	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/stretchr/testify/require"
)

func TestMaxwellBatchCodec(t *testing.T) {
	t.Parallel()
	newEncoder := newBatchEncoder

	rowCases := [][]*model.RowChangedEvent{{{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: 3, Value: 10}},
	}}, {}}
	for _, cs := range rowCases {
		encoder := newEncoder()

		events := make([]*eventsink.RowChangeCallbackableEvent, 0, len(cs))
		for _, c := range cs {
			events = append(events, &eventsink.RowChangeCallbackableEvent{
				Event: c,
			})
		}
		err := encoder.AppendRowChangedEvents(context.Background(), "", events)
		require.NoError(t, err)
		
		messages := encoder.Build()
		if len(cs) == 0 {
			require.Nil(t, messages)
			continue
		}
		require.Len(t, messages, 1)
		require.Equal(t, len(cs), messages[0].GetRowsCount())
	}

	ddlCases := [][]*model.DDLEvent{{{
		CommitTs: 1,
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema: "a", Table: "b",
			},
			TableInfo: &timodel.TableInfo{},
		},
		Query: "create table a",
		Type:  1,
	}}}
	for _, cs := range ddlCases {
		encoder := newEncoder()
		for _, ddl := range cs {
			msg, err := encoder.EncodeDDLEvent(ddl)
			require.Nil(t, err)
			require.NotNil(t, msg)
		}
	}
}

func TestMaxwellAppendRowChangedEventWithCallback(t *testing.T) {
	encoder := newBatchEncoder()
	require.NotNil(t, encoder)

	count := 0

	row := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns: []*model.Column{{
			Name:  "col1",
			Type:  mysql.TypeVarchar,
			Value: []byte("aa"),
		}},
	}

	events := make([]*eventsink.RowChangeCallbackableEvent, 0, 5)
	for i := 1; i <= 5; i++ {
		events = append(events, &eventsink.RowChangeCallbackableEvent{
			Event: row,
			Callback: func() {
				count += i
			},
		})
	}

	// Empty build makes sure that the callback build logic not broken.
	msgs := encoder.Build()
	require.Len(t, msgs, 0, "no message should be built and no panic")

	// Append the events.
	err := encoder.AppendRowChangedEvents(context.Background(), "", events)
	require.NoError(t, err)

	require.Equal(t, 0, count, "nothing should be called")

	msgs = encoder.Build()
	require.Len(t, msgs, 1, "expected one message")
	msgs[0].Callback()
	require.Equal(t, 15, count, "expected all callbacks to be called")
}
