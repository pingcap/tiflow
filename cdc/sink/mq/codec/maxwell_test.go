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

package codec

import (
	"context"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestMaxwellEventBatchCodec(t *testing.T) {
	t.Parallel()
	newEncoder := NewMaxwellEventBatchEncoder

	rowCases := [][]*model.RowChangedEvent{{{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: 3, Value: 10}},
	}}, {}}
	for _, cs := range rowCases {
		encoder := newEncoder()
		for _, row := range cs {
			err := encoder.AppendRowChangedEvent(context.Background(), "", row)
			require.Nil(t, err)
		}
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
		TableInfo: &model.SimpleTableInfo{
			Schema: "a", Table: "b",
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

func TestMaxwellFormatCol(t *testing.T) {
	t.Parallel()
	row := &maxwellMessage{
		Ts:       1,
		Database: "a",
		Table:    "b",
		Type:     "delete",
		Xid:      1,
		Xoffset:  1,
		Position: "",
		Gtid:     "",
		Data: map[string]interface{}{
			"id": "1",
		},
	}
	rowEncode, err := row.Encode()
	require.Nil(t, err)
	require.NotNil(t, rowEncode)
}
