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

package open

import (
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/internal"
	"github.com/stretchr/testify/require"
)

func TestFormatCol(t *testing.T) {
	t.Parallel()
	row := &messageRow{Update: map[string]internal.Column{"test": {
		Type:  mysql.TypeString,
		Value: "测",
	}}}
	rowEncode, err := row.encode()
	require.Nil(t, err)
	row2 := new(messageRow)
	err = row2.decode(rowEncode)
	require.Nil(t, err)
	require.Equal(t, row, row2)

	row = &messageRow{Update: map[string]internal.Column{"test": {
		Type:  mysql.TypeBlob,
		Value: []byte("测"),
	}}}
	rowEncode, err = row.encode()
	require.Nil(t, err)
	row2 = new(messageRow)
	err = row2.decode(rowEncode)
	require.Nil(t, err)
	require.Equal(t, row, row2)
}

func TestNonBinaryStringCol(t *testing.T) {
	t.Parallel()
	col := &model.Column{
		Name:  "test",
		Type:  mysql.TypeString,
		Value: "value",
	}
	mqCol := internal.Column{}
	mqCol.FromRowChangeColumn(col)
	row := &messageRow{Update: map[string]internal.Column{"test": mqCol}}
	rowEncode, err := row.encode()
	require.Nil(t, err)
	row2 := new(messageRow)
	err = row2.decode(rowEncode)
	require.Nil(t, err)
	require.Equal(t, row, row2)
	mqCol2 := row2.Update["test"]
	col2 := mqCol2.ToRowChangeColumn("test")
	col2.Value = string(col2.Value.([]byte))
	require.Equal(t, col, col2)
}

func TestVarBinaryCol(t *testing.T) {
	t.Parallel()
	col := &model.Column{
		Name:  "test",
		Type:  mysql.TypeString,
		Flag:  model.BinaryFlag,
		Value: []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A},
	}
	mqCol := internal.Column{}
	mqCol.FromRowChangeColumn(col)
	row := &messageRow{Update: map[string]internal.Column{"test": mqCol}}
	rowEncode, err := row.encode()
	require.Nil(t, err)
	row2 := new(messageRow)
	err = row2.decode(rowEncode)
	require.Nil(t, err)
	require.Equal(t, row, row2)
	mqCol2 := row2.Update["test"]
	col2 := mqCol2.ToRowChangeColumn("test")
	require.Equal(t, col, col2)
}
<<<<<<< HEAD:cdc/sink/codec/open/open_protocol_message_test.go
=======

func TestOnlyOutputUpdatedColumn(t *testing.T) {
	t.Parallel()
	cases := []struct {
		pre     interface{}
		updated interface{}
		output  bool
	}{
		{
			pre:     []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A},
			updated: []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A},
			output:  false,
		},
		{
			pre:     uint64(1),
			updated: uint64(1),
			output:  false,
		},
		{
			pre:     nil,
			updated: nil,
			output:  false,
		},
		{
			pre:     float64(6.2),
			updated: float32(6.2),
			output:  true,
		},
		{
			pre:     uint64(1),
			updated: int64(1),
			output:  true,
		},
		{
			pre:     time.Time{},
			updated: time.Time{},
			output:  false,
		},
		{
			pre:     "time.Time{}",
			updated: time.Time{},
			output:  true,
		},
		{
			pre:     "time.Time{}",
			updated: "time.Time{}",
			output:  false,
		},
	}

	for _, cs := range cases {
		col := internal.Column{
			Value: cs.pre,
		}
		col2 := internal.Column{
			Value: cs.updated,
		}
		row := &messageRow{
			Update:     map[string]internal.Column{"test": col2},
			PreColumns: map[string]internal.Column{"test": col},
		}
		_, err := row.encode(true)
		require.Nil(t, err)
		_, ok := row.PreColumns["test"]
		assert.Equal(t, cs.output, ok)
	}
}

func TestRowChanged2MsgOnlyHandleKeyColumns(t *testing.T) {
	t.Parallel()

	insertEvent := &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "schema",
			Table:  "table",
		},
		Columns: []*model.Column{
			{Name: "id", Flag: model.HandleKeyFlag, Type: mysql.TypeLonglong, Value: 1},
			{Name: "a", Type: mysql.TypeLonglong, Value: 1},
		},
	}
	_, value := rowChangeToMsg(insertEvent, true)
	_, ok := value.Update["a"]
	require.True(t, ok)

	updateEvent := &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "schema",
			Table:  "table",
		},
		Columns: []*model.Column{
			{Name: "id", Flag: model.HandleKeyFlag, Type: mysql.TypeLonglong, Value: 1},
			{Name: "a", Type: mysql.TypeLonglong, Value: 2},
		},
		PreColumns: []*model.Column{
			{Name: "id", Flag: model.HandleKeyFlag, Type: mysql.TypeLonglong, Value: 1},
			{Name: "a", Type: mysql.TypeLonglong, Value: 1},
		},
	}
	_, value = rowChangeToMsg(updateEvent, true)
	_, ok = value.PreColumns["a"]
	require.True(t, ok)

	deleteEvent := &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "schema",
			Table:  "table",
		},
		PreColumns: []*model.Column{
			{Name: "id", Flag: model.HandleKeyFlag, Type: mysql.TypeLonglong, Value: 1},
			{Name: "a", Type: mysql.TypeLonglong, Value: 2},
		},
	}
	_, value = rowChangeToMsg(deleteEvent, true)
	_, ok = value.Delete["a"]
	require.False(t, ok)

	_, value = rowChangeToMsg(deleteEvent, false)
	_, ok = value.Delete["a"]
	require.True(t, ok)
}
>>>>>>> 6537ab8fbc (config(ticdc): enable-old-value always false if using avro or csv as the encoding protocol (#9079)):pkg/sink/codec/open/open_protocol_message_test.go
