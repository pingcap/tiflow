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
	"time"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatCol(t *testing.T) {
	t.Parallel()
	row := &messageRow{Update: map[string]internal.Column{"test": {
		Type:  mysql.TypeString,
		Value: "测",
	}}}
	rowEncode, err := row.encode(false)
	require.Nil(t, err)
	row2 := new(messageRow)
	err = row2.decode(rowEncode)
	require.Nil(t, err)
	require.Equal(t, row, row2)

	row = &messageRow{Update: map[string]internal.Column{"test": {
		Type:  mysql.TypeBlob,
		Value: []byte("测"),
	}}}
	rowEncode, err = row.encode(false)
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
	rowEncode, err := row.encode(false)
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
	rowEncode, err := row.encode(false)
	require.Nil(t, err)
	row2 := new(messageRow)
	err = row2.decode(rowEncode)
	require.Nil(t, err)
	require.Equal(t, row, row2)
	mqCol2 := row2.Update["test"]
	col2 := mqCol2.ToRowChangeColumn("test")
	require.Equal(t, col, col2)
}

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
	_, value, err := rowChangeToMsg(insertEvent, true, false)
	require.NoError(t, err)
	_, ok := value.Update["a"]
	require.True(t, ok)

	key, value, err := rowChangeToMsg(insertEvent, false, true)
	require.NoError(t, err)
	require.True(t, key.OnlyHandleKey)
	_, ok = value.Update["a"]
	require.False(t, ok)

	insertEventNoHandleKey := &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "schema",
			Table:  "table",
		},
		Columns: []*model.Column{
			{Name: "id", Type: mysql.TypeLonglong, Value: 1},
			{Name: "a", Type: mysql.TypeLonglong, Value: 1},
		},
	}
	_, _, err = rowChangeToMsg(insertEventNoHandleKey, false, true)
	require.Error(t, err, cerror.ErrOpenProtocolCodecInvalidData)

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
	_, value, err = rowChangeToMsg(updateEvent, true, false)
	require.NoError(t, err)
	_, ok = value.PreColumns["a"]
	require.True(t, ok)

	key, value, err = rowChangeToMsg(updateEvent, false, true)
	require.NoError(t, err)
	require.True(t, key.OnlyHandleKey)
	_, ok = value.PreColumns["a"]
	require.False(t, ok)
	_, ok = value.Update["a"]
	require.False(t, ok)

	updateEventNoHandleKey := &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "schema",
			Table:  "table",
		},
		Columns: []*model.Column{
			{Name: "id", Type: mysql.TypeLonglong, Value: 1},
			{Name: "a", Type: mysql.TypeLonglong, Value: 2},
		},
		PreColumns: []*model.Column{
			{Name: "id", Flag: model.HandleKeyFlag, Type: mysql.TypeLonglong, Value: 1},
			{Name: "a", Type: mysql.TypeLonglong, Value: 1},
		},
	}
	_, _, err = rowChangeToMsg(updateEventNoHandleKey, false, true)
	require.Error(t, err, cerror.ErrOpenProtocolCodecInvalidData)

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
	_, value, err = rowChangeToMsg(deleteEvent, true, false)
	require.NoError(t, err)
	_, ok = value.Delete["a"]
	require.False(t, ok)

	_, value, err = rowChangeToMsg(deleteEvent, false, false)
	require.NoError(t, err)
	_, ok = value.Delete["a"]
	require.True(t, ok)

	key, value, err = rowChangeToMsg(deleteEvent, false, true)
	require.NoError(t, err)
	require.True(t, key.OnlyHandleKey)
	_, ok = value.Delete["a"]
	require.False(t, ok)

	deleteEventNoHandleKey := &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "schema",
			Table:  "table",
		},
		PreColumns: []*model.Column{
			{Name: "id", Type: mysql.TypeLonglong, Value: 1},
			{Name: "a", Type: mysql.TypeLonglong, Value: 2},
		},
	}

	_, _, err = rowChangeToMsg(deleteEventNoHandleKey, false, true)
	require.Error(t, err, cerror.ErrOpenProtocolCodecInvalidData)
}
