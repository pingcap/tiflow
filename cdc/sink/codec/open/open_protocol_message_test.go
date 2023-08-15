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
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/cdc/sink/codec/internal"
	"github.com/pingcap/tiflow/pkg/config"
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

	config := common.NewConfig(config.ProtocolOpen)
	config.DeleteOnlyHandleKeyColumns = true

	_, value := rowChangeToMsg(insertEvent, config, false)
	require.Contains(t, value.Update, "id")
	require.Contains(t, value.Update, "a")

	config.DeleteOnlyHandleKeyColumns = false
	key, value := rowChangeToMsg(insertEvent, config, true)
	require.True(t, key.OnlyHandleKey)
	require.Contains(t, value.Update, "id")
	require.NotContains(t, value.Update, "a")

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

	config.DeleteOnlyHandleKeyColumns = true
	_, value = rowChangeToMsg(updateEvent, config, false)
	require.Contains(t, value.PreColumns, "a")

	config.DeleteOnlyHandleKeyColumns = false
	key, value = rowChangeToMsg(updateEvent, config, true)
	require.True(t, key.OnlyHandleKey)
	require.NotContains(t, value.PreColumns, "a")
	require.NotContains(t, value.Update, "a")

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
	config.DeleteOnlyHandleKeyColumns = true
	_, value = rowChangeToMsg(deleteEvent, config, false)
	require.NotContains(t, value.Delete, "a")

	config.DeleteOnlyHandleKeyColumns = false
	_, value = rowChangeToMsg(deleteEvent, config, false)
	require.Contains(t, value.Delete, "a")

	config.DeleteOnlyHandleKeyColumns = false
	key, value = rowChangeToMsg(deleteEvent, config, true)
	require.True(t, key.OnlyHandleKey)
	require.NotContains(t, value.Delete, "a")
}
