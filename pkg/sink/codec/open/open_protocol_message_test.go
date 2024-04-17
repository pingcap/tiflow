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

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/codec/internal"
	"github.com/stretchr/testify/require"
)

func TestFormatCol(t *testing.T) {
	t.Parallel()
	row := &messageRow{Update: map[string]internal.Column{"test": {
		Type:  mysql.TypeString,
		Value: "测",
	}}}
	rowEncode, err := row.encode()
	require.NoError(t, err)
	row2 := new(messageRow)
	err = row2.decode(rowEncode)
	require.NoError(t, err)
	require.Equal(t, row, row2)
	//
	row = &messageRow{Update: map[string]internal.Column{"test": {
		Type:  mysql.TypeBlob,
		Value: []byte("测"),
	}}}
	rowEncode, err = row.encode()
	require.NoError(t, err)
	row2 = new(messageRow)
	err = row2.decode(rowEncode)
	require.NoError(t, err)
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
	require.NoError(t, err)
	row2 := new(messageRow)
	err = row2.decode(rowEncode)
	require.NoError(t, err)
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
	require.NoError(t, err)
	row2 := new(messageRow)
	err = row2.decode(rowEncode)
	require.NoError(t, err)
	require.Equal(t, row, row2)
	mqCol2 := row2.Update["test"]
	col2 := mqCol2.ToRowChangeColumn("test")
	require.Equal(t, col, col2)
}

func TestOnlyOutputUpdatedColumn(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	_ = helper.DDL2Event(`create table test.t (a int primary key, b int, c int)`)
	event := helper.DML2Event(`insert into test.t values (1, 1, 1)`, "test", "t")
	copy(event.PreColumns, event.Columns)

	codecConfig := common.NewConfig(config.ProtocolOpen)
	codecConfig.OnlyOutputUpdatedColumns = true

	// column not updated, so ignore it.
	_, row, err := rowChangeToMsg(event, codecConfig, false)
	require.NoError(t, err)
	_, ok := row.PreColumns["test"]
	require.False(t, ok)

	event.TableInfo.Columns[1].FieldType = *types.NewFieldType(mysql.TypeFloat)
	_, row, err = rowChangeToMsg(event, codecConfig, false)
	require.NoError(t, err)
	_, ok = row.PreColumns["test"]
	require.True(t, ok)

	tableInfoWithIntCols := model.BuildTableInfo("test", "test", []*model.Column{
		{
			Name: "test",
			Type: mysql.TypeLong,
		},
	}, nil)
	event = &model.RowChangedEvent{
		TableInfo: tableInfoWithIntCols,
		PreColumns: model.Columns2ColumnDatas([]*model.Column{
			{
				Name:  "test",
				Value: uint64(1),
			},
		}, tableInfoWithIntCols),
		Columns: model.Columns2ColumnDatas([]*model.Column{
			{
				Name:  "test",
				Value: int64(1),
			},
		}, tableInfoWithIntCols),
	}
	_, row, err = rowChangeToMsg(event, codecConfig, false)
	require.NoError(t, err)
	_, ok = row.PreColumns["test"]
	require.True(t, ok)
}

func TestRowChanged2MsgOnlyHandleKeyColumns(t *testing.T) {
	t.Parallel()
	tableInfoWithHandleKey := model.BuildTableInfo("schema", "table", []*model.Column{
		{Name: "id", Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Type: mysql.TypeLonglong},
		{Name: "a", Type: mysql.TypeLonglong, Value: 1},
	}, [][]int{{0}})
	insertEvent := &model.RowChangedEvent{
		CommitTs:  417318403368288260,
		TableInfo: tableInfoWithHandleKey,
		Columns: model.Columns2ColumnDatas([]*model.Column{
			{Name: "id", Flag: model.HandleKeyFlag, Type: mysql.TypeLonglong, Value: 1},
			{Name: "a", Type: mysql.TypeLonglong, Value: 1},
		}, tableInfoWithHandleKey),
	}

	config := common.NewConfig(config.ProtocolOpen)
	config.DeleteOnlyHandleKeyColumns = true

	_, value, err := rowChangeToMsg(insertEvent, config, false)
	require.NoError(t, err)
	require.Contains(t, value.Update, "id")
	require.Contains(t, value.Update, "a")

	config.DeleteOnlyHandleKeyColumns = false
	key, value, err := rowChangeToMsg(insertEvent, config, true)
	require.NoError(t, err)
	require.True(t, key.OnlyHandleKey)
	require.Contains(t, value.Update, "id")
	require.NotContains(t, value.Update, "a")

	tableInfoWithoutHandleKey := model.BuildTableInfo("schema", "table", []*model.Column{
		{Name: "id", Type: mysql.TypeLonglong},
		{Name: "a", Type: mysql.TypeLonglong},
	}, [][]int{{0}})
	insertEventNoHandleKey := &model.RowChangedEvent{
		CommitTs:  417318403368288260,
		TableInfo: tableInfoWithoutHandleKey,
		Columns: model.Columns2ColumnDatas([]*model.Column{
			{Name: "id", Type: mysql.TypeLonglong, Value: 1},
			{Name: "a", Type: mysql.TypeLonglong, Value: 1},
		}, tableInfoWithoutHandleKey),
	}
	_, _, err = rowChangeToMsg(insertEventNoHandleKey, config, true)
	require.Error(t, err, cerror.ErrOpenProtocolCodecInvalidData)

	updateEvent := &model.RowChangedEvent{
		CommitTs:  417318403368288260,
		TableInfo: tableInfoWithHandleKey,

		Columns: model.Columns2ColumnDatas([]*model.Column{
			{Name: "id", Flag: model.HandleKeyFlag, Type: mysql.TypeLonglong, Value: 1},
			{Name: "a", Type: mysql.TypeLonglong, Value: 2},
		}, tableInfoWithHandleKey),
		PreColumns: model.Columns2ColumnDatas([]*model.Column{
			{Name: "id", Flag: model.HandleKeyFlag, Type: mysql.TypeLonglong, Value: 1},
			{Name: "a", Type: mysql.TypeLonglong, Value: 1},
		}, tableInfoWithHandleKey),
	}

	config.DeleteOnlyHandleKeyColumns = true
	_, value, err = rowChangeToMsg(updateEvent, config, false)
	require.NoError(t, err)
	require.Contains(t, value.PreColumns, "a")

	config.DeleteOnlyHandleKeyColumns = false
	key, value, err = rowChangeToMsg(updateEvent, config, true)
	require.NoError(t, err)
	require.True(t, key.OnlyHandleKey)
	require.NotContains(t, value.PreColumns, "a")
	require.NotContains(t, value.Update, "a")

	updateEventNoHandleKey := &model.RowChangedEvent{
		CommitTs:  417318403368288260,
		TableInfo: tableInfoWithoutHandleKey,
		Columns: model.Columns2ColumnDatas([]*model.Column{
			{Name: "id", Type: mysql.TypeLonglong, Value: 1},
			{Name: "a", Type: mysql.TypeLonglong, Value: 2},
		}, tableInfoWithoutHandleKey),
		PreColumns: model.Columns2ColumnDatas([]*model.Column{
			{Name: "id", Flag: model.HandleKeyFlag, Type: mysql.TypeLonglong, Value: 1},
			{Name: "a", Type: mysql.TypeLonglong, Value: 1},
		}, tableInfoWithoutHandleKey),
	}
	_, _, err = rowChangeToMsg(updateEventNoHandleKey, config, true)
	require.Error(t, err, cerror.ErrOpenProtocolCodecInvalidData)

	deleteEvent := &model.RowChangedEvent{
		CommitTs:  417318403368288260,
		TableInfo: tableInfoWithHandleKey,
		PreColumns: model.Columns2ColumnDatas([]*model.Column{
			{Name: "id", Flag: model.HandleKeyFlag, Type: mysql.TypeLonglong, Value: 1},
			{Name: "a", Type: mysql.TypeLonglong, Value: 2},
		}, tableInfoWithHandleKey),
	}
	config.DeleteOnlyHandleKeyColumns = true
	_, value, err = rowChangeToMsg(deleteEvent, config, false)
	require.NoError(t, err)
	require.NotContains(t, value.Delete, "a")

	config.DeleteOnlyHandleKeyColumns = false
	_, value, err = rowChangeToMsg(deleteEvent, config, false)
	require.NoError(t, err)
	require.Contains(t, value.Delete, "a")

	config.DeleteOnlyHandleKeyColumns = false
	key, value, err = rowChangeToMsg(deleteEvent, config, true)
	require.NoError(t, err)
	require.True(t, key.OnlyHandleKey)
	require.NotContains(t, value.Delete, "a")

	deleteEventNoHandleKey := &model.RowChangedEvent{
		CommitTs:  417318403368288260,
		TableInfo: tableInfoWithoutHandleKey,
		PreColumns: model.Columns2ColumnDatas([]*model.Column{
			{Name: "id", Type: mysql.TypeLonglong, Value: 1},
			{Name: "a", Type: mysql.TypeLonglong, Value: 2},
		}, tableInfoWithoutHandleKey),
	}

	_, _, err = rowChangeToMsg(deleteEventNoHandleKey, config, true)
	require.Error(t, err, cerror.ErrOpenProtocolCodecInvalidData)
}
