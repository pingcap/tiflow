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

package model

import (
	"sort"
	"testing"

	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetFlag(t *testing.T) {
	t.Parallel()

	var flag ColumnFlagType
	flag.SetIsBinary()
	flag.SetIsGeneratedColumn()
	require.True(t, flag.IsBinary())
	require.False(t, flag.IsHandleKey())
	require.True(t, flag.IsGeneratedColumn())
	flag.UnsetIsBinary()
	require.False(t, flag.IsBinary())
	flag.SetIsMultipleKey()
	flag.SetIsUniqueKey()
	require.True(t, flag.IsMultipleKey() && flag.IsUniqueKey())
	flag.UnsetIsUniqueKey()
	flag.UnsetIsGeneratedColumn()
	flag.UnsetIsMultipleKey()
	require.False(t, flag.IsUniqueKey() || flag.IsGeneratedColumn() || flag.IsMultipleKey())

	flag = ColumnFlagType(0)
	flag.SetIsHandleKey()
	flag.SetIsPrimaryKey()
	flag.SetIsUnsigned()
	require.True(t, flag.IsHandleKey() && flag.IsPrimaryKey() && flag.IsUnsigned())
	flag.UnsetIsHandleKey()
	flag.UnsetIsPrimaryKey()
	flag.UnsetIsUnsigned()
	require.False(t, flag.IsHandleKey() || flag.IsPrimaryKey() || flag.IsUnsigned())
	flag.SetIsNullable()
	require.True(t, flag.IsNullable())
	flag.UnsetIsNullable()
	require.False(t, flag.IsNullable())
}

func TestFlagValue(t *testing.T) {
	t.Parallel()

	require.Equal(t, ColumnFlagType(0b1), BinaryFlag)
	require.Equal(t, ColumnFlagType(0b1), BinaryFlag)
	require.Equal(t, ColumnFlagType(0b10), HandleKeyFlag)
	require.Equal(t, ColumnFlagType(0b100), GeneratedColumnFlag)
	require.Equal(t, ColumnFlagType(0b1000), PrimaryKeyFlag)
	require.Equal(t, ColumnFlagType(0b10000), UniqueKeyFlag)
	require.Equal(t, ColumnFlagType(0b100000), MultipleKeyFlag)
	require.Equal(t, ColumnFlagType(0b1000000), NullableFlag)
}

func TestTableNameFuncs(t *testing.T) {
	t.Parallel()
	tbl := &TableName{
		Schema:  "test",
		Table:   "t1",
		TableID: 1071,
	}
	require.Equal(t, "test.t1", tbl.String())
	require.Equal(t, "`test`.`t1`", tbl.QuoteString())
	require.Equal(t, "test", tbl.GetSchema())
	require.Equal(t, "t1", tbl.GetTable())
	require.Equal(t, int64(1071), tbl.GetTableID())
}

func TestRowChangedEventFuncs(t *testing.T) {
	t.Parallel()
	deleteRow := &RowChangedEvent{
		TableInfo: &TableInfo{
			TableName: TableName{
				Schema: "test",
				Table:  "t1",
			},
		},
		PreColumns: []*Column{
			{
				Name:  "a",
				Value: 1,
				Flag:  HandleKeyFlag | PrimaryKeyFlag,
			}, {
				Name:  "b",
				Value: 2,
				Flag:  0,
			},
		},
	}
	require.True(t, deleteRow.IsDelete())
}

func TestColumnValueString(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		val      interface{}
		expected string
	}{
		{interface{}(nil), "null"},
		{interface{}(true), "1"},
		{interface{}(false), "0"},
		{interface{}(123), "123"},
		{interface{}(int8(-123)), "-123"},
		{interface{}(int16(-123)), "-123"},
		{interface{}(int32(-123)), "-123"},
		{interface{}(int64(-123)), "-123"},
		{interface{}(uint8(123)), "123"},
		{interface{}(uint16(123)), "123"},
		{interface{}(uint32(123)), "123"},
		{interface{}(uint64(123)), "123"},
		{interface{}(float32(123.01)), "123.01"},
		{interface{}(float64(123.01)), "123.01"},
		{interface{}("123.01"), "123.01"},
		{interface{}([]byte("123.01")), "123.01"},
		{interface{}(complex(1, 2)), "(1+2i)"},
	}
	for _, tc := range testCases {
		s := ColumnValueString(tc.val)
		require.Equal(t, tc.expected, s)
	}
}

func TestDDLEventFromJob(t *testing.T) {
	t.Parallel()
	ft := types.NewFieldType(mysql.TypeUnspecified)
	ft.SetFlag(mysql.PriKeyFlag)
	job := &timodel.Job{
		ID:         1071,
		TableID:    49,
		SchemaName: "test",
		Type:       timodel.ActionAddColumn,
		StartTS:    420536581131337731,
		Query:      "alter table t1 add column a int",
		BinlogInfo: &timodel.HistoryInfo{
			TableInfo: &timodel.TableInfo{
				ID:   49,
				Name: timodel.CIStr{O: "t1"},
				Columns: []*timodel.ColumnInfo{
					{ID: 1, Name: timodel.CIStr{O: "id"}, FieldType: *ft, State: timodel.StatePublic},
					{ID: 2, Name: timodel.CIStr{O: "a"}, FieldType: types.FieldType{}, State: timodel.StatePublic},
				},
			},
			FinishedTS: 420536581196873729,
		},
	}
	preTableInfo := &TableInfo{
		TableName: TableName{
			Schema:  "test",
			Table:   "t1",
			TableID: 49,
		},
		TableInfo: &timodel.TableInfo{
			ID:   49,
			Name: timodel.CIStr{O: "t1"},
			Columns: []*timodel.ColumnInfo{
				{ID: 1, Name: timodel.CIStr{O: "id"}, FieldType: *ft, State: timodel.StatePublic},
			},
		},
	}
	tableInfo := WrapTableInfo(job.SchemaID, job.SchemaName, job.BinlogInfo.FinishedTS, job.BinlogInfo.TableInfo)
	event := &DDLEvent{}
	event.FromJob(job, preTableInfo, tableInfo)
	require.Equal(t, uint64(420536581131337731), event.StartTs)
	require.Equal(t, int64(49), event.TableInfo.TableName.TableID)
	require.Equal(t, 1, len(event.PreTableInfo.TableInfo.Columns))

	event = &DDLEvent{}
	event.FromJob(job, nil, nil)
	require.Nil(t, event.PreTableInfo)
}

func TestRenameTables(t *testing.T) {
	ft := types.NewFieldType(mysql.TypeUnspecified)
	ft.SetFlag(mysql.PriKeyFlag | mysql.UniqueFlag)
	job := &timodel.Job{
		ID:         71,
		TableID:    69,
		SchemaName: "test1",
		Type:       timodel.ActionRenameTables,
		StartTS:    432853521879007233,
		Query:      "rename table test1.t1 to test1.t10, test1.t2 to test1.t20",
		BinlogInfo: &timodel.HistoryInfo{
			FinishedTS: 432853521879007238,
			MultipleTableInfos: []*timodel.TableInfo{
				{
					ID:   67,
					Name: timodel.CIStr{O: "t10"},
					Columns: []*timodel.ColumnInfo{
						{
							ID:        1,
							Name:      timodel.CIStr{O: "id"},
							FieldType: *ft,
							State:     timodel.StatePublic,
						},
					},
				},
				{
					ID:   69,
					Name: timodel.CIStr{O: "t20"},
					Columns: []*timodel.ColumnInfo{
						{
							ID:        1,
							Name:      timodel.CIStr{O: "id"},
							FieldType: *ft,
							State:     timodel.StatePublic,
						},
					},
				},
			},
		},
	}

	preTableInfo := &TableInfo{
		TableName: TableName{
			Schema:  "test1",
			Table:   "t1",
			TableID: 67,
		},
		TableInfo: &timodel.TableInfo{
			ID:   67,
			Name: timodel.CIStr{O: "t1"},
			Columns: []*timodel.ColumnInfo{
				{
					ID:        1,
					Name:      timodel.CIStr{O: "id"},
					FieldType: *ft,
					State:     timodel.StatePublic,
				},
			},
		},
	}

	tableInfo := &TableInfo{
		TableName: TableName{
			Schema:  "test1",
			Table:   "t10",
			TableID: 67,
		},
		TableInfo: &timodel.TableInfo{
			ID:   67,
			Name: timodel.CIStr{O: "t10"},
			Columns: []*timodel.ColumnInfo{
				{
					ID:        1,
					Name:      timodel.CIStr{O: "id"},
					FieldType: *ft,
					State:     timodel.StatePublic,
				},
			},
		},
	}

	event := &DDLEvent{}
	event.FromJobWithArgs(job, preTableInfo, tableInfo, "test1", "test1")
	require.Equal(t, event.PreTableInfo.TableName.TableID, int64(67))
	require.Equal(t, event.PreTableInfo.TableName.Table, "t1")
	require.Len(t, event.PreTableInfo.TableInfo.Columns, 1)
	require.Equal(t, event.TableInfo.TableName.TableID, int64(67))
	require.Equal(t, event.TableInfo.TableName.Table, "t10")
	require.Len(t, event.TableInfo.TableInfo.Columns, 1)
	require.Equal(t, event.Query, "RENAME TABLE `test1`.`t1` TO `test1`.`t10`")
	require.Equal(t, event.Type, timodel.ActionRenameTable)

	preTableInfo = &TableInfo{
		TableName: TableName{
			Schema:  "test1",
			Table:   "t2",
			TableID: 69,
		},
		TableInfo: &timodel.TableInfo{
			ID:   69,
			Name: timodel.CIStr{O: "t2"},
			Columns: []*timodel.ColumnInfo{
				{
					ID:        1,
					Name:      timodel.CIStr{O: "id"},
					FieldType: *ft,
					State:     timodel.StatePublic,
				},
			},
		},
	}

	tableInfo = &TableInfo{
		TableName: TableName{
			Schema:  "test1",
			Table:   "t20",
			TableID: 69,
		},
		TableInfo: &timodel.TableInfo{
			ID:   69,
			Name: timodel.CIStr{O: "t20"},
			Columns: []*timodel.ColumnInfo{
				{
					ID:        1,
					Name:      timodel.CIStr{O: "id"},
					FieldType: *ft,
					State:     timodel.StatePublic,
				},
			},
		},
	}

	event = &DDLEvent{}
	event.FromJobWithArgs(job, preTableInfo, tableInfo, "test1", "test1")
	require.Equal(t, event.PreTableInfo.TableName.TableID, int64(69))
	require.Equal(t, event.PreTableInfo.TableName.Table, "t2")
	require.Len(t, event.PreTableInfo.TableInfo.Columns, 1)
	require.Equal(t, event.TableInfo.TableName.TableID, int64(69))
	require.Equal(t, event.TableInfo.TableName.Table, "t20")
	require.Len(t, event.TableInfo.TableInfo.Columns, 1)
	require.Equal(t, event.Query, "RENAME TABLE `test1`.`t2` TO `test1`.`t20`")
	require.Equal(t, event.Type, timodel.ActionRenameTable)
}

func TestExchangeTablePartition(t *testing.T) {
	ft := types.NewFieldType(mysql.TypeUnspecified)
	ft.SetFlag(mysql.PriKeyFlag | mysql.UniqueFlag)
	job := &timodel.Job{
		ID:         71,
		TableID:    69,
		SchemaName: "test1",
		Type:       timodel.ActionExchangeTablePartition,
		StartTS:    432853521879007233,
		Query:      "alter table t1 exchange partition p0 with table t2",
		BinlogInfo: &timodel.HistoryInfo{
			FinishedTS: 432853521879007238,
		},
	}

	// source table
	preTableInfo := &TableInfo{
		TableName: TableName{
			Schema:  "test2",
			Table:   "t2",
			TableID: 67,
		},
		TableInfo: &timodel.TableInfo{
			ID:   67,
			Name: timodel.CIStr{O: "t1"},
			Columns: []*timodel.ColumnInfo{
				{
					ID:        1,
					Name:      timodel.CIStr{O: "id"},
					FieldType: *ft,
					State:     timodel.StatePublic,
				},
			},
		},
	}

	// target table
	tableInfo := &TableInfo{
		TableName: TableName{
			Schema:  "test1",
			Table:   "t1",
			TableID: 69,
		},
		TableInfo: &timodel.TableInfo{
			ID:   69,
			Name: timodel.CIStr{O: "t10"},
			Columns: []*timodel.ColumnInfo{
				{
					ID:        1,
					Name:      timodel.CIStr{O: "id"},
					FieldType: *ft,
					State:     timodel.StatePublic,
				},
			},
		},
	}

	event := &DDLEvent{}
	event.FromJob(job, preTableInfo, tableInfo)
	require.Equal(t, event.PreTableInfo.TableName.TableID, int64(67))
	require.Equal(t, event.PreTableInfo.TableName.Table, "t2")
	require.Len(t, event.PreTableInfo.TableInfo.Columns, 1)
	require.Equal(t, event.TableInfo.TableName.TableID, int64(69))
	require.Equal(t, event.TableInfo.TableName.Table, "t1")
	require.Len(t, event.TableInfo.TableInfo.Columns, 1)
	require.Equal(t, "ALTER TABLE `test1`.`t1` EXCHANGE PARTITION `p0` WITH TABLE `test2`.`t2`", event.Query)
	require.Equal(t, event.Type, timodel.ActionExchangeTablePartition)
}

func TestSortRowChangedEvent(t *testing.T) {
	events := []*RowChangedEvent{
		{
			PreColumns: []*Column{{}},
			Columns:    []*Column{{}},
		},
		{
			Columns: []*Column{{}},
		},
		{
			PreColumns: []*Column{{}},
		},
	}
	assert.True(t, events[0].IsUpdate())
	assert.True(t, events[1].IsInsert())
	assert.True(t, events[2].IsDelete())
	sort.Sort(txnRows(events))
	assert.True(t, events[0].IsDelete())
	assert.True(t, events[1].IsUpdate())
	assert.True(t, events[2].IsInsert())
}

func TestTrySplitAndSortUpdateEventNil(t *testing.T) {
	t.Parallel()

	events := []*RowChangedEvent{nil}
	result, err := trySplitAndSortUpdateEvent(events)
	require.NoError(t, err)
	require.Equal(t, 0, len(result))
}

func TestTrySplitAndSortUpdateEventEmpty(t *testing.T) {
	t.Parallel()

	events := []*RowChangedEvent{
		{
			StartTs:  1,
			CommitTs: 2,
		},
	}
	result, err := trySplitAndSortUpdateEvent(events)
	require.NoError(t, err)
	require.Equal(t, 0, len(result))
}

func TestTrySplitAndSortUpdateEvent(t *testing.T) {
	t.Parallel()

	// Update handle key.
	columns := []*Column{
		{
			Name:  "col1",
			Flag:  BinaryFlag,
			Value: "col1-value-updated",
		},
		{
			Name:  "col2",
			Flag:  HandleKeyFlag,
			Value: "col2-value-updated",
		},
	}
	preColumns := []*Column{
		{
			Name:  "col1",
			Flag:  BinaryFlag,
			Value: "col1-value",
		},
		{
			Name:  "col2",
			Flag:  HandleKeyFlag,
			Value: "col2-value",
		},
	}

	events := []*RowChangedEvent{
		{
			CommitTs:   1,
			Columns:    columns,
			PreColumns: preColumns,
		},
	}
	result, err := trySplitAndSortUpdateEvent(events)
	require.NoError(t, err)
	require.Equal(t, 2, len(result))
	require.True(t, result[0].IsDelete())
	require.True(t, result[1].IsInsert())

	// Update unique key.
	columns = []*Column{
		{
			Name:  "col1",
			Flag:  BinaryFlag,
			Value: "col1-value-updated",
		},
		{
			Name:  "col2",
			Flag:  UniqueKeyFlag,
			Value: "col2-value-updated",
		},
	}
	preColumns = []*Column{
		{
			Name:  "col1",
			Flag:  BinaryFlag,
			Value: "col1-value",
		},
		{
			Name:  "col2",
			Flag:  UniqueKeyFlag,
			Value: "col2-value",
		},
	}

	events = []*RowChangedEvent{
		{
			CommitTs:   1,
			Columns:    columns,
			PreColumns: preColumns,
		},
	}
	result, err = trySplitAndSortUpdateEvent(events)
	require.NoError(t, err)
	require.Equal(t, 2, len(result))
	require.True(t, result[0].IsDelete())
	require.True(t, result[0].IsDelete())
	require.True(t, result[1].IsInsert())

	// Update non-handle key.
	columns = []*Column{
		{
			Name:  "col1",
			Flag:  BinaryFlag,
			Value: "col1-value-updated",
		},
		{
			Name:  "col2",
			Flag:  HandleKeyFlag,
			Value: "col2-value",
		},
	}
	preColumns = []*Column{
		{
			Name:  "col1",
			Flag:  BinaryFlag,
			Value: "col1-value",
		},
		{
			Name:  "col2",
			Flag:  HandleKeyFlag,
			Value: "col2-value",
		},
	}

	events = []*RowChangedEvent{
		{
			CommitTs:   1,
			Columns:    columns,
			PreColumns: preColumns,
		},
	}
	result, err = trySplitAndSortUpdateEvent(events)
	require.NoError(t, err)
	require.Equal(t, 1, len(result))
}

var ukUpdatedEvent = &RowChangedEvent{
	PreColumns: []*Column{
		{
			Name:  "col1",
			Flag:  BinaryFlag,
			Value: "col1-value",
		},
		{
			Name:  "col2",
			Flag:  HandleKeyFlag | UniqueKeyFlag,
			Value: "col2-value",
		},
	},

	Columns: []*Column{
		{
			Name:  "col1",
			Flag:  BinaryFlag,
			Value: "col1-value",
		},
		{
			Name:  "col2",
			Flag:  HandleKeyFlag | UniqueKeyFlag,
			Value: "col2-value-updated",
		},
	},
}

func TestTrySplitAndSortUpdateEventOne(t *testing.T) {
	txn := &SingleTableTxn{
		Rows: []*RowChangedEvent{ukUpdatedEvent},
	}

	err := txn.TrySplitAndSortUpdateEvent(sink.KafkaScheme)
	require.NoError(t, err)
	require.Len(t, txn.Rows, 2)

	txn = &SingleTableTxn{
		Rows: []*RowChangedEvent{ukUpdatedEvent},
	}
	err = txn.TrySplitAndSortUpdateEvent(sink.MySQLScheme)
	require.NoError(t, err)
	require.Len(t, txn.Rows, 1)
}

func TestToRedoLog(t *testing.T) {
	event := &RowChangedEvent{
		StartTs:         100,
		CommitTs:        1000,
		PhysicalTableID: 1,
		TableInfo: &TableInfo{
			TableName: TableName{Schema: "test", Table: "t"},
		},
		Columns: []*Column{
			{
				Name:  "col1",
				Flag:  BinaryFlag,
				Value: "col1-value",
			},
			{
				Name:  "col2",
				Flag:  HandleKeyFlag | UniqueKeyFlag,
				Value: "col2-value-updated",
			},
		},
	}
	eventInRedoLog := event.ToRedoLog()
	require.Equal(t, event.StartTs, eventInRedoLog.RedoRow.Row.StartTs)
	require.Equal(t, event.CommitTs, eventInRedoLog.RedoRow.Row.CommitTs)
	require.Equal(t, event.PhysicalTableID, eventInRedoLog.RedoRow.Row.Table.TableID)
	require.Equal(t, event.TableInfo.GetSchemaName(), eventInRedoLog.RedoRow.Row.Table.Schema)
	require.Equal(t, event.TableInfo.GetTableName(), eventInRedoLog.RedoRow.Row.Table.Table)
	require.Equal(t, event.Columns, eventInRedoLog.RedoRow.Row.Columns)
}
