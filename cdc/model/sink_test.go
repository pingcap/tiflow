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
	"testing"

	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
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
		Table: &TableName{
			Schema: "test",
			Table:  "t1",
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
	expectedKeyCols := []*Column{
		{
			Name:  "a",
			Value: 1,
			Flag:  HandleKeyFlag | PrimaryKeyFlag,
		},
	}
	require.True(t, deleteRow.IsDelete())
	require.Equal(t, expectedKeyCols, deleteRow.PrimaryKeyColumns())
	require.Equal(t, expectedKeyCols, deleteRow.HandleKeyColumns())

	insertRow := &RowChangedEvent{
		Table: &TableName{
			Schema: "test",
			Table:  "t1",
		},
		Columns: []*Column{
			{
				Name:  "a",
				Value: 1,
				Flag:  HandleKeyFlag,
			}, {
				Name:  "b",
				Value: 2,
				Flag:  0,
			},
		},
	}
	expectedPrimaryKeyCols := []*Column{}
	expectedHandleKeyCols := []*Column{
		{
			Name:  "a",
			Value: 1,
			Flag:  HandleKeyFlag,
		},
	}
	require.False(t, insertRow.IsDelete())
	require.Equal(t, expectedPrimaryKeyCols, insertRow.PrimaryKeyColumns())
	require.Equal(t, expectedHandleKeyCols, insertRow.HandleKeyColumns())

	forceReplicaRow := &RowChangedEvent{
		Table: &TableName{
			Schema: "test",
			Table:  "t1",
		},
		Columns: []*Column{
			{
				Name:  "a",
				Value: 1,
				Flag:  0,
			}, {
				Name:  "b",
				Value: 2,
				Flag:  0,
			},
		},
	}
	require.Empty(t, forceReplicaRow.PrimaryKeyColumns())
	require.Empty(t, forceReplicaRow.HandleKeyColumns())
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

func TestFromTiColumnInfo(t *testing.T) {
	t.Parallel()
	col := &ColumnInfo{}
	col.FromTiColumnInfo(&timodel.ColumnInfo{
		Name:      timodel.CIStr{O: "col1"},
		FieldType: *types.NewFieldType(mysql.TypeLong),
	})
	require.Equal(t, "col1", col.Name)
	require.Equal(t, uint8(3), col.Type)
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
	event := &DDLEvent{}
	event.FromJob(job, preTableInfo)
	require.Equal(t, uint64(420536581131337731), event.StartTs)
	require.Equal(t, event.TableInfo.TableID, int64(49))
	require.Equal(t, 1, len(event.PreTableInfo.ColumnInfo))

	event = &DDLEvent{}
	event.FromJob(job, nil)
	require.Nil(t, event.PreTableInfo)
}

func TestDDLEventFromRenameTablesJob(t *testing.T) {
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

	tableInfo := &timodel.TableInfo{
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
	}

	event := &DDLEvent{}
	event.FromRenameTablesJob(job, "test1", "test1", preTableInfo, tableInfo)
	require.Equal(t, event.PreTableInfo.TableID, int64(67))
	require.Equal(t, event.PreTableInfo.Table, "t1")
	require.Len(t, event.PreTableInfo.ColumnInfo, 1)
	require.Equal(t, event.TableInfo.TableID, int64(67))
	require.Equal(t, event.TableInfo.Table, "t10")
	require.Len(t, event.TableInfo.ColumnInfo, 1)
	require.Equal(t, event.Query, "RENAME TABLE `test1`.`t1` TO `test1`.`t10`")

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

	tableInfo = &timodel.TableInfo{
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
	}

	event = &DDLEvent{}
	event.FromRenameTablesJob(job, "test1", "test1", preTableInfo, tableInfo)
	require.Equal(t, event.PreTableInfo.TableID, int64(69))
	require.Equal(t, event.PreTableInfo.Table, "t2")
	require.Len(t, event.PreTableInfo.ColumnInfo, 1)
	require.Equal(t, event.TableInfo.TableID, int64(69))
	require.Equal(t, event.TableInfo.Table, "t20")
	require.Len(t, event.TableInfo.ColumnInfo, 1)
	require.Equal(t, event.Query, "RENAME TABLE `test1`.`t2` TO `test1`.`t20`")
}
