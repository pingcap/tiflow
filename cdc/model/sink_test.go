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
	"github.com/pingcap/check"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

type columnFlagTypeSuite struct{}

var _ = check.Suite(&columnFlagTypeSuite{})

func (s *columnFlagTypeSuite) TestSetFlag(c *check.C) {
	defer testleak.AfterTest(c)()
	var flag ColumnFlagType
	flag.SetIsBinary()
	flag.SetIsGeneratedColumn()
	c.Assert(flag.IsBinary(), check.IsTrue)
	c.Assert(flag.IsHandleKey(), check.IsFalse)
	c.Assert(flag.IsGeneratedColumn(), check.IsTrue)
	flag.UnsetIsBinary()
	c.Assert(flag.IsBinary(), check.IsFalse)
	flag.SetIsMultipleKey()
	flag.SetIsUniqueKey()
	c.Assert(flag.IsMultipleKey() && flag.IsUniqueKey(), check.IsTrue)
	flag.UnsetIsUniqueKey()
	flag.UnsetIsGeneratedColumn()
	flag.UnsetIsMultipleKey()
	c.Assert(flag.IsUniqueKey() || flag.IsGeneratedColumn() || flag.IsMultipleKey(), check.IsFalse)

	flag = ColumnFlagType(0)
	flag.SetIsHandleKey()
	flag.SetIsPrimaryKey()
	flag.SetIsUnsigned()
	c.Assert(flag.IsHandleKey() && flag.IsPrimaryKey() && flag.IsUnsigned(), check.IsTrue)
	flag.UnsetIsHandleKey()
	flag.UnsetIsPrimaryKey()
	flag.UnsetIsUnsigned()
	c.Assert(flag.IsHandleKey() || flag.IsPrimaryKey() || flag.IsUnsigned(), check.IsFalse)
	flag.SetIsNullable()
	c.Assert(flag.IsNullable(), check.IsTrue)
	flag.UnsetIsNullable()
	c.Assert(flag.IsNullable(), check.IsFalse)
}

func (s *columnFlagTypeSuite) TestFlagValue(c *check.C) {
	defer testleak.AfterTest(c)()
	c.Assert(BinaryFlag, check.Equals, ColumnFlagType(0b1))
	c.Assert(HandleKeyFlag, check.Equals, ColumnFlagType(0b10))
	c.Assert(GeneratedColumnFlag, check.Equals, ColumnFlagType(0b100))
	c.Assert(PrimaryKeyFlag, check.Equals, ColumnFlagType(0b1000))
	c.Assert(UniqueKeyFlag, check.Equals, ColumnFlagType(0b10000))
	c.Assert(MultipleKeyFlag, check.Equals, ColumnFlagType(0b100000))
	c.Assert(NullableFlag, check.Equals, ColumnFlagType(0b1000000))
}

type commonDataStructureSuite struct{}

var _ = check.Suite(&commonDataStructureSuite{})

func (s *commonDataStructureSuite) TestTableNameFuncs(c *check.C) {
	defer testleak.AfterTest(c)()
	t := &TableName{
		Schema:  "test",
		Table:   "t1",
		TableID: 1071,
	}
	c.Assert(t.String(), check.Equals, "test.t1")
	c.Assert(t.QuoteString(), check.Equals, "`test`.`t1`")
	c.Assert(t.GetSchema(), check.Equals, "test")
	c.Assert(t.GetTable(), check.Equals, "t1")
	c.Assert(t.GetTableID(), check.Equals, int64(1071))
}

func (s *commonDataStructureSuite) TestRowChangedEventFuncs(c *check.C) {
	defer testleak.AfterTest(c)()
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
	c.Assert(deleteRow.IsDelete(), check.IsTrue)
	c.Assert(deleteRow.PrimaryKeyColumns(), check.DeepEquals, expectedKeyCols)
	c.Assert(deleteRow.HandleKeyColumns(), check.DeepEquals, expectedKeyCols)

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
	c.Assert(insertRow.IsDelete(), check.IsFalse)
	c.Assert(insertRow.PrimaryKeyColumns(), check.DeepEquals, expectedPrimaryKeyCols)
	c.Assert(insertRow.HandleKeyColumns(), check.DeepEquals, expectedHandleKeyCols)
}

func (s *commonDataStructureSuite) TestColumnValueString(c *check.C) {
	defer testleak.AfterTest(c)()
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
		c.Assert(s, check.Equals, tc.expected)
	}
}

func (s *commonDataStructureSuite) TestFromTiColumnInfo(c *check.C) {
	defer testleak.AfterTest(c)()
	col := &ColumnInfo{}
	col.FromTiColumnInfo(&timodel.ColumnInfo{
		Name:      timodel.CIStr{O: "col1"},
		FieldType: types.FieldType{Tp: 3},
	})
	c.Assert(col.Name, check.Equals, "col1")
	c.Assert(col.Type, check.Equals, uint8(3))
}

func (s *commonDataStructureSuite) TestDDLEventFromJob(c *check.C) {
	defer testleak.AfterTest(c)()
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
					{ID: 1, Name: timodel.CIStr{O: "id"}, FieldType: types.FieldType{Flag: mysql.PriKeyFlag}, State: timodel.StatePublic},
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
				{ID: 1, Name: timodel.CIStr{O: "id"}, FieldType: types.FieldType{Flag: mysql.PriKeyFlag}, State: timodel.StatePublic},
			},
		},
	}
	event := &DDLEvent{}
	event.FromJob(job, preTableInfo)
	c.Assert(event.StartTs, check.Equals, uint64(420536581131337731))
	c.Assert(event.TableInfo.TableID, check.Equals, int64(49))
	c.Assert(event.PreTableInfo.ColumnInfo, check.HasLen, 1)

	event = &DDLEvent{}
	event.FromJob(job, nil)
	c.Assert(event.PreTableInfo, check.IsNil)
}
