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
	parser_types "github.com/pingcap/parser/types"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

type schemaStorageSuite struct{}

var _ = check.Suite(&schemaStorageSuite{})

func (s *schemaStorageSuite) TestPKShouldBeInTheFirstPlaceWhenPKIsNotHandle(c *check.C) {
	defer testleak.AfterTest(c)()
	t := timodel.TableInfo{
		Columns: []*timodel.ColumnInfo{
			{
				Name: timodel.CIStr{O: "group"},
				FieldType: parser_types.FieldType{
					Flag: mysql.NotNullFlag,
				},
				State: timodel.StatePublic,
			},
			{
				Name: timodel.CIStr{O: "name"},
				FieldType: parser_types.FieldType{
					Flag: mysql.NotNullFlag,
				},
				State: timodel.StatePublic,
			},
			{
				Name:  timodel.CIStr{O: "id"},
				State: timodel.StatePublic,
			},
		},
		Indices: []*timodel.IndexInfo{
			{
				Name: timodel.CIStr{
					O: "group",
				},
				Columns: []*timodel.IndexColumn{
					{
						Name:   timodel.CIStr{O: "group"},
						Offset: 0,
					},
				},
				Unique: false,
			},
			{
				Name: timodel.CIStr{
					O: "name",
				},
				Columns: []*timodel.IndexColumn{
					{
						Name:   timodel.CIStr{O: "name"},
						Offset: 0,
					},
				},
				Unique: true,
			},
			{
				Name: timodel.CIStr{
					O: "PRIMARY",
				},
				Columns: []*timodel.IndexColumn{
					{
						Name:   timodel.CIStr{O: "id"},
						Offset: 1,
					},
				},
				Primary: true,
			},
		},
		IsCommonHandle: true,
		PKIsHandle:     false,
	}
	info := WrapTableInfo(1, "", 0, &t)
	cols := info.GetUniqueKeys()
	c.Assert(cols, check.DeepEquals, [][]string{
		{"id"}, {"name"},
	})
}

func (s *schemaStorageSuite) TestPKShouldBeInTheFirstPlaceWhenPKIsHandle(c *check.C) {
	defer testleak.AfterTest(c)()
	t := timodel.TableInfo{
		Indices: []*timodel.IndexInfo{
			{
				Name: timodel.CIStr{
					O: "uniq_job",
				},
				Columns: []*timodel.IndexColumn{
					{Name: timodel.CIStr{O: "job"}},
				},
				Unique: true,
			},
		},
		Columns: []*timodel.ColumnInfo{
			{
				Name: timodel.CIStr{
					O: "job",
				},
				FieldType: parser_types.FieldType{
					Flag: mysql.NotNullFlag,
				},
				State: timodel.StatePublic,
			},
			{
				Name: timodel.CIStr{
					O: "uid",
				},
				FieldType: parser_types.FieldType{
					Flag: mysql.PriKeyFlag,
				},
				State: timodel.StatePublic,
			},
		},
		PKIsHandle: true,
	}
	info := WrapTableInfo(1, "", 0, &t)
	cols := info.GetUniqueKeys()
	c.Assert(cols, check.DeepEquals, [][]string{
		{"uid"}, {"job"},
	})
}

func (s *schemaStorageSuite) TestUniqueKeyIsHandle(c *check.C) {
	defer testleak.AfterTest(c)()
	t := timodel.TableInfo{
		Columns: []*timodel.ColumnInfo{
			{
				Name: timodel.CIStr{O: "group"},
				FieldType: parser_types.FieldType{
					Flag: mysql.NotNullFlag,
				},
				State: timodel.StatePublic,
			},
			{
				Name: timodel.CIStr{O: "name"},
				FieldType: parser_types.FieldType{
					Flag: mysql.NotNullFlag,
				},
				State: timodel.StatePublic,
			},
		},
		Indices: []*timodel.IndexInfo{
			{
				Name: timodel.CIStr{
					O: "group",
				},
				Columns: []*timodel.IndexColumn{
					{
						Name:   timodel.CIStr{O: "group"},
						Offset: 0,
					},
				},
				Unique: false,
			},
			{
				Name: timodel.CIStr{
					O: "name",
				},
				Columns: []*timodel.IndexColumn{
					{
						Name:   timodel.CIStr{O: "name"},
						Offset: 0,
					},
				},
				Unique: true,
			},
		},
		IsCommonHandle: false,
		PKIsHandle:     false,
	}
	info := WrapTableInfo(1, "", 0, &t)
	cols := info.GetUniqueKeys()
	c.Assert(cols, check.DeepEquals, [][]string{{"name"}})
}

func (s *schemaStorageSuite) TestHandleKeyPriority(c *check.C) {
	defer testleak.AfterTest(c)()
	t := timodel.TableInfo{
		Columns: []*timodel.ColumnInfo{
			{
				Name: timodel.CIStr{O: "a"},
				FieldType: parser_types.FieldType{
					Flag: mysql.NotNullFlag | mysql.MultipleKeyFlag,
				},
				State: timodel.StatePublic,
			},
			{
				Name: timodel.CIStr{O: "b"},
				FieldType: parser_types.FieldType{
					Flag: mysql.NotNullFlag | mysql.MultipleKeyFlag,
				},
				State: timodel.StatePublic,
			},
			{
				Name: timodel.CIStr{O: "c"},
				FieldType: parser_types.FieldType{
					Flag: mysql.NotNullFlag | mysql.UniqueKeyFlag,
				},
				State: timodel.StatePublic,
			},
			{
				Name:      timodel.CIStr{O: "d"},
				FieldType: parser_types.FieldType{
					// test not null unique index
					// Flag: mysql.NotNullFlag,
				},
				State: timodel.StatePublic,
			},
			{
				Name: timodel.CIStr{O: "e"},
				FieldType: parser_types.FieldType{
					Flag: mysql.NotNullFlag,
				},
				State: timodel.StatePublic,
				// test virtual generated column is not treated as unique key
				GeneratedExprString: "as d",
				GeneratedStored:     false,
			},
		},
		Indices: []*timodel.IndexInfo{
			{
				ID: 10,
				Name: timodel.CIStr{
					O: "a,b",
				},
				Columns: []*timodel.IndexColumn{
					{Name: timodel.CIStr{O: "a"}, Offset: 0},
					{Name: timodel.CIStr{O: "b"}, Offset: 1},
				},
				Unique: true,
			},
			{
				ID: 9,
				Name: timodel.CIStr{
					O: "c",
				},
				Columns: []*timodel.IndexColumn{
					{Name: timodel.CIStr{O: "c"}, Offset: 2},
				},
				Unique: true,
			},
			{
				ID: 8,
				Name: timodel.CIStr{
					O: "b",
				},
				Columns: []*timodel.IndexColumn{
					{Name: timodel.CIStr{O: "b"}, Offset: 1},
				},
				Unique: true,
			},
			{
				ID: 7,
				Name: timodel.CIStr{
					O: "d",
				},
				Columns: []*timodel.IndexColumn{
					{Name: timodel.CIStr{O: "d"}, Offset: 3},
				},
				Unique: true,
			},
			{
				ID: 6,
				Name: timodel.CIStr{
					O: "e",
				},
				Columns: []*timodel.IndexColumn{
					{Name: timodel.CIStr{O: "e"}, Offset: 4},
				},
				Unique: true,
			},
		},
		IsCommonHandle: false,
		PKIsHandle:     false,
	}
	info := WrapTableInfo(1, "", 0, &t)
	cols := info.GetUniqueKeys()
	c.Assert(info.HandleIndexID, check.Equals, int64(8))
	c.Assert(cols, check.DeepEquals, [][]string{{"a", "b"}, {"c"}, {"b"}})
}

func (s *schemaStorageSuite) TestTableInfoGetterFuncs(c *check.C) {
	defer testleak.AfterTest(c)()
	t := timodel.TableInfo{
		ID:   1071,
		Name: timodel.CIStr{O: "t1"},
		Columns: []*timodel.ColumnInfo{
			{
				ID:   0,
				Name: timodel.CIStr{O: "a"},
				FieldType: parser_types.FieldType{
					// test binary flag
					Flag:    mysql.NotNullFlag | mysql.BinaryFlag,
					Charset: "binary",
				},
				State: timodel.StatePublic,
			},
			{
				ID:   1,
				Name: timodel.CIStr{O: "b"},
				FieldType: parser_types.FieldType{
					// test unsigned flag
					Flag: mysql.NotNullFlag | mysql.UnsignedFlag,
				},
				State: timodel.StatePublic,
			},
			{
				ID:   2,
				Name: timodel.CIStr{O: "c"},
				FieldType: parser_types.FieldType{
					Flag: mysql.NotNullFlag,
				},
				State: timodel.StatePublic,
			},
		},
		Indices: []*timodel.IndexInfo{
			{
				ID: 0,
				Name: timodel.CIStr{
					O: "c",
				},
				Columns: []*timodel.IndexColumn{
					{Name: timodel.CIStr{O: "c"}, Offset: 2},
				},
				Unique: true,
			},
		},
		IsCommonHandle: false,
		PKIsHandle:     false,
	}
	info := WrapTableInfo(1, "test", 0, &t)

	col, exists := info.GetColumnInfo(2)
	c.Assert(exists, check.IsTrue)
	c.Assert(col.Name.O, check.Equals, "c")
	_, exists = info.GetColumnInfo(4)
	c.Assert(exists, check.IsFalse)

	c.Assert(info.String(), check.Equals, "TableInfo, ID: 1071, Name:test.t1, ColNum: 3, IdxNum: 1, PKIsHandle: false")

	idx, exists := info.GetIndexInfo(0)
	c.Assert(exists, check.IsTrue)
	c.Assert(idx.Name.O, check.Equals, "c")
	_, exists = info.GetIndexInfo(1)
	c.Assert(exists, check.IsFalse)

	handleColIDs, fts, colInfos := info.GetRowColInfos()
	c.Assert(handleColIDs, check.DeepEquals, []int64{-1})
	c.Assert(len(fts), check.Equals, 3)
	c.Assert(len(colInfos), check.Equals, 3)

	c.Assert(info.IsColumnUnique(0), check.IsFalse)
	c.Assert(info.IsColumnUnique(2), check.IsTrue)
	c.Assert(info.ExistTableUniqueColumn(), check.IsTrue)

	// check IsEligible
	c.Assert(info.IsEligible(false), check.IsTrue)
	t = timodel.TableInfo{
		ID:   1073,
		Name: timodel.CIStr{O: "t2"},
		Columns: []*timodel.ColumnInfo{
			{
				ID:        0,
				Name:      timodel.CIStr{O: "a"},
				FieldType: parser_types.FieldType{},
				State:     timodel.StatePublic,
			},
		},
		Indices: []*timodel.IndexInfo{
			{
				ID:   0,
				Name: timodel.CIStr{O: "a"},
				Columns: []*timodel.IndexColumn{
					{Name: timodel.CIStr{O: "a"}, Offset: 0},
				},
				Unique: true,
			},
		},
		IsCommonHandle: false,
		PKIsHandle:     false,
	}
	info = WrapTableInfo(1, "test", 0, &t)
	c.Assert(info.IsEligible(false), check.IsFalse)
	c.Assert(info.IsEligible(true), check.IsTrue)
	t.View = &timodel.ViewInfo{}
	info = WrapTableInfo(1, "test", 0, &t)
	c.Assert(info.IsEligible(false), check.IsTrue)
}

func (s *schemaStorageSuite) TestTableInfoClone(c *check.C) {
	defer testleak.AfterTest(c)()
	t := timodel.TableInfo{
		ID:   1071,
		Name: timodel.CIStr{O: "t1"},
		Columns: []*timodel.ColumnInfo{
			{
				ID:   0,
				Name: timodel.CIStr{O: "c"},
				FieldType: parser_types.FieldType{
					Flag: mysql.NotNullFlag,
				},
				State: timodel.StatePublic,
			},
		},
		Indices: []*timodel.IndexInfo{
			{
				ID: 0,
				Name: timodel.CIStr{
					O: "c",
				},
				Columns: []*timodel.IndexColumn{
					{Name: timodel.CIStr{O: "c"}, Offset: 0},
				},
				Unique: true,
			},
		},
	}
	info := WrapTableInfo(10, "test", 0, &t)
	cloned := info.Clone()
	c.Assert(cloned.SchemaID, check.Equals, info.SchemaID)
	cloned.SchemaID = 100
	c.Assert(info.SchemaID, check.Equals, int64(10))
}
