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
	parser_types "github.com/pingcap/tidb/parser/types"
	"github.com/stretchr/testify/require"
)

func TestPKShouldBeInTheFirstPlaceWhenPKIsNotHandle(t *testing.T) {
	t.Parallel()
	ft := parser_types.NewFieldType(mysql.TypeUnspecified)
	ft.SetFlag(mysql.NotNullFlag)

	tbl := timodel.TableInfo{
		Columns: []*timodel.ColumnInfo{
			{
				Name:      timodel.CIStr{O: "group"},
				FieldType: *ft,
				State:     timodel.StatePublic,
			},
			{
				Name:      timodel.CIStr{O: "name"},
				FieldType: *ft,
				State:     timodel.StatePublic,
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
	info := WrapTableInfo(1, "", 0, &tbl)
	cols := info.GetUniqueKeys()
	require.Equal(t, [][]string{
		{"id"}, {"name"},
	}, cols)
}

func TestPKShouldBeInTheFirstPlaceWhenPKIsHandle(t *testing.T) {
	t.Parallel()
	jobft := parser_types.NewFieldType(mysql.TypeUnspecified)
	jobft.SetFlag(mysql.NotNullFlag)

	uidft := jobft.Clone()
	uidft.SetFlag(mysql.PriKeyFlag)

	tbl := timodel.TableInfo{
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
				FieldType: *jobft,
				State:     timodel.StatePublic,
			},
			{
				Name: timodel.CIStr{
					O: "uid",
				},
				FieldType: *uidft,
				State:     timodel.StatePublic,
			},
		},
		PKIsHandle: true,
	}
	info := WrapTableInfo(1, "", 0, &tbl)
	cols := info.GetUniqueKeys()
	require.Equal(t, [][]string{
		{"uid"}, {"job"},
	}, cols)
}

func TestUniqueKeyIsHandle(t *testing.T) {
	t.Parallel()
	ft := parser_types.NewFieldType(mysql.TypeUnspecified)
	ft.SetFlag(mysql.NotNullFlag)
	tbl := timodel.TableInfo{
		Columns: []*timodel.ColumnInfo{
			{
				Name:      timodel.CIStr{O: "group"},
				FieldType: *ft,
				State:     timodel.StatePublic,
			},
			{
				Name:      timodel.CIStr{O: "name"},
				FieldType: *ft,
				State:     timodel.StatePublic,
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
	info := WrapTableInfo(1, "", 0, &tbl)
	cols := info.GetUniqueKeys()
	require.Equal(t, [][]string{{"name"}}, cols)
}

func TestHandleKeyPriority(t *testing.T) {
	t.Parallel()
	ftNull := parser_types.NewFieldType(mysql.TypeUnspecified)
	ftNull.SetFlag(mysql.NotNullFlag)

	ftNotNull := parser_types.NewFieldType(mysql.TypeUnspecified)
	ftNotNull.SetFlag(mysql.NotNullFlag | mysql.MultipleKeyFlag)

	tbl := timodel.TableInfo{
		Columns: []*timodel.ColumnInfo{
			{
				Name:      timodel.CIStr{O: "a"},
				FieldType: *ftNotNull,
				State:     timodel.StatePublic,
			},
			{
				Name:      timodel.CIStr{O: "b"},
				FieldType: *ftNotNull,
				State:     timodel.StatePublic,
			},
			{
				Name:      timodel.CIStr{O: "c"},
				FieldType: *ftNotNull,
				State:     timodel.StatePublic,
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
				Name:      timodel.CIStr{O: "e"},
				FieldType: *ftNull,
				State:     timodel.StatePublic,
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
	info := WrapTableInfo(1, "", 0, &tbl)
	cols := info.GetUniqueKeys()
	require.Equal(t, int64(8), info.HandleIndexID)
	require.Equal(t, [][]string{{"a", "b"}, {"c"}, {"b"}}, cols)
}

func TestTableInfoGetterFuncs(t *testing.T) {
	t.Parallel()

	ftNull := parser_types.NewFieldType(mysql.TypeUnspecified)
	ftNull.SetFlag(mysql.NotNullFlag)

	ftNotNull := parser_types.NewFieldType(mysql.TypeUnspecified)
	ftNotNull.SetFlag(mysql.NotNullFlag | mysql.MultipleKeyFlag)

	ftNotNullBinCharset := parser_types.NewFieldType(mysql.TypeUnspecified)
	ftNotNullBinCharset.SetFlag(mysql.NotNullFlag | mysql.MultipleKeyFlag)
	ftNotNullBinCharset.SetCharset("binary")

	tbl := timodel.TableInfo{
		ID:   1071,
		Name: timodel.CIStr{O: "t1"},
		Columns: []*timodel.ColumnInfo{
			{
				ID:        0,
				Name:      timodel.CIStr{O: "a"},
				FieldType: *ftNotNullBinCharset,
				State:     timodel.StatePublic,
			},
			{
				ID:        1,
				Name:      timodel.CIStr{O: "b"},
				FieldType: *ftNotNull,
				State:     timodel.StatePublic,
			},
			{
				ID:        2,
				Name:      timodel.CIStr{O: "c"},
				FieldType: *ftNull,
				State:     timodel.StatePublic,
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
	info := WrapTableInfo(1, "test", 0, &tbl)

	col, exists := info.GetColumnInfo(2)
	require.True(t, exists)
	require.Equal(t, "c", col.Name.O)
	_, exists = info.GetColumnInfo(4)
	require.False(t, exists)

	require.Equal(t, "TableInfo, ID: 1071, Name:test.t1, ColNum: 3, IdxNum: 1, PKIsHandle: false", info.String())

	idx, exists := info.GetIndexInfo(0)
	require.True(t, exists)
	require.Equal(t, "c", idx.Name.O)
	_, exists = info.GetIndexInfo(1)
	require.False(t, exists)

	handleColIDs, fts, colInfos := info.GetRowColInfos()
	require.Equal(t, []int64{-1}, handleColIDs)
	require.Equal(t, 3, len(fts))
	require.Equal(t, 3, len(colInfos))

	require.False(t, info.IsColumnUnique(0))
	require.True(t, info.IsColumnUnique(2))
	require.True(t, info.ExistTableUniqueColumn())

	// check IsEligible
	require.True(t, info.IsEligible(false))
	tbl = timodel.TableInfo{
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
	info = WrapTableInfo(1, "test", 0, &tbl)
	require.False(t, info.IsEligible(false))
	require.True(t, info.IsEligible(true))

	// View is eligible.
	tbl.View = &timodel.ViewInfo{}
	info = WrapTableInfo(1, "test", 0, &tbl)
	require.True(t, info.IsView())
	require.True(t, info.IsEligible(false))

	// Sequence is ineligible.
	tbl.Sequence = &timodel.SequenceInfo{}
	info = WrapTableInfo(1, "test", 0, &tbl)
	require.True(t, info.IsSequence())
	require.False(t, info.IsEligible(false))
	require.False(t, info.IsEligible(true))
}

func TestTableInfoClone(t *testing.T) {
	t.Parallel()
	ft := parser_types.NewFieldType(mysql.TypeUnspecified)
	ft.SetFlag(mysql.NotNullFlag)
	tbl := timodel.TableInfo{
		ID:   1071,
		Name: timodel.CIStr{O: "t1"},
		Columns: []*timodel.ColumnInfo{
			{
				ID:        0,
				Name:      timodel.CIStr{O: "c"},
				FieldType: *ft,
				State:     timodel.StatePublic,
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
	info := WrapTableInfo(10, "test", 0, &tbl)
	cloned := info.Clone()
	require.Equal(t, cloned.SchemaID, info.SchemaID)
	cloned.SchemaID = 100
	require.Equal(t, int64(10), info.SchemaID)
}
