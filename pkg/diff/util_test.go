// Copyright 2024 PingCAP, Inc.
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

package diff

import (
	"github.com/pingcap/check"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	pmodel "github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tidb/pkg/util/dbutil/dbutiltest"
)

var _ = check.Suite(&testUtilSuite{})

type testUtilSuite struct{}

func (s *testUtilSuite) TestIgnoreColumns(c *check.C) {
	createTableSQL1 := "CREATE TABLE `test`.`atest` (`a` int, `b` int, `c` int, `d` int, primary key(`a`))"
	tableInfo1, err := dbutiltest.GetTableInfoBySQL(createTableSQL1, parser.New())
	c.Assert(err, check.IsNil)
	tbInfo := ignoreColumns(tableInfo1, []string{"a"})
	c.Assert(tbInfo.Columns, check.HasLen, 3)
	c.Assert(tbInfo.Indices, check.HasLen, 0)
	c.Assert(tbInfo.Columns[2].Offset, check.Equals, 2)

	createTableSQL2 := "CREATE TABLE `test`.`atest` (`a` int, `b` int, `c` int, `d` int, primary key(`a`), index idx(`b`, `c`))"
	tableInfo2, err := dbutiltest.GetTableInfoBySQL(createTableSQL2, parser.New())
	c.Assert(err, check.IsNil)
	tbInfo = ignoreColumns(tableInfo2, []string{"a", "b"})
	c.Assert(tbInfo.Columns, check.HasLen, 2)
	c.Assert(tbInfo.Indices, check.HasLen, 0)

	createTableSQL3 := "CREATE TABLE `test`.`atest` (`a` int, `b` int, `c` int, `d` int, primary key(`a`), index idx(`b`, `c`))"
	tableInfo3, err := dbutiltest.GetTableInfoBySQL(createTableSQL3, parser.New())
	c.Assert(err, check.IsNil)
	tbInfo = ignoreColumns(tableInfo3, []string{"b", "c"})
	c.Assert(tbInfo.Columns, check.HasLen, 2)
	c.Assert(tbInfo.Indices, check.HasLen, 1)
}

func (s *testUtilSuite) TestRowContainsCols(c *check.C) {
	row := map[string]*dbutil.ColumnData{
		"a": nil,
		"b": nil,
		"c": nil,
	}

	cols := []*model.ColumnInfo{
		{
			Name: pmodel.NewCIStr("a"),
		}, {
			Name: pmodel.NewCIStr("b"),
		}, {
			Name: pmodel.NewCIStr("c"),
		},
	}

	contain := rowContainsCols(row, cols)
	c.Assert(contain, check.Equals, true)

	delete(row, "a")
	contain = rowContainsCols(row, cols)
	c.Assert(contain, check.Equals, false)
}

func (s *testUtilSuite) TestRowToString(c *check.C) {
	row := make(map[string]*dbutil.ColumnData)
	row["id"] = &dbutil.ColumnData{
		Data:   []byte("1"),
		IsNull: false,
	}

	row["name"] = &dbutil.ColumnData{
		Data:   []byte("abc"),
		IsNull: false,
	}

	row["info"] = &dbutil.ColumnData{
		Data:   nil,
		IsNull: true,
	}

	rowStr := rowToString(row)
	c.Assert(rowStr, check.Matches, ".*id: 1.*")
	c.Assert(rowStr, check.Matches, ".*name: abc.*")
	c.Assert(rowStr, check.Matches, ".*info: IsNull.*")
}

func (s *testUtilSuite) TestMinLenInSlices(c *check.C) {
	testCases := []struct {
		slices [][]string
		expect int
	}{
		{
			[][]string{
				{"1", "2"},
				{"1", "2", "3"},
			},
			2,
		}, {
			[][]string{
				{"1", "2"},
				{},
			},
			0,
		}, {
			[][]string{},
			0,
		}, {
			[][]string{
				{"1", "2"},
				{},
				{"1", "2", "3"},
			},
			0,
		},
	}

	for _, testCase := range testCases {
		minLen := minLenInSlices(testCase.slices)
		c.Assert(minLen, check.Equals, testCase.expect)
	}
}
