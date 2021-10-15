// Copyright 2019 PingCAP, Inc.
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

package syncer

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/filter"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct{}

func (t *testUtilSuite) TestGetTableByDML(c *C) {
	cases := []struct {
		sql      string
		schema   string
		table    string
		hasError bool
	}{
		{
			sql:      "INSERT INTO db1.tbl1 VALUES (1)",
			schema:   "db1",
			table:    "tbl1",
			hasError: false,
		},
		{
			sql:      "REPLACE INTO `db1`.`tbl1` (c1) VALUES (1)", // parsed as an ast.InsertStmt
			schema:   "db1",
			table:    "tbl1",
			hasError: false,
		},
		{
			sql:      "INSERT INTO `tbl1` VALUES (2)",
			schema:   "",
			table:    "tbl1",
			hasError: false,
		},
		{
			sql:      "UPDATE `db1`.`tbl1` SET c1=2 WHERE c1=1",
			schema:   "db1",
			table:    "tbl1",
			hasError: false,
		},
		{
			sql:      "DELETE FROM tbl1 WHERE c1=2",
			schema:   "",
			table:    "tbl1",
			hasError: false,
		},
		{
			sql:      "SELECT * FROM db1.tbl1",
			schema:   "",
			table:    "",
			hasError: true,
		},
	}

	parser2 := parser.New()
	for _, cs := range cases {
		stmt, err := parser2.ParseOneStmt(cs.sql, "", "")
		c.Assert(err, IsNil)
		dml, ok := stmt.(ast.DMLNode)
		c.Assert(ok, IsTrue)
		table, err := getTableByDML(dml)
		if cs.hasError {
			c.Assert(err, NotNil)
			c.Assert(table, IsNil)
		} else {
			c.Assert(err, IsNil)
			c.Assert(table.Schema, Equals, cs.schema)
			c.Assert(table.Name, Equals, cs.table)
		}
	}
}

func (t *testUtilSuite) TestToBinlogType(c *C) {
	testCases := []struct {
		enableRelay bool
		tp          BinlogType
	}{
		{
			true,
			LocalBinlog,
		}, {
			false,
			RemoteBinlog,
		},
	}

	for _, testCase := range testCases {
		tp := toBinlogType(testCase.enableRelay)
		c.Assert(tp, Equals, testCase.tp)
	}
}

func (t *testUtilSuite) TestTableNameResultSet(c *C) {
	rs := &ast.TableSource{
		Source: &ast.TableName{
			Schema: model.NewCIStr("test"),
			Name:   model.NewCIStr("t1"),
		},
	}
	table, err := tableNameResultSet(rs)
	c.Assert(err, IsNil)
	c.Assert(table.Schema, Equals, "test")
	c.Assert(table.Name, Equals, "t1")
}

func (t *testUtilSuite) TestRecordSourceTbls(c *C) {
	sourceTbls := make(map[string]map[string]struct{})

	recordSourceTbls(sourceTbls, &ast.CreateDatabaseStmt{}, &filter.Table{Schema: "a", Name: ""})
	c.Assert(sourceTbls, HasKey, "a")
	c.Assert(sourceTbls["a"], HasKey, "")

	recordSourceTbls(sourceTbls, &ast.CreateTableStmt{}, &filter.Table{Schema: "a", Name: "b"})
	c.Assert(sourceTbls, HasKey, "a")
	c.Assert(sourceTbls["a"], HasKey, "b")

	recordSourceTbls(sourceTbls, &ast.DropTableStmt{}, &filter.Table{Schema: "a", Name: "b"})
	_, ok := sourceTbls["a"]["b"]
	c.Assert(ok, IsFalse)

	recordSourceTbls(sourceTbls, &ast.CreateTableStmt{}, &filter.Table{Schema: "a", Name: "c"})
	c.Assert(sourceTbls, HasKey, "a")
	c.Assert(sourceTbls["a"], HasKey, "c")

	recordSourceTbls(sourceTbls, &ast.DropDatabaseStmt{}, &filter.Table{Schema: "a", Name: ""})
	c.Assert(sourceTbls, HasLen, 0)
}
