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

package syncer

import (
	"database/sql"
	"fmt"
	"strconv"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
)

type testCondSuite struct{}

var _ = Suite(&testCondSuite{})

func getTableDiff(c *C, db *sql.DB, schemaName, tableName, creatSQL string) *TableDiff {
	var (
		err       error
		parser2   *parser.Parser
		tableInfo *model.TableInfo
	)
	parser2 = parser.New()
	c.Assert(err, IsNil)
	tableInfo, err = dbutil.GetTableInfoBySQL(creatSQL, parser2)
	c.Assert(err, IsNil)
	columnMap := make(map[string]*model.ColumnInfo)
	for _, col := range tableInfo.Columns {
		columnMap[col.Name.O] = col
	}
	var primaryIdx *model.IndexInfo
	for _, idx := range tableInfo.Indices {
		if idx.Primary {
			primaryIdx = idx
		}
	}
	tableDiff := &TableDiff{
		Schema:     schemaName,
		Table:      tableName,
		Info:       tableInfo,
		PrimaryKey: primaryIdx,
		ColumnMap:  columnMap,
	}
	return tableDiff
}

func formatCond(c *C, db *sql.DB, schemaName, tblName, creatSQL string, pkvs [][]string) *Cond {
	tblDiff := getTableDiff(c, db, schemaName, tblName, creatSQL)
	return &Cond{
		Table:    tblDiff,
		PkValues: pkvs,
	}
}

func (s *testCondSuite) TestCondSelectMultiKey(c *C) {
	var (
		res *sql.Rows
	)
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	creatTbl := "create table if not exists `test_cond`.`test1`(" +
		"a int," +
		"b int," +
		"c int," +
		"primary key(a, b)" +
		");"
	// get table diff
	pkValues := make([][]string, 0)
	for i := 0; i < 3; i++ {
		// 3 primary key
		key1, key2 := strconv.Itoa(i+1), strconv.Itoa(i+2)
		pkValues = append(pkValues, []string{key1, key2})
	}
	cond := formatCond(c, db, "test_cond", "test1", creatTbl, pkValues)
	// format query string
	rowsQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s;", "`test_cond`.`test1`", cond.GetWhere())
	mock.ExpectQuery(
		"SELECT COUNT\\(\\*\\) FROM `test_cond`.`test1` WHERE \\(a,b\\) in \\(\\(\\?,\\?\\),\\(\\?,\\?\\),\\(\\?,\\?\\)\\);",
	).WithArgs(
		"1", "2", "2", "3", "3", "4",
	).WillReturnRows(mock.NewRows([]string{"COUNT(*)"}).AddRow("3"))
	c.Assert(err, IsNil)
	res, err = db.Query(rowsQuery, cond.GetArgs()...)
	c.Assert(err, IsNil)
	var cnt int
	if res.Next() {
		err = res.Scan(&cnt)
	}
	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, 3)
}

func (s *testCondSuite) TestCondGetWhereArgs(c *C) {
	db, _, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	type testCase struct {
		creatTbl   string
		pks        [][]string
		tblName    string
		schemaName string
		args       []string
		where      string
	}
	cases := []testCase{
		{
			creatTbl: `create table if not exists test_cond.test2(
				a char(10),
				b int,
				c int,
				primary key(a)
				);`, // single primary key,
			pks: [][]string{
				{"10a0"}, {"200"}, {"abc"},
			},
			tblName:    "test2",
			schemaName: "test_cond",
			where:      "a in (?,?,?)",
			args: []string{
				"10a0", "200", "abc",
			},
		},
		{
			creatTbl: `create table if not exists test_cond.test3(
				a int,
				b char(10),
				c varchar(10),
				primary key(a, b, c)
				);`, // multi primary key
			pks: [][]string{
				{"10", "abc", "ef"},
				{"9897", "afdkiefkjg", "acdee"},
			},
			tblName:    "test3",
			schemaName: "test_cond",
			where:      "(a,b,c) in ((?,?,?),(?,?,?))",
			args: []string{
				"10", "abc", "ef", "9897", "afdkiefkjg", "acdee",
			},
		},
	}
	for i := 0; i < len(cases); i++ {
		cond := formatCond(c, db, cases[i].schemaName, cases[i].tblName, cases[i].creatTbl, cases[i].pks)
		c.Assert(cond.GetWhere(), Equals, cases[i].where)
		rawArgs := cond.GetArgs()
		for j := 0; j < 3; j++ {
			curData := fmt.Sprintf("%v", rawArgs[j])
			c.Assert(curData, Equals, cases[i].args[j])
		}
	}
}
