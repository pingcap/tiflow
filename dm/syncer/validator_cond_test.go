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
	// ctx := context.Background()
	// parser2, err = utils.GetParser(ctx, db)
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
		Schema:     "test_cond",
		Table:      "test1",
		Info:       tableInfo,
		PrimaryKey: primaryIdx,
		ColumnMap:  columnMap,
	}
	return tableDiff
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
	tableDiff := getTableDiff(c, db, "test_cond", "test1", creatTbl)
	pkValues := make([][]string, 0)
	for i := 0; i < 3; i++ {
		// 3 primary key
		key1, key2 := strconv.Itoa(i+1), strconv.Itoa(i+2)
		pkValues = append(pkValues, []string{key1, key2})
	}
	cond := &Cond{
		Table:    tableDiff,
		PkValues: pkValues,
	}
	// format query string
	rowsQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s;", "`test_cond`.`test1`", cond.GetWhere())
	mock.ExpectPrepare("SELECT COUNT\\(\\*\\) FROM `test_cond`.`test1` WHERE \\(a,b\\) in \\(\\(\\?,\\?\\), \\(\\?,\\?\\), \\(\\?,\\?\\)\\);").ExpectQuery().WithArgs(
		"1", "2", "2", "3", "3", "4",
	).WillReturnRows(mock.NewRows([]string{"COUNT(*)"}).AddRow("3"))
	prepare, err := db.Prepare(rowsQuery)
	c.Assert(err, IsNil)
	res, err = prepare.Query(cond.GetArgs()...)
	c.Assert(err, IsNil)
	var cnt int
	if res.Next() {
		err = res.Scan(&cnt)
	}
	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, 3)
}
