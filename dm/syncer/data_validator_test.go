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
	"context"
	"database/sql"
	"database/sql/driver"
	"os"
	"sort"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-mysql-org/go-mysql/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	parsermysql "github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
)

type testDataValidatorSuite struct {
	eventsGenerator *event.Generator
	cfg             *config.SubTaskConfig
}

var _ = Suite(&testDataValidatorSuite{})

func (d *testDataValidatorSuite) SetUpSuite(c *C) {
	previousGTIDSetStr := "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,406a3f61-690d-11e7-87c5-6c92bf46f384:1-94321383"
	previousGTIDSet, err := gtid.ParserGTID(mysql.MySQLFlavor, previousGTIDSetStr)
	if err != nil {
		c.Fatal(err)
	}
	latestGTIDStr := "3ccc475b-2343-11e7-be21-6c0b84d59f30:14"
	latestGTID, err := gtid.ParserGTID(mysql.MySQLFlavor, latestGTIDStr)
	c.Assert(err, IsNil)
	d.eventsGenerator, err = event.NewGenerator(mysql.MySQLFlavor, 1, 0, latestGTID, previousGTIDSet, 0)
	if err != nil {
		c.Fatal(err)
	}
	loaderDir, err := os.MkdirTemp("", "loader")
	c.Assert(err, IsNil)
	loaderCfg := config.LoaderConfig{
		Dir: loaderDir,
	}
	d.cfg = &config.SubTaskConfig{
		From:             config.GetDBConfigForTest(),
		To:               config.GetDBConfigForTest(),
		ServerID:         101,
		MetaSchema:       "test",
		Name:             "syncer_ut",
		ShadowTableRules: []string{config.DefaultShadowTableRules},
		TrashTableRules:  []string{config.DefaultTrashTableRules},
		Mode:             config.ModeIncrement,
		Flavor:           "mysql",
		LoaderConfig:     loaderCfg,
	}
	d.cfg.Experimental.AsyncCheckpointFlush = true
	d.cfg.From.Adjust()
	d.cfg.To.Adjust()

	d.cfg.UseRelay = false
}

func (d *testDataValidatorSuite) genDBConn(c *C, db *sql.DB) *dbconn.DBConn {
	baseDB := conn.NewBaseDB(db, func() {})
	baseConn, err := baseDB.GetBaseConn(context.Background())
	c.Assert(err, IsNil)
	cfg, err := d.cfg.Clone()
	c.Assert(err, IsNil)
	return &dbconn.DBConn{
		BaseConn: baseConn,
		Cfg:      cfg,
	}
}

func (d *testDataValidatorSuite) TestRowDataIteratorImpl(c *C) {
	var (
		iter   RowDataIterator
		dbConn *dbconn.DBConn
	)
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	type testTableSchm struct {
		a string
		b string
	}
	rowVals := []testTableSchm{
		{
			a: "a",
			b: "100",
		},
		{
			a: "c",
			b: "200",
		},
		{
			a: "d",
			b: "300",
		},
		{
			a: "e",
			b: "400",
		},
	}
	expectRows := sqlmock.NewRows([]string{"a", "b"})
	for i := 0; i < len(rowVals); i++ {
		expectRows = expectRows.AddRow(rowVals[i].a, rowVals[i].b)
	}
	mock.ExpectQuery(
		"SELECT .* FROM .* WHERE a in .*",
	).WithArgs(
		"a", "c", "d", "e",
	).WillReturnRows(expectRows)
	c.Assert(err, IsNil)
	cond := formatCond(c, db, "test_row", "test", `
		create table if not exists test_row.test(
			a char(1),
			b int,
			primary key(a)
		);
	`, [][]string{{"a"}, {"c"}, {"d"}, {"e"}})
	c.Assert(err, IsNil)
	dbConn = d.genDBConn(c, db)
	iter, err = getRowsFrom(cond, dbConn)
	c.Assert(err, IsNil)
	for i := range rowVals {
		curVal, err := iter.Next()
		c.Assert(err, IsNil)
		keys := make([]string, 0)
		for key := range curVal {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			return keys[i] < keys[j]
		})
		c.Assert(keys[0], Equals, "a")
		c.Assert(keys[1], Equals, "b")
		c.Assert(string(curVal["a"].Data), Equals, rowVals[i].a)
		c.Assert(string(curVal["b"].Data), Equals, rowVals[i].b)
	}
	nxtVal, err := iter.Next()
	c.Assert(nxtVal, IsNil)
	c.Assert(err, IsNil)
}

func (r *testDataValidatorSuite) TestRowChangeIteratorImpl(c *C) {
	db, _, err := sqlmock.New()
	c.Assert(err, IsNil)
	rows := []*rowChange{
		{
			pk:      []string{"pk", "1"},
			data:    []interface{}{"pk", "1", "some data"},
			theType: rowInsert,
		},
		{
			pk:      []string{"pk", "2"},
			data:    []interface{}{"pk", "2", "some data"},
			theType: rowDeleted,
		},
		{
			pk:      []string{"pkg", "2"},
			data:    []interface{}{"pkg", "2", "some data"},
			theType: rowDeleted,
		},
		{
			pk:      []string{"pke", "2"},
			data:    []interface{}{"pke", "2", "some data"},
			theType: rowUpdated,
		},
	}
	testCases := []map[string]string{
		{
			"pk1":   "pk",
			"pk2":   "1",
			"other": "some data",
		},
		{
			"pk1":   "pk",
			"pk2":   "2",
			"other": "some data",
		},
		{
			"pk1":   "pke",
			"pk2":   "2",
			"other": "some data",
		},
		{
			"pk1":   "pkg",
			"pk2":   "2",
			"other": "some data",
		},
	}
	creatTbl := `create table if not exists test_it.test1 (
		pk1 varchar(4),
		pk2 int,
		other text,
		primary key(pk1, pk2)
		);`
	tableDiff := getTableDiff(c, db, "test_it", "test1", creatTbl)
	var iter RowDataIterator
	iter, err = getRowChangeIterator(tableDiff, rows)
	c.Assert(err, IsNil)
	for i := 0; i < len(rows); i++ {
		res, err := iter.Next()
		c.Assert(err, IsNil)
		c.Assert(res, NotNil)
		colNames := []string{}
		for key := range res {
			colNames = append(colNames, key)
			c.Assert(string(res[key].Data), Equals, testCases[i][key])
		}
		sort.Slice(colNames, func(left, right int) bool {
			return colNames[left] < colNames[right]
		})
		c.Assert(colNames[0], Equals, "other")
		c.Assert(colNames[1], Equals, "pk1")
		c.Assert(colNames[2], Equals, "pk2")
	}
}

func (d *testDataValidatorSuite) TestGetRowsFrom(c *C) {
	type testCase struct {
		schemaName string
		tblName    string
		creatSQL   string
		pkValues   [][]string
		allCols    []string
		rowData    [][]string
		querySQL   string
	}
	testCases := []testCase{
		{
			schemaName: "test1",
			tblName:    "tbl1",
			creatSQL: `create table if not exists test1.tbl1(
				a int,
				b int,
				c int,
				primary key(a, b)
			);`,
			pkValues: [][]string{
				{"1", "2"}, {"3", "4"}, {"5", "6"},
			},
			allCols: []string{"a", "b", "c"},
			rowData: [][]string{
				{"1", "2", "3"}, {"3", "4", "5"}, {"5", "6", "7"},
			},
			querySQL: "SELECT .* FROM .*test1.*",
		},
		{
			schemaName: "test2",
			tblName:    "tbl2",
			creatSQL: `create table if not exists test2.tbl2(
				a char(10),
				other text,
				primary key(a)
			);`,
			pkValues: [][]string{
				{"a"}, {"b"}, {"c"},
			},
			allCols: []string{"a", "other"},
			rowData: [][]string{
				{"a", "some data"}, {"b", "some data"}, {"c", "some data"},
			},
			querySQL: "SELECT .* FROM .*test2.*",
		},
	}
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	for i, testCase := range testCases {
		args := []driver.Value{}
		for _, arr := range testCases[i].pkValues {
			for _, val := range arr {
				args = append(args, val)
			}
		}
		dataRows := mock.NewRows(testCase.allCols)
		for j := range testCases[i].rowData {
			args := []driver.Value{}
			for _, val := range testCase.rowData[j] {
				args = append(args, val)
			}
			dataRows = dataRows.AddRow(args...)
		}
		mock.ExpectQuery(testCase.querySQL).WithArgs(args...).WillReturnRows(dataRows)
		cond := formatCond(
			c, db,
			testCase.schemaName,
			testCase.tblName,
			testCase.creatSQL,
			testCase.pkValues,
		)
		dbConn := d.genDBConn(c, db)
		var iter RowDataIterator
		iter, err = getRowsFrom(cond, dbConn)
		c.Assert(err, IsNil)
		var res map[string]*dbutil.ColumnData
		for j := 0; j < 3; j++ {
			res, err = iter.Next()
			c.Assert(err, IsNil)
			pkRes := getPKValues(res, cond)
			// for each primary key
			for k, val := range testCase.pkValues[j] {
				c.Assert(pkRes[k], Equals, val)
			}
			// for each col
			for k, val := range testCase.rowData[j] {
				colName := testCase.allCols[k]
				c.Assert(string(res[colName].Data), Equals, val)
			}
		}
		res, err = iter.Next()
		c.Assert(res, IsNil)
		c.Assert(err, IsNil)
	}
}

func (d *testDataValidatorSuite) TestRowToString(c *C) {
	testCases := []map[string]*dbutil.ColumnData{
		{
			"pk-1": &dbutil.ColumnData{
				Data:   []byte("some data"),
				IsNull: false,
			},
			"pk-2": &dbutil.ColumnData{
				Data:   []byte("simple data"),
				IsNull: false,
			},
		},
		{
			"pk-3": &dbutil.ColumnData{
				Data:   nil,
				IsNull: true,
			},
			"pk-4": &dbutil.ColumnData{
				Data:   []byte("data"),
				IsNull: false,
			},
		},
	}
	expectedStr := []string{
		"{ pk-1: some data, pk-2: simple data,  }",
		"{ pk-3: IsNull, pk-4: data,  }",
	}
	for i, testCase := range testCases {
		ret := rowToString(testCase)
		c.Assert(ret, Equals, expectedStr[i])
	}
}

func (d *testDataValidatorSuite) TestNeedQuotes(c *C) {
	testCases := map[byte]bool{
		parsermysql.TypeTiny:       false,
		parsermysql.TypeShort:      false,
		parsermysql.TypeLong:       false,
		parsermysql.TypeLonglong:   false,
		parsermysql.TypeInt24:      false,
		parsermysql.TypeYear:       false,
		parsermysql.TypeFloat:      false,
		parsermysql.TypeDouble:     false,
		parsermysql.TypeNewDecimal: false,
		parsermysql.TypeDuration:   true,
		parsermysql.TypeDatetime:   true,
		parsermysql.TypeNewDate:    true,
		parsermysql.TypeVarchar:    true,
		parsermysql.TypeBlob:       true,
		parsermysql.TypeVarString:  true,
		parsermysql.TypeString:     true,
	}
	for typ, val := range testCases {
		c.Assert(NeedQuotes(typ), Equals, val)
	}
}

func (d *testDataValidatorSuite) TestGetPKValues(c *C) {
	testCases := []map[string]*dbutil.ColumnData{
		{
			"col1": &dbutil.ColumnData{
				Data: []byte("some data"),
			},
			"col2": &dbutil.ColumnData{
				Data: []byte("data"),
			},
			"col3": &dbutil.ColumnData{
				IsNull: true,
			},
		},
		{
			"c1": &dbutil.ColumnData{
				Data: []byte("lk"),
			},
			"c2": &dbutil.ColumnData{
				Data: []byte("1001"),
			},
		},
	}
	expectedOut := [][]string{
		{"some data"},
		{"lk", "1001"},
	}
	type testTable struct {
		schemaName string
		tableName  string
		creatSQL   string
	}
	testTables := []testTable{
		{
			schemaName: "test1",
			tableName:  "tbl1",
			creatSQL: `create table if not exists test1.tbl1(
				col1 varchar(10),
				col2 text,
				col3 varchar(20),
				primary key(col1)
			);`,
		},
		{
			schemaName: "test2",
			tableName:  "tbl2",
			creatSQL: `create table if not exists test2.tbl2(
				c1 char(2),
				c2 int,
				primary key(c1, c2)
			);`,
		},
	}
	db, _, err := sqlmock.New()
	c.Assert(err, IsNil)
	for i := range testCases {
		cond := formatCond(
			c,
			db,
			testTables[i].schemaName,
			testTables[i].tableName,
			testTables[i].creatSQL,
			[][]string{},
		)
		ret := getPKValues(testCases[i], cond)
		for j, val := range expectedOut[i] {
			c.Assert(ret[j], Equals, val)
		}
	}
}

func (d *testDataValidatorSuite) TestDoValidate(c *C) {

}
