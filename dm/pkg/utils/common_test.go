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

package utils

import (
	"bytes"
	"context"
	"fmt"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
)

var _ = Suite(&testCommonSuite{})

type testCommonSuite struct{}

func (s *testCommonSuite) TestTrimCtrlChars(c *C) {
	ddl := "create table if not exists foo.bar(id int)"
	controlChars := make([]byte, 0, 33)
	nul := byte(0x00)
	for i := 0; i < 32; i++ {
		controlChars = append(controlChars, nul)
		nul++
	}
	controlChars = append(controlChars, 0x7f)

	parser2 := parser.New()
	var buf bytes.Buffer
	for _, char := range controlChars {
		buf.WriteByte(char)
		buf.WriteByte(char)
		buf.WriteString(ddl)
		buf.WriteByte(char)
		buf.WriteByte(char)

		newDDL := TrimCtrlChars(buf.String())
		c.Assert(newDDL, Equals, ddl)

		_, err := parser2.ParseOneStmt(newDDL, "", "")
		c.Assert(err, IsNil)
		buf.Reset()
	}
}

func (s *testCommonSuite) TestTrimQuoteMark(c *C) {
	cases := [][]string{
		{`"123"`, `123`},
		{`123`, `123`},
		{`"123`, `"123`},
		{`'123'`, `'123'`},
	}
	for _, ca := range cases {
		c.Assert(TrimQuoteMark(ca[0]), Equals, ca[1])
	}
}

func (s *testCommonSuite) TestFetchAllDoTables(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	// empty filter, exclude system schemas
	ba, err := filter.New(false, nil)
	c.Assert(err, IsNil)

	// no schemas need to do.
	mock.ExpectQuery(`SHOW DATABASES`).WillReturnRows(sqlmock.NewRows([]string{"Database"}))
	got, err := FetchAllDoTables(context.Background(), db, ba)
	c.Assert(err, IsNil)
	c.Assert(got, HasLen, 0)
	c.Assert(mock.ExpectationsWereMet(), IsNil)

	// only system schemas exist, still no need to do.
	schemas := []string{"information_schema", "mysql", "performance_schema", "sys", filter.DMHeartbeatSchema}
	rows := sqlmock.NewRows([]string{"Database"})
	s.addRowsForSchemas(rows, schemas)
	mock.ExpectQuery(`SHOW DATABASES`).WillReturnRows(rows)
	got, err = FetchAllDoTables(context.Background(), db, ba)
	c.Assert(err, IsNil)
	c.Assert(got, HasLen, 0)
	c.Assert(mock.ExpectationsWereMet(), IsNil)

	// schemas without tables in them.
	doSchema := "test_db"
	schemas = []string{"information_schema", "mysql", "performance_schema", "sys", filter.DMHeartbeatSchema, doSchema}
	rows = sqlmock.NewRows([]string{"Database"})
	s.addRowsForSchemas(rows, schemas)
	mock.ExpectQuery(`SHOW DATABASES`).WillReturnRows(rows)
	mock.ExpectQuery(fmt.Sprintf("SHOW FULL TABLES IN `%s` WHERE Table_Type != 'VIEW'", doSchema)).WillReturnRows(
		sqlmock.NewRows([]string{fmt.Sprintf("Tables_in_%s", doSchema), "Table_type"}))
	got, err = FetchAllDoTables(context.Background(), db, ba)
	c.Assert(err, IsNil)
	c.Assert(got, HasLen, 0)
	c.Assert(mock.ExpectationsWereMet(), IsNil)

	// do all tables under the schema.
	rows = sqlmock.NewRows([]string{"Database"})
	s.addRowsForSchemas(rows, schemas)
	mock.ExpectQuery(`SHOW DATABASES`).WillReturnRows(rows)
	tables := []string{"tbl1", "tbl2", "exclude_tbl"}
	rows = sqlmock.NewRows([]string{fmt.Sprintf("Tables_in_%s", doSchema), "Table_type"})
	s.addRowsForTables(rows, tables)
	mock.ExpectQuery(fmt.Sprintf("SHOW FULL TABLES IN `%s` WHERE Table_Type != 'VIEW'", doSchema)).WillReturnRows(rows)
	got, err = FetchAllDoTables(context.Background(), db, ba)
	c.Assert(err, IsNil)
	c.Assert(got, HasLen, 1)
	c.Assert(got[doSchema], DeepEquals, tables)
	c.Assert(mock.ExpectationsWereMet(), IsNil)

	// use a block-allow-list to fiter some tables
	ba, err = filter.New(false, &filter.Rules{
		DoDBs: []string{doSchema},
		DoTables: []*filter.Table{
			{Schema: doSchema, Name: "tbl1"},
			{Schema: doSchema, Name: "tbl2"},
		},
	})
	c.Assert(err, IsNil)

	rows = sqlmock.NewRows([]string{"Database"})
	s.addRowsForSchemas(rows, schemas)
	mock.ExpectQuery(`SHOW DATABASES`).WillReturnRows(rows)
	rows = sqlmock.NewRows([]string{fmt.Sprintf("Tables_in_%s", doSchema), "Table_type"})
	s.addRowsForTables(rows, tables)
	mock.ExpectQuery(fmt.Sprintf("SHOW FULL TABLES IN `%s` WHERE Table_Type != 'VIEW'", doSchema)).WillReturnRows(rows)
	got, err = FetchAllDoTables(context.Background(), db, ba)
	c.Assert(err, IsNil)
	c.Assert(got, HasLen, 1)
	c.Assert(got[doSchema], DeepEquals, []string{"tbl1", "tbl2"})
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testCommonSuite) TestFetchTargetDoTables(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	// empty filter and router, just as upstream.
	ba, err := filter.New(false, nil)
	c.Assert(err, IsNil)
	r, err := router.NewTableRouter(false, nil)
	c.Assert(err, IsNil)

	schemas := []string{"shard1"}
	rows := sqlmock.NewRows([]string{"Database"})
	s.addRowsForSchemas(rows, schemas)
	mock.ExpectQuery(`SHOW DATABASES`).WillReturnRows(rows)

	tablesM := map[string][]string{
		"shard1": {"tbl1", "tbl2"},
	}
	for schema, tables := range tablesM {
		rows = sqlmock.NewRows([]string{fmt.Sprintf("Tables_in_%s", schema), "Table_type"})
		s.addRowsForTables(rows, tables)
		mock.ExpectQuery(fmt.Sprintf("SHOW FULL TABLES IN `%s` WHERE Table_Type != 'VIEW'", schema)).WillReturnRows(rows)
	}

	got, err := FetchTargetDoTables(context.Background(), db, ba, r)
	c.Assert(err, IsNil)
	c.Assert(got, HasLen, 2)
	c.Assert(got, DeepEquals, map[string][]*filter.Table{
		"`shard1`.`tbl1`": {{Schema: "shard1", Name: "tbl1"}},
		"`shard1`.`tbl2`": {{Schema: "shard1", Name: "tbl2"}},
	})
	c.Assert(mock.ExpectationsWereMet(), IsNil)

	// route to the same downstream.
	r, err = router.NewTableRouter(false, []*router.TableRule{
		{SchemaPattern: "shard*", TablePattern: "tbl*", TargetSchema: "shard", TargetTable: "tbl"},
	})
	c.Assert(err, IsNil)

	rows = sqlmock.NewRows([]string{"Database"})
	s.addRowsForSchemas(rows, schemas)
	mock.ExpectQuery(`SHOW DATABASES`).WillReturnRows(rows)
	for schema, tables := range tablesM {
		rows = sqlmock.NewRows([]string{fmt.Sprintf("Tables_in_%s", schema), "Table_type"})
		s.addRowsForTables(rows, tables)
		mock.ExpectQuery(fmt.Sprintf("SHOW FULL TABLES IN `%s` WHERE Table_Type != 'VIEW'", schema)).WillReturnRows(rows)
	}

	got, err = FetchTargetDoTables(context.Background(), db, ba, r)
	c.Assert(err, IsNil)
	c.Assert(got, HasLen, 1)
	c.Assert(got, DeepEquals, map[string][]*filter.Table{
		"`shard`.`tbl`": {
			{Schema: "shard1", Name: "tbl1"},
			{Schema: "shard1", Name: "tbl2"},
		},
	})
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testCommonSuite) addRowsForSchemas(rows *sqlmock.Rows, schemas []string) {
	for _, d := range schemas {
		rows.AddRow(d)
	}
}

func (s *testCommonSuite) addRowsForTables(rows *sqlmock.Rows, tables []string) {
	for _, table := range tables {
		rows.AddRow(table, "BASE TABLE")
	}
}

func (s *testCommonSuite) TestCompareShardingDDLs(c *C) {
	var (
		DDL1 = "alter table add column c1 int"
		DDL2 = "alter table add column c2 text"
	)

	// different DDLs
	c.Assert(CompareShardingDDLs([]string{DDL1}, []string{DDL2}), IsFalse)

	// different length
	c.Assert(CompareShardingDDLs([]string{DDL1, DDL2}, []string{DDL2}), IsFalse)

	// same DDLs
	c.Assert(CompareShardingDDLs([]string{DDL1}, []string{DDL1}), IsTrue)
	c.Assert(CompareShardingDDLs([]string{DDL1, DDL2}, []string{DDL1, DDL2}), IsTrue)

	// same contents but different order
	c.Assert(CompareShardingDDLs([]string{DDL1, DDL2}, []string{DDL2, DDL1}), IsTrue)
}

func (s *testCommonSuite) TestDDLLockID(c *C) {
	task := "test"
	ID := GenDDLLockID(task, "db", "tbl")
	c.Assert(ID, Equals, "test-`db`.`tbl`")
	c.Assert(ExtractTaskFromLockID(ID), Equals, task)

	ID = GenDDLLockID(task, "d`b", "tb`l")
	c.Assert(ID, Equals, "test-`d``b`.`tb``l`")
	c.Assert(ExtractTaskFromLockID(ID), Equals, task)

	// invalid ID
	c.Assert(ExtractTaskFromLockID("invalid-lock-id"), Equals, "")
}

func (s *testCommonSuite) TestNonRepeatStringsEqual(c *C) {
	c.Assert(NonRepeatStringsEqual([]string{}, []string{}), IsTrue)
	c.Assert(NonRepeatStringsEqual([]string{"1", "2"}, []string{"2", "1"}), IsTrue)
	c.Assert(NonRepeatStringsEqual([]string{}, []string{"1"}), IsFalse)
	c.Assert(NonRepeatStringsEqual([]string{"1", "2"}, []string{"2", "3"}), IsFalse)
}
