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
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/util/filter"
	regexprrouter "github.com/pingcap/tidb/util/regexpr-router"
	router "github.com/pingcap/tidb/util/table-router"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTrimCtrlChars(t *testing.T) {
	t.Parallel()

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
		require.Equal(t, ddl, newDDL)

		_, err := parser2.ParseOneStmt(newDDL, "", "")
		require.NoError(t, err)
		buf.Reset()
	}
}

func TestTrimQuoteMark(t *testing.T) {
	t.Parallel()

	cases := [][]string{
		{`"123"`, `123`},
		{`123`, `123`},
		{`"123`, `"123`},
		{`'123'`, `'123'`},
	}
	for _, ca := range cases {
		require.Equal(t, TrimQuoteMark(ca[0]), ca[1])
	}
}

func TestFetchAllDoTables(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	// empty filter, exclude system schemas
	ba, err := filter.New(false, nil)
	require.NoError(t, err)

	// no schemas need to do.
	mock.ExpectQuery(`SHOW DATABASES`).WillReturnRows(sqlmock.NewRows([]string{"Database"}))
	got, err := FetchAllDoTables(context.Background(), db, ba)
	require.NoError(t, err)
	require.Len(t, got, 0)
	require.NoError(t, mock.ExpectationsWereMet())

	// only system schemas exist, still no need to do.
	schemas := []string{"information_schema", "mysql", "performance_schema", "sys", filter.DMHeartbeatSchema}
	rows := sqlmock.NewRows([]string{"Database"})
	addRowsForSchemas(rows, schemas)
	mock.ExpectQuery(`SHOW DATABASES`).WillReturnRows(rows)
	got, err = FetchAllDoTables(context.Background(), db, ba)
	require.NoError(t, err)
	require.Len(t, got, 0)
	require.NoError(t, mock.ExpectationsWereMet())

	// schemas without tables in them.
	doSchema := "test_db"
	schemas = []string{"information_schema", "mysql", "performance_schema", "sys", filter.DMHeartbeatSchema, doSchema}
	rows = sqlmock.NewRows([]string{"Database"})
	addRowsForSchemas(rows, schemas)
	mock.ExpectQuery(`SHOW DATABASES`).WillReturnRows(rows)
	mock.ExpectQuery(fmt.Sprintf("SHOW FULL TABLES IN `%s` WHERE Table_Type != 'VIEW'", doSchema)).WillReturnRows(
		sqlmock.NewRows([]string{fmt.Sprintf("Tables_in_%s", doSchema), "Table_type"}))
	got, err = FetchAllDoTables(context.Background(), db, ba)
	require.NoError(t, err)
	require.Len(t, got, 0)
	require.NoError(t, mock.ExpectationsWereMet())

	// do all tables under the schema.
	rows = sqlmock.NewRows([]string{"Database"})
	addRowsForSchemas(rows, schemas)
	mock.ExpectQuery(`SHOW DATABASES`).WillReturnRows(rows)
	tables := []string{"tbl1", "tbl2", "exclude_tbl"}
	rows = sqlmock.NewRows([]string{fmt.Sprintf("Tables_in_%s", doSchema), "Table_type"})
	addRowsForTables(rows, tables)
	mock.ExpectQuery(fmt.Sprintf("SHOW FULL TABLES IN `%s` WHERE Table_Type != 'VIEW'", doSchema)).WillReturnRows(rows)
	got, err = FetchAllDoTables(context.Background(), db, ba)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, tables, got[doSchema])
	require.NoError(t, mock.ExpectationsWereMet())

	// use a block-allow-list to fiter some tables
	ba, err = filter.New(false, &filter.Rules{
		DoDBs: []string{doSchema},
		DoTables: []*filter.Table{
			{Schema: doSchema, Name: "tbl1"},
			{Schema: doSchema, Name: "tbl2"},
		},
	})
	require.NoError(t, err)

	rows = sqlmock.NewRows([]string{"Database"})
	addRowsForSchemas(rows, schemas)
	mock.ExpectQuery(`SHOW DATABASES`).WillReturnRows(rows)
	rows = sqlmock.NewRows([]string{fmt.Sprintf("Tables_in_%s", doSchema), "Table_type"})
	addRowsForTables(rows, tables)
	mock.ExpectQuery(fmt.Sprintf("SHOW FULL TABLES IN `%s` WHERE Table_Type != 'VIEW'", doSchema)).WillReturnRows(rows)
	got, err = FetchAllDoTables(context.Background(), db, ba)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, []string{"tbl1", "tbl2"}, got[doSchema])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFetchTargetDoTables(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	// empty filter and router, just as upstream.
	ba, err := filter.New(false, nil)
	require.NoError(t, err)
	r, err := regexprrouter.NewRegExprRouter(false, nil)
	require.NoError(t, err)

	schemas := []string{"shard1"}
	rows := sqlmock.NewRows([]string{"Database"})
	addRowsForSchemas(rows, schemas)
	mock.ExpectQuery(`SHOW DATABASES`).WillReturnRows(rows)

	tablesM := map[string][]string{
		"shard1": {"tbl1", "tbl2"},
	}
	for schema, tables := range tablesM {
		rows = sqlmock.NewRows([]string{fmt.Sprintf("Tables_in_%s", schema), "Table_type"})
		addRowsForTables(rows, tables)
		mock.ExpectQuery(fmt.Sprintf("SHOW FULL TABLES IN `%s` WHERE Table_Type != 'VIEW'", schema)).WillReturnRows(rows)
	}

	tablesMap, extendedCols, err := FetchTargetDoTables(context.Background(), "", db, ba, r)
	require.NoError(t, err)
	require.Equal(t, map[filter.Table][]filter.Table{
		{Schema: "shard1", Name: "tbl1"}: {{Schema: "shard1", Name: "tbl1"}},
		{Schema: "shard1", Name: "tbl2"}: {{Schema: "shard1", Name: "tbl2"}},
	}, tablesMap)
	require.Len(t, extendedCols, 0)
	require.NoError(t, mock.ExpectationsWereMet())

	// route to the same downstream.
	r, err = regexprrouter.NewRegExprRouter(false, []*router.TableRule{
		{SchemaPattern: "shard*", TablePattern: "tbl*", TargetSchema: "shard", TargetTable: "tbl"},
	})
	require.NoError(t, err)

	rows = sqlmock.NewRows([]string{"Database"})
	addRowsForSchemas(rows, schemas)
	mock.ExpectQuery(`SHOW DATABASES`).WillReturnRows(rows)
	for schema, tables := range tablesM {
		rows = sqlmock.NewRows([]string{fmt.Sprintf("Tables_in_%s", schema), "Table_type"})
		addRowsForTables(rows, tables)
		mock.ExpectQuery(fmt.Sprintf("SHOW FULL TABLES IN `%s` WHERE Table_Type != 'VIEW'", schema)).WillReturnRows(rows)
	}

	tablesMap, extendedCols, err = FetchTargetDoTables(context.Background(), "", db, ba, r)
	require.NoError(t, err)
	require.Equal(t, map[filter.Table][]filter.Table{
		{Schema: "shard", Name: "tbl"}: {
			{Schema: "shard1", Name: "tbl1"},
			{Schema: "shard1", Name: "tbl2"},
		},
	}, tablesMap)
	require.Len(t, extendedCols, 0)
	require.NoError(t, mock.ExpectationsWereMet())
}

func addRowsForSchemas(rows *sqlmock.Rows, schemas []string) {
	for _, d := range schemas {
		rows.AddRow(d)
	}
}

func addRowsForTables(rows *sqlmock.Rows, tables []string) {
	for _, table := range tables {
		rows.AddRow(table, "BASE TABLE")
	}
}

func TestCompareShardingDDLs(t *testing.T) {
	t.Parallel()

	var (
		DDL1 = "alter table add column c1 int"
		DDL2 = "alter table add column c2 text"
	)

	// different DDLs
	require.False(t, CompareShardingDDLs([]string{DDL1}, []string{DDL2}))

	// different length
	require.False(t, CompareShardingDDLs([]string{DDL1, DDL2}, []string{DDL2}))

	// same DDLs
	require.True(t, CompareShardingDDLs([]string{DDL1}, []string{DDL1}))
	require.True(t, CompareShardingDDLs([]string{DDL1, DDL2}, []string{DDL1, DDL2}))

	// same contents but different order
	require.True(t, CompareShardingDDLs([]string{DDL1, DDL2}, []string{DDL2, DDL1}))
}

func TestDDLLockID(t *testing.T) {
	t.Parallel()

	task := "test"
	id := GenDDLLockID(task, "db", "tbl")
	require.Equal(t, "test-`db`.`tbl`", id)
	require.Equal(t, task, ExtractTaskFromLockID(id))

	id = GenDDLLockID(task, "d`b", "tb`l")
	require.Equal(t, "test-`d``b`.`tb``l`", id)
	require.Equal(t, task, ExtractTaskFromLockID(id))

	// invalid ID
	require.Equal(t, "", ExtractTaskFromLockID("invalid-lock-id"))
}

func TestNonRepeatStringsEqual(t *testing.T) {
	t.Parallel()

	require.True(t, NonRepeatStringsEqual([]string{}, []string{}))
	require.True(t, NonRepeatStringsEqual([]string{"1", "2"}, []string{"2", "1"}))
	require.False(t, NonRepeatStringsEqual([]string{}, []string{"1"}))
	require.False(t, NonRepeatStringsEqual([]string{"1", "2"}, []string{"2", "3"}))
}

func TestGoLogWrapper(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	// to avoid data race since there's concurrent test case writing log.L()
	l := log.Logger{Logger: zap.NewNop()}
	go GoLogWrapper(l, func() {
		defer wg.Done()
		panic("should be captured")
	})
	wg.Wait()
	// if GoLogWrapper didn't catch it, this case will fail.
}
