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
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	"github.com/stretchr/testify/require"
)

func TestGetTableByDML(t *testing.T) {
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
		require.NoError(t, err)
		dml, ok := stmt.(ast.DMLNode)
		require.True(t, ok)
		table, err := getTableByDML(dml)
		if cs.hasError {
			require.NotNil(t, err)
			require.Nil(t, table)
		} else {
			require.Nil(t, err)
			require.Equal(t, cs.schema, table.Schema)
			require.Equal(t, cs.table, table.Name)
		}
	}
}

func TestTableNameResultSet(t *testing.T) {
	rs := &ast.TableSource{
		Source: &ast.TableName{
			Schema: pmodel.NewCIStr("test"),
			Name:   pmodel.NewCIStr("t1"),
		},
	}
	table, err := tableNameResultSet(rs)
	require.Nil(t, err)
	require.Equal(t, "test", table.Schema)
	require.Equal(t, "t1", table.Name)
}

func TestRecordSourceTbls(t *testing.T) {
	sourceTbls := make(map[string]map[string]struct{})

	recordSourceTbls(sourceTbls, &ast.CreateDatabaseStmt{}, &filter.Table{Schema: "a", Name: ""})
	require.Contains(t, sourceTbls, "a")
	require.Contains(t, sourceTbls["a"], "")

	recordSourceTbls(sourceTbls, &ast.CreateTableStmt{}, &filter.Table{Schema: "a", Name: "b"})
	require.Contains(t, sourceTbls, "a")
	require.Contains(t, sourceTbls["a"], "b")

	recordSourceTbls(sourceTbls, &ast.DropTableStmt{}, &filter.Table{Schema: "a", Name: "b"})
	_, ok := sourceTbls["a"]["b"]
	require.False(t, ok)

	recordSourceTbls(sourceTbls, &ast.CreateTableStmt{}, &filter.Table{Schema: "a", Name: "c"})
	require.Contains(t, sourceTbls, "a")
	require.Contains(t, sourceTbls["a"], "c")

	recordSourceTbls(sourceTbls, &ast.DropDatabaseStmt{}, &filter.Table{Schema: "a", Name: ""})
	require.Len(t, sourceTbls, 0)
}

func TestGetDDLStatusFromTiDB(t *testing.T) {
	var (
		cfg                       = genDefaultSubTaskConfig4Test()
		tctx                      = tcontext.Background()
		adminShowDDLJobsSQL1      = "ADMIN SHOW DDL JOBS 10"
		adminShowDDLJobsSQL2      = "ADMIN SHOW DDL JOBS 20"
		adminShowDDLJobsLimitSQL1 = "ADMIN SHOW DDL JOB QUERIES LIMIT 10 OFFSET 0"
		adminShowDDLJobsLimitSQL2 = "ADMIN SHOW DDL JOB QUERIES LIMIT 10 OFFSET 10"
	)

	var createTime time.Time
	var status string

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	conn1, err := db.Conn(context.Background())
	require.NoError(t, err)
	dbConn := dbconn.NewDBConn(cfg, conn.NewBaseConnForTest(conn1, nil))

	// test 1
	mock.ExpectQuery(adminShowDDLJobsSQL1).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "DB_NAME", "TABLE_NAME", "JOB_TYPE", "SCHEMA_STATE", "SCHEMA_ID", "TABLE_ID", "ROW_COUNT", "CREATE_TIME", "START_TIME", "END_TIME", "STATE"}).
		AddRow(61, "many_tables_test", "t6", "alter table", "public", 1, 61, 0, "2022-08-02 2:51:39", "2022-08-02 2:51:39", "NULL", "running").
		AddRow(60, "many_tables_test", "t5", "alter table", "public", 1, 60, 0, "2022-08-02 2:51:28", "2022-08-02 2:51:28", "2022-08-02 2:51:28", "synced").
		AddRow(59, "many_tables_test", "t4", "alter table", "public", 1, 59, 0, "2022-08-02 2:50:37", "2022-08-02 2:50:37", "NULL", "none").
		AddRow(58, "many_tables_test", "t3", "alter table", "public", 1, 58, 0, "2022-08-02 2:50:12", "2022-08-02 2:50:12", "2022-08-02 2:50:12", "synced").
		AddRow(57, "many_tables_test", "t2", "alter table", "public", 1, 57, 0, "2022-08-02 2:49:39", "2022-08-02 2:49:39", "2022-08-02 2:49:39", "synced").
		AddRow(56, "many_tables_test", "t1", "alter table", "public", 1, 56, 0, "2022-08-02 2:49:09", "2022-08-02 2:49:09", "2022-08-02 2:49:09", "synced").
		AddRow(55, "many_tables_test", "t6", "create table", "public", 1, 55, 0, "2022-08-02 2:48:38", "2022-08-02 2:48:38", "2022-08-02 2:48:38", "synced").
		AddRow(54, "many_tables_test", "t5", "create table", "public", 1, 54, 0, "2022-08-02 2:48:19", "2022-08-02 2:48:19", "2022-08-02 2:48:19", "synced").
		AddRow(53, "many_tables_test", "t4", "create table", "public", 1, 53, 0, "2022-08-02 2:47:55", "2022-08-02 2:47:55", "2022-08-02 2:47:55", "synced").
		AddRow(52, "many_tables_test", "t3", "create table", "public", 1, 52, 0, "2022-08-02 2:47:24", "2022-08-02 2:47:24", "2022-08-02 2:47:24", "synced"))

	mock.ExpectQuery(adminShowDDLJobsLimitSQL1).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).
		AddRow(61, "ALTER TABLE many_tables_test.t6 ADD x timestamp DEFAULT current_timestamp").
		AddRow(60, "ALTER TABLE many_tables_test.t5 ADD x timestamp DEFAULT current_timestamp").
		AddRow(59, "ALTER TABLE many_tables_test.t4 ADD x timestamp DEFAULT current_timestamp").
		AddRow(58, "ALTER TABLE many_tables_test.t3 ADD x timestamp DEFAULT current_timestamp").
		AddRow(57, "ALTER TABLE many_tables_test.t2 ADD x timestamp DEFAULT current_timestamp").
		AddRow(56, "ALTER TABLE many_tables_test.t1 ADD x timestamp DEFAULT current_timestamp").
		AddRow(55, "CREATE TABLE IF NOT EXISTS many_tables_test.t6(i TINYINT, j INT UNIQUE KEY)").
		AddRow(54, "CREATE TABLE IF NOT EXISTS many_tables_test.t5(i TINYINT, j INT UNIQUE KEY)").
		AddRow(53, "CREATE TABLE IF NOT EXISTS many_tables_test.t4(i TINYINT, j INT UNIQUE KEY)").
		AddRow(52, "CREATE TABLE IF NOT EXISTS many_tables_test.t3(i TINYINT, j INT UNIQUE KEY)"))

	createTime, err = time.Parse(timeLayout, "2022-08-02 2:51:38")
	require.NoError(t, err)
	status, err = getDDLStatusFromTiDB(tctx, dbConn, "ALTER TABLE many_tables_test.t6 ADD x timestamp DEFAULT current_timestamp", createTime.Unix())
	require.NoError(t, err)
	require.Equal(t, "running", status)

	// test 2
	mock.ExpectQuery(adminShowDDLJobsSQL1).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "DB_NAME", "TABLE_NAME", "JOB_TYPE", "SCHEMA_STATE", "SCHEMA_ID", "TABLE_ID", "ROW_COUNT", "CREATE_TIME", "START_TIME", "END_TIME", "STATE"}).
		AddRow(61, "many_tables_test", "t6", "alter table", "public", 1, 61, 0, "2022-08-02 2:51:39", "2022-08-02 2:51:39", "NULL", "running").
		AddRow(60, "many_tables_test", "t5", "alter table", "public", 1, 60, 0, "2022-08-02 2:51:28", "2022-08-02 2:51:28", "2022-08-02 2:51:28", "synced").
		AddRow(59, "many_tables_test", "t4", "alter table", "public", 1, 59, 0, "2022-08-02 2:50:37", "2022-08-02 2:50:37", "NULL", "none").
		AddRow(58, "many_tables_test", "t3", "alter table", "public", 1, 58, 0, "2022-08-02 2:50:12", "2022-08-02 2:50:12", "2022-08-02 2:50:12", "synced").
		AddRow(57, "many_tables_test", "t2", "alter table", "public", 1, 57, 0, "2022-08-02 2:49:39", "2022-08-02 2:49:39", "2022-08-02 2:49:39", "synced").
		AddRow(56, "many_tables_test", "t1", "alter table", "public", 1, 56, 0, "2022-08-02 2:49:09", "2022-08-02 2:49:09", "2022-08-02 2:49:09", "synced").
		AddRow(55, "many_tables_test", "t6", "create table", "public", 1, 55, 0, "2022-08-02 2:48:38", "2022-08-02 2:48:38", "2022-08-02 2:48:38", "synced").
		AddRow(54, "many_tables_test", "t5", "create table", "public", 1, 54, 0, "2022-08-02 2:48:19", "2022-08-02 2:48:19", "2022-08-02 2:48:19", "synced").
		AddRow(53, "many_tables_test", "t4", "create table", "public", 1, 53, 0, "2022-08-02 2:47:55", "2022-08-02 2:47:55", "2022-08-02 2:47:55", "synced").
		AddRow(52, "many_tables_test", "t3", "create table", "public", 1, 52, 0, "2022-08-02 2:47:24", "2022-08-02 2:47:24", "2022-08-02 2:47:24", "synced"))

	mock.ExpectQuery(adminShowDDLJobsLimitSQL1).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).
		AddRow(61, "ALTER TABLE many_tables_test.t6 ADD x timestamp DEFAULT current_timestamp").
		AddRow(60, "ALTER TABLE many_tables_test.t5 ADD x timestamp DEFAULT current_timestamp").
		AddRow(59, "ALTER TABLE many_tables_test.t4 ADD x timestamp DEFAULT current_timestamp").
		AddRow(58, "ALTER TABLE many_tables_test.t3 ADD x timestamp DEFAULT current_timestamp").
		AddRow(57, "ALTER TABLE many_tables_test.t2 ADD x timestamp DEFAULT current_timestamp").
		AddRow(56, "ALTER TABLE many_tables_test.t1 ADD x timestamp DEFAULT current_timestamp").
		AddRow(55, "CREATE TABLE IF NOT EXISTS many_tables_test.t6(i TINYINT, j INT UNIQUE KEY)").
		AddRow(54, "CREATE TABLE IF NOT EXISTS many_tables_test.t5(i TINYINT, j INT UNIQUE KEY)").
		AddRow(53, "CREATE TABLE IF NOT EXISTS many_tables_test.t4(i TINYINT, j INT UNIQUE KEY)").
		AddRow(52, "CREATE TABLE IF NOT EXISTS many_tables_test.t3(i TINYINT, j INT UNIQUE KEY)"))

	createTime, err = time.Parse(timeLayout, "2022-08-02 2:50:36")
	require.NoError(t, err)
	status, err = getDDLStatusFromTiDB(tctx, dbConn, "ALTER TABLE many_tables_test.t4 ADD x timestamp DEFAULT current_timestamp", createTime.Unix())
	require.NoError(t, err)
	require.Equal(t, "none", status)

	// test 3
	mock.ExpectQuery(adminShowDDLJobsSQL1).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "DB_NAME", "TABLE_NAME", "JOB_TYPE", "SCHEMA_STATE", "SCHEMA_ID", "TABLE_ID", "ROW_COUNT", "CREATE_TIME", "START_TIME", "END_TIME", "STATE"}).
		AddRow(61, "many_tables_test", "t6", "alter table", "public", 1, 61, 0, "2022-08-02 2:51:39", "2022-08-02 2:51:39", "NULL", "running").
		AddRow(60, "many_tables_test", "t5", "alter table", "public", 1, 60, 0, "2022-08-02 2:51:28", "2022-08-02 2:51:28", "2022-08-02 2:51:28", "synced").
		AddRow(59, "many_tables_test", "t4", "alter table", "public", 1, 59, 0, "2022-08-02 2:50:37", "2022-08-02 2:50:37", "NULL", "none").
		AddRow(58, "many_tables_test", "t3", "alter table", "public", 1, 58, 0, "2022-08-02 2:50:12", "2022-08-02 2:50:12", "2022-08-02 2:50:12", "synced").
		AddRow(57, "many_tables_test", "t2", "alter table", "public", 1, 57, 0, "2022-08-02 2:49:39", "2022-08-02 2:49:39", "2022-08-02 2:49:39", "synced").
		AddRow(56, "many_tables_test", "t1", "alter table", "public", 1, 56, 0, "2022-08-02 2:49:09", "2022-08-02 2:49:09", "2022-08-02 2:49:09", "synced").
		AddRow(55, "many_tables_test", "t6", "create table", "public", 1, 55, 0, "2022-08-02 2:48:38", "2022-08-02 2:48:38", "2022-08-02 2:48:38", "synced").
		AddRow(54, "many_tables_test", "t5", "create table", "public", 1, 54, 0, "2022-08-02 2:48:19", "2022-08-02 2:48:19", "2022-08-02 2:48:19", "synced").
		AddRow(53, "many_tables_test", "t4", "create table", "public", 1, 53, 0, "2022-08-02 2:47:55", "2022-08-02 2:47:55", "2022-08-02 2:47:55", "synced").
		AddRow(52, "many_tables_test", "t3", "create table", "public", 1, 52, 0, "2022-08-02 2:47:24", "2022-08-02 2:47:24", "2022-08-02 2:47:24", "synced"))

	mock.ExpectQuery(adminShowDDLJobsLimitSQL1).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).
		AddRow(61, "ALTER TABLE many_tables_test.t6 ADD x timestamp DEFAULT current_timestamp").
		AddRow(60, "ALTER TABLE many_tables_test.t5 ADD x timestamp DEFAULT current_timestamp").
		AddRow(59, "ALTER TABLE many_tables_test.t4 ADD x timestamp DEFAULT current_timestamp").
		AddRow(58, "ALTER TABLE many_tables_test.t3 ADD x timestamp DEFAULT current_timestamp").
		AddRow(57, "ALTER TABLE many_tables_test.t2 ADD x timestamp DEFAULT current_timestamp").
		AddRow(56, "ALTER TABLE many_tables_test.t1 ADD x timestamp DEFAULT current_timestamp").
		AddRow(55, "CREATE TABLE IF NOT EXISTS many_tables_test.t6(i TINYINT, j INT UNIQUE KEY)").
		AddRow(54, "CREATE TABLE IF NOT EXISTS many_tables_test.t5(i TINYINT, j INT UNIQUE KEY)").
		AddRow(53, "CREATE TABLE IF NOT EXISTS many_tables_test.t4(i TINYINT, j INT UNIQUE KEY)").
		AddRow(52, "CREATE TABLE IF NOT EXISTS many_tables_test.t3(i TINYINT, j INT UNIQUE KEY)"))

	mock.ExpectQuery(adminShowDDLJobsSQL2).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "DB_NAME", "TABLE_NAME", "JOB_TYPE", "SCHEMA_STATE", "SCHEMA_ID", "TABLE_ID", "ROW_COUNT", "CREATE_TIME", "START_TIME", "END_TIME", "STATE"}).
		AddRow(61, "many_tables_test", "t6", "alter table", "public", 1, 61, 0, "2022-08-02 2:51:39", "2022-08-02 2:51:39", "NULL", "running").
		AddRow(60, "many_tables_test", "t5", "alter table", "public", 1, 60, 0, "2022-08-02 2:51:28", "2022-08-02 2:51:28", "2022-08-02 2:51:28", "synced").
		AddRow(59, "many_tables_test", "t4", "alter table", "public", 1, 59, 0, "2022-08-02 2:50:37", "2022-08-02 2:50:37", "NULL", "none").
		AddRow(58, "many_tables_test", "t3", "alter table", "public", 1, 58, 0, "2022-08-02 2:50:12", "2022-08-02 2:50:12", "2022-08-02 2:50:12", "synced").
		AddRow(57, "many_tables_test", "t2", "alter table", "public", 1, 57, 0, "2022-08-02 2:49:39", "2022-08-02 2:49:39", "2022-08-02 2:49:39", "synced").
		AddRow(56, "many_tables_test", "t1", "alter table", "public", 1, 56, 0, "2022-08-02 2:49:09", "2022-08-02 2:49:09", "2022-08-02 2:49:09", "synced").
		AddRow(55, "many_tables_test", "t6", "create table", "public", 1, 55, 0, "2022-08-02 2:48:38", "2022-08-02 2:48:38", "2022-08-02 2:48:38", "synced").
		AddRow(54, "many_tables_test", "t5", "create table", "public", 1, 54, 0, "2022-08-02 2:48:19", "2022-08-02 2:48:19", "2022-08-02 2:48:19", "synced").
		AddRow(53, "many_tables_test", "t4", "create table", "public", 1, 53, 0, "2022-08-02 2:47:55", "2022-08-02 2:47:55", "2022-08-02 2:47:55", "synced").
		AddRow(52, "many_tables_test", "t3", "create table", "public", 1, 52, 0, "2022-08-02 2:47:24", "2022-08-02 2:47:24", "2022-08-02 2:47:24", "synced").
		AddRow(51, "many_tables_test", "t2", "create table", "public", 1, 51, 0, "2022-08-02 2:46:43", "2022-08-02 2:46:43", "2022-08-02 2:46:43", "synced").
		AddRow(50, "many_tables_test", "t1", "create table", "public", 1, 50, 0, "2022-08-02 2:46:14", "2022-08-02 2:46:14", "2022-08-02 2:46:14", "synced").
		AddRow(49, "other_test", "t7", "create table", "public", 2, 49, 0, "2022-08-02 2:45:52", "2022-08-02 2:45:52", "2022-08-02 2:45:52", "synced").
		AddRow(48, "other_test", "t6", "create table", "public", 2, 48, 0, "2022-08-02 2:45:38", "2022-08-02 2:45:38", "2022-08-02 2:45:38", "synced").
		AddRow(47, "other_test", "t5", "create table", "public", 2, 47, 0, "2022-08-02 2:44:47", "2022-08-02 2:44:47", "2022-08-02 2:44:47", "synced").
		AddRow(46, "other_test", "t4", "create table", "public", 2, 46, 0, "2022-08-02 2:44:22", "2022-08-02 2:44:22", "2022-08-02 2:44:22", "synced").
		AddRow(45, "other_test", "t3", "create table", "public", 2, 45, 0, "2022-08-02 2:43:35", "2022-08-02 2:43:35", "2022-08-02 2:43:35", "synced").
		AddRow(44, "other_test", "t2", "create table", "public", 2, 44, 0, "2022-08-02 2:43:08", "2022-08-02 2:43:08", "2022-08-02 2:43:08", "synced").
		AddRow(43, "other_test", "t1", "create table", "public", 2, 43, 0, "2022-08-02 2:42:41", "2022-08-02 2:42:41", "2022-08-02 2:42:41", "synced").
		AddRow(42, "other_test", "t0", "create table", "public", 2, 42, 0, "2022-08-02 2:42:16", "2022-08-02 2:42:16", "2022-08-02 2:42:16", "synced"))

	mock.ExpectQuery(adminShowDDLJobsLimitSQL2).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).
		AddRow(51, "CREATE TABLE IF NOT EXISTS many_tables_test.t2(i TINYINT, j INT UNIQUE KEY)").
		AddRow(50, "CREATE TABLE IF NOT EXISTS many_tables_test.t1(i TINYINT, j INT UNIQUE KEY)").
		AddRow(49, "CREATE TABLE IF NOT EXISTS other_test.t7(i TINYINT, j INT UNIQUE KEY)").
		AddRow(48, "CREATE TABLE IF NOT EXISTS other_test.t6(i TINYINT, j INT UNIQUE KEY)").
		AddRow(47, "CREATE TABLE IF NOT EXISTS other_test.t5(i TINYINT, j INT UNIQUE KEY)").
		AddRow(46, "CREATE TABLE IF NOT EXISTS other_test.t4(i TINYINT, j INT UNIQUE KEY)").
		AddRow(45, "CREATE TABLE IF NOT EXISTS other_test.t3(i TINYINT, j INT UNIQUE KEY)").
		AddRow(44, "CREATE TABLE IF NOT EXISTS other_test.t2(i TINYINT, j INT UNIQUE KEY)").
		AddRow(43, "CREATE TABLE IF NOT EXISTS other_test.t1(i TINYINT, j INT UNIQUE KEY)").
		AddRow(42, "CREATE TABLE IF NOT EXISTS other_test.t0(i TINYINT, j INT UNIQUE KEY)"))

	createTime, err = time.Parse(timeLayout, "2022-08-02 2:46:13")
	require.NoError(t, err)
	status, err = getDDLStatusFromTiDB(tctx, dbConn, "CREATE TABLE IF NOT EXISTS many_tables_test.t1(i TINYINT, j INT UNIQUE KEY)", createTime.Unix())
	require.NoError(t, err)
	require.Equal(t, "synced", status)

	// test 4
	mock.ExpectQuery(adminShowDDLJobsSQL1).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "DB_NAME", "TABLE_NAME", "JOB_TYPE", "SCHEMA_STATE", "SCHEMA_ID", "TABLE_ID", "ROW_COUNT", "CREATE_TIME", "START_TIME", "END_TIME", "STATE"}).
		AddRow(61, "many_tables_test", "t6", "alter table", "public", 1, 61, 0, "2022-08-02 2:51:39", "2022-08-02 2:51:39", "NULL", "running").
		AddRow(60, "many_tables_test", "t5", "alter table", "public", 1, 60, 0, "2022-08-02 2:51:28", "2022-08-02 2:51:28", "2022-08-02 2:51:28", "synced").
		AddRow(59, "many_tables_test", "t4", "alter table", "public", 1, 59, 0, "2022-08-02 2:50:37", "2022-08-02 2:50:37", "NULL", "none").
		AddRow(58, "many_tables_test", "t3", "alter table", "public", 1, 58, 0, "2022-08-02 2:50:12", "2022-08-02 2:50:12", "2022-08-02 2:50:12", "synced").
		AddRow(57, "many_tables_test", "t2", "alter table", "public", 1, 57, 0, "2022-08-02 2:49:39", "2022-08-02 2:49:39", "2022-08-02 2:49:39", "synced").
		AddRow(56, "many_tables_test", "t1", "alter table", "public", 1, 56, 0, "2022-08-02 2:49:09", "2022-08-02 2:49:09", "2022-08-02 2:49:09", "synced").
		AddRow(55, "many_tables_test", "t6", "create table", "public", 1, 55, 0, "2022-08-02 2:48:38", "2022-08-02 2:48:38", "2022-08-02 2:48:38", "synced").
		AddRow(54, "many_tables_test", "t5", "create table", "public", 1, 54, 0, "2022-08-02 2:48:19", "2022-08-02 2:48:19", "2022-08-02 2:48:19", "synced").
		AddRow(53, "many_tables_test", "t4", "create table", "public", 1, 53, 0, "2022-08-02 2:47:55", "2022-08-02 2:47:55", "2022-08-02 2:47:55", "synced").
		AddRow(52, "many_tables_test", "t3", "create table", "public", 1, 52, 0, "2022-08-02 2:47:24", "2022-08-02 2:47:24", "2022-08-02 2:47:24", "synced"))

	createTime, err = time.Parse(timeLayout, "2022-08-03 12:35:00")
	require.NoError(t, err)
	status, err = getDDLStatusFromTiDB(tctx, dbConn, "CREATE TABLE IF NOT EXISTS many_tables_test.t7(i TINYINT, j INT UNIQUE KEY)", createTime.Unix())
	require.NoError(t, err)
	require.Equal(t, "", status) // DDL does not exist

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)

	// multi-schema change tests
	// test 5 (for manual operation in TiDB)
	mock.ExpectQuery(adminShowDDLJobsSQL1).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "DB_NAME", "TABLE_NAME", "JOB_TYPE", "SCHEMA_STATE", "SCHEMA_ID", "TABLE_ID", "ROW_COUNT", "CREATE_TIME", "START_TIME", "END_TIME", "STATE"}).
		AddRow(59, "many_tables_test", "t4", "alter table multi-schema change", "public", 1, 59, 0, "2022-08-02 2:51:39", "2022-08-02 2:51:39", "NULL", "running").
		AddRow(59, "many_tables_test", "t4", "add column /* subjob */", "public", 1, 59, 0, "NULL", "NULL", "NULL", "done").
		AddRow(59, "many_tables_test", "t4", "add column /* subjob */", "public", 1, 59, 0, "NULL", "NULL", "NULL", "done").
		AddRow(58, "many_tables_test", "t3", "alter table", "public", 1, 58, 0, "2022-08-02 2:50:12", "2022-08-02 2:50:12", "2022-08-02 2:50:12", "synced").
		AddRow(57, "many_tables_test", "t2", "alter table", "public", 1, 57, 0, "2022-08-02 2:49:39", "2022-08-02 2:49:39", "2022-08-02 2:49:39", "synced").
		AddRow(56, "many_tables_test", "t1", "alter table", "public", 1, 56, 0, "2022-08-02 2:49:09", "2022-08-02 2:49:09", "2022-08-02 2:49:09", "synced").
		AddRow(55, "many_tables_test", "t6", "create table", "public", 1, 55, 0, "2022-08-02 2:48:38", "2022-08-02 2:48:38", "2022-08-02 2:48:38", "synced").
		AddRow(54, "many_tables_test", "t5", "create table", "public", 1, 54, 0, "2022-08-02 2:48:19", "2022-08-02 2:48:19", "2022-08-02 2:48:19", "synced").
		AddRow(53, "many_tables_test", "t4", "create table", "public", 1, 53, 0, "2022-08-02 2:47:55", "2022-08-02 2:47:55", "2022-08-02 2:47:55", "synced").
		AddRow(52, "many_tables_test", "t3", "create table", "public", 1, 52, 0, "2022-08-02 2:47:24", "2022-08-02 2:47:24", "2022-08-02 2:47:24", "synced").
		AddRow(51, "many_tables_test", "t2", "create table", "public", 1, 51, 0, "2022-08-02 2:46:43", "2022-08-02 2:46:43", "2022-08-02 2:46:43", "synced").
		AddRow(50, "many_tables_test", "t1", "create table", "public", 1, 50, 0, "2022-08-02 2:46:14", "2022-08-02 2:46:14", "2022-08-02 2:46:14", "synced"))

	mock.ExpectQuery(adminShowDDLJobsLimitSQL1).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).
		AddRow(59, "ALTER TABLE many_tables_test.t4 ADD y INT, ADD z INT").
		AddRow(58, "ALTER TABLE many_tables_test.t3 ADD x timestamp DEFAULT current_timestamp").
		AddRow(57, "ALTER TABLE many_tables_test.t2 ADD x timestamp DEFAULT current_timestamp").
		AddRow(56, "ALTER TABLE many_tables_test.t1 ADD x timestamp DEFAULT current_timestamp").
		AddRow(55, "CREATE TABLE IF NOT EXISTS many_tables_test.t6(i TINYINT, j INT UNIQUE KEY)").
		AddRow(54, "CREATE TABLE IF NOT EXISTS many_tables_test.t5(i TINYINT, j INT UNIQUE KEY)").
		AddRow(53, "CREATE TABLE IF NOT EXISTS many_tables_test.t4(i TINYINT, j INT UNIQUE KEY)").
		AddRow(52, "CREATE TABLE IF NOT EXISTS many_tables_test.t3(i TINYINT, j INT UNIQUE KEY)").
		AddRow(51, "CREATE TABLE IF NOT EXISTS many_tables_test.t2(i TINYINT, j INT UNIQUE KEY)").
		AddRow(50, "CREATE TABLE IF NOT EXISTS many_tables_test.t1(i TINYINT, j INT UNIQUE KEY)"))

	createTime, err = time.Parse(timeLayout, "2022-08-02 2:50:36")
	require.NoError(t, err)
	status, err = getDDLStatusFromTiDB(tctx, dbConn, "ALTER TABLE many_tables_test.t4 ADD y INT, ADD z INT", createTime.Unix())
	require.NoError(t, err)
	require.Equal(t, "running", status)
}
