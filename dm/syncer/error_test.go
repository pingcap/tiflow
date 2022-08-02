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
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/errno"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	"github.com/pingcap/tiflow/dm/syncer/metrics"
)

func newMysqlErr(number uint16, message string) *mysql.MySQLError {
	return &mysql.MySQLError{
		Number:  number,
		Message: message,
	}
}

func TestHandleSpecialDDLError(t *testing.T) {
	var (
		cfg                 = genDefaultSubTaskConfig4Test()
		syncer              = NewSyncer(cfg, nil, nil)
		tctx                = tcontext.Background()
		conn2               = dbconn.NewDBConn(cfg, nil)
		customErr           = errors.New("custom error")
		invalidDDL          = "SQL CAN NOT BE PARSED"
		insertDML           = "INSERT INTO tbl VALUES (1)"
		createTable         = "CREATE TABLE tbl (col INT)"
		addUK               = "ALTER TABLE tbl ADD UNIQUE INDEX idx(col)"
		addFK               = "ALTER TABLE tbl ADD CONSTRAINT fk FOREIGN KEY (col) REFERENCES tbl2 (col)"
		addColumn           = "ALTER TABLE tbl ADD COLUMN col INT"
		addIndexMulti       = "ALTER TABLE tbl ADD INDEX idx1(col1), ADD INDEX idx2(col2)"
		addIndex1           = "ALTER TABLE tbl ADD INDEX idx(col)"
		addIndex2           = "CREATE INDEX idx ON tbl(col)"
		dropColumnWithIndex = "ALTER TABLE tbl DROP c1"
		cases               = []struct {
			err     error
			ddls    []string
			index   int
			handled bool
		}{
			{
				err: mysql.ErrInvalidConn, // empty DDLs
			},
			{
				err:  mysql.ErrInvalidConn,
				ddls: []string{addColumn, addIndex1}, // error happen not on the last
			},
			{
				err:  mysql.ErrInvalidConn,
				ddls: []string{addIndex1, addColumn}, // error happen not on the last
			},
			{
				err:  mysql.ErrInvalidConn,
				ddls: []string{addIndex1, addIndex2}, // error happen not on the last
			},
			{
				err:  customErr, // not `invalid connection`
				ddls: []string{addIndex1},
			},
			{
				err:  mysql.ErrInvalidConn,
				ddls: []string{invalidDDL}, // invalid DDL
			},
			{
				err:  mysql.ErrInvalidConn,
				ddls: []string{insertDML}, // invalid DDL
			},
			{
				err:  mysql.ErrInvalidConn,
				ddls: []string{createTable}, // not `ADD INDEX`
			},
			{
				err:  mysql.ErrInvalidConn,
				ddls: []string{addColumn}, // not `ADD INDEX`
			},
			{
				err:  mysql.ErrInvalidConn,
				ddls: []string{addUK}, // not `ADD INDEX`, but `ADD UNIQUE INDEX`
			},
			{
				err:  mysql.ErrInvalidConn,
				ddls: []string{addFK}, // not `ADD INDEX`, but `ADD * FOREIGN KEY`
			},
			{
				err:  mysql.ErrInvalidConn,
				ddls: []string{addIndexMulti}, // multi `ADD INDEX` in one statement
			},
			{
				err:     mysql.ErrInvalidConn,
				ddls:    []string{addIndex1},
				handled: true,
			},
			{
				err:     mysql.ErrInvalidConn,
				ddls:    []string{addIndex2},
				handled: true,
			},
			{
				err:     mysql.ErrInvalidConn,
				ddls:    []string{addColumn, addIndex1},
				index:   1,
				handled: true,
			},
			{
				err:     mysql.ErrInvalidConn,
				ddls:    []string{addColumn, addIndex2},
				index:   1,
				handled: true,
			},
			{
				err:     mysql.ErrInvalidConn,
				ddls:    []string{addIndex1, addIndex2},
				index:   1,
				handled: true,
			},
			{
				err:   newMysqlErr(errno.ErrUnsupportedDDLOperation, "drop column xx with index"),
				ddls:  []string{addIndex1, dropColumnWithIndex},
				index: 0, // wrong index
			},
		}
	)
	conn2.ResetBaseConnFn = func(*tcontext.Context, *conn.BaseConn) (*conn.BaseConn, error) {
		return nil, nil
	}

	syncer.metricsProxies = metrics.DefaultMetricsProxies.CacheForOneTask("task", "worker", "source")

	for _, cs := range cases {
		err2 := syncer.handleSpecialDDLError(tctx, cs.err, cs.ddls, cs.index, conn2)
		if cs.handled {
			require.NoError(t, err2)
		} else {
			require.Equal(t, cs.err, err2)
		}
	}

	var (
		execErr = newMysqlErr(errno.ErrUnsupportedDDLOperation, "drop column xx with index")
		ddls    = []string{dropColumnWithIndex}
	)

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	conn1, err := db.Conn(context.Background())
	require.NoError(t, err)
	conn2.ResetBaseConnFn = func(_ *tcontext.Context, _ *conn.BaseConn) (*conn.BaseConn, error) {
		return conn.NewBaseConn(conn1, nil), nil
	}
	err = conn2.ResetConn(tctx)
	require.NoError(t, err)

	// dropColumnF test successful
	mock.ExpectQuery("SELECT INDEX_NAME FROM information_schema.statistics WHERE.*").WillReturnRows(
		sqlmock.NewRows([]string{"INDEX_NAME"}).AddRow("gen_idx"))
	mock.ExpectQuery("SELECT count\\(\\*\\) FROM information_schema.statistics WHERE.*").WillReturnRows(
		sqlmock.NewRows([]string{"count(*)"}).AddRow(1))
	mock.ExpectBegin()
	mock.ExpectExec("ALTER TABLE ``.`tbl` DROP INDEX `gen_idx`").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec(dropColumnWithIndex).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	handledErr := syncer.handleSpecialDDLError(tctx, execErr, ddls, 0, conn2)
	require.NoError(t, mock.ExpectationsWereMet())
	require.NoError(t, handledErr)

	// dropColumnF test failed because multi-column index
	mock.ExpectQuery("SELECT INDEX_NAME FROM information_schema.statistics WHERE.*").WillReturnRows(
		sqlmock.NewRows([]string{"INDEX_NAME"}).AddRow("gen_idx"))
	mock.ExpectQuery("SELECT count\\(\\*\\) FROM information_schema.statistics WHERE.*").WillReturnRows(
		sqlmock.NewRows([]string{"count(*)"}).AddRow(2))

	handledErr = syncer.handleSpecialDDLError(tctx, execErr, ddls, 0, conn2)
	require.NoError(t, mock.ExpectationsWereMet())
	require.Error(t, execErr, handledErr)
}

func TestIsConnectionRefusedError(t *testing.T) {
	isConnRefusedErr := isConnectionRefusedError(nil)
	require.False(t, isConnRefusedErr)

	isConnRefusedErr = isConnectionRefusedError(errors.New("timeout"))
	require.False(t, isConnRefusedErr)

	isConnRefusedErr = isConnectionRefusedError(errors.New("connect: connection refused"))
	require.True(t, isConnRefusedErr)
}

func TestGetDDLStatusFromTiDB(t *testing.T) {
	var (
		createDatabaseSQL          = ""
		selectTimeSQL              = ""
		createTableSQL1            = ""
		createTableSQL2            = ""
		createTableSQL3            = ""
		createTableSQL4            = ""
		createTableSQL5            = ""
		createTableSQL6            = ""
		alterTableSQL1             = ""
		alterTableSQL2             = ""
		alterTableSQL3             = ""
		alterTableSQL4             = ""
		alterTableSQL5             = ""
		alterTableSQL6             = ""
		adminShowDDLJobsSQL1       = ""
		adminShowDDLJobsSQL2       = ""
		adminShowDDLJobsLimitSQL1  = ""
		adminShowDDLJobsLimitSQL2  = ""
		adminShowDDLJobsLimitSQL3  = ""
		adminShowDDLJobsLimitSQL4  = ""
		adminShowDDLJobsLimitSQL5  = ""
		adminShowDDLJobsLimitSQL6  = ""
		adminShowDDLJobsLimitSQL7  = ""
		adminShowDDLJobsLimitSQL8  = ""
		adminShowDDLJobsLimitSQL9  = ""
		adminShowDDLJobsLimitSQL10 = ""
		adminShowDDLJobsLimitSQL11 = ""
		adminShowDDLJobsLimitSQL12 = ""
		adminShowDDLJobsLimitSQL13 = ""
		adminShowDDLJobsLimitSQL14 = ""
		adminShowDDLJobsLimitSQL15 = ""
		adminShowDDLJobsLimitSQL16 = ""
		adminShowDDLJobsLimitSQL17 = ""
		adminShowDDLJobsLimitSQL18 = ""
		adminShowDDLJobsLimitSQL19 = ""
		adminShowDDLJobsLimitSQL20 = ""
	)

	var err error
	db, mock, err := sqlmock.New()
	require.Nil(t, err)

	createDatabaseSQL = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS many_tables_test")
	selectTimeSQL = fmt.Sprintf("SELECT @@TIMESTAMP")
	createTableSQL1 = fmt.Sprintf("CREATE TABLE IF NOT EXISTS many_tables_test.t%d(i TINYINT, j INT UNIQUE KEY)", 1)
	createTableSQL2 = fmt.Sprintf("CREATE TABLE IF NOT EXISTS many_tables_test.t%d(i TINYINT, j INT UNIQUE KEY)", 2)
	createTableSQL3 = fmt.Sprintf("CREATE TABLE IF NOT EXISTS many_tables_test.t%d(i TINYINT, j INT UNIQUE KEY)", 3)
	createTableSQL4 = fmt.Sprintf("CREATE TABLE IF NOT EXISTS many_tables_test.t%d(i TINYINT, j INT UNIQUE KEY)", 4)
	createTableSQL5 = fmt.Sprintf("CREATE TABLE IF NOT EXISTS many_tables_test.t%d(i TINYINT, j INT UNIQUE KEY)", 5)
	createTableSQL6 = fmt.Sprintf("CREATE TABLE IF NOT EXISTS many_tables_test.t%d(i TINYINT, j INT UNIQUE KEY)", 6)
	alterTableSQL1 = fmt.Sprintf("ALTER TABLE many_tables_test.t%d ADD x timestamp DEFAULT current_timestamp;", 1)
	alterTableSQL2 = fmt.Sprintf("ALTER TABLE many_tables_test.t%d ADD x timestamp DEFAULT current_timestamp;", 2)
	alterTableSQL3 = fmt.Sprintf("ALTER TABLE many_tables_test.t%d ADD x timestamp DEFAULT current_timestamp;", 3)
	alterTableSQL4 = fmt.Sprintf("ALTER TABLE many_tables_test.t%d ADD x timestamp DEFAULT current_timestamp;", 4)
	alterTableSQL5 = fmt.Sprintf("ALTER TABLE many_tables_test.t%d ADD x timestamp DEFAULT current_timestamp;", 5)
	alterTableSQL6 = fmt.Sprintf("ALTER TABLE many_tables_test.t%d ADD x timestamp DEFAULT current_timestamp;", 6)
	adminShowDDLJobsSQL1 = fmt.Sprintf("ADMIN SHOW DDL JOBS 10")
	adminShowDDLJobsSQL2 = fmt.Sprintf("ADMIN SHOW DDL JOBS 20")
	adminShowDDLJobsLimitSQL1 = fmt.Sprintf("ADMIN SHOW DDL JOB QUERIES LIMIT 1 OFFSET %d", 0)
	adminShowDDLJobsLimitSQL2 = fmt.Sprintf("ADMIN SHOW DDL JOB QUERIES LIMIT 1 OFFSET %d", 1)
	adminShowDDLJobsLimitSQL3 = fmt.Sprintf("ADMIN SHOW DDL JOB QUERIES LIMIT 1 OFFSET %d", 2)
	adminShowDDLJobsLimitSQL4 = fmt.Sprintf("ADMIN SHOW DDL JOB QUERIES LIMIT 1 OFFSET %d", 3)
	adminShowDDLJobsLimitSQL5 = fmt.Sprintf("ADMIN SHOW DDL JOB QUERIES LIMIT 1 OFFSET %d", 4)
	adminShowDDLJobsLimitSQL6 = fmt.Sprintf("ADMIN SHOW DDL JOB QUERIES LIMIT 1 OFFSET %d", 5)
	adminShowDDLJobsLimitSQL7 = fmt.Sprintf("ADMIN SHOW DDL JOB QUERIES LIMIT 1 OFFSET %d", 6)
	adminShowDDLJobsLimitSQL8 = fmt.Sprintf("ADMIN SHOW DDL JOB QUERIES LIMIT 1 OFFSET %d", 7)
	adminShowDDLJobsLimitSQL9 = fmt.Sprintf("ADMIN SHOW DDL JOB QUERIES LIMIT 1 OFFSET %d", 8)
	adminShowDDLJobsLimitSQL10 = fmt.Sprintf("ADMIN SHOW DDL JOB QUERIES LIMIT 1 OFFSET %d", 9)
	adminShowDDLJobsLimitSQL11 = fmt.Sprintf("ADMIN SHOW DDL JOB QUERIES LIMIT 1 OFFSET %d", 10)
	adminShowDDLJobsLimitSQL12 = fmt.Sprintf("ADMIN SHOW DDL JOB QUERIES LIMIT 1 OFFSET %d", 11)
	adminShowDDLJobsLimitSQL13 = fmt.Sprintf("ADMIN SHOW DDL JOB QUERIES LIMIT 1 OFFSET %d", 12)
	adminShowDDLJobsLimitSQL14 = fmt.Sprintf("ADMIN SHOW DDL JOB QUERIES LIMIT 1 OFFSET %d", 13)
	adminShowDDLJobsLimitSQL15 = fmt.Sprintf("ADMIN SHOW DDL JOB QUERIES LIMIT 1 OFFSET %d", 14)
	adminShowDDLJobsLimitSQL16 = fmt.Sprintf("ADMIN SHOW DDL JOB QUERIES LIMIT 1 OFFSET %d", 15)
	adminShowDDLJobsLimitSQL17 = fmt.Sprintf("ADMIN SHOW DDL JOB QUERIES LIMIT 1 OFFSET %d", 16)
	adminShowDDLJobsLimitSQL18 = fmt.Sprintf("ADMIN SHOW DDL JOB QUERIES LIMIT 1 OFFSET %d", 17)
	adminShowDDLJobsLimitSQL19 = fmt.Sprintf("ADMIN SHOW DDL JOB QUERIES LIMIT 1 OFFSET %d", 18)
	adminShowDDLJobsLimitSQL20 = fmt.Sprintf("ADMIN SHOW DDL JOB QUERIES LIMIT 1 OFFSET %d", 19)

	mock.ExpectBegin()
	mock.ExpectExec(createDatabaseSQL).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectQuery(selectTimeSQL).WillReturnRows(sqlmock.NewRows([]string{"@@TIMESTAMP"}).AddRow(1659416697.8055072))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec(createTableSQL1).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec(createTableSQL2).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec(createTableSQL3).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec(createTableSQL4).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec(createTableSQL5).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec(createTableSQL6).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec(alterTableSQL1).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec(alterTableSQL2).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec(alterTableSQL3).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec(alterTableSQL4).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec(alterTableSQL5).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec(alterTableSQL6).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	// ADMIN SHOW DDL JOBS QUERIES
	mock.ExpectBegin()
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
	mock.ExpectCommit()
	mock.ExpectBegin()
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
	mock.ExpectCommit()

	// ADMIN SHOW DDL JOBS QUERIES LIMIT 1 OFFSET n
	mock.ExpectBegin()
	mock.ExpectQuery(adminShowDDLJobsLimitSQL1).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).AddRow(
		61, "ALTER TABLE many_tables_test.t6 ADD x timestamp DEFAULT current_timestamp"))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectQuery(adminShowDDLJobsLimitSQL2).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).AddRow(
		60, "ALTER TABLE many_tables_test.t5 ADD x timestamp DEFAULT current_timestamp"))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectQuery(adminShowDDLJobsLimitSQL3).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).AddRow(
		59, "ALTER TABLE many_tables_test.t4 ADD x timestamp DEFAULT current_timestamp"))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectQuery(adminShowDDLJobsLimitSQL4).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).AddRow(
		58, "ALTER TABLE many_tables_test.t3 ADD x timestamp DEFAULT current_timestamp"))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectQuery(adminShowDDLJobsLimitSQL5).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).AddRow(
		57, "ALTER TABLE many_tables_test.t2 ADD x timestamp DEFAULT current_timestamp"))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectQuery(adminShowDDLJobsLimitSQL6).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).AddRow(
		56, "ALTER TABLE many_tables_test.t1 ADD x timestamp DEFAULT current_timestamp"))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectQuery(adminShowDDLJobsLimitSQL7).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).AddRow(
		55, "CREATE TABLE IF NOT EXISTS many_tables_test.t6(i TINYINT, j INT UNIQUE KEY)"))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectQuery(adminShowDDLJobsLimitSQL8).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).AddRow(
		54, "CREATE TABLE IF NOT EXISTS many_tables_test.t5(i TINYINT, j INT UNIQUE KEY)"))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectQuery(adminShowDDLJobsLimitSQL9).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).AddRow(
		53, "CREATE TABLE IF NOT EXISTS many_tables_test.t4(i TINYINT, j INT UNIQUE KEY)"))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectQuery(adminShowDDLJobsLimitSQL10).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).AddRow(
		52, "CREATE TABLE IF NOT EXISTS many_tables_test.t3(i TINYINT, j INT UNIQUE KEY)"))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectQuery(adminShowDDLJobsLimitSQL11).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).AddRow(
		51, "CREATE TABLE IF NOT EXISTS many_tables_test.t2(i TINYINT, j INT UNIQUE KEY)"))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectQuery(adminShowDDLJobsLimitSQL12).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).AddRow(
		50, "CREATE TABLE IF NOT EXISTS many_tables_test.t1(i TINYINT, j INT UNIQUE KEY)"))
	mock.ExpectCommit()
	mock.ExpectQuery(adminShowDDLJobsLimitSQL13).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).AddRow(
		49, "CREATE TABLE IF NOT EXISTS other_test.t7(i TINYINT, j INT UNIQUE KEY)"))
	mock.ExpectCommit()
	mock.ExpectQuery(adminShowDDLJobsLimitSQL14).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).AddRow(
		48, "CREATE TABLE IF NOT EXISTS other_test.t6(i TINYINT, j INT UNIQUE KEY)"))
	mock.ExpectCommit()
	mock.ExpectQuery(adminShowDDLJobsLimitSQL15).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).AddRow(
		47, "CREATE TABLE IF NOT EXISTS other_test.t5(i TINYINT, j INT UNIQUE KEY)"))
	mock.ExpectCommit()
	mock.ExpectQuery(adminShowDDLJobsLimitSQL16).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).AddRow(
		46, "CREATE TABLE IF NOT EXISTS other_test.t4(i TINYINT, j INT UNIQUE KEY)"))
	mock.ExpectCommit()
	mock.ExpectQuery(adminShowDDLJobsLimitSQL17).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).AddRow(
		45, "CREATE TABLE IF NOT EXISTS other_test.t3(i TINYINT, j INT UNIQUE KEY)"))
	mock.ExpectCommit()
	mock.ExpectQuery(adminShowDDLJobsLimitSQL18).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).AddRow(
		44, "CREATE TABLE IF NOT EXISTS other_test.t2(i TINYINT, j INT UNIQUE KEY)"))
	mock.ExpectCommit()
	mock.ExpectQuery(adminShowDDLJobsLimitSQL19).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).AddRow(
		43, "CREATE TABLE IF NOT EXISTS other_test.t1(i TINYINT, j INT UNIQUE KEY)"))
	mock.ExpectCommit()
	mock.ExpectQuery(adminShowDDLJobsLimitSQL20).WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).AddRow(
		42, "CREATE TABLE IF NOT EXISTS other_test.t0(i TINYINT, j INT UNIQUE KEY)"))
	mock.ExpectCommit()

	status, err := GetDDLStatusFromTiDB(db, "ALTER TABLE many_tables_test.t6 ADD x timestamp DEFAULT current_timestamp")
	require.Equal(t, "running", status)
	require.Nil(t, err)

	status, err = GetDDLStatusFromTiDB(db, "ALTER TABLE many_tables_test.t4 ADD x timestamp DEFAULT current_timestamp")
	require.Equal(t, "none", status)
	require.Nil(t, err)

	status, err = GetDDLStatusFromTiDB(db, "CREATE TABLE IF NOT EXISTS many_tables_test.t1(i TINYINT, j INT UNIQUE KEY)")
	require.Equal(t, "synced", status)
	require.Nil(t, err)
}
