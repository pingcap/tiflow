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
	"database/sql/driver"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	tiddl "github.com/pingcap/tidb/pkg/ddl"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	timock "github.com/pingcap/tidb/pkg/util/mock"
	cdcmodel "github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/dm/config"
	connpkg "github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
	"github.com/stretchr/testify/require"
)

func mockTableInfo(t *testing.T, sql string) *timodel.TableInfo {
	t.Helper()

	p := parser.New()
	se := timock.NewContext()
	node, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	ti, err := tiddl.MockTableInfo(se, node.(*ast.CreateTableStmt), 1)
	require.NoError(t, err)
	return ti
}

func TestGenSQL(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "db", Table: "tb"}
	target := &cdcmodel.TableName{Schema: "targetSchema", Table: "targetTable"}
	createSQL := "create table db.tb(id int primary key, col1 int unique not null, col2 int unique, name varchar(24))"

	cases := []struct {
		preValues  []interface{}
		postValues []interface{}
		safeMode   bool

		expectedSQLs []string
		expectedArgs [][]interface{}
	}{
		{
			nil,
			[]interface{}{1, 2, 3, "haha"},
			false,

			[]string{"INSERT INTO `targetSchema`.`targetTable` (`id`,`col1`,`col2`,`name`) VALUES (?,?,?,?)"},
			[][]interface{}{{1, 2, 3, "haha"}},
		},
		{
			nil,
			[]interface{}{1, 2, 3, "haha"},
			true,

			[]string{"REPLACE INTO `targetSchema`.`targetTable` (`id`,`col1`,`col2`,`name`) VALUES (?,?,?,?)"},
			[][]interface{}{{1, 2, 3, "haha"}},
		},
		{
			[]interface{}{1, 2, 3, "haha"},
			nil,
			false,

			[]string{"DELETE FROM `targetSchema`.`targetTable` WHERE `id` = ? LIMIT 1"},
			[][]interface{}{{1}},
		},
		{
			[]interface{}{1, 2, 3, "haha"},
			[]interface{}{4, 5, 6, "hihi"},
			false,

			[]string{"UPDATE `targetSchema`.`targetTable` SET `id` = ?, `col1` = ?, `col2` = ?, `name` = ? WHERE `id` = ? LIMIT 1"},
			[][]interface{}{{4, 5, 6, "hihi", 1}},
		},
		{
			[]interface{}{1, 2, 3, "haha"},
			[]interface{}{1, 2, 3, "hihi"},
			true,

			[]string{"REPLACE INTO `targetSchema`.`targetTable` (`id`,`col1`,`col2`,`name`) VALUES (?,?,?,?)"},
			[][]interface{}{{1, 2, 3, "hihi"}},
		},
		{
			[]interface{}{1, 2, 3, "haha"},
			[]interface{}{4, 5, 6, "hihi"},
			true,

			[]string{"DELETE FROM `targetSchema`.`targetTable` WHERE `id` = ? LIMIT 1", "REPLACE INTO `targetSchema`.`targetTable` (`id`,`col1`,`col2`,`name`) VALUES (?,?,?,?)"},
			[][]interface{}{{1}, {4, 5, 6, "hihi"}},
		},
	}

	worker := &DMLWorker{}

	for _, c := range cases {
		tableInfo := mockTableInfo(t, createSQL)
		change := sqlmodel.NewRowChange(source, target, c.preValues, c.postValues, tableInfo, nil, nil)
		testEC := ec
		if c.safeMode {
			testEC = ecWithSafeMode
		}
		dmlJob := newDMLJob(change, testEC)
		queries, args := worker.genSQLs([]*job{dmlJob})
		require.Equal(t, c.expectedSQLs, queries)
		require.Equal(t, c.expectedArgs, args)
	}
}

func TestJudgeKeyNotFound(t *testing.T) {
	dmlWorker := &DMLWorker{
		compact:      true,
		multipleRows: true,
	}
	require.False(t, dmlWorker.judgeKeyNotFound(0, nil))
	dmlWorker.compact = false
	require.False(t, dmlWorker.judgeKeyNotFound(0, nil))
	dmlWorker.multipleRows = false
	require.False(t, dmlWorker.judgeKeyNotFound(0, nil))
	jobs := []*job{{safeMode: false}, {safeMode: true}}
	require.False(t, dmlWorker.judgeKeyNotFound(0, jobs))
	jobs[1].safeMode = false
	require.True(t, dmlWorker.judgeKeyNotFound(0, jobs))
	require.False(t, dmlWorker.judgeKeyNotFound(2, jobs))
	require.False(t, dmlWorker.judgeKeyNotFound(4, jobs))
}

func TestShouldDisableForeignKeyChecksForJob(t *testing.T) {
	t.Parallel()

	worker := &DMLWorker{}

	source := &cdcmodel.TableName{Schema: "db", Table: "tb"}
	target := &cdcmodel.TableName{Schema: "target", Table: "tb"}
	tableInfo := mockTableInfo(t, "create table db.tb(id int primary key, name varchar(10))")

	insertChange := sqlmodel.NewRowChange(source, target, nil, []interface{}{1, "v"}, tableInfo, nil, nil)
	insertJob := &job{tp: dml, safeMode: true, dml: insertChange}
	require.False(t, worker.shouldDisableForeignKeyChecksForJob(insertJob))

	worker.foreignKeyChecksEnabled = true
	require.True(t, worker.shouldDisableForeignKeyChecksForJob(insertJob))

	updateChange := sqlmodel.NewRowChange(source, target, []interface{}{1, "a"}, []interface{}{1, "b"}, tableInfo, nil, nil)
	updateJob := &job{tp: dml, safeMode: true, dml: updateChange}
	require.True(t, worker.shouldDisableForeignKeyChecksForJob(updateJob))

	deleteChange := sqlmodel.NewRowChange(source, target, []interface{}{1, "a"}, nil, tableInfo, nil, nil)
	deleteJob := &job{tp: dml, safeMode: true, dml: deleteChange}
	require.False(t, worker.shouldDisableForeignKeyChecksForJob(deleteJob))

	insertJob.safeMode = false
	require.False(t, worker.shouldDisableForeignKeyChecksForJob(insertJob))
}

func TestIsForeignKeyChecksEnabled(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		session  map[string]string
		expected bool
	}{
		{name: "nil session", session: nil, expected: false},
		{name: "disabled", session: map[string]string{"foreign_key_checks": "0"}, expected: false},
		{name: "enabled numeric", session: map[string]string{"foreign_key_checks": "1"}, expected: true},
		{name: "enabled literal", session: map[string]string{"FOREIGN_KEY_CHECKS": "ON"}, expected: true},
		{name: "enabled quoted", session: map[string]string{"foreign_key_checks": "'1'"}, expected: true},
		{name: "other value", session: map[string]string{"foreign_key_checks": "off"}, expected: false},
		{name: "unrelated", session: map[string]string{"sql_mode": ""}, expected: false},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, c.expected, isForeignKeyChecksEnabled(c.session))
		})
	}
}

func TestShouldDisableForeignKeyChecks(t *testing.T) {
	t.Parallel()

	worker := &DMLWorker{foreignKeyChecksEnabled: true}
	source := &cdcmodel.TableName{Schema: "db", Table: "tb"}
	target := &cdcmodel.TableName{Schema: "target", Table: "tbl"}
	tableInfo := mockTableInfo(t, "create table db.tb(id int primary key, col1 int unique not null, col2 int unique, name varchar(24))")

	insertChange := sqlmodel.NewRowChange(source, target, nil, []interface{}{1, 2, 3, "v"}, tableInfo, nil, nil)
	insertJob := newDMLJob(insertChange, ecWithSafeMode)
	require.True(t, worker.shouldDisableForeignKeyChecksForJob(insertJob))

	insertJob.safeMode = false
	require.False(t, worker.shouldDisableForeignKeyChecksForJob(insertJob))

	updateChange := sqlmodel.NewRowChange(source, target, []interface{}{1, 2, 3, "v"}, []interface{}{1, 2, 3, "v2"}, tableInfo, nil, nil)
	updateJob := newDMLJob(updateChange, ecWithSafeMode)
	require.True(t, worker.shouldDisableForeignKeyChecksForJob(updateJob))

	deleteChange := sqlmodel.NewRowChange(source, target, []interface{}{1, 2, 3, "v"}, nil, tableInfo, nil, nil)
	deleteJob := newDMLJob(deleteChange, ecWithSafeMode)
	require.False(t, worker.shouldDisableForeignKeyChecksForJob(deleteJob))

	worker.foreignKeyChecksEnabled = false
	anotherJob := newDMLJob(insertChange, ecWithSafeMode)
	require.False(t, worker.shouldDisableForeignKeyChecksForJob(anotherJob))
}

func TestExecuteBatchJobsWithForeignKey(t *testing.T) {
	t.Parallel()

	// helper: convert []interface{} -> []driver.Value
	toDriverValues := func(args []interface{}) []driver.Value {
		out := make([]driver.Value, len(args))
		for i, v := range args {
			out[i] = v
		}
		return out
	}

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sqlConn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer sqlConn.Close()

	baseConn := connpkg.NewBaseConnForTest(sqlConn, nil)
	cfg := &config.SubTaskConfig{Name: "test"}
	dbConn := dbconn.NewDBConn(cfg, baseConn)

	worker := &DMLWorker{
		toDBConns:               []*dbconn.DBConn{dbConn},
		syncCtx:                 tcontext.Background(),
		successFunc:             func(int, int, []*job) {},
		fatalFunc:               func(*job, error) {},
		metricProxies:           nil,
		foreignKeyChecksEnabled: true,
		logger:                  log.L(),
	}

	source := &cdcmodel.TableName{Schema: "db", Table: "tb"}
	target := &cdcmodel.TableName{Schema: "targetSchema", Table: "targetTable"}
	createSQL := "create table db.tb(id int primary key, col1 int unique not null, col2 int unique, name varchar(24))"
	tableInfo := mockTableInfo(t, createSQL)

	insertChange := sqlmodel.NewRowChange(source, target, nil, []interface{}{1, 2, 3, "normal"}, tableInfo, nil, nil)
	replaceChange := sqlmodel.NewRowChange(source, target, nil, []interface{}{2, 3, 4, "safe"}, tableInfo, nil, nil)

	insertJob := newDMLJob(insertChange, ec)
	replaceJob := newDMLJob(replaceChange, ecWithSafeMode)

	insertQueries, insertArgs := worker.genSQLs([]*job{insertJob})
	replaceQueries, replaceArgs := worker.genSQLs([]*job{replaceJob})

	require.Len(t, insertQueries, 1)
	require.Len(t, replaceQueries, 1)

	// Normal insert should execute directly
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(insertQueries[0])).
		WithArgs(toDriverValues(insertArgs[0])...).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	// Safe-mode REPLACE should trigger disable + REPLACE + restore
	// disable FKC
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("SET SESSION foreign_key_checks=0")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	// REPLACE job
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(replaceQueries[0])).
		WithArgs(toDriverValues(replaceArgs[0])...).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	// restore FKC
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("SET SESSION foreign_key_checks=1")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	// Run two jobs separately: first insert, then REPLACE
	worker.executeBatchJobs(0, []*job{insertJob}, false)
	worker.executeBatchJobs(0, []*job{replaceJob}, true)

	require.NoError(t, mock.ExpectationsWereMet())
}
