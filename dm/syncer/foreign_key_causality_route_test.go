// Copyright 2026 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/parser"
	tmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/retry"
	"github.com/pingcap/tiflow/dm/pkg/schema"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	"github.com/stretchr/testify/require"
)

func newForeignKeyRouteTestSyncer(t *testing.T, workerCount int) (*Syncer, sqlmock.Sqlmock) {
	t.Helper()

	cfg := genDefaultSubTaskConfig4Test()
	cfg.WorkerCount = workerCount
	cfg.To.Session = map[string]string{"foreign_key_checks": "1"}

	syncer := NewSyncer(cfg, nil, nil)
	var err error
	syncer.baList, err = filter.New(syncer.cfg.CaseSensitive, syncer.cfg.BAList)
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	dbConn, err := db.Conn(context.Background())
	require.NoError(t, err)

	baseConn := conn.NewBaseConnForTest(dbConn, &retry.FiniteRetryStrategy{})
	syncer.ddlDBConn = dbconn.NewDBConn(cfg, baseConn)
	syncer.downstreamTrackConn = dbconn.NewDBConn(cfg, conn.NewBaseConnForTest(dbConn, &retry.FiniteRetryStrategy{}))
	syncer.schemaTracker, err = schema.NewTestTracker(context.Background(), cfg.Name, syncer.downstreamTrackConn, log.L())
	require.NoError(t, err)

	t.Cleanup(func() {
		syncer.schemaTracker.Close()
		require.NoError(t, db.Close())
	})

	return syncer, mock
}

func setForeignKeyRouteTestBAList(t *testing.T, syncer *Syncer, rules *filter.Rules) {
	t.Helper()

	syncer.cfg.BAList = rules
	var err error
	syncer.baList, err = filter.New(syncer.cfg.CaseSensitive, syncer.cfg.BAList)
	require.NoError(t, err)
}

func execTrackedDDL(t *testing.T, tracker *schema.Tracker, p *parser.Parser, db string, sql string) {
	t.Helper()

	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	require.NoError(t, tracker.Exec(context.Background(), db, stmt))
}

func expectDownstreamSQLModeInit(mock sqlmock.Sqlmock) {
	mock.ExpectBegin()
	mock.ExpectExec(fmt.Sprintf("SET SESSION SQL_MODE = '%s'", tmysql.DefaultSQLMode)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
}

func TestPrepareDownStreamTableInfoSkipsFKCausalityForSingleWorkerRoute(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 1)
	p := parser.New()

	execTrackedDDL(t, syncer.schemaTracker, p, "", "create database db")
	execTrackedDDL(t, syncer.schemaTracker, p, "db", "create table parent(id int primary key)")
	execTrackedDDL(t, syncer.schemaTracker, p, "db", "create table child(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references parent(id))")

	sourceTable := &filter.Table{Schema: "db", Name: "child"}
	targetTable := &filter.Table{Schema: "db_r", Name: "child_r"}
	originTI, err := syncer.schemaTracker.GetTableInfo(sourceTable)
	require.NoError(t, err)

	expectDownstreamSQLModeInit(mock)
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(targetTable)).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("child_r", "create table child_r(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references parent_r(id))"),
	)

	dti, err := syncer.prepareDownStreamTableInfo(tcontext.Background(), sourceTable, targetTable, originTI)
	require.NoError(t, err)
	require.NotNil(t, dti)
	require.Empty(t, dti.ForeignKeyRelations)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPrepareDownStreamTableInfoRejectsChildRouteForMultiWorkerFKCausality(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 2)
	p := parser.New()

	execTrackedDDL(t, syncer.schemaTracker, p, "", "create database db")
	execTrackedDDL(t, syncer.schemaTracker, p, "db", "create table parent(id int primary key)")
	execTrackedDDL(t, syncer.schemaTracker, p, "db", "create table child(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references parent(id))")

	sourceTable := &filter.Table{Schema: "db", Name: "child"}
	targetTable := &filter.Table{Schema: "db_r", Name: "child_r"}
	originTI, err := syncer.schemaTracker.GetTableInfo(sourceTable)
	require.NoError(t, err)

	expectDownstreamSQLModeInit(mock)
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(targetTable)).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("child_r", "create table child_r(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references parent_r(id))"),
	)

	_, err = syncer.prepareDownStreamTableInfo(tcontext.Background(), sourceTable, targetTable, originTI)
	require.ErrorContains(t, err, "worker_count=1")
	require.ErrorContains(t, err, "child table")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPrepareDownStreamTableInfoRejectsParentRouteForMultiWorkerFKCausality(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 2)
	p := parser.New()

	execTrackedDDL(t, syncer.schemaTracker, p, "", "create database db")
	execTrackedDDL(t, syncer.schemaTracker, p, "db", "create table parent(id int primary key)")
	execTrackedDDL(t, syncer.schemaTracker, p, "db", "create table child(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references parent(id))")

	sourceTable := &filter.Table{Schema: "db", Name: "child"}
	targetTable := &filter.Table{Schema: "db", Name: "child"}
	originTI, err := syncer.schemaTracker.GetTableInfo(sourceTable)
	require.NoError(t, err)

	expectDownstreamSQLModeInit(mock)
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(targetTable)).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("child", "create table child(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references parent_r(id))"),
	)

	_, err = syncer.prepareDownStreamTableInfo(tcontext.Background(), sourceTable, targetTable, originTI)
	require.ErrorContains(t, err, "worker_count=1")
	require.ErrorContains(t, err, "upstream parent table")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPrepareDownStreamTableInfoBuildsFKRelationsWithoutRoute(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 2)
	p := parser.New()

	execTrackedDDL(t, syncer.schemaTracker, p, "", "create database db")
	execTrackedDDL(t, syncer.schemaTracker, p, "db", "create table parent(id int primary key)")
	execTrackedDDL(t, syncer.schemaTracker, p, "db", "create table child(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references parent(id))")

	sourceTable := &filter.Table{Schema: "db", Name: "child"}
	targetTable := &filter.Table{Schema: "db", Name: "child"}
	originTI, err := syncer.schemaTracker.GetTableInfo(sourceTable)
	require.NoError(t, err)

	expectDownstreamSQLModeInit(mock)
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(targetTable)).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("child", "create table child(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references parent(id))"),
	)
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(&filter.Table{Schema: "db", Name: "parent"})).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("parent", "create table parent(id int primary key)"),
	)

	dti, err := syncer.prepareDownStreamTableInfo(tcontext.Background(), sourceTable, targetTable, originTI)
	require.NoError(t, err)
	require.Len(t, dti.ForeignKeyRelations, 1)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPrepareDownStreamTableInfoRejectsFilteredParentWithForeignKeyChecksSingleWorker(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 1)
	setForeignKeyRouteTestBAList(t, syncer, &filter.Rules{
		DoTables: []*filter.Table{{Schema: "db", Name: "child"}},
	})
	p := parser.New()

	execTrackedDDL(t, syncer.schemaTracker, p, "", "create database db")
	execTrackedDDL(t, syncer.schemaTracker, p, "db", "create table parent(id int primary key)")
	execTrackedDDL(t, syncer.schemaTracker, p, "db", "create table child(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references parent(id))")

	sourceTable := &filter.Table{Schema: "db", Name: "child"}
	targetTable := &filter.Table{Schema: "db", Name: "child"}
	originTI, err := syncer.schemaTracker.GetTableInfo(sourceTable)
	require.NoError(t, err)

	expectDownstreamSQLModeInit(mock)
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(targetTable)).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("child", "create table child(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references parent(id))"),
	)

	_, err = syncer.prepareDownStreamTableInfo(tcontext.Background(), sourceTable, targetTable, originTI)
	require.ErrorContains(t, err, "block-allow-list")
	require.ErrorContains(t, err, "`db`.`parent`")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPrepareDownStreamTableInfoRejectsFilteredParentWithForeignKeyChecksMultiWorker(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 2)
	setForeignKeyRouteTestBAList(t, syncer, &filter.Rules{
		DoTables: []*filter.Table{{Schema: "db", Name: "child"}},
	})
	p := parser.New()

	execTrackedDDL(t, syncer.schemaTracker, p, "", "create database db")
	execTrackedDDL(t, syncer.schemaTracker, p, "db", "create table parent(id int primary key)")
	execTrackedDDL(t, syncer.schemaTracker, p, "db", "create table child(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references parent(id))")

	sourceTable := &filter.Table{Schema: "db", Name: "child"}
	targetTable := &filter.Table{Schema: "db", Name: "child"}
	originTI, err := syncer.schemaTracker.GetTableInfo(sourceTable)
	require.NoError(t, err)

	expectDownstreamSQLModeInit(mock)
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(targetTable)).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("child", "create table child(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references parent(id))"),
	)

	_, err = syncer.prepareDownStreamTableInfo(tcontext.Background(), sourceTable, targetTable, originTI)
	require.ErrorContains(t, err, "block-allow-list")
	require.ErrorContains(t, err, "`db`.`parent`")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPrepareDownStreamTableInfoRejectsFilteredAncestorWithForeignKeyChecks(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 2)
	setForeignKeyRouteTestBAList(t, syncer, &filter.Rules{
		DoTables: []*filter.Table{
			{Schema: "db", Name: "parent"},
			{Schema: "db", Name: "child"},
		},
	})
	p := parser.New()

	execTrackedDDL(t, syncer.schemaTracker, p, "", "create database db")
	execTrackedDDL(t, syncer.schemaTracker, p, "db", "create table grandparent(id int primary key)")
	execTrackedDDL(t, syncer.schemaTracker, p, "db", "create table parent(grandparent_id int primary key, constraint fk_parent_grandparent foreign key (grandparent_id) references grandparent(id))")
	execTrackedDDL(t, syncer.schemaTracker, p, "db", "create table child(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references parent(grandparent_id))")

	sourceTable := &filter.Table{Schema: "db", Name: "child"}
	targetTable := &filter.Table{Schema: "db", Name: "child"}
	originTI, err := syncer.schemaTracker.GetTableInfo(sourceTable)
	require.NoError(t, err)

	expectDownstreamSQLModeInit(mock)
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(targetTable)).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("child", "create table child(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references parent(grandparent_id))"),
	)

	_, err = syncer.prepareDownStreamTableInfo(tcontext.Background(), sourceTable, targetTable, originTI)
	require.ErrorContains(t, err, "block-allow-list")
	require.ErrorContains(t, err, "`db`.`grandparent`")
	require.NoError(t, mock.ExpectationsWereMet())
}
