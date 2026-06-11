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
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/pkg/parser"
	tmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/filter"
	regexprrouter "github.com/pingcap/tidb/pkg/util/regexpr-router"
	router "github.com/pingcap/tidb/pkg/util/table-router"
	cdcmodel "github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/retry"
	"github.com/pingcap/tiflow/dm/pkg/schema"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	"github.com/pingcap/tiflow/dm/syncer/metrics"
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
	syncer.fromDB = &dbconn.UpStreamConn{BaseDB: conn.NewBaseDBForTest(db)}
	syncer.tableRouter, err = regexprrouter.NewRegExprRouter(cfg.CaseSensitive, cfg.RouteRules)
	require.NoError(t, err)
	syncer.schemaTracker, err = schema.NewTestTracker(context.Background(), cfg.Name, syncer.downstreamTrackConn, log.L())
	require.NoError(t, err)

	t.Cleanup(func() {
		syncer.schemaTracker.Close()
		_ = db.Close()
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

func setForeignKeyRouteTestRoutes(t *testing.T, syncer *Syncer, rules []*router.TableRule) {
	t.Helper()

	syncer.cfg.RouteRules = rules
	var err error
	syncer.tableRouter, err = regexprrouter.NewRegExprRouter(syncer.cfg.CaseSensitive, syncer.cfg.RouteRules)
	require.NoError(t, err)
}

func setForeignKeyRouteTestSourceTableNamesInsensitive(t *testing.T, syncer *Syncer) {
	t.Helper()

	flavor := conn.LowerCaseTableNamesFlavor(conn.LCTableNamesInsensitive)
	syncer.schemaTracker.Close()
	syncer.SourceTableNamesFlavor = flavor
	syncer.schemaTracker = schema.NewTracker()
	require.NoError(t, syncer.schemaTracker.Init(context.Background(), syncer.cfg.Name, int(flavor), syncer.downstreamTrackConn, log.L()))
}

func execTrackedDDL(t *testing.T, tracker *schema.Tracker, p *parser.Parser, db string, sql string) {
	t.Helper()

	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	require.NoError(t, tracker.Exec(context.Background(), db, stmt))
}

func prepareSimpleParentChildSchema(t *testing.T, syncer *Syncer, p *parser.Parser) {
	t.Helper()

	execTrackedDDL(t, syncer.schemaTracker, p, "", "create database db")
	execTrackedDDL(t, syncer.schemaTracker, p, "db", "create table parent(id int primary key)")
	execTrackedDDL(t, syncer.schemaTracker, p, "db", "create table child(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references parent(id))")
}

func expectDownstreamSQLModeInit(mock sqlmock.Sqlmock) {
	mock.ExpectBegin()
	mock.ExpectExec(fmt.Sprintf("SET SESSION SQL_MODE = '%s'", tmysql.DefaultSQLMode)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
}

func expectFetchAllDoTables(mock sqlmock.Sqlmock, schema string, tables ...string) {
	mock.ExpectQuery("SHOW DATABASES").
		WillReturnRows(sqlmock.NewRows([]string{"Database"}).AddRow(schema))

	rows := sqlmock.NewRows([]string{"Tables_in_" + schema, "Table_type"})
	for _, table := range tables {
		rows.AddRow(table, "BASE TABLE")
	}
	mock.ExpectQuery(fmt.Sprintf("SHOW FULL TABLES IN `%s` WHERE Table_Type != 'VIEW'", schema)).
		WillReturnRows(rows)
}

func requirePreparedFKRelationForRoute(
	t *testing.T,
	mock sqlmock.Sqlmock,
	syncer *Syncer,
	sourceTable *filter.Table,
	targetTable *filter.Table,
	parentTable *filter.Table,
	targetCreateSQL string,
	parentCreateSQL string,
	expectedParent string,
) {
	t.Helper()

	originTI, err := syncer.schemaTracker.GetTableInfo(sourceTable)
	require.NoError(t, err)

	expectDownstreamSQLModeInit(mock)
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(targetTable)).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow(targetTable.Name, targetCreateSQL),
	)
	expectFetchAllDoTables(mock, sourceTable.Schema, "parent", "child")
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(parentTable)).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow(parentTable.Name, parentCreateSQL),
	)

	dti, err := syncer.prepareDownStreamTableInfo(tcontext.Background(), sourceTable, targetTable, originTI)
	require.NoError(t, err)
	require.Len(t, dti.ForeignKeyRelations, 1)
	require.Equal(t, expectedParent, dti.ForeignKeyRelations[0].ParentTable)
	require.NoError(t, mock.ExpectationsWereMet())
}

func requireSharedTargetRouteRejected(
	t *testing.T,
	mock sqlmock.Sqlmock,
	syncer *Syncer,
	sourceTable *filter.Table,
	targetTable *filter.Table,
	targetCreateSQL string,
	doTables []string,
	expectedTarget string,
	expectedSources ...string,
) {
	t.Helper()

	originTI, err := syncer.schemaTracker.GetTableInfo(sourceTable)
	require.NoError(t, err)

	expectDownstreamSQLModeInit(mock)
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(targetTable)).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow(targetTable.Name, targetCreateSQL),
	)
	expectFetchAllDoTables(mock, sourceTable.Schema, doTables...)

	_, err = syncer.prepareDownStreamTableInfo(tcontext.Background(), sourceTable, targetTable, originTI)
	require.ErrorContains(t, err, "shared-target route")
	require.ErrorContains(t, err, expectedTarget)
	for _, expectedSource := range expectedSources {
		require.ErrorContains(t, err, expectedSource)
	}
	require.NoError(t, mock.ExpectationsWereMet())
}

func requireNextCausalityJob(t *testing.T, causalityCh <-chan *job) *job {
	t.Helper()

	select {
	case job, ok := <-causalityCh:
		require.True(t, ok)
		return job
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for causality job")
		return nil
	}
}

func TestPrepareDownStreamTableInfoSkipsFKCausalityForSingleWorkerRoute(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 1)
	p := parser.New()

	prepareSimpleParentChildSchema(t, syncer, p)

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

func TestPrepareDownStreamTableInfoSkipsFKCausalityWhenForeignKeyChecksOff(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 2)
	syncer.cfg.To.Session = map[string]string{"foreign_key_checks": "0"}
	p := parser.New()

	prepareSimpleParentChildSchema(t, syncer, p)

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

func TestPrepareDownStreamTableInfoBuildsFKRelationsWithChildRoute(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 2)
	setForeignKeyRouteTestRoutes(t, syncer, []*router.TableRule{
		{SchemaPattern: "db", TablePattern: "child", TargetSchema: "db_r", TargetTable: "child_r"},
	})
	p := parser.New()

	prepareSimpleParentChildSchema(t, syncer, p)

	requirePreparedFKRelationForRoute(
		t,
		mock,
		syncer,
		&filter.Table{Schema: "db", Name: "child"},
		&filter.Table{Schema: "db_r", Name: "child_r"},
		&filter.Table{Schema: "db", Name: "parent"},
		"create table child_r(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references `db`.`parent`(id))",
		"create table parent(id int primary key)",
		"db.parent",
	)
}

func TestPrepareDownStreamTableInfoBuildsFKRelationsWithChildAndParentRoute(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 2)
	setForeignKeyRouteTestRoutes(t, syncer, []*router.TableRule{
		{SchemaPattern: "db", TablePattern: "parent", TargetSchema: "db_r", TargetTable: "parent_r"},
		{SchemaPattern: "db", TablePattern: "child", TargetSchema: "db_r", TargetTable: "child_r"},
	})
	p := parser.New()

	prepareSimpleParentChildSchema(t, syncer, p)

	sourceTable := &filter.Table{Schema: "db", Name: "child"}
	targetTable := &filter.Table{Schema: "db_r", Name: "child_r"}
	originTI, err := syncer.schemaTracker.GetTableInfo(sourceTable)
	require.NoError(t, err)

	expectDownstreamSQLModeInit(mock)
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(targetTable)).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("child_r", "create table child_r(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references parent_r(id))"),
	)
	expectFetchAllDoTables(mock, "db", "parent", "child")
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(&filter.Table{Schema: "db_r", Name: "parent_r"})).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("parent_r", "create table parent_r(id int primary key)"),
	)

	dti, err := syncer.prepareDownStreamTableInfo(tcontext.Background(), sourceTable, targetTable, originTI)
	require.NoError(t, err)
	require.Len(t, dti.ForeignKeyRelations, 1)
	require.Equal(t, "db.parent", dti.ForeignKeyRelations[0].ParentTable)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPrepareDownStreamTableInfoBuildsFKRelationsWithCaseInsensitiveRoute(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 2)
	require.False(t, syncer.cfg.CaseSensitive)
	setForeignKeyRouteTestSourceTableNamesInsensitive(t, syncer)
	setForeignKeyRouteTestRoutes(t, syncer, []*router.TableRule{
		{SchemaPattern: "db", TablePattern: "parent", TargetSchema: "db_r", TargetTable: "parent_r"},
		{SchemaPattern: "db", TablePattern: "child", TargetSchema: "db_r", TargetTable: "child_r"},
	})
	p := parser.New()

	execTrackedDDL(t, syncer.schemaTracker, p, "", "create database DB")
	execTrackedDDL(t, syncer.schemaTracker, p, "DB", "create table Parent(id int primary key)")
	execTrackedDDL(t, syncer.schemaTracker, p, "DB", "create table Child(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references Parent(id))")

	sourceTable := &filter.Table{Schema: "db", Name: "child"}
	targetTable := syncer.route(sourceTable)
	require.Equal(t, &filter.Table{Schema: "db_r", Name: "child_r"}, targetTable)
	originTI, err := syncer.schemaTracker.GetTableInfo(sourceTable)
	require.NoError(t, err)

	expectDownstreamSQLModeInit(mock)
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(targetTable)).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("child_r", "create table child_r(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references `DB_R`.`PARENT_R`(id))"),
	)
	expectFetchAllDoTables(mock, "DB", "Parent", "Child")
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(&filter.Table{Schema: "DB_R", Name: "PARENT_R"})).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("PARENT_R", "create table PARENT_R(id int primary key)"),
	)

	dti, err := syncer.prepareDownStreamTableInfo(tcontext.Background(), sourceTable, targetTable, originTI)
	require.NoError(t, err)
	require.Len(t, dti.ForeignKeyRelations, 1)
	require.Equal(t, "db.parent", dti.ForeignKeyRelations[0].ParentTable)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGenDMLUsesCaseInsensitiveSourceDomainForRoutedFKCausality(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 2)
	require.False(t, syncer.cfg.CaseSensitive)
	setForeignKeyRouteTestSourceTableNamesInsensitive(t, syncer)
	setForeignKeyRouteTestRoutes(t, syncer, []*router.TableRule{
		{SchemaPattern: "db", TablePattern: "parent", TargetSchema: "db_r", TargetTable: "parent_r"},
		{SchemaPattern: "db", TablePattern: "child", TargetSchema: "db_r", TargetTable: "child_r"},
	})
	p := parser.New()

	execTrackedDDL(t, syncer.schemaTracker, p, "", "create database DB")
	execTrackedDDL(t, syncer.schemaTracker, p, "DB", "create table Parent(id int primary key)")
	execTrackedDDL(t, syncer.schemaTracker, p, "DB", "create table Child(id int primary key, parent_id int, constraint fk_child_parent foreign key (parent_id) references Parent(id))")

	parentSource := &filter.Table{Schema: "DB", Name: "Parent"}
	parentTarget := syncer.route(parentSource)
	parentTI, err := syncer.schemaTracker.GetTableInfo(parentSource)
	require.NoError(t, err)

	expectDownstreamSQLModeInit(mock)
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(parentTarget)).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("parent_r", "create table parent_r(id int primary key)"),
	)
	expectFetchAllDoTables(mock, "DB", "Parent", "Child")

	parentDMLs, err := syncer.genAndFilterInsertDMLs(tcontext.Background(), &genDMLParam{
		sourceTable:     parentSource,
		targetTable:     parentTarget,
		originalData:    [][]any{{10}},
		sourceTableInfo: parentTI,
	}, nil)
	require.NoError(t, err)
	require.Len(t, parentDMLs, 1)
	require.Equal(t, "DB", parentDMLs[0].GetSourceTable().Schema)
	require.Equal(t, "Parent", parentDMLs[0].GetSourceTable().Table)

	parentUpdateDMLs, err := syncer.genAndFilterUpdateDMLs(tcontext.Background(), &genDMLParam{
		sourceTable:     parentSource,
		targetTable:     parentTarget,
		originalData:    [][]any{{10}, {10}},
		sourceTableInfo: parentTI,
	}, nil, nil)
	require.NoError(t, err)
	require.Len(t, parentUpdateDMLs, 1)

	parentDeleteDMLs, err := syncer.genAndFilterDeleteDMLs(tcontext.Background(), &genDMLParam{
		sourceTable:     parentSource,
		targetTable:     parentTarget,
		originalData:    [][]any{{10}},
		sourceTableInfo: parentTI,
	}, nil)
	require.NoError(t, err)
	require.Len(t, parentDeleteDMLs, 1)

	childSource := &filter.Table{Schema: "db", Name: "child"}
	childTarget := syncer.route(childSource)
	childTI, err := syncer.schemaTracker.GetTableInfo(childSource)
	require.NoError(t, err)

	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(childTarget)).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("child_r", "create table child_r(id int primary key, parent_id int, constraint fk_child_parent foreign key (parent_id) references `DB_R`.`PARENT_R`(id))"),
	)
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(&filter.Table{Schema: "DB_R", Name: "PARENT_R"})).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("PARENT_R", "create table PARENT_R(id int primary key)"),
	)

	childDMLs, err := syncer.genAndFilterInsertDMLs(tcontext.Background(), &genDMLParam{
		sourceTable:     childSource,
		targetTable:     childTarget,
		originalData:    [][]any{{100, 10}},
		sourceTableInfo: childTI,
	}, nil)
	require.NoError(t, err)
	require.Len(t, childDMLs, 1)
	require.Equal(t, "db", childDMLs[0].GetSourceTable().Schema)
	require.Equal(t, "child", childDMLs[0].GetSourceTable().Table)

	childUpdateDMLs, err := syncer.genAndFilterUpdateDMLs(tcontext.Background(), &genDMLParam{
		sourceTable:     childSource,
		targetTable:     childTarget,
		originalData:    [][]any{{100, 10}, {100, 10}},
		sourceTableInfo: childTI,
	}, nil, nil)
	require.NoError(t, err)
	require.Len(t, childUpdateDMLs, 1)

	childDeleteDMLs, err := syncer.genAndFilterDeleteDMLs(tcontext.Background(), &genDMLParam{
		sourceTable:     childSource,
		targetTable:     childTarget,
		originalData:    [][]any{{100, 10}},
		sourceTableInfo: childTI,
	}, nil)
	require.NoError(t, err)
	require.Len(t, childDeleteDMLs, 1)

	require.Contains(t, parentDMLs[0].CausalityKeys(), "10.id.db.parent")
	require.Contains(t, parentUpdateDMLs[0].CausalityKeys(), "10.id.db.parent")
	require.Contains(t, parentDeleteDMLs[0].CausalityKeys(), "10.id.db.parent")
	require.Contains(t, childDMLs[0].CausalityKeys(), "10.id.db.parent")
	require.Contains(t, childUpdateDMLs[0].CausalityKeys(), "10.id.db.parent")
	require.Contains(t, childDeleteDMLs[0].CausalityKeys(), "10.id.db.parent")

	syncer.cfg.QueueSize = 8
	syncer.metricsProxies = metrics.DefaultMetricsProxies.CacheForOneTask(syncer.cfg.Name, syncer.cfg.WorkerName, syncer.cfg.SourceID)
	jobCh := make(chan *job, 2)
	causalityCh := causalityWrap(jobCh, syncer)
	jobCh <- &job{tp: dml, dml: parentDeleteDMLs[0]}
	jobCh <- &job{tp: dml, dml: childUpdateDMLs[0]}
	close(jobCh)

	parentJob := requireNextCausalityJob(t, causalityCh)
	childJob := requireNextCausalityJob(t, causalityCh)
	require.Equal(t, dml, parentJob.tp)
	require.Equal(t, dml, childJob.tp)
	require.NotEmpty(t, parentJob.dmlQueueKey)
	require.Equal(t, parentJob.dmlQueueKey, childJob.dmlQueueKey)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGenDMLUsesCaseInsensitiveSourceDomainWithoutRouteForFKCausality(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 2)
	require.False(t, syncer.cfg.CaseSensitive)
	require.Empty(t, syncer.cfg.RouteRules)
	setForeignKeyRouteTestSourceTableNamesInsensitive(t, syncer)
	p := parser.New()

	execTrackedDDL(t, syncer.schemaTracker, p, "", "create database DB")
	execTrackedDDL(t, syncer.schemaTracker, p, "DB", "create table Parent(id int primary key)")
	execTrackedDDL(t, syncer.schemaTracker, p, "DB", "create table Child(id int primary key, parent_id int, constraint fk_child_parent foreign key (parent_id) references Parent(id))")

	parentSource := &filter.Table{Schema: "DB", Name: "Parent"}
	parentTI, err := syncer.schemaTracker.GetTableInfo(parentSource)
	require.NoError(t, err)

	expectDownstreamSQLModeInit(mock)
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(parentSource)).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("Parent", "create table Parent(id int primary key)"),
	)

	parentDMLs, err := syncer.genAndFilterInsertDMLs(tcontext.Background(), &genDMLParam{
		sourceTable:     parentSource,
		targetTable:     parentSource,
		originalData:    [][]any{{10}},
		sourceTableInfo: parentTI,
	}, nil)
	require.NoError(t, err)
	require.Len(t, parentDMLs, 1)
	require.Equal(t, "DB", parentDMLs[0].GetSourceTable().Schema)
	require.Equal(t, "Parent", parentDMLs[0].GetSourceTable().Table)

	parentUpdateDMLs, err := syncer.genAndFilterUpdateDMLs(tcontext.Background(), &genDMLParam{
		sourceTable:     parentSource,
		targetTable:     parentSource,
		originalData:    [][]any{{10}, {10}},
		sourceTableInfo: parentTI,
	}, nil, nil)
	require.NoError(t, err)
	require.Len(t, parentUpdateDMLs, 1)

	parentDeleteDMLs, err := syncer.genAndFilterDeleteDMLs(tcontext.Background(), &genDMLParam{
		sourceTable:     parentSource,
		targetTable:     parentSource,
		originalData:    [][]any{{10}},
		sourceTableInfo: parentTI,
	}, nil)
	require.NoError(t, err)
	require.Len(t, parentDeleteDMLs, 1)

	childSource := &filter.Table{Schema: "db", Name: "child"}
	childTI, err := syncer.schemaTracker.GetTableInfo(childSource)
	require.NoError(t, err)

	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(childSource)).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("child", "create table child(id int primary key, parent_id int, constraint fk_child_parent foreign key (parent_id) references Parent(id))"),
	)
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(&filter.Table{Schema: "db", Name: "Parent"})).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("Parent", "create table Parent(id int primary key)"),
	)

	childDMLs, err := syncer.genAndFilterInsertDMLs(tcontext.Background(), &genDMLParam{
		sourceTable:     childSource,
		targetTable:     childSource,
		originalData:    [][]any{{100, 10}},
		sourceTableInfo: childTI,
	}, nil)
	require.NoError(t, err)
	require.Len(t, childDMLs, 1)

	childUpdateDMLs, err := syncer.genAndFilterUpdateDMLs(tcontext.Background(), &genDMLParam{
		sourceTable:     childSource,
		targetTable:     childSource,
		originalData:    [][]any{{100, 10}, {100, 10}},
		sourceTableInfo: childTI,
	}, nil, nil)
	require.NoError(t, err)
	require.Len(t, childUpdateDMLs, 1)

	childDeleteDMLs, err := syncer.genAndFilterDeleteDMLs(tcontext.Background(), &genDMLParam{
		sourceTable:     childSource,
		targetTable:     childSource,
		originalData:    [][]any{{100, 10}},
		sourceTableInfo: childTI,
	}, nil)
	require.NoError(t, err)
	require.Len(t, childDeleteDMLs, 1)

	require.Contains(t, parentDMLs[0].CausalityKeys(), "10.id.db.parent")
	require.Contains(t, parentUpdateDMLs[0].CausalityKeys(), "10.id.db.parent")
	require.Contains(t, parentDeleteDMLs[0].CausalityKeys(), "10.id.db.parent")
	require.Contains(t, childDMLs[0].CausalityKeys(), "10.id.db.parent")
	require.Contains(t, childUpdateDMLs[0].CausalityKeys(), "10.id.db.parent")
	require.Contains(t, childDeleteDMLs[0].CausalityKeys(), "10.id.db.parent")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCausalityKeySourceTableNameGate(t *testing.T) {
	sourceTable := &filter.Table{Schema: "DB", Name: "Parent"}

	syncer, _ := newForeignKeyRouteTestSyncer(t, 2)
	require.Equal(t, &cdcmodel.TableName{Schema: "db", Table: "parent"}, syncer.causalityKeySourceTableNameForRowChange(sourceTable))

	syncer, _ = newForeignKeyRouteTestSyncer(t, 2)
	syncer.cfg.CaseSensitive = true
	require.Nil(t, syncer.causalityKeySourceTableNameForRowChange(sourceTable))

	syncer, _ = newForeignKeyRouteTestSyncer(t, 1)
	require.Nil(t, syncer.causalityKeySourceTableNameForRowChange(sourceTable))

	syncer, _ = newForeignKeyRouteTestSyncer(t, 2)
	syncer.cfg.To.Session = map[string]string{"foreign_key_checks": "0"}
	require.Nil(t, syncer.causalityKeySourceTableNameForRowChange(sourceTable))
}

func TestPrepareDownStreamTableInfoRejectsCaseMismatchWithCaseSensitiveRoute(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 2)
	syncer.cfg.CaseSensitive = true
	var err error
	syncer.baList, err = filter.New(syncer.cfg.CaseSensitive, syncer.cfg.BAList)
	require.NoError(t, err)
	setForeignKeyRouteTestSourceTableNamesInsensitive(t, syncer)
	setForeignKeyRouteTestRoutes(t, syncer, []*router.TableRule{
		{SchemaPattern: "DB", TablePattern: "Parent", TargetSchema: "db_r", TargetTable: "parent_r"},
		{SchemaPattern: "DB", TablePattern: "Child", TargetSchema: "db_r", TargetTable: "child_r"},
	})
	p := parser.New()

	execTrackedDDL(t, syncer.schemaTracker, p, "", "create database DB")
	execTrackedDDL(t, syncer.schemaTracker, p, "DB", "create table Parent(id int primary key)")
	execTrackedDDL(t, syncer.schemaTracker, p, "DB", "create table Child(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references Parent(id))")

	sourceTable := &filter.Table{Schema: "DB", Name: "Child"}
	targetTable := syncer.route(sourceTable)
	require.Equal(t, &filter.Table{Schema: "db_r", Name: "child_r"}, targetTable)
	originTI, err := syncer.schemaTracker.GetTableInfo(sourceTable)
	require.NoError(t, err)

	expectDownstreamSQLModeInit(mock)
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(targetTable)).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("child_r", "create table child_r(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references `DB_R`.`PARENT_R`(id))"),
	)
	expectFetchAllDoTables(mock, "DB", "Parent", "Child")

	_, err = syncer.prepareDownStreamTableInfo(tcontext.Background(), sourceTable, targetTable, originTI)
	require.ErrorContains(t, err, "requires 1:1 route alignment")
	require.ErrorContains(t, err, "routes to `db_r`.`parent_r`, but downstream parent table is `DB_R`.`PARENT_R`")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPrepareDownStreamTableInfoBuildsFKRelationsWithParentRoute(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 2)
	setForeignKeyRouteTestRoutes(t, syncer, []*router.TableRule{
		{SchemaPattern: "db", TablePattern: "parent", TargetSchema: "db", TargetTable: "parent_r"},
	})
	p := parser.New()

	prepareSimpleParentChildSchema(t, syncer, p)

	requirePreparedFKRelationForRoute(
		t,
		mock,
		syncer,
		&filter.Table{Schema: "db", Name: "child"},
		&filter.Table{Schema: "db", Name: "child"},
		&filter.Table{Schema: "db", Name: "parent_r"},
		"create table child(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references parent_r(id))",
		"create table parent_r(id int primary key)",
		"db.parent",
	)
}

func TestPrepareDownStreamTableInfoBuildsFKRelationsWithoutRoute(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 2)
	p := parser.New()

	prepareSimpleParentChildSchema(t, syncer, p)

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

	dti2, err := syncer.prepareDownStreamTableInfo(tcontext.Background(), sourceTable, targetTable, originTI)
	require.NoError(t, err)
	require.Same(t, dti, dti2)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPrepareDownStreamTableInfoCachesForeignKeyRouteTopologyCheck(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 2)
	setForeignKeyRouteTestRoutes(t, syncer, []*router.TableRule{
		{SchemaPattern: "db", TablePattern: "parent", TargetSchema: "db_r", TargetTable: "parent_r"},
		{SchemaPattern: "db", TablePattern: "child", TargetSchema: "db_r", TargetTable: "child_r"},
	})
	p := parser.New()

	prepareSimpleParentChildSchema(t, syncer, p)

	sourceTable := &filter.Table{Schema: "db", Name: "child"}
	targetTable := &filter.Table{Schema: "db_r", Name: "child_r"}
	originTI, err := syncer.schemaTracker.GetTableInfo(sourceTable)
	require.NoError(t, err)

	expectDownstreamSQLModeInit(mock)
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(targetTable)).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("child_r", "create table child_r(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references parent_r(id))"),
	)
	expectFetchAllDoTables(mock, "db", "parent", "child")
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(&filter.Table{Schema: "db_r", Name: "parent_r"})).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("parent_r", "create table parent_r(id int primary key)"),
	)

	dti, err := syncer.prepareDownStreamTableInfo(tcontext.Background(), sourceTable, targetTable, originTI)
	require.NoError(t, err)
	require.Len(t, dti.ForeignKeyRelations, 1)

	dti, err = syncer.prepareDownStreamTableInfo(tcontext.Background(), sourceTable, targetTable, originTI)
	require.NoError(t, err)
	require.Len(t, dti.ForeignKeyRelations, 1)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdateInvalidatesForeignKeyRouteTopologyCheckCache(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 2)
	syncer.timezone = time.UTC
	setForeignKeyRouteTestRoutes(t, syncer, []*router.TableRule{
		{SchemaPattern: "db", TablePattern: "parent", TargetSchema: "db_r", TargetTable: "parent_r"},
		{SchemaPattern: "db", TablePattern: "child", TargetSchema: "db_r", TargetTable: "child_r"},
	})
	p := parser.New()

	prepareSimpleParentChildSchema(t, syncer, p)

	sourceTable := &filter.Table{Schema: "db", Name: "child"}
	targetTable := &filter.Table{Schema: "db_r", Name: "child_r"}
	originTI, err := syncer.schemaTracker.GetTableInfo(sourceTable)
	require.NoError(t, err)

	expectDownstreamSQLModeInit(mock)
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(targetTable)).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("child_r", "create table child_r(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references parent_r(id))"),
	)
	expectFetchAllDoTables(mock, "db", "parent", "child")
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(&filter.Table{Schema: "db_r", Name: "parent_r"})).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("parent_r", "create table parent_r(id int primary key)"),
	)

	dti, err := syncer.prepareDownStreamTableInfo(tcontext.Background(), sourceTable, targetTable, originTI)
	require.NoError(t, err)
	require.Len(t, dti.ForeignKeyRelations, 1)

	newCfg, err := syncer.cfg.Clone()
	require.NoError(t, err)
	newCfg.RouteRules = []*router.TableRule{
		{SchemaPattern: "db", TablePattern: "parent", TargetSchema: "db_r", TargetTable: "parent_r"},
		{SchemaPattern: "db", TablePattern: "child", TargetSchema: "db_r", TargetTable: "child_r"},
		{SchemaPattern: "db", TablePattern: "child_shadow", TargetSchema: "db_r", TargetTable: "child_r"},
	}
	require.NoError(t, syncer.Update(context.Background(), newCfg))

	expectFetchAllDoTables(mock, "db", "parent", "child", "child_shadow")
	_, err = syncer.prepareDownStreamTableInfo(tcontext.Background(), sourceTable, targetTable, originTI)
	require.ErrorContains(t, err, "shared-target route")
	require.ErrorContains(t, err, "`db_r`.`child_r`")
	require.ErrorContains(t, err, "`db`.`child`")
	require.ErrorContains(t, err, "`db`.`child_shadow`")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPrepareDownStreamTableInfoRejectsSharedTargetRouteForMultiWorkerFKCausality(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 2)
	setForeignKeyRouteTestRoutes(t, syncer, []*router.TableRule{
		{SchemaPattern: "db", TablePattern: "parent", TargetSchema: "db_r", TargetTable: "parent_r"},
		{SchemaPattern: "db", TablePattern: "child", TargetSchema: "db_r", TargetTable: "child_r"},
		{SchemaPattern: "db", TablePattern: "child_shadow", TargetSchema: "db_r", TargetTable: "child_r"},
	})
	p := parser.New()

	prepareSimpleParentChildSchema(t, syncer, p)

	requireSharedTargetRouteRejected(
		t,
		mock,
		syncer,
		&filter.Table{Schema: "db", Name: "child"},
		&filter.Table{Schema: "db_r", Name: "child_r"},
		"create table child_r(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references parent_r(id))",
		[]string{"parent", "child", "child_shadow"},
		"`db_r`.`child_r`",
		"`db`.`child`",
		"`db`.`child_shadow`",
	)
}

func TestPrepareDownStreamTableInfoRejectsCaseInsensitiveSharedTargetRoute(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 2)
	require.False(t, syncer.cfg.CaseSensitive)
	setForeignKeyRouteTestRoutes(t, syncer, []*router.TableRule{
		{SchemaPattern: "db", TablePattern: "parent", TargetSchema: "DB_R", TargetTable: "Parent_R"},
		{SchemaPattern: "db", TablePattern: "parent_shadow", TargetSchema: "db_r", TargetTable: "parent_r"},
		{SchemaPattern: "db", TablePattern: "child", TargetSchema: "db_r", TargetTable: "child_r"},
	})
	p := parser.New()

	prepareSimpleParentChildSchema(t, syncer, p)

	sourceTable := &filter.Table{Schema: "db", Name: "child"}
	targetTable := &filter.Table{Schema: "db_r", Name: "child_r"}
	originTI, err := syncer.schemaTracker.GetTableInfo(sourceTable)
	require.NoError(t, err)

	expectDownstreamSQLModeInit(mock)
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(targetTable)).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("child_r", "create table child_r(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references parent_r(id))"),
	)
	expectFetchAllDoTables(mock, "db", "parent", "parent_shadow", "child")

	_, err = syncer.prepareDownStreamTableInfo(tcontext.Background(), sourceTable, targetTable, originTI)
	require.ErrorContains(t, err, "shared-target route")
	require.ErrorContains(t, err, "`db_r`.`parent_r`")
	require.ErrorContains(t, err, "`db`.`parent`")
	require.ErrorContains(t, err, "`db`.`parent_shadow`")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPrepareDownStreamTableInfoRejectsSharedTargetRouteForCurrentNonFKTable(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 2)
	setForeignKeyRouteTestRoutes(t, syncer, []*router.TableRule{
		{SchemaPattern: "db", TablePattern: "a", TargetSchema: "db_r", TargetTable: "t"},
		{SchemaPattern: "db", TablePattern: "b", TargetSchema: "db_r", TargetTable: "t"},
	})
	p := parser.New()

	execTrackedDDL(t, syncer.schemaTracker, p, "", "create database db")
	execTrackedDDL(t, syncer.schemaTracker, p, "db", "create table a(id int primary key)")
	execTrackedDDL(t, syncer.schemaTracker, p, "db", "create table b(id int primary key)")

	sourceTable := &filter.Table{Schema: "db", Name: "a"}
	targetTable := &filter.Table{Schema: "db_r", Name: "t"}
	originTI, err := syncer.schemaTracker.GetTableInfo(sourceTable)
	require.NoError(t, err)

	expectDownstreamSQLModeInit(mock)
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(targetTable)).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("t", "create table t(id int primary key)"),
	)
	expectFetchAllDoTables(mock, "db", "a", "b")

	_, err = syncer.prepareDownStreamTableInfo(tcontext.Background(), sourceTable, targetTable, originTI)
	require.ErrorContains(t, err, "shared-target route")
	require.ErrorContains(t, err, "`db_r`.`t`")
	require.ErrorContains(t, err, "`db`.`a`")
	require.ErrorContains(t, err, "`db`.`b`")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPrepareDownStreamTableInfoRejectsTaskSharedTargetRouteBeforeUnrelatedDML(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 2)
	setForeignKeyRouteTestRoutes(t, syncer, []*router.TableRule{
		{SchemaPattern: "db", TablePattern: "parent", TargetSchema: "db_r", TargetTable: "parent_r"},
		{SchemaPattern: "db", TablePattern: "child", TargetSchema: "db_r", TargetTable: "child_r"},
		{SchemaPattern: "db", TablePattern: "a", TargetSchema: "db_r", TargetTable: "t"},
		{SchemaPattern: "db", TablePattern: "b", TargetSchema: "db_r", TargetTable: "t"},
	})
	p := parser.New()

	prepareSimpleParentChildSchema(t, syncer, p)
	execTrackedDDL(t, syncer.schemaTracker, p, "db", "create table a(id int primary key)")
	execTrackedDDL(t, syncer.schemaTracker, p, "db", "create table b(id int primary key)")

	sourceTable := &filter.Table{Schema: "db", Name: "child"}
	targetTable := &filter.Table{Schema: "db_r", Name: "child_r"}
	originTI, err := syncer.schemaTracker.GetTableInfo(sourceTable)
	require.NoError(t, err)

	expectDownstreamSQLModeInit(mock)
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(targetTable)).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("child_r", "create table child_r(parent_id int primary key, constraint fk_child_parent foreign key (parent_id) references parent_r(id))"),
	)
	expectFetchAllDoTables(mock, "db", "parent", "child", "a", "b")

	_, err = syncer.prepareDownStreamTableInfo(tcontext.Background(), sourceTable, targetTable, originTI)
	require.ErrorContains(t, err, "shared-target route")
	require.ErrorContains(t, err, "`db_r`.`t`")
	require.ErrorContains(t, err, "`db`.`a`")
	require.ErrorContains(t, err, "`db`.`b`")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPrepareDownStreamTableInfoRejectsSharedTargetParentRouteBeforeChildDML(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 2)
	setForeignKeyRouteTestRoutes(t, syncer, []*router.TableRule{
		{SchemaPattern: "db", TablePattern: "parent", TargetSchema: "db_r", TargetTable: "parent_r"},
		{SchemaPattern: "db", TablePattern: "parent_shadow", TargetSchema: "db_r", TargetTable: "parent_r"},
		{SchemaPattern: "db", TablePattern: "child", TargetSchema: "db_r", TargetTable: "child_r"},
	})
	p := parser.New()

	prepareSimpleParentChildSchema(t, syncer, p)

	requireSharedTargetRouteRejected(
		t,
		mock,
		syncer,
		&filter.Table{Schema: "db", Name: "parent"},
		&filter.Table{Schema: "db_r", Name: "parent_r"},
		"create table parent_r(id int primary key)",
		[]string{"parent", "parent_shadow", "child"},
		"`db_r`.`parent_r`",
		"`db`.`parent`",
		"`db`.`parent_shadow`",
	)
}

func TestPrepareDownStreamTableInfoRejectsSharedTargetAncestorRouteForMultiWorkerFKCausality(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 2)
	setForeignKeyRouteTestRoutes(t, syncer, []*router.TableRule{
		{SchemaPattern: "db", TablePattern: "grandparent", TargetSchema: "db_r", TargetTable: "grandparent_r"},
		{SchemaPattern: "db", TablePattern: "grandparent_shadow", TargetSchema: "db_r", TargetTable: "grandparent_r"},
		{SchemaPattern: "db", TablePattern: "parent", TargetSchema: "db_r", TargetTable: "parent_r"},
		{SchemaPattern: "db", TablePattern: "child", TargetSchema: "db_r", TargetTable: "child_r"},
	})
	p := parser.New()

	execTrackedDDL(t, syncer.schemaTracker, p, "", "create database db")
	execTrackedDDL(t, syncer.schemaTracker, p, "db", "create table grandparent(id int primary key)")
	execTrackedDDL(t, syncer.schemaTracker, p, "db", "create table parent(id int primary key, grandparent_id int, constraint fk_parent_grandparent foreign key (grandparent_id) references grandparent(id))")
	execTrackedDDL(t, syncer.schemaTracker, p, "db", "create table child(id int primary key, parent_id int, constraint fk_child_parent foreign key (parent_id) references parent(id))")

	sourceTable := &filter.Table{Schema: "db", Name: "child"}
	targetTable := &filter.Table{Schema: "db_r", Name: "child_r"}
	originTI, err := syncer.schemaTracker.GetTableInfo(sourceTable)
	require.NoError(t, err)

	expectDownstreamSQLModeInit(mock)
	mock.ExpectQuery("SHOW CREATE TABLE " + utils.GenTableID(targetTable)).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("child_r", "create table child_r(id int primary key, parent_id int, constraint fk_child_parent foreign key (parent_id) references parent_r(id))"),
	)
	expectFetchAllDoTables(mock, "db", "grandparent", "grandparent_shadow", "parent", "child")

	_, err = syncer.prepareDownStreamTableInfo(tcontext.Background(), sourceTable, targetTable, originTI)
	require.ErrorContains(t, err, "shared-target route")
	require.ErrorContains(t, err, "`db_r`.`grandparent_r`")
	require.ErrorContains(t, err, "`db`.`grandparent`")
	require.ErrorContains(t, err, "`db`.`grandparent_shadow`")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPrepareDownStreamTableInfoRejectsFilteredParentWithForeignKeyChecksSingleWorker(t *testing.T) {
	syncer, mock := newForeignKeyRouteTestSyncer(t, 1)
	setForeignKeyRouteTestBAList(t, syncer, &filter.Rules{
		DoTables: []*filter.Table{{Schema: "db", Name: "child"}},
	})
	p := parser.New()

	prepareSimpleParentChildSchema(t, syncer, p)

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

	prepareSimpleParentChildSchema(t, syncer, p)

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
