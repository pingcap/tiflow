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
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/util/filter"
	regexprrouter "github.com/pingcap/tidb/util/regexpr-router"
	router "github.com/pingcap/tidb/util/table-router"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/retry"
	"github.com/pingcap/tiflow/dm/pkg/schema"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	"github.com/stretchr/testify/require"
)

func TestValidatorCheckpointPersist(t *testing.T) {
	var (
		schemaName     = "test"
		tableName      = "tbl"
		tbl            = filter.Table{Schema: schemaName, Name: tableName}
		createTableSQL = "CREATE TABLE `" + tableName + "`(id int primary key, v varchar(100))"
	)
	cfg := genSubtaskConfig(t)
	_, dbMock, err := conn.InitMockDBFull()
	require.NoError(t, err)
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()
	dbMock.ExpectQuery("select .* from .*_validator_checkpoint.*").WillReturnRows(
		dbMock.NewRows([]string{"", "", "", "", "", "", ""}).AddRow("mysql-bin.000001", 100, "", 0, 0, 0, 1))
	dbMock.ExpectQuery("select .* from .*_validator_pending_change.*").WillReturnRows(
		dbMock.NewRows([]string{"", "", "", "", ""}).
			// insert with pk=11
			AddRow(schemaName, tableName, "11",
				"{\"key\": \"11\", \"data\": [\"11\", \"a\"], \"tp\": 0, \"first-ts\": 0, \"failed-cnt\": 0}", 1).
			// delete with pk=12
			AddRow(schemaName, tableName, "12",
				"{\"key\": \"12\", \"data\": [\"12\", \"a\"], \"tp\": 1, \"first-ts\": 0, \"failed-cnt\": 0}", 1).
			// update with pk=13
			AddRow(schemaName, tableName, "13",
				"{\"key\": \"13\", \"data\": [\"13\", \"a\"], \"tp\": 2, \"first-ts\": 0, \"failed-cnt\": 0}", 1),
	)
	dbMock.ExpectQuery("select .* from .*_validator_table_status.*").WillReturnRows(
		dbMock.NewRows([]string{"", "", "", "", "", ""}).AddRow(schemaName, tableName, schemaName, tableName, 2, ""))

	syncerObj := NewSyncer(cfg, nil, nil)
	syncerObj.running.Store(true)
	syncerObj.tableRouter, err = regexprrouter.NewRegExprRouter(cfg.CaseSensitive, []*router.TableRule{})
	require.NoError(t, err)
	currLoc := binlog.MustZeroLocation(cfg.Flavor)
	currLoc.Position = mysql.Position{
		Name: "mysql-bin.000001",
		Pos:  3000,
	}
	syncerObj.checkpoint = &mockedCheckPointForValidator{
		currLoc: binlog.MustZeroLocation(cfg.Flavor),
		nextLoc: currLoc,
		cnt:     2,
	}
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	mock.ExpectBegin()
	mock.ExpectExec("SET SESSION SQL_MODE.*").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	mock.ExpectQuery("SHOW CREATE TABLE.*").WillReturnRows(
		mock.NewRows([]string{"Table", "Create Table"}).AddRow(tableName, createTableSQL))
	dbConn, err := db.Conn(context.Background())
	require.NoError(t, err)
	syncerObj.downstreamTrackConn = dbconn.NewDBConn(cfg, conn.NewBaseConnForTest(dbConn, &retry.FiniteRetryStrategy{}))
	syncerObj.schemaTracker, err = schema.NewTestTracker(context.Background(), cfg.Name, syncerObj.downstreamTrackConn, log.L())
	defer syncerObj.schemaTracker.Close()
	require.NoError(t, err)
	require.NoError(t, syncerObj.schemaTracker.CreateSchemaIfNotExists(schemaName))
	stmt, err := parseSQL(createTableSQL)
	require.NoError(t, err)
	require.NoError(t, syncerObj.schemaTracker.Exec(context.Background(), schemaName, stmt))

	require.Nil(t, failpoint.Enable("github.com/pingcap/tiflow/dm/syncer/ValidatorMockUpstreamTZ", `return()`))
	defer func() {
		require.Nil(t, failpoint.Disable("github.com/pingcap/tiflow/dm/syncer/ValidatorMockUpstreamTZ"))
	}()
	validator := NewContinuousDataValidator(cfg, syncerObj, false)
	validator.validateInterval = 10 * time.Minute // we don't want worker start validate
	validator.persistHelper.schemaInitialized.Store(true)
	require.NoError(t, validator.initialize())
	validator.Stop()
	require.NoError(t, validator.loadPersistedData())
	require.Equal(t, int64(1), validator.persistHelper.revision)
	require.Equal(t, 1, len(validator.loadedPendingChanges))
	require.Equal(t, 3, len(validator.loadedPendingChanges[tbl.String()].jobs))

	require.NoError(t, validator.initialize())
	defer validator.Stop()
	validator.persistHelper.setRevision(100)
	validator.loadedPendingChanges = nil
	validator.startValidateWorkers()
	validator.tableStatus = map[string]*tableValidateStatus{tbl.String(): {
		tbl, tbl, pb.Stage_Running, "",
	}}
	tblInfo := genValidateTableInfo(t, createTableSQL)
	validator.workers[0].errorRows = append(validator.workers[0].errorRows, &validateFailedRow{
		tp:      deletedRowExists,
		dstData: []*sql.NullString{{String: "1", Valid: true}, {String: "a", Valid: true}},
		srcJob:  genRowChangeJob(tbl, tblInfo, "1", rowDeleted, []interface{}{1, "a"}),
	})
	validator.dispatchRowChange("1", genRowChangeJob(tbl, tblInfo, "1", rowInsert, []interface{}{1, "a"}))
	validator.newErrorRowCount.Store(1)

	// fail on first persist
	dbMock.ExpectExec("INSERT INTO .*_validator_table_status.*ON DUPLICATE.*").WillReturnResult(driver.ResultNoRows)
	dbMock.ExpectExec("INSERT INTO .*_validator_error_change.*ON DUPLICATE.*").WillReturnResult(driver.ResultNoRows)
	dbMock.ExpectExec("DELETE FROM .*_validator_pending_change.*WHERE source = \\? and revision = \\?").
		WithArgs("", 101).WillReturnResult(driver.ResultNoRows)
	dbMock.ExpectExec("INSERT INTO .*_validator_pending_change.*VALUES \\(\\?, \\?, \\?, \\?, \\?, \\?\\)").
		WillReturnResult(driver.ResultNoRows)
	dbMock.ExpectExec("INSERT INTO .*_validator_checkpoint.*ON DUPLICATE.*").
		WillReturnError(errors.New("Error 1406 failed on persist checkpoint"))
	require.Nil(t, validator.flushedLoc)
	err2 := validator.persistCheckpointAndData(*validator.location)
	require.EqualError(t, err2, "Error 1406 failed on persist checkpoint")
	require.Equal(t, int64(100), validator.persistHelper.revision)
	require.Len(t, validator.workers[0].errorRows, 1)
	require.Nil(t, validator.flushedLoc)
	require.Equal(t, int64(1), validator.newErrorRowCount.Load())

	// fail on last clean up， but it doesn't matter
	dbMock.ExpectExec("INSERT INTO .*_validator_table_status.*ON DUPLICATE.*").WillReturnResult(driver.ResultNoRows)
	dbMock.ExpectExec("INSERT INTO .*_validator_error_change.*ON DUPLICATE.*").WillReturnResult(driver.ResultNoRows)
	dbMock.ExpectExec("DELETE FROM .*_validator_pending_change.*WHERE source = \\? and revision = \\?").
		WithArgs("", 101).WillReturnResult(driver.ResultNoRows)
	dbMock.ExpectExec("INSERT INTO .*_validator_pending_change.*VALUES \\(\\?, \\?, \\?, \\?, \\?, \\?\\)").
		WillReturnResult(driver.ResultNoRows)
	dbMock.ExpectExec("INSERT INTO .*_validator_checkpoint.*ON DUPLICATE.*").
		WillReturnResult(driver.ResultNoRows)
	dbMock.ExpectExec("DELETE FROM .*_validator_pending_change.*WHERE source = \\? and revision != \\?").
		WillReturnError(errors.New("Error 1406 failed on delete pending change"))
	err2 = validator.persistCheckpointAndData(*validator.location)
	require.NoError(t, err2)
	require.Equal(t, int64(101), validator.persistHelper.revision)
	require.Len(t, validator.workers[0].errorRows, 0)
	require.Equal(t, validator.location.String(), validator.flushedLoc.String())
	require.Equal(t, int64(0), validator.newErrorRowCount.Load())

	// all success
	dbMock.ExpectExec("INSERT INTO .*_validator_table_status.*ON DUPLICATE.*").WillReturnResult(driver.ResultNoRows)
	dbMock.ExpectExec("INSERT INTO .*_validator_error_change.*ON DUPLICATE.*").WillReturnResult(driver.ResultNoRows)
	dbMock.ExpectExec("DELETE FROM .*_validator_pending_change.*WHERE source = \\? and revision = \\?").
		WithArgs("", 102).WillReturnResult(driver.ResultNoRows)
	dbMock.ExpectExec("INSERT INTO .*_validator_pending_change.*VALUES \\(\\?, \\?, \\?, \\?, \\?, \\?\\)").
		WillReturnResult(driver.ResultNoRows)
	dbMock.ExpectExec("INSERT INTO .*_validator_checkpoint.*ON DUPLICATE.*").
		WillReturnResult(driver.ResultNoRows)
	dbMock.ExpectExec("DELETE FROM .*_validator_pending_change.*WHERE source = \\? and revision != \\?").
		WithArgs("", 102).WillReturnResult(driver.ResultNoRows)
	validator.workers[0].errorRows = append(validator.workers[0].errorRows, &validateFailedRow{
		tp:      deletedRowExists,
		dstData: []*sql.NullString{{String: "1", Valid: true}, {String: "a", Valid: true}},
		srcJob:  genRowChangeJob(tbl, tblInfo, "1", rowDeleted, []interface{}{1, "a"}),
	})
	validator.newErrorRowCount.Store(1)
	validator.flushedLoc = nil
	err2 = validator.persistCheckpointAndData(*validator.location)
	require.NoError(t, err2)
	require.Equal(t, int64(102), validator.persistHelper.revision)
	require.Len(t, validator.workers[0].errorRows, 0)
	require.Equal(t, validator.location.String(), validator.flushedLoc.String())
	require.Equal(t, int64(0), validator.newErrorRowCount.Load())
}
