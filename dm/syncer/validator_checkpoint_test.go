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
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/util/filter"
	regexprrouter "github.com/pingcap/tidb/util/regexpr-router"
	router "github.com/pingcap/tidb/util/table-router"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/retry"
	"github.com/pingcap/tiflow/dm/pkg/schema"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
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
		dbMock.NewRows([]string{"", "", ""}).AddRow("mysql-bin.000001", 100, ""))
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
	dbMock.ExpectQuery("select .* from .*_validator_error_change.*").WillReturnRows(
		dbMock.NewRows([]string{"", ""}).AddRow(pb.ValidateErrorState_NewErr, 2).AddRow(pb.ValidateErrorState_IgnoredErr, 3).
			AddRow(pb.ValidateErrorState_ResolvedErr, 4))

	syncerObj := NewSyncer(cfg, nil, nil)
	syncerObj.running.Store(true)
	syncerObj.tableRouter, err = regexprrouter.NewRegExprRouter(cfg.CaseSensitive, []*router.TableRule{})
	require.NoError(t, err)
	currLoc := binlog.NewLocation(cfg.Flavor)
	currLoc.Position = mysql.Position{
		Name: "mysql-bin.000001",
		Pos:  3000,
	}
	syncerObj.checkpoint = &mockedCheckPointForValidator{
		currLoc: binlog.NewLocation(cfg.Flavor),
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
	syncerObj.downstreamTrackConn = dbconn.NewDBConn(cfg, conn.NewBaseConn(dbConn, &retry.FiniteRetryStrategy{}))
	syncerObj.schemaTracker, err = schema.NewTracker(context.Background(), cfg.Name, defaultTestSessionCfg, syncerObj.downstreamTrackConn)
	defer syncerObj.schemaTracker.Close()
	require.NoError(t, err)
	require.NoError(t, syncerObj.schemaTracker.CreateSchemaIfNotExists(schemaName))
	require.NoError(t, syncerObj.schemaTracker.Exec(context.Background(), schemaName, createTableSQL))

	validator := NewContinuousDataValidator(cfg, syncerObj, false)
	validator.validateInterval = 10 * time.Minute // we don't want worker start validate
	validator.persistHelper.schemaInitialized.Store(true)
	validator.Start(pb.Stage_Stopped)
	validator.Stop()
	require.NoError(t, validator.loadPersistedData())
	require.Equal(t, int64(1), validator.persistHelper.revision)
	require.Equal(t, 1, len(validator.loadedPendingChanges))
	require.Equal(t, 3, len(validator.loadedPendingChanges[tbl.String()].jobs))

	testFunc := func(errStr string) {
		validator.Start(pb.Stage_Stopped)
		defer validator.Stop()
		require.NoError(t, failpoint.Enable("github.com/pingcap/tiflow/dm/syncer/ValidatorCheckPointSkipExecuteSQL", `return("`+errStr+`")`))
		defer func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tiflow/dm/syncer/ValidatorCheckPointSkipExecuteSQL"))
		}()
		validator.startValidateWorkers()
		tblInfo := genValidateTableInfo(t, createTableSQL)
		validator.workers[0].errorRows = append(validator.workers[0].errorRows, &validateFailedRow{
			tp:      deletedRowExists,
			dstData: []*sql.NullString{{String: "1", Valid: true}, {String: "a", Valid: true}},
			srcJob:  genRowChangeJob(tbl, tblInfo, "1", rowDeleted, []interface{}{1, "a"}),
		})
		lastRev := validator.persistHelper.revision
		err2 := validator.persistCheckpointAndData(*validator.location)
		if errStr == "" {
			require.NoError(t, err2)
			require.Equal(t, lastRev+1, validator.persistHelper.revision)
			require.Len(t, validator.workers[0].errorRows, 0)
		} else {
			require.Error(t, err2)
			require.Equal(t, lastRev, validator.persistHelper.revision)
			require.Len(t, validator.workers[0].errorRows, 1)
		}
	}

	testFunc("")
	testFunc("failed")
}
