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
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/util/filter"
	regexprrouter "github.com/pingcap/tidb/util/regexpr-router"
	router "github.com/pingcap/tidb/util/table-router"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/retry"
	"github.com/pingcap/tiflow/dm/pkg/schema"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
)

func genEventGenerator(t *testing.T) *event.Generator {
	t.Helper()
	previousGTIDSetStr := "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14"
	previousGTIDSet, err := gtid.ParserGTID(mysql.MySQLFlavor, previousGTIDSetStr)
	require.NoError(t, err)
	latestGTIDStr := "3ccc475b-2343-11e7-be21-6c0b84d59f30:14"
	latestGTID, err := gtid.ParserGTID(mysql.MySQLFlavor, latestGTIDStr)
	require.NoError(t, err)
	eventsGenerator, err := event.NewGenerator(mysql.MySQLFlavor, 1, 0, latestGTID, previousGTIDSet, 0)
	require.NoError(t, err)
	require.NoError(t, err)

	return eventsGenerator
}

func genSubtaskConfig(t *testing.T) *config.SubTaskConfig {
	t.Helper()
	loaderCfg := config.LoaderConfig{
		Dir: t.TempDir(),
	}
	cfg := &config.SubTaskConfig{
		From:             config.GetDBConfigForTest(),
		To:               config.GetDBConfigForTest(),
		Timezone:         "UTC",
		ServerID:         101,
		Name:             "validator_ut",
		ShadowTableRules: []string{config.DefaultShadowTableRules},
		TrashTableRules:  []string{config.DefaultTrashTableRules},
		Mode:             config.ModeIncrement,
		Flavor:           mysql.MySQLFlavor,
		LoaderConfig:     loaderCfg,
		SyncerConfig: config.SyncerConfig{
			EnableGTID: false,
		},
		ValidatorCfg: config.ValidatorConfig{
			Mode:        config.ModeFull,
			WorkerCount: 1,
		},
	}
	cfg.Experimental.AsyncCheckpointFlush = true
	cfg.From.Adjust()
	cfg.To.Adjust()
	require.NoError(t, cfg.ValidatorCfg.Adjust())

	cfg.UseRelay = false

	return cfg
}

func TestValidatorStartStop(t *testing.T) {
	cfg := genSubtaskConfig(t)
	syncerObj := NewSyncer(cfg, nil, nil)

	// validator already running
	validator := NewContinuousDataValidator(cfg, syncerObj, false)
	validator.stage = pb.Stage_Running
	validator.Start(pb.Stage_InvalidStage)
	// if validator already running, Start will return immediately, so we check validator.ctx which has not initialized.
	require.Nil(t, validator.ctx)

	// failed to init
	cfg.From = config.DBConfig{
		Host: "invalid host",
		Port: 3306,
		User: "root",
	}
	validator = NewContinuousDataValidator(cfg, syncerObj, false)
	validator.Start(pb.Stage_Stopped)
	require.Equal(t, pb.Stage_Stopped, validator.Stage())
	require.Len(t, validator.result.Errors, 1)

	// start with Stopped stage
	_, _, err := conn.InitMockDBFull()
	require.NoError(t, err)
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()
	validator = NewContinuousDataValidator(cfg, syncerObj, false)
	validator.persistHelper.schemaInitialized.Store(true)
	validator.Start(pb.Stage_Stopped)
	require.Equal(t, pb.Stage_Stopped, validator.Stage())
	require.Len(t, validator.result.Errors, 0)

	// normal start & stop
	validator = NewContinuousDataValidator(cfg, syncerObj, false)
	validator.persistHelper.schemaInitialized.Store(true)
	validator.Start(pb.Stage_Running)
	defer validator.Stop() // in case assert failed before Stop
	require.Equal(t, pb.Stage_Running, validator.Stage())
	require.True(t, validator.Started())
	validator.Stop()
	require.Equal(t, pb.Stage_Stopped, validator.Stage())

	// stop before start, should not panic
	validator = NewContinuousDataValidator(cfg, syncerObj, false)
	validator.persistHelper.schemaInitialized.Store(true)
	validator.Stop()
}

func TestValidatorFillResult(t *testing.T) {
	cfg := genSubtaskConfig(t)
	syncerObj := NewSyncer(cfg, nil, nil)
	_, _, err := conn.InitMockDBFull()
	require.NoError(t, err)
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()

	validator := NewContinuousDataValidator(cfg, syncerObj, false)
	validator.persistHelper.schemaInitialized.Store(true)
	validator.Start(pb.Stage_Running)
	defer validator.Stop() // in case assert failed before Stop
	validator.fillResult(errors.New("test error"), false)
	require.Len(t, validator.result.Errors, 1)
	validator.fillResult(errors.New("test error"), true)
	require.Len(t, validator.result.Errors, 2)
	validator.Stop()
	validator.fillResult(validator.ctx.Err(), true)
	require.Len(t, validator.result.Errors, 2)
}

func TestValidatorErrorProcessRoutine(t *testing.T) {
	cfg := genSubtaskConfig(t)
	syncerObj := NewSyncer(cfg, nil, nil)
	_, _, err := conn.InitMockDBFull()
	require.NoError(t, err)
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()

	validator := NewContinuousDataValidator(cfg, syncerObj, false)
	validator.persistHelper.schemaInitialized.Store(true)
	validator.Start(pb.Stage_Running)
	defer validator.Stop()
	require.Equal(t, pb.Stage_Running, validator.Stage())
	validator.sendError(errors.New("test error"))
	require.True(t, utils.WaitSomething(20, 100*time.Millisecond, func() bool {
		return validator.Stage() == pb.Stage_Stopped
	}))
	require.Len(t, validator.result.Errors, 1)
}

func TestValidatorDeadLock(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tiflow/dm/syncer/ValidatorMockUpstreamTZ", `return()`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tiflow/dm/syncer/ValidatorMockUpstreamTZ"))
	}()
	cfg := genSubtaskConfig(t)
	syncerObj := NewSyncer(cfg, nil, nil)
	_, _, err := conn.InitMockDBFull()
	require.NoError(t, err)
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()

	validator := NewContinuousDataValidator(cfg, syncerObj, false)
	validator.persistHelper.schemaInitialized.Store(true)
	validator.Start(pb.Stage_Running)
	require.Equal(t, pb.Stage_Running, validator.Stage())
	validator.wg.Add(1)
	go func() {
		defer func() {
			// ignore panic when try to insert error to a closed channel,
			// which will happen after the validator is successfully stopped.
			// The panic is expected.
			validator.wg.Done()
			// nolint:errcheck
			recover()
		}()
		for i := 0; i < 100; i++ {
			validator.sendError(context.Canceled) // prevent from stopping the validator
		}
	}()
	// stuck if the validator doesn't unlock before waiting wg
	validator.Stop()
	require.Equal(t, pb.Stage_Stopped, validator.Stage())
}

type mockedCheckPointForValidator struct {
	CheckPoint
	cnt     int
	currLoc binlog.Location
	nextLoc binlog.Location
}

func (c *mockedCheckPointForValidator) FlushedGlobalPoint() binlog.Location {
	c.cnt++
	if c.cnt <= 2 {
		return c.currLoc
	}
	return c.nextLoc
}

func TestValidatorWaitSyncerSynced(t *testing.T) {
	cfg := genSubtaskConfig(t)
	syncerObj := NewSyncer(cfg, nil, nil)
	_, _, err := conn.InitMockDBFull()
	require.NoError(t, err)
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()

	currLoc := binlog.NewLocation(cfg.Flavor)
	validator := NewContinuousDataValidator(cfg, syncerObj, false)
	validator.persistHelper.schemaInitialized.Store(true)
	validator.Start(pb.Stage_Stopped)
	require.NoError(t, validator.waitSyncerSynced(currLoc))

	// cancelled
	currLoc.Position = mysql.Position{
		Name: "mysql-bin.000001",
		Pos:  100,
	}
	validator = NewContinuousDataValidator(cfg, syncerObj, false)
	validator.persistHelper.schemaInitialized.Store(true)
	validator.Start(pb.Stage_Stopped)
	validator.cancel()
	require.ErrorIs(t, validator.waitSyncerSynced(currLoc), context.Canceled)

	currLoc.Position = mysql.Position{
		Name: "mysql-bin.000001",
		Pos:  100,
	}
	syncerObj.checkpoint = &mockedCheckPointForValidator{
		currLoc: binlog.NewLocation(cfg.Flavor),
		nextLoc: currLoc,
	}
	validator = NewContinuousDataValidator(cfg, syncerObj, false)
	validator.persistHelper.schemaInitialized.Store(true)
	validator.Start(pb.Stage_Stopped)
	require.NoError(t, validator.waitSyncerSynced(currLoc))
}

func TestValidatorWaitSyncerRunning(t *testing.T) {
	cfg := genSubtaskConfig(t)
	syncerObj := NewSyncer(cfg, nil, nil)
	_, _, err := conn.InitMockDBFull()
	require.NoError(t, err)
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()

	validator := NewContinuousDataValidator(cfg, syncerObj, false)
	validator.persistHelper.schemaInitialized.Store(true)
	validator.Start(pb.Stage_Stopped)
	validator.cancel()
	require.Error(t, validator.waitSyncerRunning())

	validator = NewContinuousDataValidator(cfg, syncerObj, false)
	validator.persistHelper.schemaInitialized.Store(true)
	validator.Start(pb.Stage_Stopped)
	syncerObj.running.Store(true)
	require.NoError(t, validator.waitSyncerRunning())

	validator = NewContinuousDataValidator(cfg, syncerObj, false)
	validator.persistHelper.schemaInitialized.Store(true)
	validator.Start(pb.Stage_Stopped)
	syncerObj.running.Store(false)
	go func() {
		time.Sleep(3 * time.Second)
		syncerObj.running.Store(true)
	}()
	require.NoError(t, validator.waitSyncerRunning())
}

func TestValidatorDoValidate(t *testing.T) {
	var (
		schemaName      = "test"
		tableName       = "tbl"
		tableName2      = "tbl2"
		tableName3      = "tbl3"
		tableName4      = "tbl4"
		createTableSQL  = "CREATE TABLE `" + tableName + "`(id int primary key, v varchar(100))"
		createTableSQL2 = "CREATE TABLE `" + tableName2 + "`(id int primary key)"
		createTableSQL3 = "CREATE TABLE `" + tableName3 + "`(id int, v varchar(100))"
		tableNameInfo   = filter.Table{Schema: schemaName, Name: tableName}
		tableNameInfo2  = filter.Table{Schema: schemaName, Name: tableName2}
		tableNameInfo3  = filter.Table{Schema: schemaName, Name: tableName3}
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
		dbMock.NewRows([]string{"", "", "", "", ""}).AddRow(schemaName, tableName, "11",
			// insert with pk=11
			"{\"key\": \"11\", \"data\": [\"11\", \"a\"], \"tp\": 0, \"first-validate-ts\": 0, \"failed-cnt\": 0}", 1))
	dbMock.ExpectQuery("select .* from .*_validator_table_status.*").WillReturnRows(
		dbMock.NewRows([]string{"", "", "", "", "", ""}).AddRow(schemaName, tableName4, schemaName, tableName4, pb.Stage_Stopped, "load from meta"))

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
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(
		mock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""),
	)
	mock.ExpectBegin()
	mock.ExpectExec("SET SESSION SQL_MODE.*").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	mock.ExpectQuery("SHOW CREATE TABLE " + tableNameInfo.String() + ".*").WillReturnRows(
		mock.NewRows([]string{"Table", "Create Table"}).AddRow(tableName, createTableSQL),
	)
	mock.ExpectQuery("SHOW CREATE TABLE " + tableNameInfo2.String() + ".*").WillReturnRows(
		mock.NewRows([]string{"Table", "Create Table"}).AddRow(tableName2, createTableSQL2),
	)
	mock.ExpectQuery("SHOW CREATE TABLE " + tableNameInfo3.String() + ".*").WillReturnRows(
		mock.NewRows([]string{"Table", "Create Table"}).AddRow(tableName3, createTableSQL3),
	)
	dbConn, err := db.Conn(context.Background())
	require.NoError(t, err)
	syncerObj.downstreamTrackConn = dbconn.NewDBConn(cfg, conn.NewBaseConn(dbConn, &retry.FiniteRetryStrategy{}))
	syncerObj.schemaTracker, err = schema.NewTracker(context.Background(), cfg.Name, defaultTestSessionCfg, syncerObj.downstreamTrackConn)
	defer syncerObj.schemaTracker.Close()
	require.NoError(t, err)
	require.NoError(t, syncerObj.schemaTracker.CreateSchemaIfNotExists(schemaName))
	require.NoError(t, syncerObj.schemaTracker.Exec(context.Background(), schemaName, createTableSQL))
	require.NoError(t, syncerObj.schemaTracker.Exec(context.Background(), schemaName, createTableSQL2))
	require.NoError(t, syncerObj.schemaTracker.Exec(context.Background(), schemaName, createTableSQL3))

	generator := genEventGenerator(t)
	rotateEvent, _, err := generator.Rotate("mysql-bin.000001", 0)
	require.NoError(t, err)
	insertData := []*event.DMLData{
		{
			TableID:    11,
			Schema:     schemaName,
			Table:      tableName,
			ColumnType: []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING},
			Rows: [][]interface{}{
				{int32(1), "a"},
				{int32(2), "b"},
				{int32(3), "c"},
			},
		},
		// 2 columns in binlog, but ddl of tbl2 only has one column
		{
			TableID:    12,
			Schema:     schemaName,
			Table:      tableName2,
			ColumnType: []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING},
			Rows: [][]interface{}{
				{int32(1), "a"},
				{int32(2), "b"},
				{int32(3), "c"},
			},
		},
		// tbl3 has no primary key
		{
			TableID:    13,
			Schema:     schemaName,
			Table:      tableName3,
			ColumnType: []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING},
			Rows: [][]interface{}{
				{int32(1), "a"},
				{int32(2), "b"},
				{int32(3), "c"},
			},
		},
		// tbl3 has no primary key, since we met it before, will return immediately
		{
			TableID:    13,
			Schema:     schemaName,
			Table:      tableName3,
			ColumnType: []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING},
			Rows: [][]interface{}{
				{int32(4), "a"},
			},
		},
	}
	updateData := []*event.DMLData{
		{
			TableID:    11,
			Schema:     schemaName,
			Table:      tableName,
			ColumnType: []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING},
			Rows: [][]interface{}{
				// update non-primary column
				{int32(3), "c"},
				{int32(3), "d"},
				// update primary column and non-primary column
				{int32(1), "a"},
				{int32(4), "b"},
			},
		},
	}
	deleteData := []*event.DMLData{
		{
			TableID:    11,
			Schema:     schemaName,
			Table:      tableName,
			ColumnType: []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING},
			Rows: [][]interface{}{
				{int32(3), "c"},
			},
		},
		// no ddl for this table
		{
			TableID:    14,
			Schema:     schemaName,
			Table:      tableName4,
			ColumnType: []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING},
			Rows: [][]interface{}{
				{int32(4), "c"},
			},
		},
	}
	dmlEvents, _, err := generator.GenDMLEvents(replication.WRITE_ROWS_EVENTv2, insertData, 0)
	require.NoError(t, err)
	updateEvents, _, err := generator.GenDMLEvents(replication.UPDATE_ROWS_EVENTv2, updateData, 0)
	require.NoError(t, err)
	deleteEvents, _, err := generator.GenDMLEvents(replication.DELETE_ROWS_EVENTv2, deleteData, 0)
	require.NoError(t, err)
	allEvents := []*replication.BinlogEvent{rotateEvent}
	allEvents = append(allEvents, dmlEvents...)
	allEvents = append(allEvents, updateEvents...)
	allEvents = append(allEvents, deleteEvents...)
	mockStreamerProducer := &MockStreamProducer{events: allEvents}
	mockStreamer, err := mockStreamerProducer.generateStreamer(binlog.NewLocation(""))
	require.NoError(t, err)

	validator := NewContinuousDataValidator(cfg, syncerObj, false)
	validator.validateInterval = 10 * time.Minute // we don't want worker start validate
	validator.persistHelper.schemaInitialized.Store(true)
	validator.Start(pb.Stage_Stopped)
	validator.streamerController = &StreamerController{
		streamerProducer: mockStreamerProducer,
		streamer:         mockStreamer,
		closed:           false,
	}
	validator.wg.Add(1) // wg.Done is run in doValidate
	validator.doValidate()
	validator.Stop()
	// 3 real insert, 1 transformed from an update(updating key)
	require.Equal(t, int64(4), validator.processedRowCounts[rowInsert].Load())
	require.Equal(t, int64(1), validator.processedRowCounts[rowUpdated].Load())
	// 1 real delete, 1 transformed from an update(updating key)
	require.Equal(t, int64(2), validator.processedRowCounts[rowDeleted].Load())

	require.NotNil(t, validator.location)
	require.Equal(t, mysql.Position{Name: "mysql-bin.000001", Pos: 100}, validator.location.Position)
	require.Equal(t, "", validator.location.GTIDSetStr())
	require.Len(t, validator.loadedPendingChanges, 1)
	ft := filter.Table{Schema: schemaName, Name: tableName}
	require.Contains(t, validator.loadedPendingChanges, ft.String())
	require.Len(t, validator.loadedPendingChanges[ft.String()].jobs, 1)
	require.Contains(t, validator.loadedPendingChanges[ft.String()].jobs, "11")
	require.Equal(t, validator.loadedPendingChanges[ft.String()].jobs["11"].Tp, rowInsert)
	require.Len(t, validator.tableStatus, 4)
	require.Contains(t, validator.tableStatus, ft.String())
	require.Equal(t, pb.Stage_Running, validator.tableStatus[ft.String()].stage)
	require.Zero(t, validator.newErrorRowCount.Load())

	ft = filter.Table{Schema: schemaName, Name: tableName2}
	require.Contains(t, validator.tableStatus, ft.String())
	require.Equal(t, pb.Stage_Stopped, validator.tableStatus[ft.String()].stage)
	require.Equal(t, moreColumnInBinlogMsg, validator.tableStatus[ft.String()].message)
	ft = filter.Table{Schema: schemaName, Name: tableName3}
	require.Contains(t, validator.tableStatus, ft.String())
	require.Equal(t, pb.Stage_Stopped, validator.tableStatus[ft.String()].stage)
	require.Equal(t, tableWithoutPrimaryKeyMsg, validator.tableStatus[ft.String()].message)
	// this one is loaded from meta data
	ft = filter.Table{Schema: schemaName, Name: tableName4}
	require.Contains(t, validator.tableStatus, ft.String())
	require.Equal(t, pb.Stage_Stopped, validator.tableStatus[ft.String()].stage)
	require.Equal(t, "load from meta", validator.tableStatus[ft.String()].message)
}

func TestValidatorGetRowChangeType(t *testing.T) {
	require.Equal(t, rowInsert, getRowChangeType(replication.WRITE_ROWS_EVENTv0))
	require.Equal(t, rowInsert, getRowChangeType(replication.WRITE_ROWS_EVENTv1))
	require.Equal(t, rowInsert, getRowChangeType(replication.WRITE_ROWS_EVENTv2))
	require.Equal(t, rowUpdated, getRowChangeType(replication.UPDATE_ROWS_EVENTv0))
	require.Equal(t, rowUpdated, getRowChangeType(replication.UPDATE_ROWS_EVENTv1))
	require.Equal(t, rowUpdated, getRowChangeType(replication.UPDATE_ROWS_EVENTv2))
	require.Equal(t, rowDeleted, getRowChangeType(replication.DELETE_ROWS_EVENTv0))
	require.Equal(t, rowDeleted, getRowChangeType(replication.DELETE_ROWS_EVENTv1))
	require.Equal(t, rowDeleted, getRowChangeType(replication.DELETE_ROWS_EVENTv2))
}

func TestValidatorGenRowKey(t *testing.T) {
	require.Equal(t, "a", genRowKeyByString([]string{"a"}))
	require.Equal(t, "a\tb", genRowKeyByString([]string{"a", "b"}))
	require.Equal(t, "a\tb\tc", genRowKeyByString([]string{"a", "b", "c"}))
	var bytes []byte
	for i := 0; i < 100; i++ {
		bytes = append(bytes, 'a')
	}
	{
		longStr := string(bytes[:maxRowKeyLength])
		require.Equal(t, longStr, genRowKeyByString([]string{longStr}))
	}
	{
		longStr := string(bytes[:maxRowKeyLength+1])
		sum := sha256.Sum256([]byte(longStr))
		sha := hex.EncodeToString(sum[:])
		require.Equal(t, sha, genRowKeyByString([]string{longStr}))
	}
}

func TestValidatorGetValidationStatus(t *testing.T) {
	cfg := genSubtaskConfig(t)
	syncerObj := NewSyncer(cfg, nil, nil)
	validator := NewContinuousDataValidator(cfg, syncerObj, false)
	expected := map[string]*pb.ValidationTableStatus{
		"`db`.`tbl1`": {
			SrcTable: "`db`.`tbl1`",
			DstTable: "`db`.`tbl1`",
			Stage:    pb.Stage_Running,
			Message:  "",
		},
		"`db`.`tbl2`": {
			SrcTable: "`db`.`tbl2`",
			DstTable: "`db`.`tbl2`",
			Stage:    pb.Stage_Stopped,
			Message:  tableWithoutPrimaryKeyMsg,
		},
	}
	validator.tableStatus = map[string]*tableValidateStatus{
		"`db`.`tbl1`": {
			source: filter.Table{Schema: "db", Name: "tbl1"},
			target: filter.Table{Schema: "db", Name: "tbl1"},
			stage:  pb.Stage_Running,
		},
		"`db`.`tbl2`": {
			source:  filter.Table{Schema: "db", Name: "tbl2"},
			target:  filter.Table{Schema: "db", Name: "tbl2"},
			stage:   pb.Stage_Stopped,
			message: tableWithoutPrimaryKeyMsg,
		},
	}
	ret := validator.GetValidatorTableStatus(pb.Stage_InvalidStage)
	require.Equal(t, len(expected), len(ret))
	for _, result := range ret {
		ent, ok := expected[result.SrcTable]
		require.Equal(t, ok, true)
		require.EqualValues(t, ent, result)
	}
	ret = validator.GetValidatorTableStatus(pb.Stage_Running)
	require.Equal(t, 1, len(ret))
	for _, result := range ret {
		ent, ok := expected[result.SrcTable]
		require.Equal(t, ok, true)
		require.EqualValues(t, ent, result)
	}
	ret = validator.GetValidatorTableStatus(pb.Stage_Stopped)
	require.Equal(t, 1, len(ret))
	for _, result := range ret {
		ent, ok := expected[result.SrcTable]
		require.Equal(t, ok, true)
		require.EqualValues(t, ent, result)
	}
}

func TestValidatorGetValidationError(t *testing.T) {
	require.Nil(t, failpoint.Enable("github.com/pingcap/tiflow/dm/syncer/MockValidationQuery", `return(true)`))
	defer func() {
		require.Nil(t, failpoint.Disable("github.com/pingcap/tiflow/dm/syncer/MockValidationQuery"))
	}()
	db, dbMock, err := sqlmock.New()
	require.Equal(t, log.InitLogger(&log.Config{}), nil)
	require.NoError(t, err)
	cfg := genSubtaskConfig(t)
	syncerObj := NewSyncer(cfg, nil, nil)
	validator := NewContinuousDataValidator(cfg, syncerObj, false)
	validator.ctx, validator.cancel = context.WithCancel(context.Background())
	validator.tctx = tcontext.NewContext(validator.ctx, validator.L)
	// all error
	dbMock.ExpectQuery("SELECT .* FROM " + validator.persistHelper.errorChangeTableName + " WHERE source=?").WithArgs(validator.cfg.SourceID).WillReturnRows(
		sqlmock.NewRows([]string{"id", "source", "src_schema_name", "src_table_name", "dst_schema_name", "dst_table_name", "data", "dst_data", "error_type", "status", "update_time"}).AddRow(
			1, "mysql-replica", "srcdb", "srctbl", "dstdb", "dsttbl", "source data", "unexpected data", 2, 1, "2022-03-01",
		),
	)
	// filter by status
	dbMock.ExpectQuery("SELECT .* FROM "+validator.persistHelper.errorChangeTableName+" WHERE source = \\? AND status=\\?").
		WithArgs(validator.cfg.SourceID, int(pb.ValidateErrorState_IgnoredErr)).
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "source", "src_schema_name", "src_table_name", "dst_schema_name", "dst_table_name", "data", "dst_data", "error_type", "status", "update_time"}).AddRow(
				2, "mysql-replica", "srcdb", "srctbl", "dstdb", "dsttbl", "source data1", "unexpected data1", 2, 2, "2022-03-01",
			).AddRow(
				3, "mysql-replica", "srcdb", "srctbl", "dstdb", "dsttbl", "source data2", "unexpected data2", 2, 2, "2022-03-01",
			),
		)
	expected := [][]*pb.ValidationError{
		{
			{
				Id:        "1",
				Source:    "mysql-replica",
				SrcTable:  "`srcdb`.`srctbl`",
				DstTable:  "`dstdb`.`dsttbl`",
				SrcData:   "source data",
				DstData:   "unexpected data",
				ErrorType: "Column data not matched",
				Status:    pb.ValidateErrorState_NewErr,
				Time:      "2022-03-01",
			},
		},
		{
			{
				Id:        "2",
				Source:    "mysql-replica",
				SrcTable:  "`srcdb`.`srctbl`",
				DstTable:  "`dstdb`.`dsttbl`",
				SrcData:   "source data1",
				DstData:   "unexpected data1",
				ErrorType: "Column data not matched",
				Status:    pb.ValidateErrorState_IgnoredErr,
				Time:      "2022-03-01",
			},
			{
				Id:        "3",
				Source:    "mysql-replica",
				SrcTable:  "`srcdb`.`srctbl`",
				DstTable:  "`dstdb`.`dsttbl`",
				SrcData:   "source data2",
				DstData:   "unexpected data2",
				ErrorType: "Column data not matched",
				Status:    pb.ValidateErrorState_IgnoredErr,
				Time:      "2022-03-01",
			},
		},
	}
	validator.persistHelper.db = conn.NewBaseDB(db, func() {})
	res, err := validator.GetValidatorError(pb.ValidateErrorState_InvalidErr)
	require.Nil(t, err)
	require.EqualValues(t, expected[0], res)
	res, err = validator.GetValidatorError(pb.ValidateErrorState_IgnoredErr)
	require.Nil(t, err)
	require.EqualValues(t, expected[1], res)
}

func TestValidatorOperateValidationError(t *testing.T) {
	require.Nil(t, failpoint.Enable("github.com/pingcap/tiflow/dm/syncer/MockValidationQuery", `return(true)`))
	defer func() {
		require.Nil(t, failpoint.Disable("github.com/pingcap/tiflow/dm/syncer/MockValidationQuery"))
	}()
	var err error
	db, dbMock, err := sqlmock.New()
	require.Equal(t, log.InitLogger(&log.Config{}), nil)
	require.NoError(t, err)
	cfg := genSubtaskConfig(t)
	syncerObj := NewSyncer(cfg, nil, nil)
	validator := NewContinuousDataValidator(cfg, syncerObj, false)
	validator.ctx, validator.cancel = context.WithCancel(context.Background())
	validator.tctx = tcontext.NewContext(validator.ctx, validator.L)
	validator.persistHelper.db = conn.NewBaseDB(db, func() {})
	sourceID := validator.cfg.SourceID
	// 1. clear all error
	dbMock.ExpectExec("DELETE FROM " + validator.persistHelper.errorChangeTableName + " WHERE source=\\?").
		WithArgs(sourceID).WillReturnResult(sqlmock.NewResult(0, 1))
	// 2. clear error of errID
	dbMock.ExpectExec("DELETE FROM "+validator.persistHelper.errorChangeTableName+" WHERE source=\\? AND id=\\?").
		WithArgs(sourceID, 1).WillReturnResult(sqlmock.NewResult(0, 1))
	// 3. mark all error as resolved
	dbMock.ExpectExec("UPDATE "+validator.persistHelper.errorChangeTableName+" SET status=\\? WHERE source=\\?").
		WithArgs(int(pb.ValidateErrorState_ResolvedErr), sourceID).WillReturnResult(sqlmock.NewResult(0, 1))
	// 4. mark all error as ignored
	dbMock.ExpectExec("UPDATE "+validator.persistHelper.errorChangeTableName+" SET status=\\? WHERE source=\\?").
		WithArgs(int(pb.ValidateErrorState_IgnoredErr), sourceID).WillReturnResult(sqlmock.NewResult(0, 1))
	// 5. mark error as resolved of errID
	dbMock.ExpectExec("UPDATE "+validator.persistHelper.errorChangeTableName+" SET status=\\? WHERE source=\\? AND id=\\?").
		WithArgs(int(pb.ValidateErrorState_ResolvedErr), sourceID, 1).WillReturnResult(sqlmock.NewResult(0, 1))
	// 6. mark error as ignored of errID
	dbMock.ExpectExec("UPDATE "+validator.persistHelper.errorChangeTableName+" SET status=\\? WHERE source=\\? AND id=\\?").
		WithArgs(int(pb.ValidateErrorState_IgnoredErr), sourceID, 1).WillReturnResult(sqlmock.NewResult(0, 1))

	// clear all error
	err = validator.OperateValidatorError(pb.ValidationErrOp_ClearErrOp, 0, true)
	require.NoError(t, err)
	// clear error with id
	err = validator.OperateValidatorError(pb.ValidationErrOp_ClearErrOp, 1, false)
	require.NoError(t, err)
	// mark all as resolved
	err = validator.OperateValidatorError(pb.ValidationErrOp_ResolveErrOp, 0, true)
	require.NoError(t, err)
	// mark all as ignored
	err = validator.OperateValidatorError(pb.ValidationErrOp_IgnoreErrOp, 0, true)
	require.NoError(t, err)
	// mark error as resolved with id
	err = validator.OperateValidatorError(pb.ValidationErrOp_ResolveErrOp, 1, false)
	require.NoError(t, err)
	// mark error as ignored with id
	err = validator.OperateValidatorError(pb.ValidationErrOp_IgnoreErrOp, 1, false)
	require.NoError(t, err)
}
