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
	"database/sql"
	"encoding/hex"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/filter"
	regexprrouter "github.com/pingcap/tidb/util/regexpr-router"
	router "github.com/pingcap/tidb/util/table-router"
	"github.com/shopspring/decimal"
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

func genDBConn(t *testing.T, db *sql.DB, cfg *config.SubTaskConfig) *dbconn.DBConn {
	t.Helper()
	baseDB := conn.NewBaseDB(db, func() {})
	baseConn, err := baseDB.GetBaseConn(context.Background())
	require.NoError(t, err)
	return &dbconn.DBConn{
		BaseConn: baseConn,
		Cfg:      cfg,
	}
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
	validator.errChan <- errors.New("test error")
	require.True(t, utils.WaitSomething(20, 100*time.Millisecond, func() bool {
		return validator.Stage() == pb.Stage_Stopped
	}))
	require.Len(t, validator.result.Errors, 1)
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
	dbMock.ExpectQuery("select .* from .*_validator_error_change.*").WillReturnRows(
		dbMock.NewRows([]string{"", ""}).AddRow(newValidateErrorRow, 2).AddRow(ignoredValidateErrorRow, 3).
			AddRow(resolvedValidateErrorRow, 4))

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
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(
		mock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""),
	)
	dbConn, err := db.Conn(context.Background())
	require.NoError(t, err)
	syncerObj.downstreamTrackConn = &dbconn.DBConn{Cfg: cfg, BaseConn: conn.NewBaseConn(dbConn, &retry.FiniteRetryStrategy{})}
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
	require.Len(t, validator.loadedPendingChanges[ft.String()].rows, 1)
	require.Contains(t, validator.loadedPendingChanges[ft.String()].rows, "11")
	require.Equal(t, validator.loadedPendingChanges[ft.String()].rows["11"].Tp, rowInsert)
	require.Len(t, validator.tableStatus, 4)
	require.Contains(t, validator.tableStatus, ft.String())
	require.Equal(t, pb.Stage_Running, validator.tableStatus[ft.String()].stage)
	require.Equal(t, int64(2), validator.errorRowCounts[newValidateErrorRow].Load())
	require.Equal(t, int64(3), validator.errorRowCounts[ignoredValidateErrorRow].Load())
	require.Equal(t, int64(4), validator.errorRowCounts[resolvedValidateErrorRow].Load())

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
	require.Equal(t, "a", genRowKey([]string{"a"}))
	require.Equal(t, "a\tb", genRowKey([]string{"a", "b"}))
	require.Equal(t, "a\tb\tc", genRowKey([]string{"a", "b", "c"}))
	var bytes []byte
	for i := 0; i < 100; i++ {
		bytes = append(bytes, 'a')
	}
	{
		longStr := string(bytes[:maxRowKeyLength])
		require.Equal(t, longStr, genRowKey([]string{longStr}))
	}
	{
		longStr := string(bytes[:maxRowKeyLength+1])
		sum := sha256.Sum256([]byte(longStr))
		sha := hex.EncodeToString(sum[:])
		require.Equal(t, sha, genRowKey([]string{longStr}))
	}
}

func TestValidatorGenColData(t *testing.T) {
	res := genColData(1)
	require.Equal(t, "1", res)
	res = genColData(1.2)
	require.Equal(t, "1.2", res)
	res = genColData("abc")
	require.Equal(t, "abc", res)
	res = genColData([]byte{'\x01', '\x02', '\x03'})
	require.Equal(t, "\x01\x02\x03", res)
	res = genColData(decimal.NewFromInt(222123123))
	require.Equal(t, "222123123", res)
}

func TestGetValidationStatus(t *testing.T) {
	var err error
	createTableSQL1 := "CREATE TABLE `db`.`tbl1` (id int primary key, v varchar(100))"
	createTableSQL2 := "CREATE TABLE `db`.`tbl2` (id int primary key, v varchar(100))"
	cfg := genSubtaskConfig(t)
	generator := genEventGenerator(t)
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()
	require.Equal(t, log.InitLogger(&log.Config{}), nil)
	syncerObj := NewSyncer(cfg, nil, nil)
	syncerObj.schemaLoaded.Store(true)
	syncerObj.schemaTracker, err = schema.NewTracker(context.Background(), cfg.Name, defaultTestSessionCfg, syncerObj.downstreamTrackConn)
	require.NoError(t, err)
	defer syncerObj.schemaTracker.Close()
	syncerObj.tableRouter, err = regexprrouter.NewRegExprRouter(cfg.CaseSensitive, []*router.TableRule{})
	require.NoError(t, err)
	validator := NewContinuousDataValidator(cfg, syncerObj, false)
	validator.ctx, validator.cancel = context.WithCancel(context.Background())
	validator.tctx = tcontext.NewContext(validator.ctx, validator.L)
	validator.workerCnt = 1
	validator.workers = []*validateWorker{{rowChangeCh: make(chan *rowChange, workerChannelSize)}}
	defer close(validator.workers[0].rowChangeCh)
	require.NoError(t, syncerObj.schemaTracker.CreateSchemaIfNotExists("db"))
	require.NoError(t, syncerObj.schemaTracker.Exec(context.Background(), "db", createTableSQL1))
	require.NoError(t, syncerObj.schemaTracker.Exec(context.Background(), "db", createTableSQL2))
	dmlData := []*event.DMLData{
		{
			TableID:    11,
			Schema:     "db",
			Table:      "tbl1",
			ColumnType: []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING},
			Rows: [][]interface{}{
				{int32(3), "c"},
				{int32(3), "d"},
			},
		},
		{
			TableID:    12,
			Schema:     "db",
			Table:      "tbl2",
			ColumnType: []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING},
			Rows: [][]interface{}{
				{int32(3), "c"},
				{int32(3), "d"},
			},
		},
	}
	dmlEvents, _, err := generator.GenDMLEvents(replication.WRITE_ROWS_EVENTv2, dmlData, 0)
	require.NoError(t, err)
	for _, ev := range dmlEvents {
		if _, ok := ev.Event.(*replication.RowsEvent); ok {
			err = validator.processRowsEvent(ev.Header, ev.Event.(*replication.RowsEvent))
			require.NoError(t, err)
		}
	}
	expected := map[string]*pb.ValidationStatus{
		"`db`.`tbl1`": {
			SrcTable:         "`db`.`tbl1`",
			DstTable:         "`db`.`tbl1`",
			ValidationStatus: pb.Stage_Running.String(),
			Message:          "",
		},
		"`db`.`tbl2`": {
			SrcTable:         "`db`.`tbl2`",
			DstTable:         "`db`.`tbl2`",
			ValidationStatus: pb.Stage_Running.String(),
			Message:          "",
		},
	}
	ret := validator.GetValidationStatus()
	require.Equal(t, len(expected), len(ret))
	for _, result := range ret {
		ent, ok := expected[result.SrcTable]
		require.Equal(t, ok, true)
		require.EqualValues(t, ent, result)
	}
}
