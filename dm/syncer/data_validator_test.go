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
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/retry"
	"github.com/pingcap/tiflow/dm/pkg/router"
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
	validator := NewContinuousDataValidator(cfg, syncerObj)
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
	validator = NewContinuousDataValidator(cfg, syncerObj)
	validator.Start(pb.Stage_Stopped)
	require.Equal(t, pb.Stage_Stopped, validator.Stage())
	require.Len(t, validator.result.Errors, 1)

	// start with Stopped stage
	_, _, err := conn.InitMockDBFull()
	require.NoError(t, err)
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()
	validator = NewContinuousDataValidator(cfg, syncerObj)
	validator.Start(pb.Stage_Stopped)
	require.Equal(t, pb.Stage_Stopped, validator.Stage())
	require.Len(t, validator.result.Errors, 0)

	// normal start & stop
	validator = NewContinuousDataValidator(cfg, syncerObj)
	validator.Start(pb.Stage_Running)
	defer validator.Stop() // in case assert failed before Stop
	require.Equal(t, pb.Stage_Running, validator.Stage())
	require.True(t, validator.Started())
	validator.Stop()
	require.Equal(t, pb.Stage_Stopped, validator.Stage())

	// stop before start, should not panic
	validator = NewContinuousDataValidator(cfg, syncerObj)
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

	validator := NewContinuousDataValidator(cfg, syncerObj)
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

	validator := NewContinuousDataValidator(cfg, syncerObj)
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
	validator := NewContinuousDataValidator(cfg, syncerObj)
	validator.Start(pb.Stage_Stopped)
	require.NoError(t, validator.waitSyncerSynced(currLoc))

	// cancelled
	currLoc.Position = mysql.Position{
		Name: "mysql-bin.000001",
		Pos:  100,
	}
	validator = NewContinuousDataValidator(cfg, syncerObj)
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
	validator = NewContinuousDataValidator(cfg, syncerObj)
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

	validator := NewContinuousDataValidator(cfg, syncerObj)
	validator.Start(pb.Stage_Stopped)
	validator.cancel()
	require.Error(t, validator.waitSyncerRunning())

	validator = NewContinuousDataValidator(cfg, syncerObj)
	validator.Start(pb.Stage_Stopped)
	syncerObj.running.Store(true)
	require.NoError(t, validator.waitSyncerRunning())

	validator = NewContinuousDataValidator(cfg, syncerObj)
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
	_, _, err := conn.InitMockDBFull()
	require.NoError(t, err)
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()

	syncerObj := NewSyncer(cfg, nil, nil)
	syncerObj.running.Store(true)
	syncerObj.tableRouter, err = router.NewRouter(cfg.CaseSensitive, []*router.TableRule{})
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

	validator := NewContinuousDataValidator(cfg, syncerObj)
	validator.Start(pb.Stage_Stopped)
	validator.streamerController = &StreamerController{
		streamerProducer: mockStreamerProducer,
		streamer:         mockStreamer,
		closed:           false,
	}
	validator.wg.Add(1) // wg.Done is run in doValidate
	validator.doValidate()
	require.Equal(t, int64(1), validator.changeEventCount[rowInsert].Load())
	require.Equal(t, int64(1), validator.changeEventCount[rowUpdated].Load())
	require.Equal(t, int64(1), validator.changeEventCount[rowDeleted].Load())
	ft := filter.Table{Schema: schemaName, Name: tableName2}
	require.Contains(t, validator.unsupportedTable, ft.String())
	ft = filter.Table{Schema: schemaName, Name: tableName3}
	require.Contains(t, validator.unsupportedTable, ft.String())
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
