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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/log"
	tidbddl "github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/cputil"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	dlog "github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/retry"
	"github.com/pingcap/tiflow/dm/pkg/schema"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap/zapcore"
)

var (
	cpid                 = "test_for_db"
	schemaCreateSQL      = ""
	tableCreateSQL       = ""
	clearCheckPointSQL   = ""
	loadCheckPointSQL    = ""
	flushCheckPointSQL   = ""
	deleteCheckPointSQL  = ""
	deleteSchemaPointSQL = ""
)

type testCheckpointSuite struct {
	suite.Suite
	cfg     *config.SubTaskConfig
	mock    sqlmock.Sqlmock
	tracker *schema.Tracker
}

func (s *testCheckpointSuite) SetupSuite() {
	s.cfg = &config.SubTaskConfig{
		ServerID:   101,
		MetaSchema: "test",
		Name:       "syncer_checkpoint_ut",
		Flavor:     mysql.MySQLFlavor,
	}

	log.SetLevel(zapcore.ErrorLevel)
	var err error

	s.tracker, err = schema.NewTestTracker(context.Background(), s.cfg.Name, nil, dlog.L())
	s.Require().NoError(err)
}

func (s *testCheckpointSuite) TestUpTest() {
	s.tracker.Reset()
}

func (s *testCheckpointSuite) prepareCheckPointSQL() {
	schemaCreateSQL = fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS `%s`", s.cfg.MetaSchema)
	tableCreateSQL = fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` .*", s.cfg.MetaSchema, cputil.SyncerCheckpoint(s.cfg.Name))
	flushCheckPointSQL = fmt.Sprintf("INSERT INTO `%s`.`%s` .* VALUES.* ON DUPLICATE KEY UPDATE .*", s.cfg.MetaSchema, cputil.SyncerCheckpoint(s.cfg.Name))
	clearCheckPointSQL = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE id = \\?", s.cfg.MetaSchema, cputil.SyncerCheckpoint(s.cfg.Name))
	loadCheckPointSQL = fmt.Sprintf("SELECT .* FROM `%s`.`%s` WHERE id = \\?", s.cfg.MetaSchema, cputil.SyncerCheckpoint(s.cfg.Name))
	deleteCheckPointSQL = fmt.Sprintf("DELETE FROM %s WHERE id = \\? AND cp_schema = \\? AND cp_table = \\?", dbutil.TableName(s.cfg.MetaSchema, cputil.SyncerCheckpoint(s.cfg.Name)))
	deleteSchemaPointSQL = fmt.Sprintf("DELETE FROM %s WHERE id = \\? AND cp_schema = \\?", dbutil.TableName(s.cfg.MetaSchema, cputil.SyncerCheckpoint(s.cfg.Name)))
}

// this test case uses sqlmock to simulate all SQL operations in tests.
func (s *testCheckpointSuite) TestCheckPoint() {
	tctx := tcontext.Background()

	cp := NewRemoteCheckPoint(tctx, s.cfg, nil, cpid)
	defer func() {
		s.mock.ExpectClose()
		cp.Close()
	}()

	var err error
	db, mock, err := sqlmock.New()
	s.Require().NoError(err)
	s.mock = mock

	s.prepareCheckPointSQL()

	mock.ExpectBegin()
	mock.ExpectExec(schemaCreateSQL).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec(tableCreateSQL).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec(clearCheckPointSQL).WithArgs(cpid).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	dbConn, err := db.Conn(tcontext.Background().Context())
	s.Require().NoError(err)
	conn := dbconn.NewDBConn(s.cfg, conn.NewBaseConnForTest(dbConn, &retry.FiniteRetryStrategy{}))
	cp.(*RemoteCheckPoint).dbConn = conn
	err = cp.(*RemoteCheckPoint).prepare(tctx)
	s.Require().NoError(err)
	s.Require().Nil(cp.Clear(tctx))

	// test operation for global checkpoint
	s.testGlobalCheckPoint(cp)

	// test operation for table checkpoint
	s.testTableCheckPoint(cp)
}

func (s *testCheckpointSuite) testGlobalCheckPoint(cp CheckPoint) {
	tctx := tcontext.Background()

	// global checkpoint init to min
	s.Require().Equal(binlog.MinPosition, cp.GlobalPoint().Position)
	s.Require().Equal(binlog.MinPosition, cp.FlushedGlobalPoint().Position)

	// try load, but should load nothing
	s.mock.ExpectQuery(loadCheckPointSQL).WillReturnRows(sqlmock.NewRows(nil))
	err := cp.Load(tctx)
	s.Require().NoError(err)
	s.Require().Equal(binlog.MinPosition, cp.GlobalPoint().Position)
	s.Require().Equal(binlog.MinPosition, cp.FlushedGlobalPoint().Position)

	oldMode := s.cfg.Mode
	oldDir := s.cfg.Dir
	defer func() {
		s.cfg.Mode = oldMode
		s.cfg.Dir = oldDir
	}()

	pos1 := mysql.Position{
		Name: "mysql-bin.000003",
		Pos:  1943,
	}

	s.mock.ExpectQuery(loadCheckPointSQL).WithArgs(cpid).WillReturnRows(sqlmock.NewRows(nil))
	err = cp.Load(tctx)
	s.Require().NoError(err)
	cp.SaveGlobalPoint(binlog.Location{Position: pos1})

	s.mock.ExpectBegin()
	s.mock.ExpectExec("(162)?"+flushCheckPointSQL).WithArgs(cpid, "", "", pos1.Name, pos1.Pos, "", "", 0, "", "null", true).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	// Create a new snapshot, and discard it, then create a new snapshot again.
	cp.Snapshot(true)
	cp.DiscardPendingSnapshots()
	snap := cp.Snapshot(true)
	err = cp.FlushPointsExcept(tctx, snap.id, nil, nil, nil)
	s.Require().NoError(err)
	s.Require().Equal(pos1, cp.GlobalPoint().Position)
	s.Require().Equal(pos1, cp.FlushedGlobalPoint().Position)

	// try load from config
	pos1.Pos = 2044
	s.cfg.Mode = config.ModeIncrement
	s.cfg.Meta = &config.Meta{BinLogName: pos1.Name, BinLogPos: pos1.Pos}
	err = cp.LoadMeta(tctx.Ctx)
	s.Require().NoError(err)
	s.Require().Equal(pos1, cp.GlobalPoint().Position)
	s.Require().Equal(pos1, cp.FlushedGlobalPoint().Position)

	s.cfg.Mode = oldMode
	s.cfg.Meta = nil

	// test save global point
	pos2 := mysql.Position{
		Name: "mysql-bin.000005",
		Pos:  2052,
	}
	cp.SaveGlobalPoint(binlog.Location{Position: pos2})
	s.Require().Equal(pos2, cp.GlobalPoint().Position)
	s.Require().Equal(pos1, cp.FlushedGlobalPoint().Position)

	// test rollback
	cp.Rollback()
	s.Require().Equal(pos1, cp.GlobalPoint().Position)
	s.Require().Equal(pos1, cp.FlushedGlobalPoint().Position)

	// save again
	cp.SaveGlobalPoint(binlog.Location{Position: pos2})
	s.Require().Equal(pos2, cp.GlobalPoint().Position)
	s.Require().Equal(pos1, cp.FlushedGlobalPoint().Position)

	// flush + rollback
	s.mock.ExpectBegin()
	s.mock.ExpectExec("(202)?"+flushCheckPointSQL).WithArgs(cpid, "", "", pos2.Name, pos2.Pos, "", "", 0, "", "null", true).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	err = cp.FlushPointsExcept(tctx, cp.Snapshot(true).id, nil, nil, nil)
	s.Require().NoError(err)
	cp.Rollback()
	s.Require().Equal(pos2, cp.GlobalPoint().Position)
	s.Require().Equal(pos2, cp.FlushedGlobalPoint().Position)

	// try load from DB
	pos3 := pos2
	pos3.Pos = pos2.Pos + 1000 // > pos2 to enable save
	cp.SaveGlobalPoint(binlog.Location{Position: pos3})
	columns := []string{"cp_schema", "cp_table", "binlog_name", "binlog_pos", "binlog_gtid", "exit_safe_binlog_name", "exit_safe_binlog_pos", "exit_safe_binlog_gtid", "table_info", "is_global"}
	s.mock.ExpectQuery(loadCheckPointSQL).WithArgs(cpid).WillReturnRows(sqlmock.NewRows(columns).AddRow("", "", pos2.Name, pos2.Pos, "", "", 0, "", "null", true))
	err = cp.Load(tctx)
	s.Require().NoError(err)
	s.Require().Equal(pos2, cp.GlobalPoint().Position)
	s.Require().Equal(pos2, cp.FlushedGlobalPoint().Position)

	// test save older point
	/*var buf bytes.Buffer
	log.SetOutput(&buf)
	cp.SaveGlobalPoint(pos1)
	s.Require().Equal(pos2, cp.GlobalPoint())
	s.Require().Equal(pos2, cp.FlushedGlobalPoint())
	matchStr := fmt.Sprintf(".*try to save %s is older than current pos %s", pos1, pos2)
	matchStr = strings.Replace(strings.Replace(matchStr, ")", "\\)", -1), "(", "\\(", -1)
	c.Assert(strings.TrimSpace(buf.String()), Matches, matchStr)
	log.SetOutput(os.Stdout)*/

	// test clear
	s.mock.ExpectBegin()
	s.mock.ExpectExec(clearCheckPointSQL).WithArgs(cpid).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	err = cp.Clear(tctx)
	s.Require().NoError(err)
	s.Require().Equal(binlog.MinPosition, cp.GlobalPoint().Position)
	s.Require().Equal(binlog.MinPosition, cp.FlushedGlobalPoint().Position)

	s.mock.ExpectQuery(loadCheckPointSQL).WillReturnRows(sqlmock.NewRows(nil))
	err = cp.Load(tctx)
	s.Require().NoError(err)
	s.Require().Equal(binlog.MinPosition, cp.GlobalPoint().Position)
	s.Require().Equal(binlog.MinPosition, cp.FlushedGlobalPoint().Position)

	// try load from mydumper's output
	dir := s.T().TempDir()

	filename := filepath.Join(dir, "metadata")
	err = os.WriteFile(filename, []byte(
		fmt.Sprintf("SHOW MASTER STATUS:\n\tLog: %s\n\tPos: %d\n\tGTID:\n\nSHOW SLAVE STATUS:\n\tHost: %s\n\tLog: %s\n\tPos: %d\n\tGTID:\n\n", pos1.Name, pos1.Pos, "slave_host", pos1.Name, pos1.Pos+1000)),
		0o644)
	s.Require().NoError(err)
	s.cfg.Mode = config.ModeAll
	s.cfg.Dir = dir
	s.Require().Nil(cp.LoadMeta(tctx.Ctx))

	// should flush because checkpoint hasn't been updated before (cp.globalPointCheckOrSaveTime.IsZero() == true).
	snapshot := cp.Snapshot(true)
	s.Require().Equal(4, snapshot.id)

	s.mock.ExpectQuery(loadCheckPointSQL).WillReturnRows(sqlmock.NewRows(nil))
	err = cp.Load(tctx)
	s.Require().NoError(err)
	s.Require().Equal(pos1, cp.GlobalPoint().Position)
	s.Require().Equal(pos1, cp.FlushedGlobalPoint().Position)

	s.mock.ExpectBegin()
	s.mock.ExpectExec(clearCheckPointSQL).WithArgs(cpid).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	err = cp.Clear(tctx)
	s.Require().NoError(err)

	// check dumpling write exitSafeModeLocation in metadata
	err = os.WriteFile(filename, []byte(
		fmt.Sprintf(`SHOW MASTER STATUS:
	Log: %s
	Pos: %d
	GTID:

SHOW SLAVE STATUS:
	Host: %s
	Log: %s
	Pos: %d
	GTID:

SHOW MASTER STATUS: /* AFTER CONNECTION POOL ESTABLISHED */
	Log: %s
	Pos: %d
	GTID:
`, pos1.Name, pos1.Pos, "slave_host", pos1.Name, pos1.Pos+1000, pos2.Name, pos2.Pos)), 0o644)
	s.Require().NoError(err)
	s.Require().Nil(cp.LoadMeta(tctx.Ctx))

	// should flush because exitSafeModeLocation is true
	snapshot = cp.Snapshot(true)
	s.Require().NotNil(snapshot)
	s.mock.ExpectBegin()
	s.mock.ExpectExec("(202)?"+flushCheckPointSQL).WithArgs(cpid, "", "", pos1.Name, pos1.Pos, "", pos2.Name, pos2.Pos, "", "null", true).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	err = cp.FlushPointsExcept(tctx, snapshot.id, nil, nil, nil)
	s.Require().NoError(err)
	s.mock.ExpectQuery(loadCheckPointSQL).WillReturnRows(sqlmock.NewRows(nil))
	err = cp.Load(tctx)
	s.Require().NoError(err)
	s.Require().Equal(pos1, cp.GlobalPoint().Position)
	s.Require().Equal(pos1, cp.FlushedGlobalPoint().Position)
	s.Require().Equal(pos2, cp.SafeModeExitPoint().Position)

	// when use async flush, even exitSafeModeLocation is true we won't flush
	s.Require().Nil(cp.LoadMeta(tctx.Ctx))
	snapshot = cp.Snapshot(false)
	s.Require().Nil(snapshot)
}

func (s *testCheckpointSuite) testTableCheckPoint(cp CheckPoint) {
	var (
		tctx  = tcontext.Background()
		table = &filter.Table{
			Schema: "test_db",
			Name:   "test_table",
		}
		schemaName = "test_db"
		tableName  = "test_table"
		pos1       = mysql.Position{
			Name: "mysql-bin.000008",
			Pos:  123,
		}
		pos2 = mysql.Position{
			Name: "mysql-bin.000008",
			Pos:  456,
		}
		err error
	)

	// not exist
	older := cp.IsOlderThanTablePoint(table, binlog.Location{Position: pos1})
	s.Require().False(older)

	// save
	cp.SaveTablePoint(table, binlog.Location{Position: pos2}, nil)
	older = cp.IsOlderThanTablePoint(table, binlog.Location{Position: pos1})
	s.Require().True(older)

	// rollback, to min
	cp.Rollback()
	older = cp.IsOlderThanTablePoint(table, binlog.Location{Position: pos1})
	s.Require().False(older)

	// save again
	cp.SaveTablePoint(table, binlog.Location{Position: pos2}, nil)
	older = cp.IsOlderThanTablePoint(table, binlog.Location{Position: pos1})
	s.Require().True(older)

	// flush + rollback
	s.mock.ExpectBegin()
	s.mock.ExpectExec("(284)?"+flushCheckPointSQL).WithArgs(cpid, table.Schema, table.Name, pos2.Name, pos2.Pos, "", "", 0, "", sqlmock.AnyArg(), false).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	err = cp.FlushPointsExcept(tctx, cp.Snapshot(true).id, nil, nil, nil)
	s.Require().NoError(err)
	cp.Rollback()
	older = cp.IsOlderThanTablePoint(table, binlog.Location{Position: pos1})
	s.Require().True(older)

	// save
	cp.SaveTablePoint(table, binlog.Location{Position: pos2}, nil)
	older = cp.IsOlderThanTablePoint(table, binlog.Location{Position: pos1})
	s.Require().True(older)

	// delete
	s.mock.ExpectBegin()
	s.mock.ExpectExec(deleteCheckPointSQL).WithArgs(cpid, schemaName, tableName).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	s.Require().Nil(cp.DeleteTablePoint(tctx, table))
	s.mock.ExpectBegin()
	s.mock.ExpectExec(deleteSchemaPointSQL).WithArgs(cpid, schemaName).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	s.Require().Nil(cp.DeleteSchemaPoint(tctx, schemaName))

	ctx := context.Background()

	// test save with table info and rollback
	s.Require().Nil(s.tracker.CreateSchemaIfNotExists(schemaName))
	stmt, err := parseSQL("create table " + tableName + " (c int);")
	s.Require().NoError(err)
	err = s.tracker.Exec(ctx, schemaName, stmt)
	s.Require().NoError(err)
	ti, err := s.tracker.GetTableInfo(table)
	s.Require().NoError(err)
	cp.SaveTablePoint(table, binlog.Location{Position: pos1}, ti)
	rcp := cp.(*RemoteCheckPoint)
	s.Require().NotNil(rcp.points[schemaName][tableName].TableInfo())
	s.Require().Nil(rcp.points[schemaName][tableName].flushedPoint.ti)

	cp.Rollback()
	rcp = cp.(*RemoteCheckPoint)
	s.Require().Nil(rcp.points[schemaName][tableName].TableInfo())
	s.Require().Nil(rcp.points[schemaName][tableName].flushedPoint.ti)

	// test save, flush and rollback to not nil table info
	cp.SaveTablePoint(table, binlog.Location{Position: pos1}, ti)
	tiBytes, _ := json.Marshal(ti)
	s.mock.ExpectBegin()
	s.mock.ExpectExec(flushCheckPointSQL).WithArgs(cpid, schemaName, tableName, pos1.Name, pos1.Pos, "", "", 0, "", string(tiBytes), false).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	lastGlobalPoint := cp.GlobalPoint()
	lastGlobalPointSavedTime := cp.GlobalPointSaveTime()
	s.Require().Nil(cp.FlushPointsExcept(tctx, cp.Snapshot(true).id, nil, nil, nil))
	s.Require().Equal(lastGlobalPoint, cp.GlobalPoint())
	s.Require().Equal(lastGlobalPointSavedTime, cp.GlobalPointSaveTime())
	stmt, err = parseSQL("alter table " + tableName + " add c2 int;")
	s.Require().NoError(err)
	err = s.tracker.Exec(ctx, schemaName, stmt)
	s.Require().NoError(err)
	ti2, err := s.tracker.GetTableInfo(table)
	s.Require().NoError(err)
	cp.SaveTablePoint(table, binlog.Location{Position: pos2}, ti2)
	cp.Rollback()

	// clear, to min
	s.mock.ExpectBegin()
	s.mock.ExpectExec(clearCheckPointSQL).WithArgs(cpid).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	err = cp.Clear(tctx)
	s.Require().NoError(err)
	older = cp.IsOlderThanTablePoint(table, binlog.Location{Position: pos1})
	s.Require().False(older)

	// test save table point less than global point
	func() {
		defer func() {
			r := recover()
			matchStr := ".*less than global checkpoint.*"
			s.Require().Regexp(matchStr, r)
		}()
		cp.SaveGlobalPoint(binlog.Location{Position: pos2})
		cp.SaveTablePoint(table, binlog.Location{Position: pos1}, nil)
	}()

	// flush but except + rollback
	s.mock.ExpectBegin()
	s.mock.ExpectExec("(320)?"+flushCheckPointSQL).WithArgs(cpid, "", "", pos2.Name, pos2.Pos, "", "", 0, "", "null", true).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	lastGlobalPoint = cp.GlobalPoint()
	lastGlobalPointSavedTime = cp.GlobalPointSaveTime()
	err = cp.FlushPointsExcept(tctx, cp.Snapshot(true).id, []*filter.Table{table}, nil, nil)
	fmt.Println(cp.GlobalPoint(), lastGlobalPoint)
	s.Require().Equal(lastGlobalPoint, cp.GlobalPoint())
	s.Require().NotEqual(lastGlobalPointSavedTime, cp.GlobalPointSaveTime())
	s.Require().NoError(err)
	cp.Rollback()
	older = cp.IsOlderThanTablePoint(table, binlog.Location{Position: pos1})
	s.Require().False(older)

	s.mock.ExpectBegin()
	s.mock.ExpectExec(clearCheckPointSQL).WithArgs(cpid).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	s.Require().Nil(cp.Clear(tctx))
	// load table point and exitSafe, with enable GTID
	s.cfg.EnableGTID = true
	flavor := mysql.MySQLFlavor
	gSetStr := "03fc0263-28c7-11e7-a653-6c0b84d59f30:123"
	gs, _ := gtid.ParserGTID(flavor, gSetStr)
	columns := []string{"cp_schema", "cp_table", "binlog_name", "binlog_pos", "binlog_gtid", "exit_safe_binlog_name", "exit_safe_binlog_pos", "exit_safe_binlog_gtid", "table_info", "is_global"}
	s.mock.ExpectQuery(loadCheckPointSQL).WithArgs(cpid).WillReturnRows(
		sqlmock.NewRows(columns).AddRow("", "", pos2.Name, pos2.Pos, gs.String(), pos2.Name, pos2.Pos, gs.String(), "null", true).
			AddRow(schemaName, tableName, pos2.Name, pos2.Pos, gs.String(), "", 0, "", tiBytes, false))
	err = cp.Load(tctx)
	s.Require().NoError(err)
	s.Require().Equal(binlog.NewLocation(pos2, gs), cp.GlobalPoint())
	rcp = cp.(*RemoteCheckPoint)
	s.Require().NotNil(rcp.points[schemaName][tableName].TableInfo())
	s.Require().NotNil(rcp.points[schemaName][tableName].flushedPoint.ti)
	s.Require().Equal(binlog.NewLocation(pos2, gs), *rcp.safeModeExitPoint)
}

func TestRemoteCheckPointLoadIntoSchemaTracker(t *testing.T) {
	cfg := genDefaultSubTaskConfig4Test()
	cfg.WorkerCount = 0
	ctx := context.Background()

	db, _, err := sqlmock.New()
	require.NoError(t, err)
	dbConn, err := db.Conn(ctx)
	require.NoError(t, err)
	downstreamTrackConn := dbconn.NewDBConn(cfg, conn.NewBaseConnForTest(dbConn, &retry.FiniteRetryStrategy{}))
	schemaTracker, err := schema.NewTestTracker(ctx, cfg.Name, downstreamTrackConn, dlog.L())
	require.NoError(t, err)
	defer schemaTracker.Close() //nolint

	tbl1 := &filter.Table{Schema: "test", Name: "tbl1"}
	tbl2 := &filter.Table{Schema: "test", Name: "tbl2"}

	// before load
	_, err = schemaTracker.GetTableInfo(tbl1)
	require.Error(t, err)
	_, err = schemaTracker.GetTableInfo(tbl2)
	require.Error(t, err)

	cp := NewRemoteCheckPoint(tcontext.Background(), cfg, nil, "1")
	checkpoint := cp.(*RemoteCheckPoint)

	parser, err := conn.GetParserFromSQLModeStr("")
	require.NoError(t, err)
	createNode, err := parser.ParseOneStmt("create table tbl1(id int)", "", "")
	require.NoError(t, err)
	ti, err := tidbddl.BuildTableInfoFromAST(metabuild.NewContext(), createNode.(*ast.CreateTableStmt))
	require.NoError(t, err)

	tp1 := tablePoint{ti: ti}
	tp2 := tablePoint{}
	checkpoint.points[tbl1.Schema] = make(map[string]*binlogPoint)
	checkpoint.points[tbl1.Schema][tbl1.Name] = &binlogPoint{flushedPoint: tp1}
	checkpoint.points[tbl2.Schema][tbl2.Name] = &binlogPoint{flushedPoint: tp2}

	// after load
	err = checkpoint.LoadIntoSchemaTracker(ctx, schemaTracker)
	require.NoError(t, err)
	tableInfo, err := schemaTracker.GetTableInfo(tbl1)
	require.NoError(t, err)
	require.Len(t, tableInfo.Columns, 1)
	_, err = schemaTracker.GetTableInfo(tbl2)
	require.Error(t, err)

	// test BatchCreateTableWithInfo will not meet kv entry too large error

	// create 100K comment string
	comment := make([]byte, 0, 100000)
	for i := 0; i < 100000; i++ {
		comment = append(comment, 'A')
	}
	ti.Comment = string(comment)

	tp1 = tablePoint{ti: ti}
	amount := 100
	for i := 0; i < amount; i++ {
		tableName := fmt.Sprintf("tbl_%d", i)
		checkpoint.points[tbl1.Schema][tableName] = &binlogPoint{flushedPoint: tp1}
	}
	err = checkpoint.LoadIntoSchemaTracker(ctx, schemaTracker)
	require.NoError(t, err)
}

func TestLastFlushOutdated(t *testing.T) {
	cfg := genDefaultSubTaskConfig4Test()
	cfg.WorkerCount = 0
	cfg.CheckpointFlushInterval = 1

	cp := NewRemoteCheckPoint(tcontext.Background(), cfg, nil, "1")
	checkpoint := cp.(*RemoteCheckPoint)
	checkpoint.globalPointSaveTime = time.Now().Add(-2 * time.Second)

	require.True(t, checkpoint.LastFlushOutdated())
	require.Nil(t, checkpoint.Snapshot(true))
	// though snapshot is nil, checkpoint is not outdated
	require.False(t, checkpoint.LastFlushOutdated())
}
