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
	"strings"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/cputil"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/retry"
	"github.com/pingcap/dm/pkg/schema"
	"github.com/pingcap/dm/syncer/dbconn"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-mysql-org/go-mysql/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/filter"
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

var _ = Suite(&testCheckpointSuite{})

type testCheckpointSuite struct {
	cfg     *config.SubTaskConfig
	mock    sqlmock.Sqlmock
	tracker *schema.Tracker
}

func (s *testCheckpointSuite) SetUpSuite(c *C) {
	s.cfg = &config.SubTaskConfig{
		ServerID:   101,
		MetaSchema: "test",
		Name:       "syncer_checkpoint_ut",
		Flavor:     mysql.MySQLFlavor,
	}

	log.SetLevel(zapcore.ErrorLevel)
	var (
		err                   error
		defaultTestSessionCfg = map[string]string{
			"sql_mode":             "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION",
			"tidb_skip_utf8_check": "0",
		}
	)

	s.tracker, err = schema.NewTracker(context.Background(), s.cfg.Name, defaultTestSessionCfg, nil)
	c.Assert(err, IsNil)
}

func (s *testCheckpointSuite) TestUpTest(c *C) {
	err := s.tracker.Reset()
	c.Assert(err, IsNil)
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
func (s *testCheckpointSuite) TestCheckPoint(c *C) {
	tctx := tcontext.Background()

	cp := NewRemoteCheckPoint(tctx, s.cfg, cpid)
	defer func() {
		s.mock.ExpectClose()
		cp.Close()
	}()

	var err error
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
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
	c.Assert(err, IsNil)
	conn := &dbconn.DBConn{Cfg: s.cfg, BaseConn: conn.NewBaseConn(dbConn, &retry.FiniteRetryStrategy{})}

	cp.(*RemoteCheckPoint).dbConn = conn
	err = cp.(*RemoteCheckPoint).prepare(tctx)
	c.Assert(err, IsNil)
	c.Assert(cp.Clear(tctx), IsNil)

	// test operation for global checkpoint
	s.testGlobalCheckPoint(c, cp)

	// test operation for table checkpoint
	s.testTableCheckPoint(c, cp)
}

func (s *testCheckpointSuite) testGlobalCheckPoint(c *C, cp CheckPoint) {
	tctx := tcontext.Background()

	// global checkpoint init to min
	c.Assert(cp.GlobalPoint().Position, Equals, binlog.MinPosition)
	c.Assert(cp.FlushedGlobalPoint().Position, Equals, binlog.MinPosition)

	// try load, but should load nothing
	s.mock.ExpectQuery(loadCheckPointSQL).WillReturnRows(sqlmock.NewRows(nil))
	err := cp.Load(tctx)
	c.Assert(err, IsNil)
	c.Assert(cp.GlobalPoint().Position, Equals, binlog.MinPosition)
	c.Assert(cp.FlushedGlobalPoint().Position, Equals, binlog.MinPosition)

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
	c.Assert(err, IsNil)
	cp.SaveGlobalPoint(binlog.Location{Position: pos1})

	s.mock.ExpectBegin()
	s.mock.ExpectExec("(162)?"+flushCheckPointSQL).WithArgs(cpid, "", "", pos1.Name, pos1.Pos, "", "", 0, "", "null", true).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	err = cp.FlushPointsExcept(tctx, nil, nil, nil)
	c.Log(errors.ErrorStack(err))
	c.Assert(err, IsNil)
	c.Assert(cp.GlobalPoint().Position, Equals, pos1)
	c.Assert(cp.FlushedGlobalPoint().Position, Equals, pos1)

	// try load from config
	pos1.Pos = 2044
	s.cfg.Mode = config.ModeIncrement
	s.cfg.Meta = &config.Meta{BinLogName: pos1.Name, BinLogPos: pos1.Pos}
	err = cp.LoadMeta()
	c.Assert(err, IsNil)
	c.Assert(cp.GlobalPoint().Position, Equals, pos1)
	c.Assert(cp.FlushedGlobalPoint().Position, Equals, pos1)

	s.cfg.Mode = oldMode
	s.cfg.Meta = nil

	// test save global point
	pos2 := mysql.Position{
		Name: "mysql-bin.000005",
		Pos:  2052,
	}
	cp.SaveGlobalPoint(binlog.Location{Position: pos2})
	c.Assert(cp.GlobalPoint().Position, Equals, pos2)
	c.Assert(cp.FlushedGlobalPoint().Position, Equals, pos1)

	// test rollback
	cp.Rollback(s.tracker)
	c.Assert(cp.GlobalPoint().Position, Equals, pos1)
	c.Assert(cp.FlushedGlobalPoint().Position, Equals, pos1)

	// save again
	cp.SaveGlobalPoint(binlog.Location{Position: pos2})
	c.Assert(cp.GlobalPoint().Position, Equals, pos2)
	c.Assert(cp.FlushedGlobalPoint().Position, Equals, pos1)

	// flush + rollback
	s.mock.ExpectBegin()
	s.mock.ExpectExec("(202)?"+flushCheckPointSQL).WithArgs(cpid, "", "", pos2.Name, pos2.Pos, "", "", 0, "", "null", true).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	err = cp.FlushPointsExcept(tctx, nil, nil, nil)
	c.Assert(err, IsNil)
	cp.Rollback(s.tracker)
	c.Assert(cp.GlobalPoint().Position, Equals, pos2)
	c.Assert(cp.FlushedGlobalPoint().Position, Equals, pos2)

	// try load from DB
	pos3 := pos2
	pos3.Pos = pos2.Pos + 1000 // > pos2 to enable save
	cp.SaveGlobalPoint(binlog.Location{Position: pos3})
	columns := []string{"cp_schema", "cp_table", "binlog_name", "binlog_pos", "binlog_gtid", "exit_safe_binlog_name", "exit_safe_binlog_pos", "exit_safe_binlog_gtid", "table_info", "is_global"}
	s.mock.ExpectQuery(loadCheckPointSQL).WithArgs(cpid).WillReturnRows(sqlmock.NewRows(columns).AddRow("", "", pos2.Name, pos2.Pos, "", "", 0, "", "null", true))
	err = cp.Load(tctx)
	c.Assert(err, IsNil)
	c.Assert(cp.GlobalPoint().Position, Equals, pos2)
	c.Assert(cp.FlushedGlobalPoint().Position, Equals, pos2)

	// test save older point
	/*var buf bytes.Buffer
	log.SetOutput(&buf)
	cp.SaveGlobalPoint(pos1)
	c.Assert(cp.GlobalPoint(), Equals, pos2)
	c.Assert(cp.FlushedGlobalPoint(), Equals, pos2)
	matchStr := fmt.Sprintf(".*try to save %s is older than current pos %s", pos1, pos2)
	matchStr = strings.Replace(strings.Replace(matchStr, ")", "\\)", -1), "(", "\\(", -1)
	c.Assert(strings.TrimSpace(buf.String()), Matches, matchStr)
	log.SetOutput(os.Stdout)*/

	// test clear
	s.mock.ExpectBegin()
	s.mock.ExpectExec(clearCheckPointSQL).WithArgs(cpid).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	err = cp.Clear(tctx)
	c.Assert(err, IsNil)
	c.Assert(cp.GlobalPoint().Position, Equals, binlog.MinPosition)
	c.Assert(cp.FlushedGlobalPoint().Position, Equals, binlog.MinPosition)

	s.mock.ExpectQuery(loadCheckPointSQL).WillReturnRows(sqlmock.NewRows(nil))
	err = cp.Load(tctx)
	c.Assert(err, IsNil)
	c.Assert(cp.GlobalPoint().Position, Equals, binlog.MinPosition)
	c.Assert(cp.FlushedGlobalPoint().Position, Equals, binlog.MinPosition)

	// try load from mydumper's output
	dir, err := os.MkdirTemp("", "test_global_checkpoint")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	filename := filepath.Join(dir, "metadata")
	err = os.WriteFile(filename, []byte(
		fmt.Sprintf("SHOW MASTER STATUS:\n\tLog: %s\n\tPos: %d\n\tGTID:\n\nSHOW SLAVE STATUS:\n\tHost: %s\n\tLog: %s\n\tPos: %d\n\tGTID:\n\n", pos1.Name, pos1.Pos, "slave_host", pos1.Name, pos1.Pos+1000)),
		0o644)
	c.Assert(err, IsNil)
	s.cfg.Mode = config.ModeAll
	s.cfg.Dir = dir
	c.Assert(cp.LoadMeta(), IsNil)

	// should flush because globalPointSaveTime is zero
	s.mock.ExpectBegin()
	s.mock.ExpectExec("(202)?"+flushCheckPointSQL).WithArgs(cpid, "", "", pos1.Name, pos1.Pos, "", "", 0, "", "null", true).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	err = cp.FlushPointsExcept(tctx, nil, nil, nil)
	c.Assert(err, IsNil)
	s.mock.ExpectQuery(loadCheckPointSQL).WillReturnRows(sqlmock.NewRows(nil))
	err = cp.Load(tctx)
	c.Assert(err, IsNil)
	c.Assert(cp.GlobalPoint().Position, Equals, pos1)
	c.Assert(cp.FlushedGlobalPoint().Position, Equals, pos1)

	s.mock.ExpectBegin()
	s.mock.ExpectExec(clearCheckPointSQL).WithArgs(cpid).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	err = cp.Clear(tctx)
	c.Assert(err, IsNil)

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
	c.Assert(err, IsNil)
	c.Assert(cp.LoadMeta(), IsNil)

	// should flush because globalPointSaveTime is zero
	s.mock.ExpectBegin()
	s.mock.ExpectExec("(202)?"+flushCheckPointSQL).WithArgs(cpid, "", "", pos1.Name, pos1.Pos, "", pos2.Name, pos2.Pos, "", "null", true).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	err = cp.FlushPointsExcept(tctx, nil, nil, nil)
	c.Assert(err, IsNil)
	s.mock.ExpectQuery(loadCheckPointSQL).WillReturnRows(sqlmock.NewRows(nil))
	err = cp.Load(tctx)
	c.Assert(err, IsNil)
	c.Assert(cp.GlobalPoint().Position, Equals, pos1)
	c.Assert(cp.FlushedGlobalPoint().Position, Equals, pos1)
	c.Assert(cp.SafeModeExitPoint().Position, Equals, pos2)
}

func (s *testCheckpointSuite) testTableCheckPoint(c *C, cp CheckPoint) {
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
	older := cp.IsOlderThanTablePoint(table, binlog.Location{Position: pos1}, false)
	c.Assert(older, IsFalse)

	// save
	cp.SaveTablePoint(table, binlog.Location{Position: pos2}, nil)
	older = cp.IsOlderThanTablePoint(table, binlog.Location{Position: pos1}, false)
	c.Assert(older, IsTrue)

	// rollback, to min
	cp.Rollback(s.tracker)
	older = cp.IsOlderThanTablePoint(table, binlog.Location{Position: pos1}, false)
	c.Assert(older, IsFalse)

	// save again
	cp.SaveTablePoint(table, binlog.Location{Position: pos2}, nil)
	older = cp.IsOlderThanTablePoint(table, binlog.Location{Position: pos1}, false)
	c.Assert(older, IsTrue)

	// flush + rollback
	s.mock.ExpectBegin()
	s.mock.ExpectExec("(284)?"+flushCheckPointSQL).WithArgs(cpid, table.Schema, table.Name, pos2.Name, pos2.Pos, "", "", 0, "", sqlmock.AnyArg(), false).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	err = cp.FlushPointsExcept(tctx, nil, nil, nil)
	c.Assert(err, IsNil)
	cp.Rollback(s.tracker)
	older = cp.IsOlderThanTablePoint(table, binlog.Location{Position: pos1}, false)
	c.Assert(older, IsTrue)

	// save
	cp.SaveTablePoint(table, binlog.Location{Position: pos2}, nil)
	older = cp.IsOlderThanTablePoint(table, binlog.Location{Position: pos1}, false)
	c.Assert(older, IsTrue)

	// delete
	s.mock.ExpectBegin()
	s.mock.ExpectExec(deleteCheckPointSQL).WithArgs(cpid, schemaName, tableName).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	c.Assert(cp.DeleteTablePoint(tctx, table), IsNil)
	s.mock.ExpectBegin()
	s.mock.ExpectExec(deleteSchemaPointSQL).WithArgs(cpid, schemaName).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	c.Assert(cp.DeleteSchemaPoint(tctx, schemaName), IsNil)

	ctx := context.Background()

	// test save with table info and rollback
	c.Assert(s.tracker.CreateSchemaIfNotExists(schemaName), IsNil)
	err = s.tracker.Exec(ctx, schemaName, "create table "+tableName+" (c int);")
	c.Assert(err, IsNil)
	ti, err := s.tracker.GetTableInfo(table)
	c.Assert(err, IsNil)
	cp.SaveTablePoint(table, binlog.Location{Position: pos1}, ti)
	rcp := cp.(*RemoteCheckPoint)
	c.Assert(rcp.points[schemaName][tableName].TableInfo(), NotNil)
	c.Assert(rcp.points[schemaName][tableName].flushedTI, IsNil)

	cp.Rollback(s.tracker)
	rcp = cp.(*RemoteCheckPoint)
	c.Assert(rcp.points[schemaName][tableName].TableInfo(), IsNil)
	c.Assert(rcp.points[schemaName][tableName].flushedTI, IsNil)

	_, err = s.tracker.GetTableInfo(table)
	c.Assert(strings.Contains(err.Error(), "doesn't exist"), IsTrue)

	// test save, flush and rollback to not nil table info
	err = s.tracker.Exec(ctx, schemaName, "create table "+tableName+" (c int);")
	c.Assert(err, IsNil)
	ti, err = s.tracker.GetTableInfo(table)
	c.Assert(err, IsNil)
	cp.SaveTablePoint(table, binlog.Location{Position: pos1}, ti)
	tiBytes, _ := json.Marshal(ti)
	s.mock.ExpectBegin()
	s.mock.ExpectExec(flushCheckPointSQL).WithArgs(cpid, schemaName, tableName, pos1.Name, pos1.Pos, "", "", 0, "", string(tiBytes), false).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	c.Assert(cp.FlushPointsExcept(tctx, nil, nil, nil), IsNil)
	err = s.tracker.Exec(ctx, schemaName, "alter table "+tableName+" add c2 int;")
	c.Assert(err, IsNil)
	ti2, err := s.tracker.GetTableInfo(table)
	c.Assert(err, IsNil)
	cp.SaveTablePoint(table, binlog.Location{Position: pos2}, ti2)
	cp.Rollback(s.tracker)
	ti11, err := s.tracker.GetTableInfo(table)
	c.Assert(err, IsNil)
	c.Assert(ti11.Columns, HasLen, 1)

	// clear, to min
	s.mock.ExpectBegin()
	s.mock.ExpectExec(clearCheckPointSQL).WithArgs(cpid).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	err = cp.Clear(tctx)
	c.Assert(err, IsNil)
	older = cp.IsOlderThanTablePoint(table, binlog.Location{Position: pos1}, false)
	c.Assert(older, IsFalse)

	// test save table point less than global point
	func() {
		defer func() {
			r := recover()
			matchStr := ".*less than global checkpoint.*"
			c.Assert(r, Matches, matchStr)
		}()
		cp.SaveGlobalPoint(binlog.Location{Position: pos2})
		cp.SaveTablePoint(table, binlog.Location{Position: pos1}, nil)
	}()

	// flush but except + rollback
	s.mock.ExpectBegin()
	s.mock.ExpectExec("(320)?"+flushCheckPointSQL).WithArgs(cpid, "", "", pos2.Name, pos2.Pos, "", "", 0, "", "null", true).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	err = cp.FlushPointsExcept(tctx, []*filter.Table{table}, nil, nil)
	c.Assert(err, IsNil)
	cp.Rollback(s.tracker)
	older = cp.IsOlderThanTablePoint(table, binlog.Location{Position: pos1}, false)
	c.Assert(older, IsFalse)

	s.mock.ExpectBegin()
	s.mock.ExpectExec(clearCheckPointSQL).WithArgs(cpid).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	c.Assert(cp.Clear(tctx), IsNil)
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
	c.Assert(err, IsNil)
	c.Assert(cp.GlobalPoint(), DeepEquals, binlog.InitLocation(pos2, gs))
	rcp = cp.(*RemoteCheckPoint)
	c.Assert(rcp.points[schemaName][tableName].TableInfo(), NotNil)
	c.Assert(rcp.points[schemaName][tableName].flushedTI, NotNil)
	c.Assert(*rcp.safeModeExitPoint, DeepEquals, binlog.InitLocation(pos2, gs))
}
