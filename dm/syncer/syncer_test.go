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
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/pingcap/tiflow/dm/pkg/binlog/reader"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/cputil"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/log"
	parserpkg "github.com/pingcap/tiflow/dm/pkg/parser"
	"github.com/pingcap/tiflow/dm/pkg/retry"
	"github.com/pingcap/tiflow/dm/pkg/schema"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	"github.com/pingcap/tiflow/pkg/errorutil"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	cm "github.com/pingcap/tidb-tools/pkg/column-mapping"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	pmysql "github.com/pingcap/tidb/parser/mysql"
	"go.uber.org/zap"
)

var _ = Suite(&testSyncerSuite{})

var defaultTestSessionCfg = map[string]string{
	"sql_mode":             "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION",
	"tidb_skip_utf8_check": "0",
}

func TestSuite(t *testing.T) {
	TestingT(t)
}

type (
	mockBinlogEvents []mockBinlogEvent
	mockBinlogEvent  struct {
		typ  int
		args []interface{}
	}
)

const (
	DBCreate = iota
	DBDrop
	TableCreate
	TableDrop

	DDL

	Write
	Update
	Delete

	DMLQuery

	Headers
	Rotate
)

type testSyncerSuite struct {
	cfg             *config.SubTaskConfig
	eventsGenerator *event.Generator
}

type MockStreamer struct {
	events []*replication.BinlogEvent
	idx    uint32
}

func (m *MockStreamer) GetEvent(ctx context.Context) (*replication.BinlogEvent, error) {
	if int(m.idx) >= len(m.events) {
		return nil, context.Canceled
	}
	e := m.events[m.idx]
	m.idx++
	return e, nil
}

type MockStreamProducer struct {
	events []*replication.BinlogEvent
}

func (mp *MockStreamProducer) generateStreamer(location binlog.Location) (reader.Streamer, error) {
	if location.Position.Pos == 4 {
		return &MockStreamer{mp.events, 0}, nil
	}
	bytesLen := 0
	idx := uint32(0)
	for i, e := range mp.events {
		bytesLen += len(e.RawData)
		if location.Position.Pos == uint32(bytesLen) {
			idx = uint32(i)
			break
		}
	}
	return &MockStreamer{mp.events, idx}, nil
}

func (s *testSyncerSuite) SetUpSuite(c *C) {
	loaderDir, err := os.MkdirTemp("", "loader")
	c.Assert(err, IsNil)
	loaderCfg := config.LoaderConfig{
		Dir: loaderDir,
	}
	s.cfg = &config.SubTaskConfig{
		From:             config.GetDBConfigForTest(),
		To:               config.GetDBConfigForTest(),
		ServerID:         101,
		MetaSchema:       "test",
		Name:             "syncer_ut",
		ShadowTableRules: []string{config.DefaultShadowTableRules},
		TrashTableRules:  []string{config.DefaultTrashTableRules},
		Mode:             config.ModeIncrement,
		Flavor:           "mysql",
		LoaderConfig:     loaderCfg,
	}
	s.cfg.Experimental.AsyncCheckpointFlush = true
	s.cfg.From.Adjust()
	s.cfg.To.Adjust()

	s.cfg.UseRelay = false
	s.resetEventsGenerator(c)
	c.Assert(log.InitLogger(&log.Config{}), IsNil)
}

func (s *testSyncerSuite) generateEvents(binlogEvents mockBinlogEvents, c *C) []*replication.BinlogEvent {
	events := make([]*replication.BinlogEvent, 0, 1024)
	for _, e := range binlogEvents {
		switch e.typ {
		case DBCreate:
			evs, _, err := s.eventsGenerator.GenCreateDatabaseEvents(e.args[0].(string))
			c.Assert(err, IsNil)
			events = append(events, evs...)
		case DBDrop:
			evs, _, err := s.eventsGenerator.GenDropDatabaseEvents(e.args[0].(string))
			c.Assert(err, IsNil)
			events = append(events, evs...)
		case TableCreate:
			evs, _, err := s.eventsGenerator.GenCreateTableEvents(e.args[0].(string), e.args[1].(string))
			c.Assert(err, IsNil)
			events = append(events, evs...)
		case TableDrop:
			evs, _, err := s.eventsGenerator.GenDropTableEvents(e.args[0].(string), e.args[1].(string))
			c.Assert(err, IsNil)
			events = append(events, evs...)

		case DDL:
			evs, _, err := s.eventsGenerator.GenDDLEvents(e.args[0].(string), e.args[1].(string), 0)
			c.Assert(err, IsNil)
			events = append(events, evs...)

		case Write, Update, Delete:
			dmlData := []*event.DMLData{
				{
					TableID:    e.args[0].(uint64),
					Schema:     e.args[1].(string),
					Table:      e.args[2].(string),
					ColumnType: e.args[3].([]byte),
					Rows:       e.args[4].([][]interface{}),
				},
			}
			var eventType replication.EventType
			switch e.typ {
			case Write:
				eventType = replication.WRITE_ROWS_EVENTv2
			case Update:
				eventType = replication.UPDATE_ROWS_EVENTv2
			case Delete:
				eventType = replication.DELETE_ROWS_EVENTv2
			default:
				c.Fatal(fmt.Sprintf("mock event generator don't support event type: %d", e.typ))
			}
			evs, _, err := s.eventsGenerator.GenDMLEvents(eventType, dmlData, 0)
			c.Assert(err, IsNil)
			events = append(events, evs...)
		}
	}
	return events
}

func (s *testSyncerSuite) resetEventsGenerator(c *C) {
	previousGTIDSetStr := "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,406a3f61-690d-11e7-87c5-6c92bf46f384:1-94321383"
	previousGTIDSet, err := gtid.ParserGTID(s.cfg.Flavor, previousGTIDSetStr)
	if err != nil {
		c.Fatal(err)
	}
	latestGTIDStr := "3ccc475b-2343-11e7-be21-6c0b84d59f30:14"
	latestGTID, err := gtid.ParserGTID(s.cfg.Flavor, latestGTIDStr)
	c.Assert(err, IsNil)
	s.eventsGenerator, err = event.NewGenerator(s.cfg.Flavor, s.cfg.ServerID, 0, latestGTID, previousGTIDSet, 0)
	if err != nil {
		c.Fatal(err)
	}
}

func (s *testSyncerSuite) TearDownSuite(c *C) {
	os.RemoveAll(s.cfg.Dir)
}

func (s *testSyncerSuite) mockGetServerUnixTS(mock sqlmock.Sqlmock) {
	ts := time.Now().Unix()
	rows := sqlmock.NewRows([]string{"UNIX_TIMESTAMP()"}).AddRow(strconv.FormatInt(ts, 10))
	mock.ExpectQuery("SELECT UNIX_TIMESTAMP()").WillReturnRows(rows)
}

func (s *testSyncerSuite) TestSelectDB(c *C) {
	s.cfg.BAList = &filter.Rules{
		DoDBs: []string{"~^b.*", "s1", "stest"},
	}

	schemas := [][]byte{[]byte("s1"), []byte("s2"), []byte("btest"), []byte("b1"), []byte("stest"), []byte("st")}
	skips := []bool{false, true, false, false, false, true}
	type Case struct {
		schema []byte
		query  []byte
		skip   bool
	}
	cases := make([]Case, 0, 2*len(schemas))
	for i, schema := range schemas {
		cases = append(cases, Case{
			schema: schema,
			query:  append([]byte("create database "), schema...),
			skip:   skips[i],
		})
		cases = append(cases, Case{
			schema: schema,
			query:  append([]byte("drop database "), schema...),
			skip:   skips[i],
		})
	}

	p := parser.New()
	cfg, err := s.cfg.Clone()
	c.Assert(err, IsNil)
	syncer := NewSyncer(cfg, nil, nil)
	syncer.baList, err = filter.New(syncer.cfg.CaseSensitive, syncer.cfg.BAList)
	c.Assert(err, IsNil)
	err = syncer.genRouter()
	c.Assert(err, IsNil)

	header := &replication.EventHeader{
		Timestamp: uint32(time.Now().Unix()),
		ServerID:  101,
		Flags:     0x01,
	}

	statusVars := []byte{4, 0, 0, 0, 0, 46, 0}
	for _, cs := range cases {
		e, err := event.GenQueryEvent(header, 123, 0, 0, 0, statusVars, cs.schema, cs.query)
		c.Assert(err, IsNil)
		c.Assert(e, NotNil)
		ev, ok := e.Event.(*replication.QueryEvent)
		c.Assert(ok, IsTrue)

		sql := string(ev.Query)
		schema := string(ev.Schema)
		qec := &queryEventContext{
			p:               p,
			ddlSchema:       schema,
			eventStatusVars: ev.StatusVars,
		}
		ddlInfo, err := syncer.genDDLInfo(qec, sql)
		c.Assert(err, IsNil)

		qec.originSQL = sql
		needSkip, err := syncer.skipQueryEvent(qec, ddlInfo)
		c.Assert(err, IsNil)
		c.Assert(needSkip, Equals, cs.skip)
	}
}

func (s *testSyncerSuite) TestSelectTable(c *C) {
	s.cfg.BAList = &filter.Rules{
		DoDBs: []string{"t2", "stest", "~^ptest*"},
		DoTables: []*filter.Table{
			{Schema: "stest", Name: "log"},
			{Schema: "stest", Name: "~^t.*"},
			{Schema: "~^ptest*", Name: "~^t.*"},
		},
	}
	s.resetEventsGenerator(c)
	events := mockBinlogEvents{
		mockBinlogEvent{typ: DBCreate, args: []interface{}{"s1"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"s1", "create table s1.log(id int)"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"s1"}},

		mockBinlogEvent{typ: TableCreate, args: []interface{}{"mysql", "create table mysql.test(id int)"}},
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"mysql", "test"}},
		mockBinlogEvent{typ: DBCreate, args: []interface{}{"stest"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"stest", "create table stest.log(id int)"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"stest", "create table stest.t(id int)"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"stest", "create table stest.log2(id int)"}},
		mockBinlogEvent{typ: Write, args: []interface{}{uint64(8), "stest", "t", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(10)}}}},
		mockBinlogEvent{typ: Write, args: []interface{}{uint64(9), "stest", "log", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(10)}}}},
		mockBinlogEvent{typ: Write, args: []interface{}{uint64(10), "stest", "log2", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(10)}}}},
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"stest", "log"}},
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"stest", "t"}},
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"stest", "log2"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"stest"}},

		mockBinlogEvent{typ: DBCreate, args: []interface{}{"t2"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"t2", "create table t2.log(id int)"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"t2", "create table t2.log1(id int)"}},
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"t2", "log"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"t2"}},

		mockBinlogEvent{typ: DBCreate, args: []interface{}{"ptest1"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"ptest1", "create table ptest1.t1(id int)"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"ptest1"}},
	}

	allEvents := s.generateEvents(events, c)

	res := [][]bool{
		{true},
		{true},
		{true},

		{true},
		{true},
		{false},
		{false},
		{false},
		{true},
		{false},
		{false},
		{true},
		{false},
		{false},
		{true},
		{false},

		{false},
		{true},
		{true},
		{true},
		{false},

		{false},
		{false},
		{false},
	}

	p := parser.New()
	cfg, err := s.cfg.Clone()
	c.Assert(err, IsNil)
	syncer := NewSyncer(cfg, nil, nil)
	syncer.baList, err = filter.New(syncer.cfg.CaseSensitive, syncer.cfg.BAList)
	c.Assert(err, IsNil)
	c.Assert(syncer.genRouter(), IsNil)

	checkEventWithTableResult(c, syncer, allEvents, p, res)
}

func (s *testSyncerSuite) TestIgnoreDB(c *C) {
	s.cfg.BAList = &filter.Rules{
		IgnoreDBs: []string{"~^b.*", "s1", "stest"},
	}

	s.resetEventsGenerator(c)
	events := mockBinlogEvents{
		mockBinlogEvent{typ: DBCreate, args: []interface{}{"s1"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"s1"}},
		mockBinlogEvent{typ: DBCreate, args: []interface{}{"s2"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"s2"}},
		mockBinlogEvent{typ: DBCreate, args: []interface{}{"btest"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"btest"}},
		mockBinlogEvent{typ: DBCreate, args: []interface{}{"b1"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"b1"}},
		mockBinlogEvent{typ: DBCreate, args: []interface{}{"stest"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"stest"}},
		mockBinlogEvent{typ: DBCreate, args: []interface{}{"st"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"st"}},
	}

	allEvents := s.generateEvents(events, c)

	res := []bool{true, true, false, false, true, true, true, true, true, true, false, false}

	p := parser.New()
	cfg, err := s.cfg.Clone()
	c.Assert(err, IsNil)
	syncer := NewSyncer(cfg, nil, nil)
	syncer.baList, err = filter.New(syncer.cfg.CaseSensitive, syncer.cfg.BAList)
	c.Assert(err, IsNil)
	c.Assert(syncer.genRouter(), IsNil)
	i := 0

	statusVars := []byte{4, 0, 0, 0, 0, 46, 0}
	for _, e := range allEvents {
		ev, ok := e.Event.(*replication.QueryEvent)
		if !ok {
			continue
		}
		sql := string(ev.Query)
		schema := string(ev.Schema)
		qec := &queryEventContext{
			p:               p,
			ddlSchema:       schema,
			eventStatusVars: statusVars,
		}
		ddlInfo, err := syncer.genDDLInfo(qec, sql)
		c.Assert(err, IsNil)

		qec.originSQL = sql
		needSkip, err := syncer.skipQueryEvent(qec, ddlInfo)
		c.Assert(err, IsNil)
		c.Assert(needSkip, Equals, res[i])
		i++
	}
}

func (s *testSyncerSuite) TestIgnoreTable(c *C) {
	s.cfg.BAList = &filter.Rules{
		IgnoreDBs: []string{"t2"},
		IgnoreTables: []*filter.Table{
			{Schema: "stest", Name: "log"},
			{Schema: "stest", Name: "~^t.*"},
		},
	}

	s.resetEventsGenerator(c)
	events := mockBinlogEvents{
		mockBinlogEvent{typ: DBCreate, args: []interface{}{"s1"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"s1", "create table s1.log(id int)"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"s1"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"mysql", "create table mysql.test(id int)"}},
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"mysql", "test"}},

		mockBinlogEvent{typ: DBCreate, args: []interface{}{"stest"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"stest", "create table stest.log(id int)"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"stest", "create table stest.t(id int)"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"stest", "create table stest.log2(id int)"}},

		mockBinlogEvent{typ: Write, args: []interface{}{uint64(8), "stest", "t", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(10)}}}},
		mockBinlogEvent{typ: Write, args: []interface{}{uint64(9), "stest", "log", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(10)}}}},
		mockBinlogEvent{typ: Write, args: []interface{}{uint64(10), "stest", "log2", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(10)}}}},
		// TODO event generator support generate an event with multiple tables DDL
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"stest", "log"}},
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"stest", "t"}},
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"stest", "log2"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"stest"}},

		mockBinlogEvent{typ: DBCreate, args: []interface{}{"t2"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"t2", "create table t2.log(id int)"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"t2", "create table t2.log1(id int)"}},
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"t2", "log"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"t2"}},
	}
	allEvents := s.generateEvents(events, c)

	res := [][]bool{
		{false},
		{false},
		{false},
		{true},
		{true},

		{false},
		{true},
		{true},
		{false},

		{true},
		{true},
		{false},

		{true},
		{true},
		{false},
		{false},

		{true},
		{true},
		{true},
		{true},
		{true},
	}

	p := parser.New()
	cfg, err := s.cfg.Clone()
	c.Assert(err, IsNil)
	syncer := NewSyncer(cfg, nil, nil)
	syncer.baList, err = filter.New(syncer.cfg.CaseSensitive, syncer.cfg.BAList)
	c.Assert(err, IsNil)
	c.Assert(syncer.genRouter(), IsNil)

	checkEventWithTableResult(c, syncer, allEvents, p, res)
}

func (s *testSyncerSuite) TestSkipDML(c *C) {
	s.cfg.FilterRules = []*bf.BinlogEventRule{
		{
			SchemaPattern: "*",
			TablePattern:  "",
			Events:        []bf.EventType{bf.UpdateEvent},
			Action:        bf.Ignore,
		}, {
			SchemaPattern: "foo",
			TablePattern:  "",
			Events:        []bf.EventType{bf.DeleteEvent},
			Action:        bf.Ignore,
		}, {
			SchemaPattern: "foo1",
			TablePattern:  "bar1",
			Events:        []bf.EventType{bf.DeleteEvent},
			Action:        bf.Ignore,
		}, {
			SchemaPattern: "foo1",
			TablePattern:  "bar2",
			Events:        []bf.EventType{bf.EventType(strings.ToUpper(string(bf.DeleteEvent)))},
			Action:        bf.Ignore,
		},
	}
	s.cfg.BAList = nil

	s.resetEventsGenerator(c)

	type SQLChecker struct {
		events   []*replication.BinlogEvent
		isDML    bool
		expected bool
	}

	sqls := make([]SQLChecker, 0, 16)

	evs := s.generateEvents([]mockBinlogEvent{{DBCreate, []interface{}{"foo"}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: false, expected: false})

	evs = s.generateEvents([]mockBinlogEvent{{TableCreate, []interface{}{"foo", "create table foo.bar(id int)"}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: false, expected: false})

	evs = s.generateEvents([]mockBinlogEvent{{Write, []interface{}{uint64(8), "foo", "bar", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(1)}}}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: true, expected: false})

	evs = s.generateEvents([]mockBinlogEvent{{Update, []interface{}{uint64(8), "foo", "bar", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(2)}, {int32(1)}}}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: true, expected: true})

	evs = s.generateEvents([]mockBinlogEvent{{Delete, []interface{}{uint64(8), "foo", "bar", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(2)}}}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: true, expected: true})

	evs = s.generateEvents([]mockBinlogEvent{{DBDrop, []interface{}{"foo1"}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: false, expected: false})

	evs = s.generateEvents([]mockBinlogEvent{{DBCreate, []interface{}{"foo1"}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: false, expected: false})

	evs = s.generateEvents([]mockBinlogEvent{{TableCreate, []interface{}{"foo1", "create table foo1.bar1(id int)"}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: false, expected: false})

	evs = s.generateEvents([]mockBinlogEvent{{Write, []interface{}{uint64(9), "foo1", "bar1", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(1)}}}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: true, expected: false})

	evs = s.generateEvents([]mockBinlogEvent{{Update, []interface{}{uint64(9), "foo1", "bar1", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(2)}, {int32(1)}}}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: true, expected: true})

	evs = s.generateEvents([]mockBinlogEvent{{Delete, []interface{}{uint64(9), "foo1", "bar1", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(2)}}}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: true, expected: true})

	evs = s.generateEvents([]mockBinlogEvent{{TableCreate, []interface{}{"foo1", "create table foo1.bar2(id int)"}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: false, expected: false})

	evs = s.generateEvents([]mockBinlogEvent{{Write, []interface{}{uint64(10), "foo1", "bar2", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(1)}}}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: true, expected: false})

	evs = s.generateEvents([]mockBinlogEvent{{Update, []interface{}{uint64(10), "foo1", "bar2", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(2)}, {int32(1)}}}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: true, expected: true})

	evs = s.generateEvents([]mockBinlogEvent{{Delete, []interface{}{uint64(10), "foo1", "bar2", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(2)}}}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: true, expected: true})

	p := parser.New()
	var err error

	cfg, err := s.cfg.Clone()
	c.Assert(err, IsNil)
	syncer := NewSyncer(cfg, nil, nil)
	c.Assert(syncer.genRouter(), IsNil)

	syncer.binlogFilter, err = bf.NewBinlogEvent(false, s.cfg.FilterRules)
	c.Assert(err, IsNil)

	for _, sql := range sqls {
		events := sql.events
		for _, e := range events {
			switch ev := e.Event.(type) {
			case *replication.QueryEvent:
				_, err = p.ParseOneStmt(string(ev.Query), "", "")
				c.Assert(err, IsNil)
			case *replication.RowsEvent:
				table := &filter.Table{
					Schema: string(ev.Table.Schema),
					Name:   string(ev.Table.Table),
				}
				needSkip, err := syncer.skipRowsEvent(table, e.Header.EventType)
				c.Assert(err, IsNil)
				c.Assert(needSkip, Equals, sql.expected)
			default:
				continue
			}
		}
	}
}

func (s *testSyncerSuite) TestColumnMapping(c *C) {
	rules := []*cm.Rule{
		{
			PatternSchema: "stest*",
			PatternTable:  "log*",
			TargetColumn:  "id",
			Expression:    cm.AddPrefix,
			Arguments:     []string{"test:"},
		},
		{
			PatternSchema: "stest*",
			PatternTable:  "t*",
			TargetColumn:  "id",
			Expression:    cm.PartitionID,
			Arguments:     []string{"1", "stest_", "t_"},
		},
	}

	s.resetEventsGenerator(c)

	// create db and tables
	events := mockBinlogEvents{
		mockBinlogEvent{typ: DBCreate, args: []interface{}{"stest_3"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"stest_3", "create table stest_3.log(id varchar(45))"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"stest_3", "create table stest_3.t_2(name varchar(45), id bigint)"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"stest_3", "create table stest_3.a(id int)"}},
	}

	createEvents := s.generateEvents(events, c)

	// dmls
	type dml struct {
		events []*replication.BinlogEvent
		column []string
		data   []interface{}
	}

	dmls := make([]dml, 0, 3)

	evs := s.generateEvents([]mockBinlogEvent{{typ: Write, args: []interface{}{uint64(8), "stest_3", "t_2", []byte{mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_LONG}, [][]interface{}{{"ian", int32(10)}}}}}, c)
	dmls = append(dmls, dml{events: evs, column: []string{"name", "id"}, data: []interface{}{"ian", int64(1<<59 | 3<<52 | 2<<44 | 10)}})

	evs = s.generateEvents([]mockBinlogEvent{{typ: Write, args: []interface{}{uint64(9), "stest_3", "log", []byte{mysql.MYSQL_TYPE_STRING}, [][]interface{}{{"10"}}}}}, c)
	dmls = append(dmls, dml{events: evs, column: []string{"id"}, data: []interface{}{"test:10"}})

	evs = s.generateEvents([]mockBinlogEvent{{typ: Write, args: []interface{}{uint64(10), "stest_3", "a", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(10)}}}}}, c)
	dmls = append(dmls, dml{events: evs, column: []string{"id"}, data: []interface{}{int32(10)}})

	dmlEvents := make([]*replication.BinlogEvent, 0, 15)
	for _, dml := range dmls {
		dmlEvents = append(dmlEvents, dml.events...)
	}

	// drop tables and db
	events = mockBinlogEvents{
		// TODO event generator support generate an event with multiple tables DDL
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"stest_3", "log"}},
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"stest_3", "t_2"}},
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"stest_3", "a"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"stest_3"}},
	}
	dropEvents := s.generateEvents(events, c)

	p := parser.New()
	var err error
	mapping, err := cm.NewMapping(false, rules)
	c.Assert(err, IsNil)

	allEvents := createEvents
	allEvents = append(allEvents, dmlEvents...)
	allEvents = append(allEvents, dropEvents...)
	dmlIndex := 0
	for _, e := range allEvents {
		switch ev := e.Event.(type) {
		case *replication.QueryEvent:
			_, err = p.ParseOneStmt(string(ev.Query), "", "")
			c.Assert(err, IsNil)
		case *replication.RowsEvent:
			r, _, err := mapping.HandleRowValue(string(ev.Table.Schema), string(ev.Table.Table), dmls[dmlIndex].column, ev.Rows[0])
			c.Assert(err, IsNil)
			c.Assert(r, DeepEquals, dmls[dmlIndex].data)
			dmlIndex++
		default:
			continue
		}
	}
}

func (s *testSyncerSuite) TestcheckpointID(c *C) {
	cfg, err := s.cfg.Clone()
	c.Assert(err, IsNil)
	syncer := NewSyncer(cfg, nil, nil)
	checkpointID := syncer.checkpointID()
	c.Assert(checkpointID, Equals, "101")
}

// TODO: add `TestSharding` later.

func (s *testSyncerSuite) TestRun(c *C) {
	// 1. run syncer with column mapping
	// 2. execute some sqls which will trigger casuality
	// 3. check the generated jobs
	// 4. update config, add route rules, and update syncer
	// 5. execute somes sqls and then check jobs generated

	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	s.mockGetServerUnixTS(mock)
	dbConn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	checkPointDB, checkPointMock, err := sqlmock.New()
	c.Assert(err, IsNil)
	checkPointDBConn, err := checkPointDB.Conn(context.Background())
	c.Assert(err, IsNil)

	testJobs.jobs = testJobs.jobs[:0]

	s.cfg.BAList = &filter.Rules{
		DoDBs: []string{"test_1"},
		DoTables: []*filter.Table{
			{Schema: "test_1", Name: "t_1"},
			{Schema: "test_1", Name: "t_2"},
		},
	}

	s.cfg.ColumnMappingRules = []*cm.Rule{
		{
			PatternSchema: "test_*",
			PatternTable:  "t_*",
			SourceColumn:  "id",
			TargetColumn:  "id",
			Expression:    cm.PartitionID,
			Arguments:     []string{"1", "test_", "t_"},
		},
	}

	s.cfg.Batch = 1000
	s.cfg.WorkerCount = 2
	s.cfg.MaxRetry = 1

	cfg, err := s.cfg.Clone()
	c.Assert(err, IsNil)
	syncer := NewSyncer(cfg, nil, nil)
	syncer.cfg.CheckpointFlushInterval = 30
	syncer.fromDB = &dbconn.UpStreamConn{BaseDB: conn.NewBaseDB(db)}
	syncer.toDBConns = []*dbconn.DBConn{
		{Cfg: s.cfg, BaseConn: conn.NewBaseConn(dbConn, &retry.FiniteRetryStrategy{})},
		{Cfg: s.cfg, BaseConn: conn.NewBaseConn(dbConn, &retry.FiniteRetryStrategy{})},
	}
	syncer.ddlDBConn = &dbconn.DBConn{Cfg: s.cfg, BaseConn: conn.NewBaseConn(dbConn, &retry.FiniteRetryStrategy{})}
	syncer.downstreamTrackConn = &dbconn.DBConn{Cfg: s.cfg, BaseConn: conn.NewBaseConn(dbConn, &retry.FiniteRetryStrategy{})}
	syncer.schemaTracker, err = schema.NewTracker(context.Background(), s.cfg.Name, defaultTestSessionCfg, syncer.downstreamTrackConn)
	mock.ExpectBegin()
	mock.ExpectExec(fmt.Sprintf("SET SESSION SQL_MODE = '%s'", pmysql.DefaultSQLMode)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	mock.ExpectQuery("SHOW CREATE TABLE " + "`test_1`.`t_1`").WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("t_1", "create table t_1(id int primary key, name varchar(24))"))
	mock.ExpectQuery("SHOW CREATE TABLE " + "`test_1`.`t_1`").WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("t_1", "create table t_1(id int primary key, name varchar(24), KEY `index1` (`name`))"))
	s.mockGetServerUnixTS(mock)
	mock.ExpectQuery("SHOW CREATE TABLE " + "`test_1`.`t_2`").WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("t_2", "create table t_2(id int primary key, name varchar(24))"))
	syncer.exprFilterGroup = NewExprFilterGroup(utils.NewSessionCtx(nil), nil)
	c.Assert(err, IsNil)
	c.Assert(syncer.Type(), Equals, pb.UnitType_Sync)

	syncer.columnMapping, err = cm.NewMapping(s.cfg.CaseSensitive, s.cfg.ColumnMappingRules)
	c.Assert(err, IsNil)
	c.Assert(syncer.genRouter(), IsNil)

	syncer.setupMockCheckpoint(c, checkPointDBConn, checkPointMock)

	syncer.reset()
	events1 := mockBinlogEvents{
		mockBinlogEvent{typ: DBCreate, args: []interface{}{"test_1"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"test_1", "create table test_1.t_1(id int primary key, name varchar(24))"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"test_1", "create table test_1.t_2(id int primary key, name varchar(24))"}},

		mockBinlogEvent{typ: Write, args: []interface{}{uint64(8), "test_1", "t_1", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING}, [][]interface{}{{int32(1), "a"}}}},
		mockBinlogEvent{typ: DDL, args: []interface{}{"test_1", "alter table test_1.t_1 add index index1(name)"}},
		mockBinlogEvent{typ: Write, args: []interface{}{uint64(8), "test_1", "t_1", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING}, [][]interface{}{{int32(2), "b"}}}},
		mockBinlogEvent{typ: Delete, args: []interface{}{uint64(8), "test_1", "t_1", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING}, [][]interface{}{{int32(1), "a"}}}},
		mockBinlogEvent{typ: Update, args: []interface{}{uint64(8), "test_1", "t_1", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING}, [][]interface{}{{int32(2), "b"}, {int32(1), "b"}}}},

		mockBinlogEvent{typ: TableCreate, args: []interface{}{"test_1", "create table test_1.t_3(id int primary key, name varchar(24))"}},
		mockBinlogEvent{typ: DDL, args: []interface{}{"test_1", "alter table test_1.t_3 drop primary key"}},
		mockBinlogEvent{typ: DDL, args: []interface{}{"test_1", "alter table test_1.t_3 add primary key(id, name)"}},
	}

	mockStreamerProducer := &MockStreamProducer{s.generateEvents(events1, c)}
	mockStreamer, err := mockStreamerProducer.generateStreamer(binlog.NewLocation(""))
	c.Assert(err, IsNil)
	syncer.streamerController = &StreamerController{
		streamerProducer: mockStreamerProducer,
		streamer:         mockStreamer,
	}
	syncer.checkpointFlushWorker = &checkpointFlushWorker{
		input:        make(chan *checkpointFlushTask, 16),
		cp:           syncer.checkpoint,
		execError:    &syncer.execError,
		afterFlushFn: syncer.afterFlushCheckpoint,
		addCountFunc: func(bool, string, opType, int64, *filter.Table) {},
	}

	syncer.handleJobFunc = syncer.addJobToMemory

	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan pb.ProcessResult)

	go syncer.Process(ctx, resultCh)

	expectJobs1 := []*expectJob{
		// now every ddl job will start with a flush job
		{
			flush,
			nil,
			nil,
		}, {
			ddl,
			[]string{"CREATE DATABASE IF NOT EXISTS `test_1`"},
			nil,
		}, {
			flush,
			nil,
			nil,
		}, {
			ddl,
			[]string{"CREATE TABLE IF NOT EXISTS `test_1`.`t_1` (`id` INT PRIMARY KEY,`name` VARCHAR(24))"},
			nil,
		}, {
			flush,
			nil,
			nil,
		}, {
			ddl,
			[]string{"CREATE TABLE IF NOT EXISTS `test_1`.`t_2` (`id` INT PRIMARY KEY,`name` VARCHAR(24))"},
			nil,
		}, {
			insert,
			[]string{"REPLACE INTO `test_1`.`t_1` (`id`,`name`) VALUES (?,?)"},
			[][]interface{}{{int64(580981944116838401), "a"}},
		}, {
			flush,
			nil,
			nil,
		}, {
			ddl,
			[]string{"ALTER TABLE `test_1`.`t_1` ADD INDEX `index1`(`name`)"},
			nil,
		}, {
			insert,
			[]string{"REPLACE INTO `test_1`.`t_1` (`id`,`name`) VALUES (?,?)"},
			[][]interface{}{{int64(580981944116838402), "b"}},
		}, {
			del,
			[]string{"DELETE FROM `test_1`.`t_1` WHERE `id` = ? LIMIT 1"},
			[][]interface{}{{int64(580981944116838401)}},
		}, {
			// safe mode is true, will split update to delete + replace
			update,
			[]string{"DELETE FROM `test_1`.`t_1` WHERE `id` = ? LIMIT 1", "REPLACE INTO `test_1`.`t_1` (`id`,`name`) VALUES (?,?)"},
			[][]interface{}{{int64(580981944116838402)}, {int64(580981944116838401), "b"}},
		}, {
			flush,
			nil,
			nil,
		}, {
			ddl,
			[]string{"CREATE TABLE IF NOT EXISTS `test_1`.`t_3` (`id` INT PRIMARY KEY,`name` VARCHAR(24))"},
			nil,
		}, {
			flush,
			nil,
			nil,
		}, {
			ddl,
			[]string{"ALTER TABLE `test_1`.`t_3` DROP PRIMARY KEY"},
			nil,
		}, {
			flush,
			nil,
			nil,
		}, {
			ddl,
			[]string{"ALTER TABLE `test_1`.`t_3` ADD PRIMARY KEY(`id`, `name`)"},
			nil,
		}, {
			flush,
			nil,
			nil,
		},
	}

	executeSQLAndWait(len(expectJobs1))
	c.Assert(syncer.Status(nil).(*pb.SyncStatus).TotalEvents, Equals, int64(0))
	syncer.mockFinishJob(expectJobs1)

	testJobs.Lock()
	checkJobs(c, testJobs.jobs, expectJobs1)
	testJobs.jobs = testJobs.jobs[:0]
	testJobs.Unlock()

	s.cfg.ColumnMappingRules = nil
	s.cfg.RouteRules = []*router.TableRule{
		{
			SchemaPattern: "test_1",
			TablePattern:  "t_1",
			TargetSchema:  "test_1",
			TargetTable:   "t_2",
		},
	}

	cancel()
	<-resultCh // wait for the process to finish
	// when syncer exit Run(), will flush job
	syncer.Pause()

	mockDBProvider := conn.InitMockDB(c)
	mockDBProvider.ExpectQuery("SELECT cast\\(TIMEDIFF\\(NOW\\(6\\), UTC_TIMESTAMP\\(6\\)\\) as time\\);").
		WillReturnRows(sqlmock.NewRows([]string{""}).AddRow("01:00:00"))
	c.Assert(syncer.Update(context.Background(), s.cfg), IsNil)
	c.Assert(syncer.timezone.String(), Equals, "+01:00")

	events2 := mockBinlogEvents{
		mockBinlogEvent{typ: Write, args: []interface{}{uint64(8), "test_1", "t_1", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING}, [][]interface{}{{int32(3), "c"}}}},
		mockBinlogEvent{typ: Delete, args: []interface{}{uint64(8), "test_1", "t_1", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING}, [][]interface{}{{int32(3), "c"}}}},
	}

	ctx, cancel = context.WithCancel(context.Background())
	resultCh = make(chan pb.ProcessResult)
	// simulate `syncer.Resume` here, but doesn't reset database conns
	syncer.reset()
	mockStreamerProducer = &MockStreamProducer{s.generateEvents(events2, c)}
	mockStreamer, err = mockStreamerProducer.generateStreamer(binlog.NewLocation(""))
	c.Assert(err, IsNil)
	syncer.streamerController = &StreamerController{
		streamerProducer: mockStreamerProducer,
		streamer:         mockStreamer,
	}
	syncer.checkpointFlushWorker = &checkpointFlushWorker{
		input:        make(chan *checkpointFlushTask, 16),
		cp:           syncer.checkpoint,
		execError:    &syncer.execError,
		afterFlushFn: syncer.afterFlushCheckpoint,
		addCountFunc: func(bool, string, opType, int64, *filter.Table) {},
	}

	// When crossing safeModeExitPoint, will generate a flush sql
	checkPointMock.ExpectBegin()
	checkPointMock.ExpectExec(".*INSERT INTO .* VALUES.* ON DUPLICATE KEY UPDATE.*").WillReturnResult(sqlmock.NewResult(0, 1))
	checkPointMock.ExpectCommit()
	// Simulate resume from syncer, last time we exit successfully, so we shouldn't open safe mode here
	go syncer.Process(ctx, resultCh)

	expectJobs2 := []*expectJob{
		{
			insert,
			[]string{"INSERT INTO `test_1`.`t_2` (`id`,`name`) VALUES (?,?)"},
			[][]interface{}{{int64(3), "c"}},
		}, {
			del,
			[]string{"DELETE FROM `test_1`.`t_2` WHERE `id` = ? LIMIT 1"},
			[][]interface{}{{int64(3)}},
		}, {
			flush,
			nil,
			nil,
		},
	}

	executeSQLAndWait(len(expectJobs2))
	c.Assert(syncer.Status(nil).(*pb.SyncStatus).TotalEvents, Equals, int64(len(expectJobs1)))
	syncer.mockFinishJob(expectJobs2)
	c.Assert(syncer.Status(nil).(*pb.SyncStatus).TotalEvents, Equals, int64(len(expectJobs1)+len(expectJobs2)))

	testJobs.RLock()
	checkJobs(c, testJobs.jobs, expectJobs2)
	testJobs.RUnlock()

	cancel()
	<-resultCh // wait for the process to finish
	syncer.Close()
	c.Assert(syncer.isClosed(), IsTrue)

	if err := mock.ExpectationsWereMet(); err != nil {
		c.Errorf("db unfulfilled expectations: %s", err)
	}

	if err := checkPointMock.ExpectationsWereMet(); err != nil {
		c.Errorf("checkpointDB unfulfilled expectations: %s", err)
	}
}

func (s *testSyncerSuite) TestExitSafeModeByConfig(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	s.mockGetServerUnixTS(mock)

	dbConn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	checkPointDB, checkPointMock, err := sqlmock.New()
	c.Assert(err, IsNil)
	checkPointDBConn, err := checkPointDB.Conn(context.Background())
	c.Assert(err, IsNil)

	testJobs.jobs = testJobs.jobs[:0]

	s.cfg.BAList = &filter.Rules{
		DoDBs: []string{"test_1"},
		DoTables: []*filter.Table{
			{Schema: "test_1", Name: "t_1"},
		},
	}

	cfg, err := s.cfg.Clone()
	c.Assert(err, IsNil)
	syncer := NewSyncer(cfg, nil, nil)
	syncer.fromDB = &dbconn.UpStreamConn{BaseDB: conn.NewBaseDB(db)}
	syncer.toDBConns = []*dbconn.DBConn{
		{Cfg: s.cfg, BaseConn: conn.NewBaseConn(dbConn, &retry.FiniteRetryStrategy{})},
		{Cfg: s.cfg, BaseConn: conn.NewBaseConn(dbConn, &retry.FiniteRetryStrategy{})},
	}
	syncer.ddlDBConn = &dbconn.DBConn{Cfg: s.cfg, BaseConn: conn.NewBaseConn(dbConn, &retry.FiniteRetryStrategy{})}
	syncer.downstreamTrackConn = &dbconn.DBConn{Cfg: s.cfg, BaseConn: conn.NewBaseConn(dbConn, &retry.FiniteRetryStrategy{})}
	mock.ExpectBegin()
	mock.ExpectExec(fmt.Sprintf("SET SESSION SQL_MODE = '%s'", pmysql.DefaultSQLMode)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	mock.ExpectQuery("SHOW CREATE TABLE " + "`test_1`.`t_1`").WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("t_1", "create table t_1(id int primary key, name varchar(24))"))

	syncer.schemaTracker, err = schema.NewTracker(context.Background(), s.cfg.Name, defaultTestSessionCfg, syncer.ddlDBConn)
	syncer.exprFilterGroup = NewExprFilterGroup(utils.NewSessionCtx(nil), nil)
	c.Assert(err, IsNil)
	c.Assert(syncer.Type(), Equals, pb.UnitType_Sync)

	c.Assert(syncer.genRouter(), IsNil)

	syncer.setupMockCheckpoint(c, checkPointDBConn, checkPointMock)

	syncer.reset()

	events1 := mockBinlogEvents{
		mockBinlogEvent{typ: DBCreate, args: []interface{}{"test_1"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"test_1", "create table test_1.t_1(id int primary key, name varchar(24))"}},

		mockBinlogEvent{typ: Write, args: []interface{}{uint64(8), "test_1", "t_1", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING}, [][]interface{}{{int32(1), "a"}}}},
		mockBinlogEvent{typ: Delete, args: []interface{}{uint64(8), "test_1", "t_1", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING}, [][]interface{}{{int32(1), "a"}}}},
		mockBinlogEvent{typ: Update, args: []interface{}{uint64(8), "test_1", "t_1", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING}, [][]interface{}{{int32(2), "b"}, {int32(1), "b"}}}},
	}

	generatedEvents1 := s.generateEvents(events1, c)
	// make sure [18] is last event, and use [18]'s position as safeModeExitLocation
	c.Assert(len(generatedEvents1), Equals, 19)
	safeModeExitLocation := binlog.NewLocation("")
	safeModeExitLocation.Position.Pos = generatedEvents1[18].Header.LogPos
	syncer.checkpoint.SaveSafeModeExitPoint(&safeModeExitLocation)

	// check after safeModeExitLocation, safe mode is turned off
	events2 := mockBinlogEvents{
		mockBinlogEvent{typ: Write, args: []interface{}{uint64(8), "test_1", "t_1", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING}, [][]interface{}{{int32(1), "a"}}}},
		mockBinlogEvent{typ: Delete, args: []interface{}{uint64(8), "test_1", "t_1", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING}, [][]interface{}{{int32(1), "a"}}}},
		mockBinlogEvent{typ: Update, args: []interface{}{uint64(8), "test_1", "t_1", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING}, [][]interface{}{{int32(2), "b"}, {int32(1), "b"}}}},
	}
	generatedEvents2 := s.generateEvents(events2, c)

	generatedEvents := generatedEvents1
	generatedEvents = append(generatedEvents, generatedEvents2...)

	mockStreamerProducer := &MockStreamProducer{generatedEvents}
	mockStreamer, err := mockStreamerProducer.generateStreamer(binlog.NewLocation(""))
	c.Assert(err, IsNil)
	syncer.streamerController = &StreamerController{
		streamerProducer: mockStreamerProducer,
		streamer:         mockStreamer,
	}
	syncer.checkpointFlushWorker = &checkpointFlushWorker{
		input:        make(chan *checkpointFlushTask, 16),
		cp:           syncer.checkpoint,
		execError:    &syncer.execError,
		afterFlushFn: syncer.afterFlushCheckpoint,
		addCountFunc: func(bool, string, opType, int64, *filter.Table) {},
	}

	syncer.handleJobFunc = syncer.addJobToMemory

	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan pb.ProcessResult)

	// When crossing safeModeExitPoint, will generate a flush sql
	checkPointMock.ExpectBegin()
	checkPointMock.ExpectExec(".*INSERT INTO .* VALUES.* ON DUPLICATE KEY UPDATE.*").WillReturnResult(sqlmock.NewResult(0, 1))
	checkPointMock.ExpectCommit()
	// disable 1-minute safe mode
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/syncer/SafeModeInitPhaseSeconds", "return(0)"), IsNil)
	go syncer.Process(ctx, resultCh)

	expectJobs := []*expectJob{
		// now every ddl job will start with a flush job
		{
			flush,
			nil,
			nil,
		}, {
			ddl,
			[]string{"CREATE DATABASE IF NOT EXISTS `test_1`"},
			nil,
		}, {
			flush,
			nil,
			nil,
		}, {
			ddl,
			[]string{"CREATE TABLE IF NOT EXISTS `test_1`.`t_1` (`id` INT PRIMARY KEY,`name` VARCHAR(24))"},
			nil,
		}, {
			insert,
			[]string{"REPLACE INTO `test_1`.`t_1` (`id`,`name`) VALUES (?,?)"},
			[][]interface{}{{int64(1), "a"}},
		}, {
			del,
			[]string{"DELETE FROM `test_1`.`t_1` WHERE `id` = ? LIMIT 1"},
			[][]interface{}{{int64(1)}},
		}, {
			update,
			[]string{"DELETE FROM `test_1`.`t_1` WHERE `id` = ? LIMIT 1", "REPLACE INTO `test_1`.`t_1` (`id`,`name`) VALUES (?,?)"},
			[][]interface{}{{int64(2)}, {int64(1), "b"}},
		}, {
			// start from this event, location passes safeModeExitLocation and safe mode should exit
			insert,
			[]string{"INSERT INTO `test_1`.`t_1` (`id`,`name`) VALUES (?,?)"},
			[][]interface{}{{int64(1), "a"}},
		}, {
			del,
			[]string{"DELETE FROM `test_1`.`t_1` WHERE `id` = ? LIMIT 1"},
			[][]interface{}{{int64(1)}},
		}, {
			update,
			[]string{"UPDATE `test_1`.`t_1` SET `id` = ?, `name` = ? WHERE `id` = ? LIMIT 1"},
			[][]interface{}{{int64(1), "b", int64(2)}},
		}, {
			flush,
			nil,
			nil,
		},
	}

	executeSQLAndWait(len(expectJobs))
	c.Assert(syncer.Status(nil).(*pb.SyncStatus).TotalEvents, Equals, int64(0))
	syncer.mockFinishJob(expectJobs)

	testJobs.Lock()
	checkJobs(c, testJobs.jobs, expectJobs)
	testJobs.jobs = testJobs.jobs[:0]
	testJobs.Unlock()

	cancel()
	syncer.Close()
	c.Assert(syncer.isClosed(), IsTrue)

	if err := mock.ExpectationsWereMet(); err != nil {
		c.Errorf("db unfulfilled expectations: %s", err)
	}

	if err := checkPointMock.ExpectationsWereMet(); err != nil {
		c.Errorf("checkpointDB unfulfilled expectations: %s", err)
	}
	c.Assert(failpoint.Disable("github.com/pingcap/tiflow/dm/syncer/SafeModeInitPhaseSeconds"), IsNil)
}

func (s *testSyncerSuite) TestRemoveMetadataIsFine(c *C) {
	cfg, err := s.cfg.Clone()
	c.Assert(err, IsNil)
	cfg.Mode = config.ModeAll
	syncer := NewSyncer(cfg, nil, nil)
	fresh, err := syncer.IsFreshTask(context.Background())
	c.Assert(err, IsNil)
	c.Assert(fresh, IsTrue)

	filename := filepath.Join(s.cfg.Dir, "metadata")
	err = os.WriteFile(filename, []byte("SHOW MASTER STATUS:\n\tLog: BAD METADATA"), 0o644)
	c.Assert(err, IsNil)
	c.Assert(syncer.checkpoint.LoadMeta(), NotNil)

	err = os.WriteFile(filename, []byte("SHOW MASTER STATUS:\n\tLog: mysql-bin.000003\n\tPos: 1234\n\tGTID:\n\n"), 0o644)
	c.Assert(err, IsNil)
	c.Assert(syncer.checkpoint.LoadMeta(), IsNil)

	c.Assert(os.Remove(filename), IsNil)

	// after successful LoadMeta, IsFreshTask should return false so don't load again
	fresh, err = syncer.IsFreshTask(context.Background())
	c.Assert(err, IsNil)
	c.Assert(fresh, IsFalse)
}

func (s *testSyncerSuite) TestTrackDDL(c *C) {
	var (
		testDB   = "test_db"
		testTbl  = "test_tbl"
		testTbl2 = "test_tbl2"
		ec       = &eventContext{tctx: tcontext.Background()}
		qec      = &queryEventContext{
			eventContext:    ec,
			ddlSchema:       testDB,
			p:               parser.New(),
			eventStatusVars: []byte{4, 0, 0, 0, 0, 46, 0},
		}
	)
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	dbConn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)

	checkPointDB, checkPointMock, err := sqlmock.New()
	c.Assert(err, IsNil)
	checkPointDBConn, err := checkPointDB.Conn(context.Background())
	c.Assert(err, IsNil)

	cfg, err := s.cfg.Clone()
	c.Assert(err, IsNil)
	syncer := NewSyncer(cfg, nil, nil)
	syncer.toDBConns = []*dbconn.DBConn{
		{Cfg: s.cfg, BaseConn: conn.NewBaseConn(dbConn, &retry.FiniteRetryStrategy{})},
		{Cfg: s.cfg, BaseConn: conn.NewBaseConn(dbConn, &retry.FiniteRetryStrategy{})},
	}
	syncer.ddlDBConn = &dbconn.DBConn{Cfg: s.cfg, BaseConn: conn.NewBaseConn(dbConn, &retry.FiniteRetryStrategy{})}
	syncer.checkpoint.(*RemoteCheckPoint).dbConn = &dbconn.DBConn{Cfg: s.cfg, BaseConn: conn.NewBaseConn(checkPointDBConn, &retry.FiniteRetryStrategy{})}
	syncer.schemaTracker, err = schema.NewTracker(context.Background(), s.cfg.Name, defaultTestSessionCfg, syncer.ddlDBConn)
	syncer.exprFilterGroup = NewExprFilterGroup(utils.NewSessionCtx(nil), nil)
	c.Assert(syncer.genRouter(), IsNil)
	c.Assert(err, IsNil)

	cases := []struct {
		sql      string
		callback func()
	}{
		{"CREATE DATABASE IF NOT EXISTS " + testDB, func() {}},
		{"ALTER DATABASE " + testDB + " DEFAULT COLLATE utf8_bin", func() {}},
		{"DROP DATABASE IF EXISTS " + testDB, func() {}},
		{fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (c int)", testDB, testTbl), func() {}},
		{fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", testDB, testTbl), func() {}},
		{"CREATE INDEX idx1 ON " + testTbl + " (c)", func() {
			mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(
				sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
			mock.ExpectQuery("SHOW CREATE TABLE.*").WillReturnRows(
				sqlmock.NewRows([]string{"Table", "Create Table"}).
					AddRow(testTbl, " CREATE TABLE `"+testTbl+"` (\n  `c` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
		}},
		{fmt.Sprintf("ALTER TABLE %s.%s add c2 int", testDB, testTbl), func() {
			mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(
				sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
			mock.ExpectQuery("SHOW CREATE TABLE.*").WillReturnRows(
				sqlmock.NewRows([]string{"Table", "Create Table"}).
					AddRow(testTbl, " CREATE TABLE `"+testTbl+"` (\n  `c` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
		}},

		// alter add FK will not executed on tracker (otherwise will report error tb2 not exist)
		{fmt.Sprintf("ALTER TABLE %s.%s add constraint foreign key (c) references tb2(c)", testDB, testTbl), func() {
			mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(
				sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
			mock.ExpectQuery("SHOW CREATE TABLE.*").WillReturnRows(
				sqlmock.NewRows([]string{"Table", "Create Table"}).
					AddRow(testTbl, " CREATE TABLE `"+testTbl+"` (\n  `c` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
		}},
		{"TRUNCATE TABLE " + testTbl, func() {}},

		// test CREATE TABLE that reference another table
		{fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s LIKE %s", testDB, testTbl, testTbl2), func() {
			mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(
				sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
			mock.ExpectQuery("SHOW CREATE TABLE.*").WillReturnRows(
				sqlmock.NewRows([]string{"Table", "Create Table"}).
					AddRow(testTbl, " CREATE TABLE `"+testTbl+"` (\n  `c` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
		}},

		// 'CREATE TABLE ... SELECT' is not implemented yet
		// {fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s AS SELECT * FROM %s, %s.%s WHERE %s.n=%s.%s.n", testDB, testTbl, testTbl2, testDB2, testTbl3, testTbl2, testDB2, testTbl3), func() {
		//	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(
		//		sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
		//	mock.ExpectQuery(fmt.Sprintf("SHOW CREATE TABLE \\`%s\\`.\\`%s\\`.*", testDB, testTbl2)).WillReturnRows(
		//		sqlmock.NewRows([]string{"Table", "Create Table"}).
		//			AddRow(testTbl, " CREATE TABLE `"+testTbl+"` (\n  `c` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
		//	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(
		//		sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
		//	mock.ExpectQuery(fmt.Sprintf("SHOW CREATE TABLE \\`%s\\`.\\`%s\\`.*", testDB2, testTbl3)).WillReturnRows(
		//		sqlmock.NewRows([]string{"Table", "Create Table"}).
		//			AddRow(testTbl, " CREATE TABLE `"+testTbl+"` (\n  `c` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
		// }},

		// test RENAME TABLE
		{fmt.Sprintf("RENAME TABLE %s.%s TO %s.%s", testDB, testTbl, testDB, testTbl2), func() {
			mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(
				sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
			mock.ExpectQuery("SHOW CREATE TABLE.*").WillReturnRows(
				sqlmock.NewRows([]string{"Table", "Create Table"}).
					AddRow(testTbl, " CREATE TABLE `"+testTbl+"` (\n  `c` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
		}},
		{fmt.Sprintf("ALTER TABLE %s.%s RENAME %s.%s", testDB, testTbl, testDB, testTbl2), func() {
			mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(
				sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
			mock.ExpectQuery("SHOW CREATE TABLE.*").WillReturnRows(
				sqlmock.NewRows([]string{"Table", "Create Table"}).
					AddRow(testTbl, " CREATE TABLE `"+testTbl+"` (\n  `c` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
		}},
	}

	for _, ca := range cases {
		ddlInfo, err := syncer.genDDLInfo(qec, ca.sql)
		c.Assert(err, IsNil)
		ca.callback()

		c.Assert(syncer.trackDDL(testDB, ddlInfo, ec), IsNil)
		c.Assert(syncer.schemaTracker.Reset(), IsNil)
		c.Assert(mock.ExpectationsWereMet(), IsNil)
		c.Assert(checkPointMock.ExpectationsWereMet(), IsNil)
	}
}

func checkEventWithTableResult(c *C, syncer *Syncer, allEvents []*replication.BinlogEvent, p *parser.Parser, res [][]bool) {
	i := 0
	tctx := tcontext.Background().WithLogger(log.With(zap.String("test", "checkEventWithTableResult")))
	ec := &eventContext{
		tctx: tctx,
	}
	statusVars := []byte{4, 0, 0, 0, 0, 46, 0}
	for _, e := range allEvents {
		switch ev := e.Event.(type) {
		case *replication.QueryEvent:
			qec := &queryEventContext{
				eventContext:    ec,
				originSQL:       string(ev.Query),
				ddlSchema:       string(ev.Schema),
				p:               p,
				eventStatusVars: statusVars,
			}
			stmt, err := parseOneStmt(qec)
			c.Assert(err, IsNil)

			if _, ok := stmt.(ast.DDLNode); !ok {
				continue // BEGIN event
			}
			qec.splitDDLs, err = parserpkg.SplitDDL(stmt, qec.ddlSchema)
			c.Assert(err, IsNil)
			for _, sql := range qec.splitDDLs {
				sqls, err := syncer.processOneDDL(qec, sql)
				c.Assert(err, IsNil)
				qec.appliedDDLs = append(qec.appliedDDLs, sqls...)
			}
			if len(qec.appliedDDLs) == 0 {
				c.Assert(res[i], HasLen, 1)
				c.Assert(res[i][0], Equals, true)
				i++
				continue
			}

			for j, sql := range qec.appliedDDLs {
				ddlInfo, err := syncer.genDDLInfo(qec, sql)
				c.Assert(err, IsNil)

				needSkip, err := syncer.skipQueryEvent(qec, ddlInfo)
				c.Assert(err, IsNil)
				c.Assert(needSkip, Equals, res[i][j])
			}
		case *replication.RowsEvent:
			table := &filter.Table{
				Schema: string(ev.Table.Schema),
				Name:   string(ev.Table.Table),
			}
			needSkip, err := syncer.skipRowsEvent(table, e.Header.EventType)
			c.Assert(err, IsNil)
			c.Assert(needSkip, Equals, res[i][0])
		default:
			continue
		}
		i++
	}
}

func executeSQLAndWait(expectJobNum int) {
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)

		testJobs.RLock()
		jobNum := len(testJobs.jobs)
		testJobs.RUnlock()

		if jobNum >= expectJobNum {
			break
		}
	}
}

type expectJob struct {
	tp       opType
	sqlInJob []string
	args     [][]interface{}
}

func checkJobs(c *C, jobs []*job, expectJobs []*expectJob) {
	c.Assert(len(jobs), Equals, len(expectJobs), Commentf("jobs = %q", jobs))
	for i, job := range jobs {
		c.Assert(job.tp, Equals, expectJobs[i].tp)
		if job.tp == ddl {
			c.Assert(job.ddls, DeepEquals, expectJobs[i].sqlInJob)
		} else if job.tp == insert || job.tp == update || job.tp == del {
			sqls, args := job.dml.genSQL()
			c.Assert(sqls, DeepEquals, expectJobs[i].sqlInJob)
			c.Assert(args, DeepEquals, expectJobs[i].args)
		}
	}
}

var testJobs struct {
	sync.RWMutex
	jobs []*job
}

func (s *Syncer) mockFinishJob(jobs []*expectJob) {
	for _, job := range jobs {
		switch job.tp {
		case ddl, insert, update, del, flush:
			s.addCount(true, "test", job.tp, 1, &filter.Table{})
		}
	}
}

func (s *Syncer) addJobToMemory(job *job) (bool, error) {
	log.L().Info("add job to memory", zap.Stringer("job", job))

	switch job.tp {
	case ddl, insert, update, del, flush:
		s.addCount(false, "test", job.tp, 1, &filter.Table{})
		testJobs.Lock()
		testJobs.jobs = append(testJobs.jobs, job)
		testJobs.Unlock()
	}

	switch job.tp {
	case xid:
		s.saveGlobalPoint(job.location)
		s.checkpoint.(*RemoteCheckPoint).globalPoint.flush()
	case ddl:
		s.saveGlobalPoint(job.location)
		s.checkpoint.(*RemoteCheckPoint).globalPoint.flush()
		for sourceSchema, tbs := range job.sourceTbls {
			if len(sourceSchema) == 0 {
				continue
			}
			for _, sourceTable := range tbs {
				s.saveTablePoint(sourceTable, job.location)
				s.checkpoint.(*RemoteCheckPoint).points[sourceSchema][sourceTable.Name].flush()
			}
		}
		s.resetShardingGroup(job.targetTable)
	case insert, update, del:
		for sourceSchema, tbs := range job.sourceTbls {
			if len(sourceSchema) == 0 {
				continue
			}
			for _, sourceTable := range tbs {
				s.saveTablePoint(sourceTable, job.currentLocation)
				s.checkpoint.(*RemoteCheckPoint).points[sourceSchema][sourceTable.Name].flush()
			}
		}
	}

	return true, nil
}

func (s *Syncer) setupMockCheckpoint(c *C, checkPointDBConn *sql.Conn, checkPointMock sqlmock.Sqlmock) {
	checkPointMock.ExpectBegin()
	checkPointMock.ExpectExec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS `%s`", s.cfg.MetaSchema)).WillReturnResult(sqlmock.NewResult(1, 1))
	checkPointMock.ExpectCommit()
	checkPointMock.ExpectBegin()
	checkPointMock.ExpectExec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s`", s.cfg.MetaSchema, cputil.SyncerCheckpoint(s.cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	checkPointMock.ExpectCommit()

	// mock syncer.checkpoint.Init() function
	s.checkpoint.(*RemoteCheckPoint).dbConn = &dbconn.DBConn{Cfg: s.cfg, BaseConn: conn.NewBaseConn(checkPointDBConn, &retry.FiniteRetryStrategy{})}
	// mock syncer.flushCpWorker init
	s.checkpointFlushWorker = &checkpointFlushWorker{
		input:        nil,
		cp:           s.checkpoint,
		execError:    &s.execError,
		afterFlushFn: s.afterFlushCheckpoint,
		addCountFunc: func(bool, string, opType, int64, *filter.Table) {},
	}
	c.Assert(s.checkpoint.(*RemoteCheckPoint).prepare(tcontext.Background()), IsNil)
	// disable flush checkpoint periodically
	s.checkpoint.(*RemoteCheckPoint).globalPointSaveTime = time.Now()
}

func (s *testSyncerSuite) TestTrackDownstreamTableWontOverwrite(c *C) {
	syncer := Syncer{}
	ctx := context.Background()
	tctx := tcontext.Background()

	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	dbConn, err := db.Conn(ctx)
	c.Assert(err, IsNil)
	baseConn := conn.NewBaseConn(dbConn, &retry.FiniteRetryStrategy{})
	syncer.ddlDBConn = &dbconn.DBConn{Cfg: s.cfg, BaseConn: baseConn}
	syncer.downstreamTrackConn = &dbconn.DBConn{Cfg: s.cfg, BaseConn: conn.NewBaseConn(dbConn, &retry.FiniteRetryStrategy{})}
	syncer.schemaTracker, err = schema.NewTracker(ctx, s.cfg.Name, defaultTestSessionCfg, syncer.downstreamTrackConn)
	c.Assert(err, IsNil)

	upTable := &filter.Table{
		Schema: "test",
		Name:   "up",
	}
	downTable := &filter.Table{
		Schema: "test",
		Name:   "down",
	}
	createTableSQL := "CREATE TABLE up (c1 int, c2 int);"

	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(
		sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE.*").WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow(downTable.Name, " CREATE TABLE `"+downTable.Name+"` (\n  `c` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	c.Assert(syncer.schemaTracker.CreateSchemaIfNotExists(upTable.Schema), IsNil)
	c.Assert(syncer.schemaTracker.Exec(ctx, "test", createTableSQL), IsNil)
	ti, err := syncer.getTableInfo(tctx, upTable, downTable)
	c.Assert(err, IsNil)
	c.Assert(ti.Columns, HasLen, 2)
	c.Assert(syncer.trackTableInfoFromDownstream(tctx, upTable, downTable), IsNil)
	newTi, err := syncer.getTableInfo(tctx, upTable, downTable)
	c.Assert(err, IsNil)
	c.Assert(newTi, DeepEquals, ti)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testSyncerSuite) TestDownstreamTableHasAutoRandom(c *C) {
	syncer := Syncer{}
	ctx := context.Background()
	tctx := tcontext.Background()

	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	dbConn, err := db.Conn(ctx)
	c.Assert(err, IsNil)
	baseConn := conn.NewBaseConn(dbConn, &retry.FiniteRetryStrategy{})
	syncer.ddlDBConn = &dbconn.DBConn{Cfg: s.cfg, BaseConn: baseConn}
	syncer.downstreamTrackConn = &dbconn.DBConn{Cfg: s.cfg, BaseConn: conn.NewBaseConn(dbConn, &retry.FiniteRetryStrategy{})}
	syncer.schemaTracker, err = schema.NewTracker(ctx, s.cfg.Name, defaultTestSessionCfg, syncer.downstreamTrackConn)
	c.Assert(err, IsNil)

	schemaName := "test"
	tableName := "tbl"
	table := &filter.Table{
		Schema: "test",
		Name:   "tbl",
	}

	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(
		sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	// create table t (c bigint primary key auto_random);
	mock.ExpectQuery("SHOW CREATE TABLE.*").WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow(tableName, " CREATE TABLE `"+tableName+"` (\n  `c` bigint(20) NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,\n  PRIMARY KEY (`c`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	c.Assert(syncer.schemaTracker.CreateSchemaIfNotExists(schemaName), IsNil)
	c.Assert(syncer.trackTableInfoFromDownstream(tctx, table, table), IsNil)
	ti, err := syncer.getTableInfo(tctx, table, table)
	c.Assert(err, IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)

	c.Assert(syncer.schemaTracker.DropTable(table), IsNil)
	sql := "create table tbl (c bigint primary key);"
	c.Assert(syncer.schemaTracker.Exec(ctx, schemaName, sql), IsNil)
	ti2, err := syncer.getTableInfo(tctx, table, table)
	c.Assert(err, IsNil)

	ti.ID = ti2.ID
	ti.UpdateTS = ti2.UpdateTS

	c.Assert(ti, DeepEquals, ti2)

	// test if user set ON clustered index, no need to modify
	sessionCfg := map[string]string{
		"sql_mode":                "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION",
		"tidb_skip_utf8_check":    "0",
		schema.TiDBClusteredIndex: "ON",
	}
	syncer.schemaTracker, err = schema.NewTracker(ctx, s.cfg.Name, sessionCfg, syncer.downstreamTrackConn)
	c.Assert(err, IsNil)
	v, ok := syncer.schemaTracker.GetSystemVar(schema.TiDBClusteredIndex)
	c.Assert(v, Equals, "ON")
	c.Assert(ok, IsTrue)

	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(
		sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	// create table t (c bigint primary key auto_random);
	mock.ExpectQuery("SHOW CREATE TABLE.*").WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow(tableName, " CREATE TABLE `"+tableName+"` (\n  `c` bigint(20) NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,\n  PRIMARY KEY (`c`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	c.Assert(syncer.schemaTracker.CreateSchemaIfNotExists(schemaName), IsNil)
	c.Assert(syncer.trackTableInfoFromDownstream(tctx, table, table), IsNil)
	ti, err = syncer.getTableInfo(tctx, table, table)
	c.Assert(err, IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)

	c.Assert(syncer.schemaTracker.DropTable(table), IsNil)
	sql = "create table tbl (c bigint primary key auto_random);"
	c.Assert(syncer.schemaTracker.Exec(ctx, schemaName, sql), IsNil)
	ti2, err = syncer.getTableInfo(tctx, table, table)
	c.Assert(err, IsNil)

	ti.ID = ti2.ID
	ti.UpdateTS = ti2.UpdateTS

	c.Assert(ti, DeepEquals, ti2)
}

func (s *testSyncerSuite) TestExecuteSQLSWithIgnore(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	dbConn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	conn := &dbconn.DBConn{
		BaseConn: &conn.BaseConn{
			DBConn:        dbConn,
			RetryStrategy: &retry.FiniteRetryStrategy{},
		},
		Cfg: &config.SubTaskConfig{
			Name: "test",
		},
	}

	sqls := []string{"alter table t1 add column a int", "alter table t1 add column b int"}

	// will ignore the first error, and continue execute the second sql
	mock.ExpectBegin()
	mock.ExpectExec(sqls[0]).WillReturnError(newMysqlErr(uint16(infoschema.ErrColumnExists.Code()), "column a already exists"))
	mock.ExpectExec(sqls[1]).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	tctx := tcontext.Background().WithLogger(log.With(zap.String("test", "TestExecuteSQLSWithIgnore")))
	n, err := conn.ExecuteSQLWithIgnore(tctx, errorutil.IsIgnorableMySQLDDLError, sqls)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 2)

	// will return error when execute the first sql
	mock.ExpectBegin()
	mock.ExpectExec(sqls[0]).WillReturnError(newMysqlErr(uint16(infoschema.ErrColumnExists.Code()), "column a already exists"))
	mock.ExpectRollback()

	n, err = conn.ExecuteSQL(tctx, sqls)
	c.Assert(err, ErrorMatches, ".*column a already exists.*")
	c.Assert(n, Equals, 0)

	c.Assert(mock.ExpectationsWereMet(), IsNil)
}
