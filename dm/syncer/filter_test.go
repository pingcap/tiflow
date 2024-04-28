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

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	. "github.com/pingcap/check"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/schema"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
)

type testFilterSuite struct {
	baseConn *conn.BaseConn
	db       *sql.DB
}

var _ = Suite(&testFilterSuite{})

func (s *testFilterSuite) SetUpSuite(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	s.db = db
	mock.ExpectClose()
	con, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	s.baseConn = conn.NewBaseConnForTest(con, nil)
}

func (s *testFilterSuite) TearDownSuite(c *C) {
	c.Assert(s.baseConn.DBConn.Close(), IsNil)
	c.Assert(s.db.Close(), IsNil)
}

func (s *testFilterSuite) TestSkipQueryEvent(c *C) {
	cfg := &config.SubTaskConfig{
		Flavor: mysql.MySQLFlavor,
		BAList: &filter.Rules{
			IgnoreTables: []*filter.Table{{Schema: "s1", Name: "test"}},
		},
	}
	syncer := NewSyncer(cfg, nil, nil)
	c.Assert(syncer.genRouter(), IsNil)
	var err error
	syncer.baList, err = filter.New(syncer.cfg.CaseSensitive, syncer.cfg.BAList)
	c.Assert(err, IsNil)

	syncer.ddlDBConn = dbconn.NewDBConn(syncer.cfg, s.baseConn)
	syncer.schemaTracker, err = schema.NewTestTracker(context.Background(), syncer.cfg.Name, syncer.ddlDBConn, log.L())
	c.Assert(err, IsNil)
	defer syncer.schemaTracker.Close()
	syncer.exprFilterGroup = NewExprFilterGroup(tcontext.Background(), utils.NewSessionCtx(nil), nil)

	// test binlog filter
	filterRules := []*bf.BinlogEventRule{
		{
			SchemaPattern: "foo*",
			TablePattern:  "",
			Events:        []bf.EventType{bf.CreateTable},
			SQLPattern:    []string{"^create\\s+table"},
			Action:        bf.Ignore,
		},
	}
	syncer.binlogFilter, err = bf.NewBinlogEvent(false, filterRules)
	c.Assert(err, IsNil)

	cases := []struct {
		sql           string
		schema        string
		expectSkipped bool
		isEmptySQL    bool
	}{
		{
			// system table
			"create table mysql.test (id int)",
			"mysql",
			true,
			false,
		}, {
			// test filter one event
			"drop table foo.test",
			"foo",
			false,
			false,
		}, {
			"create table foo.test (id int)",
			"foo",
			true,
			true,
		}, {
			"rename table s1.test to s1.test1",
			"s1",
			true,
			false,
		}, {
			"rename table s1.test1 to s1.test",
			"s1",
			true,
			false,
		}, {
			"rename table s1.test1 to s1.test2",
			"s1",
			false,
			false,
		},
	}
	p := parser.New()

	loc := binlog.MustZeroLocation(mysql.MySQLFlavor)

	ddlWorker := NewDDLWorker(&syncer.tctx.Logger, syncer)
	for _, ca := range cases {
		qec := &queryEventContext{
			eventContext: &eventContext{
				tctx:         tcontext.Background(),
				lastLocation: loc,
			},
			p:         p,
			ddlSchema: ca.schema,
		}
		ddlInfo, err := ddlWorker.genDDLInfo(qec, ca.sql)
		c.Assert(err, IsNil)
		qec.ddlSchema = ca.schema
		qec.originSQL = ca.sql
		skipped, err2 := syncer.skipQueryEvent(qec, ddlInfo)
		c.Assert(err2, IsNil)
		c.Assert(skipped, Equals, ca.expectSkipped)
		c.Assert(len(ddlInfo.originDDL) == 0, Equals, ca.isEmptySQL)
	}
}

func (s *testFilterSuite) TestSkipRowsEvent(c *C) {
	syncer := &Syncer{}
	filterRules := []*bf.BinlogEventRule{
		{
			SchemaPattern: "foo*",
			TablePattern:  "",
			Events:        []bf.EventType{bf.InsertEvent},
			SQLPattern:    []string{""},
			Action:        bf.Ignore,
		},
	}
	var err error
	syncer.binlogFilter, err = bf.NewBinlogEvent(false, filterRules)
	c.Assert(err, IsNil)
	syncer.onlineDDL = mockOnlinePlugin{}

	cases := []struct {
		table     *filter.Table
		eventType replication.EventType
		expected  bool
	}{
		{
			// test un-realTable
			&filter.Table{Schema: "foo", Name: "_test_gho"},
			replication.UNKNOWN_EVENT,
			true,
		}, {
			// test filter one event
			&filter.Table{Schema: "foo", Name: "test"},
			replication.WRITE_ROWS_EVENTv0,
			true,
		}, {
			&filter.Table{Schema: "foo", Name: "test"},
			replication.UPDATE_ROWS_EVENTv0,
			false,
		}, {
			&filter.Table{Schema: "foo", Name: "test"},
			replication.DELETE_ROWS_EVENTv0,
			false,
		},
	}
	for _, ca := range cases {
		needSkip, err2 := syncer.skipRowsEvent(ca.table, ca.eventType)
		c.Assert(err2, IsNil)
		c.Assert(needSkip, Equals, ca.expected)
	}
}

func (s *testFilterSuite) TestSkipByFilter(c *C) {
	cfg := &config.SubTaskConfig{
		Flavor: mysql.MySQLFlavor,
		BAList: &filter.Rules{
			IgnoreDBs: []string{"s1"},
		},
	}
	syncer := NewSyncer(cfg, nil, nil)
	var err error
	syncer.baList, err = filter.New(syncer.cfg.CaseSensitive, syncer.cfg.BAList)
	c.Assert(err, IsNil)
	// test binlog filter
	filterRules := []*bf.BinlogEventRule{
		{
			// rule 1
			SchemaPattern: "*",
			TablePattern:  "",
			Events:        []bf.EventType{bf.DropTable},
			SQLPattern:    []string{"^drop\\s+table"},
			Action:        bf.Ignore,
		}, {
			// rule 2
			SchemaPattern: "foo*",
			TablePattern:  "",
			Events:        []bf.EventType{bf.CreateTable},
			SQLPattern:    []string{"^create\\s+table"},
			Action:        bf.Do,
		}, {
			// rule 3
			// compare to rule 2, finer granularity has higher priority
			SchemaPattern: "foo*",
			TablePattern:  "bar*",
			Events:        []bf.EventType{bf.CreateTable},
			SQLPattern:    []string{"^create\\s+table"},
			Action:        bf.Ignore,
		},
	}
	syncer.binlogFilter, err = bf.NewBinlogEvent(false, filterRules)
	c.Assert(err, IsNil)

	cases := []struct {
		sql           string
		table         *filter.Table
		eventType     bf.EventType
		expectSkipped bool
	}{
		{
			// test binlog filter
			"drop table tx.test",
			&filter.Table{Schema: "tx", Name: "test"},
			bf.DropTable,
			true,
		}, {
			"create table foo.test (id int)",
			&filter.Table{Schema: "foo", Name: "test"},
			bf.CreateTable,
			false,
		}, {
			"create table foo.bar (id int)",
			&filter.Table{Schema: "foo", Name: "bar"},
			bf.CreateTable,
			true,
		},
	}
	for _, ca := range cases {
		skipped, err2 := syncer.skipByFilter(ca.table, ca.eventType, ca.sql)
		c.Assert(err2, IsNil)
		c.Assert(skipped, Equals, ca.expectSkipped)
	}
}

func (s *testFilterSuite) TestSkipByTable(c *C) {
	cfg := &config.SubTaskConfig{
		Flavor: mysql.MySQLFlavor,
		BAList: &filter.Rules{
			IgnoreDBs: []string{"s1"},
		},
	}
	syncer := NewSyncer(cfg, nil, nil)
	var err error
	syncer.baList, err = filter.New(syncer.cfg.CaseSensitive, syncer.cfg.BAList)
	c.Assert(err, IsNil)

	cases := []struct {
		table    *filter.Table
		expected bool
	}{
		{
			// system table
			&filter.Table{Schema: "mysql", Name: "test"},
			true,
		}, {
			// test balist
			&filter.Table{Schema: "s1", Name: "test"},
			true,
		}, {
			// test balist
			&filter.Table{Schema: "s2", Name: "test"},
			false,
		},
	}
	for _, ca := range cases {
		needSkip := syncer.skipByTable(ca.table)
		c.Assert(needSkip, Equals, ca.expected)
	}
}
