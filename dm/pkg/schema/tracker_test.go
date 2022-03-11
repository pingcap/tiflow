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

package schema

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	timock "github.com/pingcap/tidb/util/mock"
	"go.uber.org/zap/zapcore"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	dlog "github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&trackerSuite{})

var defaultTestSessionCfg = map[string]string{"sql_mode": "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"}

type trackerSuite struct {
	dbConn     *dbconn.DBConn
	db         *sql.DB
	backupKeys []string
	cfg        *config.SubTaskConfig
}

func (s *trackerSuite) SetUpSuite(c *C) {
	s.cfg = &config.SubTaskConfig{}
	s.backupKeys = downstreamVars
	downstreamVars = []string{"sql_mode"}
	db, _, err := sqlmock.New()
	s.db = db
	c.Assert(err, IsNil)
	con, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	s.dbConn = &dbconn.DBConn{Cfg: s.cfg, BaseConn: conn.NewBaseConn(con, nil)}
}

func (s *trackerSuite) TearDownSuite(c *C) {
	s.db.Close()
	downstreamVars = s.backupKeys
}

func (s *trackerSuite) TestTiDBAndSessionCfg(c *C) {
	log.SetLevel(zapcore.ErrorLevel)
	table := &filter.Table{
		Schema: "testdb",
		Name:   "foo",
	}

	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	con, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	baseConn := conn.NewBaseConn(con, nil)
	dbConn := &dbconn.DBConn{Cfg: s.cfg, BaseConn: baseConn}
	// user give correct session config

	_, err = NewTracker(context.Background(), "test-tracker", defaultTestSessionCfg, dbConn)
	c.Assert(err, IsNil)

	// user give wrong session session, will return error
	sessionCfg := map[string]string{"sql_mode": "HaHa"}
	_, err = NewTracker(context.Background(), "test-tracker", sessionCfg, dbConn)
	c.Assert(err, NotNil)

	// discover session config failed, will return error
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(
		sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("sql_mode", "HaHa"))
	_, err = NewTracker(context.Background(), "test-tracker", nil, dbConn)
	c.Assert(err, NotNil)

	// empty or default config in downstream
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(
		sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("sql_mode", defaultTestSessionCfg["sql_mode"]))
	tracker, err := NewTracker(context.Background(), "test-tracker", nil, dbConn)
	c.Assert(err, IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
	err = tracker.Exec(context.Background(), "", "create database testdb;")
	c.Assert(err, IsNil)

	// found session config in downstream
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(
		sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("sql_mode", "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE"))
	tracker, err = NewTracker(context.Background(), "test-tracker", nil, dbConn)
	c.Assert(err, IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
	c.Assert(tracker.se.GetSessionVars().SQLMode.HasOnlyFullGroupBy(), IsTrue)
	c.Assert(tracker.se.GetSessionVars().SQLMode.HasStrictMode(), IsTrue)

	ctx := context.Background()
	err = tracker.Exec(ctx, "", "create database testdb;")
	c.Assert(err, IsNil)

	// Now create the table with ZERO_DATE
	err = tracker.Exec(ctx, "testdb", "create table foo (a varchar(255) primary key, b DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00')")
	c.Assert(err, NotNil)

	// user set session config, get tracker config from downstream
	// no `STRICT_TRANS_TABLES`, no error now
	sessionCfg = map[string]string{"sql_mode": "NO_ZERO_DATE,NO_ZERO_IN_DATE,ANSI_QUOTES"}
	tracker, err = NewTracker(context.Background(), "test-tracker", sessionCfg, dbConn)
	c.Assert(err, IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)

	err = tracker.Exec(ctx, "", "create database testdb;")
	c.Assert(err, IsNil)

	// Now create the table with ANSI_QUOTES and ZERO_DATE
	err = tracker.Exec(ctx, "testdb", "create table \"foo\" (a varchar(255) primary key, b DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00')")
	c.Assert(err, IsNil)

	cts, err := tracker.GetCreateTable(context.Background(), table)
	c.Assert(err, IsNil)
	c.Assert(cts, Equals, "CREATE TABLE \"foo\" ( \"a\" varchar(255) NOT NULL, \"b\" datetime NOT NULL DEFAULT '0000-00-00 00:00:00', PRIMARY KEY (\"a\") /*T![clustered_index] NONCLUSTERED */) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")

	// Drop one column from the table.
	err = tracker.Exec(ctx, "testdb", "alter table foo drop column \"b\"")
	c.Assert(err, IsNil)

	cts, err = tracker.GetCreateTable(context.Background(), table)
	c.Assert(err, IsNil)
	c.Assert(cts, Equals, "CREATE TABLE \"foo\" ( \"a\" varchar(255) NOT NULL, PRIMARY KEY (\"a\") /*T![clustered_index] NONCLUSTERED */) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")

	// test alter primary key
	err = tracker.Exec(ctx, "testdb", "alter table \"foo\" drop primary key")
	c.Assert(err, IsNil)

	// test user could specify tidb_enable_clustered_index in config
	sessionCfg = map[string]string{
		"sql_mode":                    "NO_ZERO_DATE,NO_ZERO_IN_DATE,ANSI_QUOTES",
		"tidb_enable_clustered_index": "ON",
	}
	tracker, err = NewTracker(context.Background(), "test-tracker", sessionCfg, dbConn)
	c.Assert(err, IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)

	err = tracker.Exec(ctx, "", "create database testdb;")
	c.Assert(err, IsNil)
	err = tracker.Exec(ctx, "testdb", "create table \"foo\" (a varchar(255) primary key)")
	c.Assert(err, IsNil)
	cts, err = tracker.GetCreateTable(context.Background(), table)
	c.Assert(err, IsNil)
	c.Assert(cts, Equals, "CREATE TABLE \"foo\" ( \"a\" varchar(255) NOT NULL, PRIMARY KEY (\"a\") /*T![clustered_index] CLUSTERED */) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
}

func (s *trackerSuite) TestDDL(c *C) {
	log.SetLevel(zapcore.ErrorLevel)
	table := &filter.Table{
		Schema: "testdb",
		Name:   "foo",
	}

	tracker, err := NewTracker(context.Background(), "test-tracker", defaultTestSessionCfg, s.dbConn)
	c.Assert(err, IsNil)

	// Table shouldn't exist before initialization.
	_, err = tracker.GetTableInfo(table)
	c.Assert(err, ErrorMatches, `.*Table 'testdb\.foo' doesn't exist`)
	c.Assert(IsTableNotExists(err), IsTrue)

	_, err = tracker.GetCreateTable(context.Background(), table)
	c.Assert(err, ErrorMatches, `.*Table 'testdb\.foo' doesn't exist`)
	c.Assert(IsTableNotExists(err), IsTrue)

	ctx := context.Background()
	err = tracker.Exec(ctx, "", "create database testdb;")
	c.Assert(err, IsNil)

	_, err = tracker.GetTableInfo(table)
	c.Assert(err, ErrorMatches, `.*Table 'testdb\.foo' doesn't exist`)
	c.Assert(IsTableNotExists(err), IsTrue)

	// Now create the table with 3 columns.
	err = tracker.Exec(ctx, "testdb", "create table foo (a varchar(255) primary key, b varchar(255) as (concat(a, a)), c int)")
	c.Assert(err, IsNil)

	// Verify the table has 3 columns.
	ti, err := tracker.GetTableInfo(table)
	c.Assert(err, IsNil)
	c.Assert(ti.Columns, HasLen, 3)
	c.Assert(ti.Columns[0].Name.L, Equals, "a")
	c.Assert(ti.Columns[0].IsGenerated(), IsFalse)
	c.Assert(ti.Columns[1].Name.L, Equals, "b")
	c.Assert(ti.Columns[1].IsGenerated(), IsTrue)
	c.Assert(ti.Columns[2].Name.L, Equals, "c")
	c.Assert(ti.Columns[2].IsGenerated(), IsFalse)

	// Verify the table info not changed (pointer equal) when getting again.
	ti2, err := tracker.GetTableInfo(table)
	c.Assert(err, IsNil)
	c.Assert(ti, Equals, ti2)

	cts, err := tracker.GetCreateTable(context.Background(), table)
	c.Assert(err, IsNil)
	c.Assert(cts, Equals, "CREATE TABLE `foo` ( `a` varchar(255) NOT NULL, `b` varchar(255) GENERATED ALWAYS AS (concat(`a`, `a`)) VIRTUAL, `c` int(11) DEFAULT NULL, PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")

	// Drop one column from the table.
	err = tracker.Exec(ctx, "testdb", "alter table foo drop column b")
	c.Assert(err, IsNil)

	// Verify that 2 columns remain.
	ti2, err = tracker.GetTableInfo(table)
	c.Assert(err, IsNil)
	c.Assert(ti, Not(Equals), ti2) // changed (not pointer equal) after applied DDL.
	c.Assert(ti2.Columns, HasLen, 2)
	c.Assert(ti2.Columns[0].Name.L, Equals, "a")
	c.Assert(ti2.Columns[0].IsGenerated(), IsFalse)
	c.Assert(ti2.Columns[1].Name.L, Equals, "c")
	c.Assert(ti2.Columns[1].IsGenerated(), IsFalse)

	cts, err = tracker.GetCreateTable(context.Background(), table)
	c.Assert(err, IsNil)
	c.Assert(cts, Equals, "CREATE TABLE `foo` ( `a` varchar(255) NOT NULL, `c` int(11) DEFAULT NULL, PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")

	// test expression index on tidb_shard.
	err = tracker.Exec(ctx, "testdb", "CREATE TABLE bar (f_id INT PRIMARY KEY, UNIQUE KEY uniq_order_id ((tidb_shard(f_id)),f_id))")
	c.Assert(err, IsNil)
}

func (s *trackerSuite) TestGetSingleColumnIndices(c *C) {
	log.SetLevel(zapcore.ErrorLevel)

	tracker, err := NewTracker(context.Background(), "test-tracker", defaultTestSessionCfg, s.dbConn)
	c.Assert(err, IsNil)

	ctx := context.Background()
	err = tracker.Exec(ctx, "", "create database testdb;")
	c.Assert(err, IsNil)
	err = tracker.Exec(ctx, "testdb", "create table foo (a int, b int, c int)")
	c.Assert(err, IsNil)

	// check GetSingleColumnIndices could return all legal indices
	err = tracker.Exec(ctx, "testdb", "alter table foo add index idx_a1(a)")
	c.Assert(err, IsNil)
	err = tracker.Exec(ctx, "testdb", "alter table foo add index idx_a2(a)")
	c.Assert(err, IsNil)
	infos, err := tracker.GetSingleColumnIndices("testdb", "foo", "a")
	c.Assert(err, IsNil)
	c.Assert(infos, HasLen, 2)
	names := []string{infos[0].Name.L, infos[1].Name.L}
	sort.Strings(names)
	c.Assert(names, DeepEquals, []string{"idx_a1", "idx_a2"})

	// check return nothing for both multi-column and single-column indices
	err = tracker.Exec(ctx, "testdb", "alter table foo add index idx_ab(a, b)")
	c.Assert(err, IsNil)
	err = tracker.Exec(ctx, "testdb", "alter table foo add index idx_b(b)")
	c.Assert(err, IsNil)
	infos, err = tracker.GetSingleColumnIndices("testdb", "foo", "b")
	c.Assert(err, NotNil)
	c.Assert(infos, HasLen, 0)

	// check no indices
	infos, err = tracker.GetSingleColumnIndices("testdb", "foo", "c")
	c.Assert(err, IsNil)
	c.Assert(infos, HasLen, 0)
}

func (s *trackerSuite) TestCreateSchemaIfNotExists(c *C) {
	log.SetLevel(zapcore.ErrorLevel)

	tracker, err := NewTracker(context.Background(), "test-tracker", defaultTestSessionCfg, s.dbConn)
	c.Assert(err, IsNil)

	// We cannot create a table without a database.
	ctx := context.Background()
	err = tracker.Exec(ctx, "testdb", "create table foo(a int)")
	c.Assert(err, ErrorMatches, `.*Unknown database 'testdb'`)

	// We can create the database directly.
	err = tracker.CreateSchemaIfNotExists("testdb")
	c.Assert(err, IsNil)

	// Creating the same database twice is no-op.
	err = tracker.CreateSchemaIfNotExists("testdb")
	c.Assert(err, IsNil)

	// Now creating a table should be successful
	err = tracker.Exec(ctx, "testdb", "create table foo(a int)")
	c.Assert(err, IsNil)

	ti, err := tracker.GetTableInfo(&filter.Table{Schema: "testdb", Name: "foo"})
	c.Assert(err, IsNil)
	c.Assert(ti.Name.L, Equals, "foo")
}

func (s *trackerSuite) TestMultiDrop(c *C) {
	log.SetLevel(zapcore.ErrorLevel)

	tracker, err := NewTracker(context.Background(), "test-tracker", defaultTestSessionCfg, s.dbConn)
	c.Assert(err, IsNil)

	ctx := context.Background()
	err = tracker.CreateSchemaIfNotExists("testdb")
	c.Assert(err, IsNil)
	err = tracker.Exec(ctx, "testdb", `create table foo(a int, b int, c int)
        partition by range( a ) (
			partition p1 values less than (1991),
			partition p2 values less than (1996),
			partition p3 values less than (2001)
	    );`)
	c.Assert(err, IsNil)

	err = tracker.Exec(ctx, "testdb", "alter table foo drop partition p1, p2")
	c.Assert(err, IsNil)

	err = tracker.Exec(ctx, "testdb", "alter table foo drop b, drop c")
	c.Assert(err, IsNil)
}

// clearVolatileInfo removes generated information like TS and ID so DeepEquals
// of two compatible schemas can pass.
func clearVolatileInfo(ti *model.TableInfo) {
	ti.ID = 0
	ti.UpdateTS = 0
	if ti.Partition != nil {
		for i := range ti.Partition.Definitions {
			ti.Partition.Definitions[i].ID = 0
		}
	}
}

// asJSON is a convenient wrapper to print a TableInfo in its JSON representation.
type asJSON struct{ *model.TableInfo }

func (aj asJSON) String() string {
	b, _ := json.Marshal(aj.TableInfo)
	return string(b)
}

func (s *trackerSuite) TestCreateTableIfNotExists(c *C) {
	log.SetLevel(zapcore.ErrorLevel)
	table := &filter.Table{
		Schema: "testdb",
		Name:   "foo",
	}

	tracker, err := NewTracker(context.Background(), "test-tracker", defaultTestSessionCfg, s.dbConn)
	c.Assert(err, IsNil)

	// Create some sort of complicated table.
	err = tracker.CreateSchemaIfNotExists("testdb")
	c.Assert(err, IsNil)

	ctx := context.Background()
	err = tracker.Exec(ctx, "testdb", `
		create table foo(
			a int primary key auto_increment,
			b int as (c+1) not null,
			c int comment 'some cmt',
			d text,
			key dk(d(255))
		) comment 'more cmt' partition by range columns (a) (
			partition x41 values less than (41),
			partition x82 values less than (82),
			partition rest values less than maxvalue comment 'part cmt'
		);
	`)
	c.Assert(err, IsNil)

	// Save the table info
	ti1, err := tracker.GetTableInfo(table)
	c.Assert(err, IsNil)
	c.Assert(ti1, NotNil)
	c.Assert(ti1.Name.O, Equals, "foo")
	ti1 = ti1.Clone()
	clearVolatileInfo(ti1)

	// Remove the table. Should not be found anymore.
	err = tracker.DropTable(table)
	c.Assert(err, IsNil)

	_, err = tracker.GetTableInfo(table)
	c.Assert(err, ErrorMatches, `.*Table 'testdb\.foo' doesn't exist`)

	// Recover the table using the table info.
	err = tracker.CreateTableIfNotExists(&filter.Table{Schema: "testdb", Name: "foo"}, ti1)
	c.Assert(err, IsNil)

	// The new table info should be equivalent to the old one except the TS and generated IDs.
	ti2, err := tracker.GetTableInfo(table)
	c.Assert(err, IsNil)
	clearVolatileInfo(ti2)
	c.Assert(ti2, DeepEquals, ti1, Commentf("ti2 = %s\nti1 = %s", asJSON{ti2}, asJSON{ti1}))

	// no error if table already exist
	err = tracker.CreateTableIfNotExists(&filter.Table{Schema: "testdb", Name: "foo"}, ti1)
	c.Assert(err, IsNil)

	// error if db not exist
	err = tracker.CreateTableIfNotExists(&filter.Table{Schema: "test-another-db", Name: "foo"}, ti1)
	c.Assert(err, ErrorMatches, ".*Unknown database.*")

	// Can use the table info to recover a table using a different name.
	err = tracker.CreateTableIfNotExists(&filter.Table{Schema: "testdb", Name: "bar"}, ti1)
	c.Assert(err, IsNil)

	ti3, err := tracker.GetTableInfo(&filter.Table{Schema: "testdb", Name: "bar"})
	c.Assert(err, IsNil)
	c.Assert(ti3.Name.O, Equals, "bar")
	clearVolatileInfo(ti3)
	ti3.Name = ti1.Name
	c.Assert(ti3, DeepEquals, ti1, Commentf("ti3 = %s\nti1 = %s", asJSON{ti3}, asJSON{ti1}))

	start := time.Now()
	for n := 0; n < 100; n++ {
		err = tracker.CreateTableIfNotExists(&filter.Table{Schema: "testdb", Name: fmt.Sprintf("foo-%d", n)}, ti1)
		c.Assert(err, IsNil)
	}
	duration := time.Since(start)
	c.Assert(duration.Seconds(), Less, float64(30))
}

func (s *trackerSuite) TestBatchCreateTableIfNotExist(c *C) {
	log.SetLevel(zapcore.ErrorLevel)
	tracker, err := NewTracker(context.Background(), "test-tracker", defaultTestSessionCfg, s.dbConn)
	c.Assert(err, IsNil)
	err = tracker.CreateSchemaIfNotExists("testdb")
	c.Assert(err, IsNil)
	err = tracker.CreateSchemaIfNotExists("testdb2")
	c.Assert(err, IsNil)

	tables := []*filter.Table{
		{
			Schema: "testdb",
			Name:   "foo",
		},
		{
			Schema: "testdb",
			Name:   "foo1",
		},
		{
			Schema: "testdb2",
			Name:   "foo3",
		},
	}
	execStmt := []string{
		`create table foo(
			a int primary key auto_increment,
			b int as (c+1) not null,
			c int comment 'some cmt',
			d text,
			key dk(d(255))
		) comment 'more cmt' partition by range columns (a) (
			partition x41 values less than (41),
			partition x82 values less than (82),
			partition rest values less than maxvalue comment 'part cmt'
		);`,
		`create table foo1(
			a int primary key,
			b text not null,
			d datetime,
			e varchar(5)
		);`,
		`create table foo3(
			a int,
			b int,
			primary key(a));`,
	}
	tiInfos := make([]*model.TableInfo, len(tables))
	for i := range tables {
		ctx := context.Background()
		err = tracker.Exec(ctx, tables[i].Schema, execStmt[i])
		c.Assert(err, IsNil)
		tiInfos[i], err = tracker.GetTableInfo(tables[i])
		c.Assert(err, IsNil)
		c.Assert(tiInfos[i], NotNil)
		c.Assert(tiInfos[i].Name.O, Equals, tables[i].Name)
		tiInfos[i] = tiInfos[i].Clone()
		clearVolatileInfo(tiInfos[i])
	}
	// drop all tables and recover
	// 1. drop
	for i := range tables {
		err = tracker.DropTable(tables[i])
		c.Assert(err, IsNil)
		_, err = tracker.GetTableInfo(tables[i])
		c.Assert(err, ErrorMatches, `.*Table 'testdb.*\.foo.*' doesn't exist`) // drop table success
	}
	// 2. recover
	tablesToCreate := map[string]map[string]*model.TableInfo{}
	tablesToCreate["testdb"] = map[string]*model.TableInfo{}
	tablesToCreate["testdb2"] = map[string]*model.TableInfo{}
	for i := range tables {
		tablesToCreate[tables[i].Schema][tables[i].Name] = tiInfos[i]
	}
	err = tracker.BatchCreateTableIfNotExist(tablesToCreate)
	c.Assert(err, IsNil)
	// 3. check all create success
	for i := range tables {
		var ti *model.TableInfo
		ti, err = tracker.GetTableInfo(tables[i])
		c.Assert(err, IsNil)
		cloneTi := ti.Clone()
		clearVolatileInfo(cloneTi)
		c.Assert(cloneTi, DeepEquals, tiInfos[i])
	}

	// drop two tables and create all three
	// expect: silently succeed
	// 1. drop table
	err = tracker.DropTable(tables[2])
	c.Assert(err, IsNil)
	err = tracker.DropTable(tables[0])
	c.Assert(err, IsNil)
	// 2. batch create
	err = tracker.BatchCreateTableIfNotExist(tablesToCreate)
	c.Assert(err, IsNil)
	// 3. check
	for i := range tables {
		var ti *model.TableInfo
		ti, err = tracker.GetTableInfo(tables[i])
		c.Assert(err, IsNil)
		clearVolatileInfo(ti)
		c.Assert(ti, DeepEquals, tiInfos[i])
	}

	// drop schema and raise error
	ctx := context.Background()
	err = tracker.Exec(ctx, "", `drop database testdb`)
	c.Assert(err, IsNil)
	err = tracker.BatchCreateTableIfNotExist(tablesToCreate)
	c.Assert(err, NotNil)
}

func (s *trackerSuite) TestAllSchemas(c *C) {
	log.SetLevel(zapcore.ErrorLevel)
	ctx := context.Background()

	tracker, err := NewTracker(context.Background(), "test-tracker", defaultTestSessionCfg, s.dbConn)
	c.Assert(err, IsNil)

	// nothing should exist...
	c.Assert(tracker.AllSchemas(), HasLen, 0)

	// Create several schemas and tables.
	err = tracker.CreateSchemaIfNotExists("testdb1")
	c.Assert(err, IsNil)
	err = tracker.CreateSchemaIfNotExists("testdb2")
	c.Assert(err, IsNil)
	err = tracker.CreateSchemaIfNotExists("testdb3")
	c.Assert(err, IsNil)
	err = tracker.Exec(ctx, "testdb2", "create table a(a int)")
	c.Assert(err, IsNil)
	err = tracker.Exec(ctx, "testdb1", "create table b(a int)")
	c.Assert(err, IsNil)
	err = tracker.Exec(ctx, "testdb1", "create table c(a int)")
	c.Assert(err, IsNil)

	// check schema tables
	tables, err := tracker.ListSchemaTables("testdb1")
	c.Assert(err, IsNil)
	c.Assert(tables, DeepEquals, []string{"b", "c"})
	// check schema not exists
	notExistSchemaName := "testdb_not_found"
	_, err = tracker.ListSchemaTables(notExistSchemaName)
	c.Assert(terror.ErrSchemaTrackerUnSchemaNotExist.Equal(err), IsTrue)

	// check that all schemas and tables are present.
	allSchemas := tracker.AllSchemas()
	c.Assert(allSchemas, HasLen, 3)
	existingNames := 0
	for _, schema := range allSchemas {
		switch schema.Name.O {
		case "testdb1":
			existingNames |= 1
			c.Assert(schema.Tables, HasLen, 2)
			for _, table := range schema.Tables {
				switch table.Name.O {
				case "b":
					existingNames |= 8
				case "c":
					existingNames |= 16
				default:
					c.Errorf("unexpected table testdb1.%s", table.Name)
				}
			}
		case "testdb2":
			existingNames |= 2
			c.Assert(schema.Tables, HasLen, 1)
			table := schema.Tables[0]
			c.Assert(table.Name.O, Equals, "a")
			c.Assert(table.Columns, HasLen, 1)
			// the table should be equivalent to the result of GetTable.
			table2, err2 := tracker.GetTableInfo(&filter.Table{Schema: "testdb2", Name: "a"})
			c.Assert(err2, IsNil)
			c.Assert(table2, DeepEquals, table)
		case "testdb3":
			existingNames |= 4
		default:
			c.Errorf("unexpected schema %s", schema.Name)
		}
	}
	c.Assert(existingNames, Equals, 31)

	// reset the tracker. all schemas should be gone.
	err = tracker.Reset()
	c.Assert(err, IsNil)
	c.Assert(tracker.AllSchemas(), HasLen, 0)
	_, err = tracker.GetTableInfo(&filter.Table{Schema: "testdb2", Name: "a"})
	c.Assert(err, ErrorMatches, `.*Table 'testdb2\.a' doesn't exist`)
}

func (s *trackerSuite) TestNotSupportedVariable(c *C) {
	log.SetLevel(zapcore.ErrorLevel)

	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	con, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	baseConn := conn.NewBaseConn(con, nil)
	dbConn := &dbconn.DBConn{Cfg: s.cfg, BaseConn: baseConn}

	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(
		sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("sql_mode", defaultTestSessionCfg["sql_mode"]))

	oldSessionVar := map[string]string{
		"tidb_enable_change_column_type": "ON",
	}
	_, err = NewTracker(context.Background(), "test-tracker", oldSessionVar, dbConn)
	c.Assert(err, IsNil)
}

func (s *trackerSuite) TestInitDownStreamSQLModeAndParser(c *C) {
	log.SetLevel(zapcore.ErrorLevel)

	// tracker and sqlmock
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	con, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	baseConn := conn.NewBaseConn(con, nil)
	dbConn := &dbconn.DBConn{Cfg: s.cfg, BaseConn: baseConn}

	tracker, err := NewTracker(context.Background(), "test-tracker", defaultTestSessionCfg, dbConn)
	c.Assert(err, IsNil)

	mock.ExpectBegin()
	mock.ExpectExec(fmt.Sprintf("SET SESSION SQL_MODE = '%s'", mysql.DefaultSQLMode)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tctx := tcontext.NewContext(context.Background(), dlog.L())

	err = tracker.initDownStreamSQLModeAndParser(tctx)
	c.Assert(err, IsNil)
	c.Assert(tracker.dsTracker.stmtParser, NotNil)
}

func (s *trackerSuite) TestGetDownStreamIndexInfo(c *C) {
	log.SetLevel(zapcore.ErrorLevel)

	// origin table info
	p := parser.New()
	se := timock.NewContext()
	node, err := p.ParseOneStmt("create table t(a int, b int, c varchar(10))", "utf8mb4", "utf8mb4_bin")
	c.Assert(err, IsNil)
	oriTi, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), 1)
	c.Assert(err, IsNil)

	// tracker and sqlmock
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	con, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	baseConn := conn.NewBaseConn(con, nil)
	dbConn := &dbconn.DBConn{Cfg: s.cfg, BaseConn: baseConn}
	tracker, err := NewTracker(context.Background(), "test-tracker", defaultTestSessionCfg, dbConn)
	c.Assert(err, IsNil)

	mock.ExpectBegin()
	mock.ExpectExec(fmt.Sprintf("SET SESSION SQL_MODE = '%s'", mysql.DefaultSQLMode)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tableID := "`test`.`test`"

	// downstream has no pk/uk
	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int, b int, c varchar(10))"))
	dti, err := tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	c.Assert(dti, NotNil)
	c.Assert(dti.AbsoluteUKIndexInfo, IsNil)
	delete(tracker.dsTracker.tableInfos, tableID)

	// downstream has pk(not constraints like "create table t(a int primary key,b int not null)"
	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int, b int, c varchar(10), PRIMARY KEY (c))"))
	dti, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	c.Assert(dti, NotNil)
	c.Assert(dti.AbsoluteUKIndexInfo, NotNil)
	delete(tracker.dsTracker.tableInfos, tableID)

	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int primary key, b int, c varchar(10))"))
	dti, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	c.Assert(dti, NotNil)
	c.Assert(dti.AbsoluteUKIndexInfo, NotNil)
	delete(tracker.dsTracker.tableInfos, tableID)

	// downstream has composite pks
	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int, b int, c varchar(10), PRIMARY KEY (a,b))"))
	dti, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	c.Assert(dti, NotNil)
	c.Assert(dti.AbsoluteUKIndexInfo, NotNil)
	c.Assert(len(dti.AbsoluteUKIndexInfo.Columns) == 2, IsTrue)
	delete(tracker.dsTracker.tableInfos, tableID)

	// downstream has uk(not null)
	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int unique not null, b int, c varchar(10))"))
	dti, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	c.Assert(dti, NotNil)
	c.Assert(dti.AbsoluteUKIndexInfo, NotNil)
	c.Assert(dti.AbsoluteUKIndexInfo.Columns, NotNil)
	delete(tracker.dsTracker.tableInfos, tableID)

	// downstream has uk(without not null)
	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int unique, b int, c varchar(10))"))
	dti, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	c.Assert(dti, NotNil)
	c.Assert(dti.AbsoluteUKIndexInfo, IsNil)
	c.Assert(dti.AvailableUKIndexList, NotNil)
	delete(tracker.dsTracker.tableInfos, tableID)

	// downstream has uks
	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int unique, b int unique, c varchar(10) unique not null)"))
	dti, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	c.Assert(dti, NotNil)
	c.Assert(dti.AbsoluteUKIndexInfo, NotNil)
	c.Assert(len(dti.AvailableUKIndexList) == 3, IsTrue)
	c.Assert(dti.AvailableUKIndexList[0] == dti.AbsoluteUKIndexInfo, IsTrue)
	delete(tracker.dsTracker.tableInfos, tableID)

	// downstream has pk and uk, pk has priority
	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int unique not null , b int, c varchar(10), PRIMARY KEY (c))"))
	dti, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	c.Assert(dti.AbsoluteUKIndexInfo.Primary, IsTrue)
	c.Assert(len(dti.AvailableUKIndexList) == 2, IsTrue)
	c.Assert(dti.AvailableUKIndexList[0] == dti.AbsoluteUKIndexInfo, IsTrue)
	delete(tracker.dsTracker.tableInfos, tableID)

	// downstream has more columns than upstream, and that column in used in PK
	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int , d int PRIMARY KEY, c varchar(10), b int unique not null)"))
	dti, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	c.Assert(dti.AbsoluteUKIndexInfo, NotNil)
	c.Assert(dti.AbsoluteUKIndexInfo.Primary, IsFalse)
	c.Assert(len(dti.AvailableUKIndexList) == 1, IsTrue)
	delete(tracker.dsTracker.tableInfos, tableID)

	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int , d int PRIMARY KEY, c varchar(10), b int unique)"))
	dti, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	c.Assert(dti.AbsoluteUKIndexInfo, IsNil)
	c.Assert(len(dti.AvailableUKIndexList) == 1, IsTrue)
	delete(tracker.dsTracker.tableInfos, tableID)

	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int , d int PRIMARY KEY, c varchar(10), b int)"))
	dti, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	c.Assert(dti.AbsoluteUKIndexInfo, IsNil)
	delete(tracker.dsTracker.tableInfos, tableID)

	// downstream has more columns than upstream, and that column in used in UK(not null)
	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int , d int unique not null, c varchar(10), b int unique not null)"))
	dti, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	c.Assert(dti.AbsoluteUKIndexInfo, NotNil)
	c.Assert(dti.AbsoluteUKIndexInfo.Columns[0].Name.L == "b", IsTrue)
	delete(tracker.dsTracker.tableInfos, tableID)

	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int , d int unique not null, c varchar(10), b int unique)"))
	dti, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	c.Assert(dti.AbsoluteUKIndexInfo, IsNil)
	c.Assert(len(dti.AvailableUKIndexList) == 1, IsTrue)
	delete(tracker.dsTracker.tableInfos, tableID)

	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int , d int unique not null, c varchar(10), b int)"))
	dti, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	c.Assert(dti.AbsoluteUKIndexInfo, IsNil)
	delete(tracker.dsTracker.tableInfos, tableID)
}

func (s *trackerSuite) TestGetAvailableDownStreamUKIIndexInfo(c *C) {
	log.SetLevel(zapcore.ErrorLevel)

	// origin table info
	p := parser.New()
	se := timock.NewContext()
	node, err := p.ParseOneStmt("create table t(a int, b int, c varchar(10))", "utf8mb4", "utf8mb4_bin")
	c.Assert(err, IsNil)
	oriTi, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), 1)
	c.Assert(err, IsNil)

	// tracker and sqlmock
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	con, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	baseConn := conn.NewBaseConn(con, nil)
	dbConn := &dbconn.DBConn{Cfg: s.cfg, BaseConn: baseConn}
	tracker, err := NewTracker(context.Background(), "test-tracker", defaultTestSessionCfg, dbConn)
	c.Assert(err, IsNil)

	mock.ExpectBegin()
	mock.ExpectExec(fmt.Sprintf("SET SESSION SQL_MODE = '%s'", mysql.DefaultSQLMode)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tableID := "`test`.`test`"

	// downstream has no uk
	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int, b int, c varchar(10))"))
	dti, err := tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	c.Assert(dti.AbsoluteUKIndexInfo, IsNil)
	data := []interface{}{1, 2, 3}
	indexinfo := tracker.GetAvailableDownStreamUKIndexInfo(tableID, data)
	c.Assert(indexinfo, IsNil)
	delete(tracker.dsTracker.tableInfos, tableID)

	// downstream has uk but data is null
	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int unique, b int, c varchar(10))"))
	dti, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	c.Assert(dti.AbsoluteUKIndexInfo, IsNil)
	data = []interface{}{nil, 2, 3}
	indexinfo = tracker.GetAvailableDownStreamUKIndexInfo(tableID, data)
	c.Assert(indexinfo, IsNil)
	delete(tracker.dsTracker.tableInfos, tableID)

	// downstream has uk and data is not null
	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int unique, b int, c varchar(10))"))
	dti, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	c.Assert(dti.AbsoluteUKIndexInfo, IsNil)
	data = []interface{}{1, 2, 3}
	indexinfo = tracker.GetAvailableDownStreamUKIndexInfo(tableID, data)
	c.Assert(indexinfo, NotNil)
	delete(tracker.dsTracker.tableInfos, tableID)

	// downstream has union uk but data has null
	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int, b int, c varchar(10), unique key(a, b))"))
	dti, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	c.Assert(dti.AbsoluteUKIndexInfo, IsNil)
	data = []interface{}{1, nil, 3}
	indexinfo = tracker.GetAvailableDownStreamUKIndexInfo(tableID, data)
	c.Assert(indexinfo, IsNil)
	delete(tracker.dsTracker.tableInfos, tableID)

	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int, b int, c varchar(10), unique key(a, b))"))
	dti, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	c.Assert(dti.AbsoluteUKIndexInfo, IsNil)
	data = []interface{}{1, nil, nil}
	indexinfo = tracker.GetAvailableDownStreamUKIndexInfo(tableID, data)
	c.Assert(indexinfo, IsNil)
	delete(tracker.dsTracker.tableInfos, tableID)

	// downstream has union uk but data has null
	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int, b int, c varchar(10), unique key(a, b))"))
	dti, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	c.Assert(dti.AbsoluteUKIndexInfo, IsNil)
	data = []interface{}{1, 2, 3}
	indexinfo = tracker.GetAvailableDownStreamUKIndexInfo(tableID, data)
	c.Assert(indexinfo, NotNil)
	delete(tracker.dsTracker.tableInfos, tableID)
}

func (s *trackerSuite) TestReTrackDownStreamIndex(c *C) {
	log.SetLevel(zapcore.ErrorLevel)

	// origin table info
	p := parser.New()
	se := timock.NewContext()
	node, err := p.ParseOneStmt("create table t(a int, b int, c varchar(10))", "utf8mb4", "utf8mb4_bin")
	c.Assert(err, IsNil)
	oriTi, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), 1)
	c.Assert(err, IsNil)

	// tracker and sqlmock
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	con, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	baseConn := conn.NewBaseConn(con, nil)
	dbConn := &dbconn.DBConn{Cfg: s.cfg, BaseConn: baseConn}
	tracker, err := NewTracker(context.Background(), "test-tracker", defaultTestSessionCfg, dbConn)
	c.Assert(err, IsNil)

	mock.ExpectBegin()
	mock.ExpectExec(fmt.Sprintf("SET SESSION SQL_MODE = '%s'", mysql.DefaultSQLMode)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tableID := "`test`.`test`"

	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int, b int, c varchar(10), PRIMARY KEY (a,b))"))
	_, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	_, ok := tracker.dsTracker.tableInfos[tableID]
	c.Assert(ok, IsTrue)

	// just table
	targetTables := []*filter.Table{{Schema: "test", Name: "a"}, {Schema: "test", Name: "test"}}
	tracker.RemoveDownstreamSchema(tcontext.Background(), targetTables)
	_, ok = tracker.dsTracker.tableInfos[tableID]
	c.Assert(ok, IsFalse)

	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int, b int, c varchar(10), PRIMARY KEY (a,b))"))
	_, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	_, ok = tracker.dsTracker.tableInfos[tableID]
	c.Assert(ok, IsTrue)

	tracker.RemoveDownstreamSchema(tcontext.Background(), targetTables)
	_, ok = tracker.dsTracker.tableInfos[tableID]
	c.Assert(ok, IsFalse)

	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int primary key, b int, c varchar(10))"))
	_, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	_, ok = tracker.dsTracker.tableInfos[tableID]
	c.Assert(ok, IsTrue)

	// just schema
	targetTables = []*filter.Table{{Schema: "test", Name: "a"}, {Schema: "test", Name: ""}}
	tracker.RemoveDownstreamSchema(tcontext.Background(), targetTables)
	_, ok = tracker.dsTracker.tableInfos[tableID]
	c.Assert(ok, IsFalse)

	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int, b int, c varchar(10), PRIMARY KEY (a,b))"))
	_, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	_, ok = tracker.dsTracker.tableInfos[tableID]
	c.Assert(ok, IsTrue)

	tracker.RemoveDownstreamSchema(tcontext.Background(), targetTables)
	_, ok = tracker.dsTracker.tableInfos[tableID]
	c.Assert(ok, IsFalse)

	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int primary key, b int, c varchar(10))"))
	_, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	_, ok = tracker.dsTracker.tableInfos[tableID]
	c.Assert(ok, IsTrue)
}

func (s *trackerSuite) TestVarchar20000(c *C) {
	log.SetLevel(zapcore.ErrorLevel)

	// origin table info
	p := parser.New()
	node, err := p.ParseOneStmt("create table t(c varchar(20000)) charset=utf8", "", "")
	c.Assert(err, IsNil)
	oriTi, err := ddl.BuildTableInfoFromAST(node.(*ast.CreateTableStmt))
	c.Assert(err, IsNil)

	// tracker and sqlmock
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	con, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	baseConn := conn.NewBaseConn(con, nil)
	dbConn := &dbconn.DBConn{Cfg: s.cfg, BaseConn: baseConn}
	tracker, err := NewTracker(context.Background(), "test-tracker", defaultTestSessionCfg, dbConn)
	c.Assert(err, IsNil)

	mock.ExpectBegin()
	mock.ExpectExec(fmt.Sprintf("SET SESSION SQL_MODE = '%s'", mysql.DefaultSQLMode)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tableID := "`test`.`test`"

	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(c varchar(20000)) charset=utf8"))
	_, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	c.Assert(err, IsNil)
	_, ok := tracker.dsTracker.tableInfos[tableID]
	c.Assert(ok, IsTrue)
}
