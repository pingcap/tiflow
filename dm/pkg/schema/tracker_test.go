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
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/filter"
	timock "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	dlog "github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	"github.com/stretchr/testify/require"
)

func parseSQL(t *testing.T, p *parser.Parser, sql string) ast.StmtNode {
	t.Helper()

	ret, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	return ret
}

func TestNeedSessionCfgInOldImpl(t *testing.T) {
	ctx := context.Background()
	table := &filter.Table{
		Schema: "testdb",
		Name:   "foo",
	}

	tracker, err := NewTestTracker(context.Background(), "test-tracker", nil, dlog.L())
	require.NoError(t, err)

	p := parser.New()

	err = tracker.Exec(context.Background(), "", parseSQL(t, p, "create database testdb;"))
	require.NoError(t, err)
	tracker.Close()

	createAST := parseSQL(t, p, "create table foo (a varchar(255) primary key, b DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00')")

	// test create table with zero datetime
	err = tracker.Exec(ctx, "testdb", createAST)
	require.NoError(t, err)
	err = tracker.DropTable(table)
	require.NoError(t, err)

	// caller should set SQL Mode through status vars.
	p.SetSQLMode(mysql.ModeANSIQuotes)
	createAST = parseSQL(t, p, "create table \"foo\" (a varchar(255) primary key, b DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00')")

	// Now create the table with ANSI_QUOTES and ZERO_DATE
	err = tracker.Exec(ctx, "testdb", createAST)
	require.NoError(t, err)

	sql, err := tracker.GetCreateTable(context.Background(), table)
	require.NoError(t, err)
	// the result is not ANSI_QUOTES
	require.Equal(t, "CREATE TABLE `foo` ( `a` varchar(255) NOT NULL, `b` datetime NOT NULL DEFAULT '0000-00-00 00:00:00', PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin", sql)

	// test alter primary key
	alterAST := parseSQL(t, p, "alter table \"foo\" drop primary key")
	err = tracker.Exec(ctx, "testdb", alterAST)
	require.NoError(t, err)

	tracker.Close()
}

func TestDDL(t *testing.T) {
	table := &filter.Table{
		Schema: "testdb",
		Name:   "foo",
	}

	ctx := context.Background()
	p := parser.New()

	tracker, err := NewTestTracker(ctx, "test-tracker", nil, dlog.L())
	require.NoError(t, err)
	defer tracker.Close()

	// Table shouldn't exist before initialization.
	_, err = tracker.GetTableInfo(table)
	require.ErrorContains(t, err, "Unknown database 'testdb'")
	require.True(t, IsTableNotExists(err))

	_, err = tracker.GetCreateTable(ctx, table)
	require.ErrorContains(t, err, "Unknown database 'testdb'")
	require.True(t, IsTableNotExists(err))

	err = tracker.Exec(ctx, "", parseSQL(t, p, "create database testdb;"))
	require.NoError(t, err)

	_, err = tracker.GetTableInfo(table)
	require.ErrorContains(t, err, "Table 'testdb.foo' doesn't exist")
	require.True(t, IsTableNotExists(err))

	// Now create the table with 3 columns.
	createAST := parseSQL(t, p, "create table foo (a varchar(255) primary key, b varchar(255) as (concat(a, a)), c int)")
	err = tracker.Exec(ctx, "testdb", createAST)
	require.NoError(t, err)

	// Verify the table has 3 columns.
	ti, err := tracker.GetTableInfo(table)
	require.NoError(t, err)
	require.Len(t, ti.Columns, 3)
	require.Equal(t, "a", ti.Columns[0].Name.O)
	require.False(t, ti.Columns[0].IsGenerated())
	require.Equal(t, "b", ti.Columns[1].Name.O)
	require.True(t, ti.Columns[1].IsGenerated())
	require.Equal(t, "c", ti.Columns[2].Name.O)
	require.False(t, ti.Columns[2].IsGenerated())

	sql, err := tracker.GetCreateTable(ctx, table)
	require.NoError(t, err)
	require.Equal(t, "CREATE TABLE `foo` ( `a` varchar(255) NOT NULL, `b` varchar(255) GENERATED ALWAYS AS (concat(`a`, `a`)) VIRTUAL, `c` int(11) DEFAULT NULL, PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin", sql)

	// Drop one column from the table.
	err = tracker.Exec(ctx, "testdb", parseSQL(t, p, "alter table foo drop column b"))
	require.NoError(t, err)

	// Verify that 2 columns remain.
	ti, err = tracker.GetTableInfo(table)
	require.NoError(t, err)
	require.Len(t, ti.Columns, 2)
	require.Equal(t, "a", ti.Columns[0].Name.O)
	require.False(t, ti.Columns[0].IsGenerated())
	require.Equal(t, "c", ti.Columns[1].Name.O)
	require.False(t, ti.Columns[1].IsGenerated())

	sql, err = tracker.GetCreateTable(ctx, table)
	require.NoError(t, err)
	require.Equal(t, "CREATE TABLE `foo` ( `a` varchar(255) NOT NULL, `c` int(11) DEFAULT NULL, PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin", sql)

	// test expression index on tidb_shard.
	createAST = parseSQL(t, p, "CREATE TABLE bar (f_id INT PRIMARY KEY, UNIQUE KEY uniq_order_id ((tidb_shard(f_id)),f_id))")
	err = tracker.Exec(ctx, "testdb", createAST)
	require.NoError(t, err)
}

func TestGetSingleColumnIndices(t *testing.T) {
	ctx := context.Background()
	p := parser.New()

	tracker, err := NewTestTracker(ctx, "test-tracker", nil, dlog.L())
	require.NoError(t, err)
	defer tracker.Close()

	err = tracker.Exec(ctx, "", parseSQL(t, p, "create database testdb;"))
	require.NoError(t, err)
	err = tracker.Exec(ctx, "testdb", parseSQL(t, p, "create table foo (a int, b int, c int)"))
	require.NoError(t, err)

	// check GetSingleColumnIndices could return all legal indices
	err = tracker.Exec(ctx, "testdb", parseSQL(t, p, "alter table foo add index idx_a1(a)"))
	require.NoError(t, err)
	err = tracker.Exec(ctx, "testdb", parseSQL(t, p, "alter table foo add index idx_a2(a)"))
	require.NoError(t, err)
	infos, err := tracker.GetSingleColumnIndices("testdb", "foo", "a")
	require.NoError(t, err)
	require.Len(t, infos, 2)
	names := []string{infos[0].Name.L, infos[1].Name.L}
	sort.Strings(names)
	require.Equal(t, []string{"idx_a1", "idx_a2"}, names)

	// check return nothing for both multi-column and single-column indices
	err = tracker.Exec(ctx, "testdb", parseSQL(t, p, "alter table foo add index idx_ab(a, b)"))
	require.NoError(t, err)
	err = tracker.Exec(ctx, "testdb", parseSQL(t, p, "alter table foo add index idx_b(b)"))
	require.NoError(t, err)
	infos, err = tracker.GetSingleColumnIndices("testdb", "foo", "b")
	require.Error(t, err)
	require.Len(t, infos, 0)

	// check no indices
	infos, err = tracker.GetSingleColumnIndices("testdb", "foo", "c")
	require.NoError(t, err)
	require.Len(t, infos, 0)
}

func TestCreateSchemaIfNotExists(t *testing.T) {
	ctx := context.Background()
	p := parser.New()

	tracker, err := NewTestTracker(ctx, "test-tracker", nil, dlog.L())
	require.NoError(t, err)
	defer tracker.Close()

	// We cannot create a table without a database.
	err = tracker.Exec(ctx, "testdb", parseSQL(t, p, "create table foo(a int)"))
	require.ErrorContains(t, err, "Unknown database 'testdb'")

	// We can create the database directly.
	err = tracker.CreateSchemaIfNotExists("testdb")
	require.NoError(t, err)

	// Creating the same database twice is no-op.
	err = tracker.CreateSchemaIfNotExists("testdb")
	require.NoError(t, err)

	// Now creating a table should be successful
	err = tracker.Exec(ctx, "testdb", parseSQL(t, p, "create table foo(a int)"))
	require.NoError(t, err)

	ti, err := tracker.GetTableInfo(&filter.Table{Schema: "testdb", Name: "foo"})
	require.NoError(t, err)
	require.Equal(t, "foo", ti.Name.O)
}

func TestMultiDrop(t *testing.T) {
	ctx := context.Background()
	p := parser.New()

	tracker, err := NewTestTracker(ctx, "test-tracker", nil, dlog.L())
	require.NoError(t, err)
	defer tracker.Close()

	err = tracker.CreateSchemaIfNotExists("testdb")
	require.NoError(t, err)
	createAST := parseSQL(t, p, `create table foo(a int, b int, c int)
       partition by range( a ) (
			partition p1 values less than (1991),
			partition p2 values less than (1996),
			partition p3 values less than (2001)
	    );`)
	err = tracker.Exec(ctx, "testdb", createAST)
	require.NoError(t, err)

	err = tracker.Exec(ctx, "testdb", parseSQL(t, p, "alter table foo drop partition p1, p2"))
	require.NoError(t, err)

	err = tracker.Exec(ctx, "testdb", parseSQL(t, p, "alter table foo drop b, drop c"))
	require.NoError(t, err)
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

func TestCreateTableIfNotExists(t *testing.T) {
	table := &filter.Table{
		Schema: "testdb",
		Name:   "foo",
	}

	ctx := context.Background()
	p := parser.New()

	tracker, err := NewTestTracker(ctx, "test-tracker", nil, dlog.L())
	require.NoError(t, err)
	defer tracker.Close()

	// Create some sort of complicated table.
	err = tracker.CreateSchemaIfNotExists("testdb")
	require.NoError(t, err)

	err = tracker.Exec(ctx, "testdb", parseSQL(t, p, `
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
	`))
	require.NoError(t, err)

	// Save the table info
	ti1, err := tracker.GetTableInfo(table)
	require.NoError(t, err)
	require.Equal(t, "foo", ti1.Name.O)
	ti1 = ti1.Clone()
	clearVolatileInfo(ti1)

	// Remove the table. Should not be found anymore.
	err = tracker.DropTable(table)
	require.NoError(t, err)

	_, err = tracker.GetTableInfo(table)
	require.ErrorContains(t, err, "Table 'testdb.foo' doesn't exist")

	// Recover the table using the table info.
	err = tracker.CreateTableIfNotExists(&filter.Table{Schema: "testdb", Name: "foo"}, ti1)
	require.NoError(t, err)

	// The new table info should be equivalent to the old one except the TS and generated IDs.
	ti2, err := tracker.GetTableInfo(table)
	require.NoError(t, err)
	clearVolatileInfo(ti2)
	require.Equal(t, ti1, ti2, "ti2 = %s\nti1 = %s", asJSON{ti2}, asJSON{ti1})

	// no error if table already exist
	err = tracker.CreateTableIfNotExists(&filter.Table{Schema: "testdb", Name: "foo"}, ti1)
	require.NoError(t, err)

	// error if db not exist
	err = tracker.CreateTableIfNotExists(&filter.Table{Schema: "test-another-db", Name: "foo"}, ti1)
	require.ErrorContains(t, err, "Unknown database")

	// Can use the table info to recover a table using a different name.
	err = tracker.CreateTableIfNotExists(&filter.Table{Schema: "testdb", Name: "bar"}, ti1)
	require.NoError(t, err)

	ti3, err := tracker.GetTableInfo(&filter.Table{Schema: "testdb", Name: "bar"})
	require.NoError(t, err)
	require.Equal(t, "bar", ti3.Name.O)
	clearVolatileInfo(ti3)
	ti3.Name = ti1.Name
	require.Equal(t, ti1, ti3, "ti3 = %s\nti1 = %s", asJSON{ti3}, asJSON{ti1})

	start := time.Now()
	for n := 0; n < 100; n++ {
		err = tracker.CreateTableIfNotExists(&filter.Table{Schema: "testdb", Name: fmt.Sprintf("foo-%d", n)}, ti1)
		require.NoError(t, err)
	}
	duration := time.Since(start)
	require.Less(t, duration.Seconds(), float64(30))
}

func TestBatchCreateTableIfNotExist(t *testing.T) {
	ctx := context.Background()
	p := parser.New()

	tracker, err := NewTestTracker(ctx, "test-tracker", nil, dlog.L())
	require.NoError(t, err)
	defer tracker.Close()

	err = tracker.CreateSchemaIfNotExists("testdb")
	require.NoError(t, err)
	err = tracker.CreateSchemaIfNotExists("testdb2")
	require.NoError(t, err)

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
		err = tracker.Exec(ctx, tables[i].Schema, parseSQL(t, p, execStmt[i]))
		require.NoError(t, err)
		tiInfos[i], err = tracker.GetTableInfo(tables[i])
		require.NoError(t, err)
		require.Equal(t, tables[i].Name, tiInfos[i].Name.O)
		tiInfos[i] = tiInfos[i].Clone()
		clearVolatileInfo(tiInfos[i])
	}
	// drop all tables and recover
	// 1. drop
	for i := range tables {
		err = tracker.DropTable(tables[i])
		require.NoError(t, err)
		_, err = tracker.GetTableInfo(tables[i])
		require.ErrorContains(t, err, "doesn't exist")
	}
	// 2. test empty load
	tablesToCreate := map[string]map[string]*model.TableInfo{}
	tablesToCreate["testdb"] = map[string]*model.TableInfo{}
	tablesToCreate["testdb2"] = map[string]*model.TableInfo{}
	err = tracker.BatchCreateTableIfNotExist(tablesToCreate)
	require.NoError(t, err)
	// 3. recover
	for i := range tables {
		tablesToCreate[tables[i].Schema][tables[i].Name] = tiInfos[i]
	}
	err = tracker.BatchCreateTableIfNotExist(tablesToCreate)
	require.NoError(t, err)
	// 4. check all create success
	for i := range tables {
		var ti *model.TableInfo
		ti, err = tracker.GetTableInfo(tables[i])
		require.NoError(t, err)
		cloneTi := ti.Clone()
		clearVolatileInfo(cloneTi)
		require.Equal(t, tiInfos[i], cloneTi)
	}

	// drop two tables and create all three
	// expect: silently succeed
	// 1. drop table
	err = tracker.DropTable(tables[2])
	require.NoError(t, err)
	err = tracker.DropTable(tables[0])
	require.NoError(t, err)
	// 2. batch create
	err = tracker.BatchCreateTableIfNotExist(tablesToCreate)
	require.NoError(t, err)
	// 3. check
	for i := range tables {
		var ti *model.TableInfo
		ti, err = tracker.GetTableInfo(tables[i])
		require.NoError(t, err)
		clearVolatileInfo(ti)
		require.Equal(t, tiInfos[i], ti)
	}

	// BatchCreateTableIfNotExist will also create database
	err = tracker.Exec(ctx, "", parseSQL(t, p, `drop database testdb`))
	require.NoError(t, err)
	err = tracker.BatchCreateTableIfNotExist(tablesToCreate)
	require.NoError(t, err)
}

func TestAllSchemas(t *testing.T) {
	ctx := context.Background()
	p := parser.New()

	tracker, err := NewTestTracker(ctx, "test-tracker", nil, dlog.L())
	require.NoError(t, err)
	defer tracker.Close()

	// nothing should exist...
	require.Len(t, tracker.AllSchemas(), 0)

	// Create several schemas and tables.
	err = tracker.CreateSchemaIfNotExists("testdb1")
	require.NoError(t, err)
	err = tracker.CreateSchemaIfNotExists("testdb2")
	require.NoError(t, err)
	err = tracker.CreateSchemaIfNotExists("testdb3")
	require.NoError(t, err)
	err = tracker.Exec(ctx, "testdb2", parseSQL(t, p, "create table a(a int)"))
	require.NoError(t, err)
	err = tracker.Exec(ctx, "testdb1", parseSQL(t, p, "create table b(a int)"))
	require.NoError(t, err)
	err = tracker.Exec(ctx, "testdb1", parseSQL(t, p, "create table c(a int)"))
	require.NoError(t, err)

	// check schema tables
	tables, err := tracker.ListSchemaTables("testdb1")
	require.NoError(t, err)
	sort.Strings(tables)
	require.Equal(t, []string{"b", "c"}, tables)
	// check schema not exists
	notExistSchemaName := "testdb_not_found"
	_, err = tracker.ListSchemaTables(notExistSchemaName)
	require.True(t, terror.ErrSchemaTrackerUnSchemaNotExist.Equal(err))

	// check that all schemas and tables are present.
	allSchemas := tracker.AllSchemas()
	require.Len(t, allSchemas, 3)
	existingNames := 0
	for _, schema := range allSchemas {
		switch schema {
		case "testdb1":
			existingNames |= 1
			tables, err = tracker.ListSchemaTables(schema)
			require.NoError(t, err)
			require.Len(t, tables, 2)
			for _, table := range tables {
				switch table {
				case "b":
					existingNames |= 8
				case "c":
					existingNames |= 16
				default:
					t.Fatalf("unexpected table testdb1.%s", table)
				}
			}
		case "testdb2":
			existingNames |= 2
			tables, err = tracker.ListSchemaTables(schema)
			require.NoError(t, err)
			require.Len(t, tables, 1)
			table := tables[0]
			require.Equal(t, "a", table)
		case "testdb3":
			existingNames |= 4
		default:
			t.Fatalf("unexpected schema %s", schema)
		}
	}
	require.Equal(t, 31, existingNames)

	// reset the tracker. all schemas should be gone.
	tracker.Reset()
	require.Len(t, tracker.AllSchemas(), 0)
	_, err = tracker.GetTableInfo(&filter.Table{Schema: "testdb2", Name: "a"})
	require.ErrorContains(t, err, "Unknown database 'testdb2'")
}

func mockBaseConn(t *testing.T) (*dbconn.DBConn, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})
	c, err := db.Conn(context.Background())
	require.NoError(t, err)
	baseConn := conn.NewBaseConnForTest(c, nil)
	dbConn := dbconn.NewDBConn(&config.SubTaskConfig{}, baseConn)
	return dbConn, mock
}

func TestInitDownStreamSQLModeAndParser(t *testing.T) {
	dbConn, mock := mockBaseConn(t)

	tracker, err := NewTestTracker(context.Background(), "test-tracker", dbConn, dlog.L())
	require.NoError(t, err)
	defer tracker.Close()

	mock.ExpectBegin()
	mock.ExpectExec(fmt.Sprintf("SET SESSION SQL_MODE = '%s'", mysql.DefaultSQLMode)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tctx := tcontext.NewContext(context.Background(), dlog.L())

	err = tracker.downstreamTracker.initDownStreamSQLModeAndParser(tctx)
	require.NoError(t, err)
	require.NotNil(t, tracker.downstreamTracker.stmtParser)
}

func TestGetDownStreamIndexInfo(t *testing.T) {
	// origin table info
	p := parser.New()
	se := timock.NewContext()
	ctx := context.Background()
	node, err := p.ParseOneStmt("create table t(a int, b int, c varchar(10))", "utf8mb4", "utf8mb4_bin")
	require.NoError(t, err)
	oriTi, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), 1)
	require.NoError(t, err)

	// tracker and sqlmock
	dbConn, mock := mockBaseConn(t)
	tracker, err := NewTestTracker(ctx, "test-tracker", dbConn, dlog.L())
	require.NoError(t, err)
	defer tracker.Close()

	mock.ExpectBegin()
	mock.ExpectExec(fmt.Sprintf("SET SESSION SQL_MODE = '%s'", mysql.DefaultSQLMode)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tableID := "`test`.`test`"

	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int, b int, c varchar(20000), primary key(a, b), key(c(20000)))/*!90000 SHARD_ROW_ID_BITS=6 */"))
	dti, err := tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	require.NoError(t, err)
	require.NotNil(t, dti.WhereHandle.UniqueNotNullIdx)
}

func TestGetDownStreamIndexInfoExceedsMaxIndexLength(t *testing.T) {
	// origin table info
	p := parser.New()
	se := timock.NewContext()
	ctx := context.Background()
	node, err := p.ParseOneStmt("create table t(a int, b int, c varchar(10))", "utf8mb4", "utf8mb4_bin")
	require.NoError(t, err)
	oriTi, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), 1)
	require.NoError(t, err)

	// tracker and sqlmock
	dbConn, mock := mockBaseConn(t)
	tracker, err := NewTestTracker(ctx, "test-tracker", dbConn, dlog.L())
	require.NoError(t, err)
	defer tracker.Close()

	mock.ExpectBegin()
	mock.ExpectExec(fmt.Sprintf("SET SESSION SQL_MODE = '%s'", mysql.DefaultSQLMode)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tableID := "`test`.`test`"

	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a bigint(20), b varbinary(767), c varbinary(767), d varbinary(767), e varbinary(767), primary key(a), key(b, c, d, e, a))"))
	dti, err := tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	require.NoError(t, err)
	require.NotNil(t, dti.WhereHandle.UniqueNotNullIdx)
}

func TestReTrackDownStreamIndex(t *testing.T) {
	// origin table info
	p := parser.New()
	se := timock.NewContext()
	node, err := p.ParseOneStmt("create table t(a int, b int, c varchar(10))", "utf8mb4", "utf8mb4_bin")
	require.NoError(t, err)
	oriTi, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), 1)
	require.NoError(t, err)

	dbConn, mock := mockBaseConn(t)
	tracker, err := NewTestTracker(context.Background(), "test-tracker", dbConn, dlog.L())
	require.NoError(t, err)
	defer tracker.Close()

	mock.ExpectBegin()
	mock.ExpectExec(fmt.Sprintf("SET SESSION SQL_MODE = '%s'", mysql.DefaultSQLMode)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tableID := "`test`.`test`"

	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int, b int, c varchar(10), PRIMARY KEY (a,b))"))
	_, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	require.NoError(t, err)
	_, ok := tracker.downstreamTracker.tableInfos[tableID]
	require.True(t, ok)

	// just table
	targetTables := []*filter.Table{{Schema: "test", Name: "a"}, {Schema: "test", Name: "test"}}
	tracker.RemoveDownstreamSchema(tcontext.Background(), targetTables)
	_, ok = tracker.downstreamTracker.tableInfos[tableID]
	require.False(t, ok)

	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int, b int, c varchar(10), PRIMARY KEY (a,b))"))
	_, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	require.NoError(t, err)
	_, ok = tracker.downstreamTracker.tableInfos[tableID]
	require.True(t, ok)

	tracker.RemoveDownstreamSchema(tcontext.Background(), targetTables)
	_, ok = tracker.downstreamTracker.tableInfos[tableID]
	require.False(t, ok)

	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int primary key, b int, c varchar(10))"))
	_, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	require.NoError(t, err)
	_, ok = tracker.downstreamTracker.tableInfos[tableID]
	require.True(t, ok)

	// just schema
	targetTables = []*filter.Table{{Schema: "test", Name: "a"}, {Schema: "test", Name: ""}}
	tracker.RemoveDownstreamSchema(tcontext.Background(), targetTables)
	_, ok = tracker.downstreamTracker.tableInfos[tableID]
	require.False(t, ok)

	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int, b int, c varchar(10), PRIMARY KEY (a,b))"))
	_, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	require.NoError(t, err)
	_, ok = tracker.downstreamTracker.tableInfos[tableID]
	require.True(t, ok)

	tracker.RemoveDownstreamSchema(tcontext.Background(), targetTables)
	_, ok = tracker.downstreamTracker.tableInfos[tableID]
	require.False(t, ok)

	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(a int primary key, b int, c varchar(10))"))
	_, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	require.NoError(t, err)
	_, ok = tracker.downstreamTracker.tableInfos[tableID]
	require.True(t, ok)
}

func TestVarchar20000(t *testing.T) {
	// origin table info
	p := parser.New()
	node, err := p.ParseOneStmt("create table t(c varchar(20000)) charset=utf8", "", "")
	require.NoError(t, err)
	oriTi, err := ddl.BuildTableInfoFromAST(metabuild.NewContext(), node.(*ast.CreateTableStmt))
	require.NoError(t, err)

	// tracker and sqlmock
	dbConn, mock := mockBaseConn(t)
	tracker, err := NewTestTracker(context.Background(), "test-tracker", dbConn, dlog.L())
	require.NoError(t, err)
	defer tracker.Close()

	mock.ExpectBegin()
	mock.ExpectExec(fmt.Sprintf("SET SESSION SQL_MODE = '%s'", mysql.DefaultSQLMode)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tableID := "`test`.`test`"

	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", "create table t(c varchar(20000)) charset=utf8"))
	_, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	require.NoError(t, err)
	_, ok := tracker.downstreamTracker.tableInfos[tableID]
	require.True(t, ok)
}

func TestPlacementRule(t *testing.T) {
	// origin table info
	p := parser.New()
	node, err := p.ParseOneStmt("create table t(c int) charset=utf8mb4", "", "")
	require.NoError(t, err)
	oriTi, err := ddl.BuildTableInfoFromAST(metabuild.NewContext(), node.(*ast.CreateTableStmt))
	require.NoError(t, err)

	dbConn, mock := mockBaseConn(t)
	tracker, err := NewTestTracker(context.Background(), "test-tracker", dbConn, dlog.L())
	require.NoError(t, err)
	defer tracker.Close()

	mock.ExpectBegin()
	mock.ExpectExec(fmt.Sprintf("SET SESSION SQL_MODE = '%s'", mysql.DefaultSQLMode)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tableID := "`test`.`test`"

	mock.ExpectQuery("SHOW CREATE TABLE " + tableID).WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("test", ""+
				"CREATE TABLE `t` ("+
				"   `c` int(11) DEFAULT NULL"+
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`acdc` */;"))
	_, err = tracker.GetDownStreamTableInfo(tcontext.Background(), tableID, oriTi)
	require.NoError(t, err)
	_, ok := tracker.downstreamTracker.tableInfos[tableID]
	require.True(t, ok)
}

func TestTimeTypes(t *testing.T) {
	ctx := context.Background()
	p := parser.New()
	p.SetSQLMode(0)

	tracker, err := NewTestTracker(ctx, "test-tracker", nil, dlog.L())
	require.NoError(t, err)
	defer tracker.Close()

	// Create some sort of complicated table.
	err = tracker.CreateSchemaIfNotExists("testdb")
	require.NoError(t, err)

	err = tracker.Exec(ctx, "testdb", parseSQL(t, p, `
		create table foo(
			c0 datetime primary key,
			c1 datetime default current_timestamp,
			c2 datetime default '0000-00-00 00:00:00',
			c3 datetime default '2020-02-02 00:00:00',
			c4 timestamp default current_timestamp,
			c5 timestamp default '0000-00-00 00:00:00',
			c6 timestamp default '2020-02-02 00:00:00'
		);
	`))
	require.NoError(t, err)
}

func TestNeedRestrictedSQLExecutor(t *testing.T) {
	ctx := context.Background()
	p := parser.New()

	tracker, err := NewTestTracker(ctx, "test-tracker", nil, dlog.L())
	require.NoError(t, err)
	defer tracker.Close()

	// Create some sort of complicated table.
	err = tracker.CreateSchemaIfNotExists("testdb")
	require.NoError(t, err)

	err = tracker.Exec(ctx, "testdb", parseSQL(t, p, `create table testdb.t (a int, b int);`))
	require.NoError(t, err)

	err = tracker.Exec(ctx, "testdb", parseSQL(t, p, `alter table testdb.t modify column a int not null;`))
	require.NoError(t, err)
}

func TestMustNotUseMockStore(t *testing.T) {
	ctx := context.Background()
	tracker, err := NewTestTracker(ctx, "test-tracker", nil, dlog.L())
	require.NoError(t, err)
	defer tracker.Close()

	require.Nil(t, tracker.se.GetStore(), "see https://github.com/pingcap/tiflow/issues/5334")
}
