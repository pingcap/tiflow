// Copyright 2020 PingCAP, Inc.
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

package shardddl

import (
	"context"
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	tiddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/mock"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/shardddl/optimism"
	"github.com/pingcap/dm/pkg/terror"
)

type testOptimist struct{}

var _ = Suite(&testOptimist{})

// clear keys in etcd test cluster.
func clearOptimistTestSourceInfoOperation(c *C) {
	c.Assert(optimism.ClearTestInfoOperationSchema(etcdTestCli), IsNil)
}

func createTableInfo(c *C, p *parser.Parser, se sessionctx.Context, tableID int64, sql string) *model.TableInfo {
	node, err := p.ParseOneStmt(sql, "utf8mb4", "utf8mb4_bin")
	if err != nil {
		c.Fatalf("fail to parse stmt, %v", err)
	}
	createStmtNode, ok := node.(*ast.CreateTableStmt)
	if !ok {
		c.Fatalf("%s is not a CREATE TABLE statement", sql)
	}
	info, err := tiddl.MockTableInfo(se, createStmtNode, tableID)
	if err != nil {
		c.Fatalf("fail to create table info, %v", err)
	}
	return info
}

func (t *testOptimist) TestOptimist(c *C) {
	defer clearOptimistTestSourceInfoOperation(c)

	var (
		task         = "task-optimist"
		source       = "mysql-replicate-1"
		sourceTables = map[string]map[string]map[string]map[string]struct{}{
			"foo": {"bar": {
				"foo-1": {"bar-1": struct{}{}, "bar-2": struct{}{}},
				"foo-2": {"bar-3": struct{}{}, "bar-4": struct{}{}},
			}},
		}
		downSchema, downTable = "foo", "bar"
		ID                    = fmt.Sprintf("%s-`%s`.`%s`", task, downSchema, downTable)

		logger = log.L()
		o      = NewOptimist(&logger, etcdTestCli, task, source)

		p              = parser.New()
		se             = mock.NewContext()
		tblID    int64 = 222
		DDLs1          = []string{"ALTER TABLE bar ADD COLUMN c1 TEXT"}
		DDLs2          = []string{"ALTER TABLE bar ADD COLUMN c1 DATETIME"}
		tiBefore       = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		tiAfter1       = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
		tiAfter2       = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 DATETIME)`)
		info1          = o.ConstructInfo("foo-1", "bar-1", downSchema, downTable, DDLs1, tiBefore, []*model.TableInfo{tiAfter1})
		op1            = optimism.NewOperation(ID, task, source, info1.UpSchema, info1.UpTable, DDLs1, optimism.ConflictNone, "", false, []string{})
		info2          = o.ConstructInfo("foo-1", "bar-2", downSchema, downTable, DDLs2, tiBefore, []*model.TableInfo{tiAfter2})
		op2            = optimism.NewOperation(ID, task, source, info2.UpSchema, info2.UpTable, DDLs2, optimism.ConflictDetected, terror.ErrShardDDLOptimismTrySyncFail.Generate(ID, "conflict").Error(), false, []string{})

		infoCreate = o.ConstructInfo("foo-new", "bar-new", downSchema, downTable,
			[]string{`CREATE TABLE bar (id INT PRIMARY KEY)`}, tiBefore, []*model.TableInfo{tiBefore}) // same table info.
		infoDrop = o.ConstructInfo("foo-new", "bar-new", downSchema, downTable,
			[]string{`DROP TABLE bar`}, nil, nil) // both table infos are nil.
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// init with some source tables.
	err := o.Init(sourceTables)
	c.Assert(err, IsNil)
	stm, _, err := optimism.GetAllSourceTables(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(stm, HasLen, 1)
	c.Assert(stm[task], HasLen, 1)
	c.Assert(stm[task][source], DeepEquals, o.tables)

	// no info and operation in pending.
	c.Assert(o.PendingInfo(), IsNil)
	c.Assert(o.PendingOperation(), IsNil)

	// put shard DDL info.
	rev1, err := o.PutInfo(info1)
	c.Assert(err, IsNil)
	c.Assert(rev1, Greater, int64(0))

	// have info in pending.
	info1c := o.PendingInfo()
	c.Assert(info1c, NotNil)
	c.Assert(*info1c, DeepEquals, info1)

	// put the lock operation.
	rev2, putted, err := optimism.PutOperation(etcdTestCli, false, op1, rev1)
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)
	c.Assert(putted, IsTrue)

	// wait for the lock operation.
	op1c, err := o.GetOperation(ctx, info1, rev1)
	c.Assert(err, IsNil)
	c.Assert(op1c, DeepEquals, op1)

	// have operation in pending.
	op1cc := o.PendingOperation()
	c.Assert(op1cc, NotNil)
	c.Assert(*op1cc, DeepEquals, op1)

	// mark the operation as done.
	c.Assert(o.DoneOperation(op1), IsNil)

	// verify the operation and info.
	ifm, _, err := optimism.GetAllInfo(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(ifm, HasLen, 1)
	c.Assert(ifm[task], HasLen, 1)
	c.Assert(ifm[task][source], HasLen, 1)
	c.Assert(ifm[task][source][info1.UpSchema], HasLen, 1)
	info1WithVer := info1
	info1WithVer.Version = 1
	info1WithVer.Revision = rev1
	c.Assert(ifm[task][source][info1.UpSchema][info1.UpTable], DeepEquals, info1WithVer)
	opc := op1c
	opc.Done = true
	opm, _, err := optimism.GetAllOperations(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(opm, HasLen, 1)
	c.Assert(opm[task], HasLen, 1)
	c.Assert(opm[task][source], HasLen, 1)
	c.Assert(opm[task][source][op1.UpSchema], HasLen, 1)
	c.Assert(opm[task][source][op1.UpSchema][op1.UpTable], DeepEquals, opc)

	// no info and operation in pending now.
	c.Assert(o.PendingInfo(), IsNil)
	c.Assert(o.PendingOperation(), IsNil)

	// handle `CREATE TABLE`.
	rev3, err := o.PutInfoAddTable(infoCreate)
	c.Assert(err, IsNil)
	c.Assert(rev3, Greater, rev2)
	ifm, _, err = optimism.GetAllInfo(etcdTestCli)
	c.Assert(err, IsNil)
	infoCreateWithVer := infoCreate
	infoCreateWithVer.Version = 1
	infoCreateWithVer.Revision = rev3
	c.Assert(ifm[task][source][infoCreate.UpSchema][infoCreate.UpTable], DeepEquals, infoCreateWithVer)
	c.Assert(o.tables.Tables[infoCreate.DownSchema][infoCreate.DownTable][infoCreate.UpSchema], HasKey, infoCreate.UpTable)

	// handle `DROP TABLE`.
	rev4, err := o.DeleteInfoRemoveTable(infoDrop)
	c.Assert(err, IsNil)
	c.Assert(rev4, Greater, rev3)
	ifm, _, err = optimism.GetAllInfo(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(ifm[task][source][infoDrop.UpSchema], IsNil)
	c.Assert(o.tables.Tables[infoCreate.DownSchema][infoCreate.DownTable][infoCreate.UpSchema], IsNil)

	// put another info.
	rev5, err := o.PutInfo(info2)
	c.Assert(err, IsNil)
	c.Assert(o.PendingInfo(), NotNil)
	c.Assert(*o.PendingInfo(), DeepEquals, info2)
	c.Assert(o.PendingOperation(), IsNil)

	// put another lock operation.
	rev6, putted, err := optimism.PutOperation(etcdTestCli, false, op2, rev5)
	c.Assert(err, IsNil)
	c.Assert(rev6, Greater, rev5)
	c.Assert(putted, IsTrue)
	// wait for the lock operation.
	_, err = o.GetOperation(ctx, info2, rev5)
	c.Assert(err, IsNil)
	c.Assert(o.PendingOperation(), NotNil)
	c.Assert(*o.PendingOperation(), DeepEquals, op2)

	// reset the optimist.
	o.Reset()
	c.Assert(o.PendingInfo(), IsNil)
	c.Assert(o.PendingOperation(), IsNil)
}

func (t *testOptimist) TestGetTableInfo(c *C) {
	defer clearOptimistTestSourceInfoOperation(c)

	var (
		task   = "test-get-table-info"
		source = "mysql-replica-1"
		logger = log.L()
		o      = NewOptimist(&logger, etcdTestCli, task, source)

		downSchema       = "foo"
		downTable        = "bar"
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		is               = optimism.NewInitSchema(task, downSchema, downTable,
			createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`))
	)

	// no table info exist now
	ti, err := o.GetTableInfo(downSchema, downTable)
	c.Assert(err, IsNil)
	c.Assert(ti, IsNil)

	// put the table schema.
	_, putted, err := optimism.PutInitSchemaIfNotExist(etcdTestCli, is)
	c.Assert(err, IsNil)
	c.Assert(putted, IsTrue)

	// can get the table info now.
	ti, err = o.GetTableInfo(downSchema, downTable)
	c.Assert(err, IsNil)
	c.Assert(ti, DeepEquals, is.TableInfo)

	// no table info for database.
	ti, err = o.GetTableInfo(downSchema, "")
	c.Assert(err, IsNil)
	c.Assert(ti, IsNil)
}
