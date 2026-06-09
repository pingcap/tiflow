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
	"testing"

	tiddl "github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/shardddl/optimism"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

// clear keys in etcd test cluster.
func clearOptimistTestSourceInfoOperation(t *testing.T) {
	require.NoError(t, optimism.ClearTestInfoOperationColumn(etcdTestCli))
}

func createTableInfo(t *testing.T, p *parser.Parser, se sessionctx.Context, tableID int64, sql string) *model.TableInfo {
	node, err := p.ParseOneStmt(sql, "utf8mb4", "utf8mb4_bin")
	if err != nil {
		t.Fatalf("fail to parse stmt, %v", err)
	}
	createStmtNode, ok := node.(*ast.CreateTableStmt)
	if !ok {
		t.Fatalf("%s is not a CREATE TABLE statement", sql)
	}
	info, err := tiddl.MockTableInfo(se, createStmtNode, tableID)
	if err != nil {
		t.Fatalf("fail to create table info, %v", err)
	}
	return info
}

func testOptimist(t *testing.T) {
	defer clearOptimistTestSourceInfoOperation(t)

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
		tiBefore       = createTableInfo(t, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		tiAfter1       = createTableInfo(t, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
		tiAfter2       = createTableInfo(t, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 DATETIME)`)
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

	tables := o.Tables()
	require.Equal(t, 0, len(tables))

	// init with some source tables.
	err := o.Init(sourceTables)
	require.NoError(t, err)
	stm, _, err := optimism.GetAllSourceTables(etcdTestCli)
	require.NoError(t, err)
	require.Len(t, stm, 1)
	require.Len(t, stm[task], 1)
	require.Equal(t, o.tables, stm[task][source])

	tables = o.Tables()
	require.Equal(t, 4, len(tables))

	// no info and operation in pending.
	require.Nil(t, o.PendingInfo())
	require.Nil(t, o.PendingOperation())

	// put shard DDL info.
	rev1, err := o.PutInfo(info1)
	require.NoError(t, err)
	require.Greater(t, rev1, int64(0))

	// have info in pending.
	info1c := o.PendingInfo()
	require.NotNil(t, info1c)
	require.Equal(t, info1, *info1c)

	// put the lock operation.
	rev2, putted, err := optimism.PutOperation(etcdTestCli, false, op1, rev1)
	require.NoError(t, err)
	require.Greater(t, rev2, rev1)
	require.True(t, putted)

	// wait for the lock operation.
	op1c, err := o.GetOperation(ctx, info1, rev1)
	require.NoError(t, err)
	op1.Revision = rev2
	require.Equal(t, op1, op1c)

	// have operation in pending.
	op1cc := o.PendingOperation()
	require.NotNil(t, op1cc)
	require.Equal(t, op1, *op1cc)

	// mark the operation as done.
	require.NoError(t, o.DoneOperation(op1))

	// verify the operation and info.
	ifm, _, err := optimism.GetAllInfo(etcdTestCli)
	require.NoError(t, err)
	require.Len(t, ifm, 1)
	require.Len(t, ifm[task], 1)
	require.Len(t, ifm[task][source], 1)
	require.Len(t, ifm[task][source][info1.UpSchema], 1)
	info1WithVer := info1
	info1WithVer.Version = 1
	info1WithVer.Revision = rev1
	require.Equal(t, info1WithVer, ifm[task][source][info1.UpSchema][info1.UpTable])
	opc := op1c
	opc.Done = true
	opm, _, err := optimism.GetAllOperations(etcdTestCli)
	require.NoError(t, err)
	require.Len(t, opm, 1)
	require.Len(t, opm[task], 1)
	require.Len(t, opm[task][source], 1)
	require.Len(t, opm[task][source][op1.UpSchema], 1)
	// Revision is in DoneOperation, skip this check
	opc.Revision = opm[task][source][op1.UpSchema][op1.UpTable].Revision
	require.Equal(t, opc, opm[task][source][op1.UpSchema][op1.UpTable])

	// no info and operation in pending now.
	require.Nil(t, o.PendingInfo())
	require.Nil(t, o.PendingOperation())

	// handle `CREATE TABLE`.
	rev3, err := o.AddTable(infoCreate)
	require.NoError(t, err)
	require.Greater(t, rev3, rev2)

	// handle `DROP TABLE`.
	rev4, err := o.RemoveTable(infoDrop)
	require.NoError(t, err)
	require.Greater(t, rev4, rev3)
	ifm, _, err = optimism.GetAllInfo(etcdTestCli)
	require.NoError(t, err)
	require.Nil(t, ifm[task][source][infoDrop.UpSchema])
	require.Nil(t, o.tables.Tables[infoCreate.DownSchema][infoCreate.DownTable][infoCreate.UpSchema])

	// put another info.
	rev5, err := o.PutInfo(info2)
	require.NoError(t, err)
	require.NotNil(t, o.PendingInfo())
	require.Equal(t, info2, *o.PendingInfo())
	require.Nil(t, o.PendingOperation())

	// put another lock operation.
	rev6, putted, err := optimism.PutOperation(etcdTestCli, false, op2, rev5)
	require.NoError(t, err)
	require.Greater(t, rev6, rev5)
	require.True(t, putted)
	// wait for the lock operation.
	_, err = o.GetOperation(ctx, info2, rev5)
	require.NoError(t, err)
	require.NotNil(t, o.PendingOperation())
	op2.Revision = rev6
	require.Equal(t, op2, *o.PendingOperation())

	// reset the optimist.
	o.Reset()
	require.Nil(t, o.PendingInfo())
	require.Nil(t, o.PendingOperation())
}
