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

	tiddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/shardddl/optimism"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/engine/pkg/dm/message"
	dmproto "github.com/pingcap/tiflow/engine/pkg/dm/proto"
	tmock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

func TestOptimistSuite(t *testing.T) {
	suite.Run(t, new(testOptimistSuite))
}

type testOptimistSuite struct {
	suite.Suite
	mockCluster *integration.ClusterV3
	etcdTestCli *clientv3.Client
}

func (t *testOptimistSuite) SetupSuite() {
	integration.BeforeTestExternal(t.T())
	t.mockCluster = integration.NewClusterV3(t.T(), &integration.ClusterConfig{Size: 1})
	t.etcdTestCli = t.mockCluster.RandClient()
}

func (t *testOptimistSuite) TearDownSuite() {
	t.mockCluster.Terminate(t.T())
}

// clear keys in etcd test cluster.
func (t *testOptimistSuite) clearOptimistTestSourceInfoOperation() {
	require.NoError(t.T(), optimism.ClearTestInfoOperationColumn(t.etcdTestCli))
}

func createTableInfo(t *testing.T, p *parser.Parser, se sessionctx.Context, tableID int64, sql string) *model.TableInfo {
	node, err := p.ParseOneStmt(sql, "utf8mb4", "utf8mb4_bin")
	require.NoError(t, err)
	createStmtNode, ok := node.(*ast.CreateTableStmt)
	require.True(t, ok)
	info, err := tiddl.MockTableInfo(se, createStmtNode, tableID)
	require.NoError(t, err)
	return info
}

func (t *testOptimistSuite) TestOptimistDM() {
	defer t.clearOptimistTestSourceInfoOperation()

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
		o      = NewOptimist(&logger, t.etcdTestCli, nil, task, source, "")

		p              = parser.New()
		se             = mock.NewContext()
		tblID    int64 = 222
		DDLs1          = []string{"ALTER TABLE bar ADD COLUMN c1 TEXT"}
		DDLs2          = []string{"ALTER TABLE bar ADD COLUMN c1 DATETIME"}
		tiBefore       = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		tiAfter1       = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
		tiAfter2       = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 DATETIME)`)
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
	require.Len(t.T(), tables, 0)

	// init with some source tables.
	err := o.Init(sourceTables)
	require.NoError(t.T(), err)
	stm, _, err := optimism.GetAllSourceTables(t.etcdTestCli)
	require.NoError(t.T(), err)
	require.Len(t.T(), stm, 1)
	require.Len(t.T(), stm[task], 1)
	ts := o.(*OptimistDM).tables
	require.Equal(t.T(), stm[task][source], ts)

	tables = o.Tables()
	require.Len(t.T(), tables, 4)

	// no info and operation in pending.
	require.Nil(t.T(), o.PendingInfo())
	require.Nil(t.T(), o.PendingOperation())

	// put shard DDL info.
	rev1, err := o.PutInfo(info1)
	require.NoError(t.T(), err)
	require.Greater(t.T(), rev1, int64(0))

	// have info in pending.
	info1c := o.PendingInfo()
	require.NotNil(t.T(), info1c)
	require.Equal(t.T(), info1, *info1c)

	// put the lock operation.
	rev2, putted, err := optimism.PutOperation(t.etcdTestCli, false, op1, rev1)
	require.NoError(t.T(), err)
	require.Greater(t.T(), rev2, rev1)
	require.True(t.T(), putted)

	// wait for the lock operation.
	op1c, err := o.GetOperation(ctx, info1, rev1)
	require.NoError(t.T(), err)
	op1.Revision = rev2
	require.Equal(t.T(), op1, op1c)

	// have operation in pending.
	op1cc := o.PendingOperation()
	require.NotNil(t.T(), op1cc)
	require.Equal(t.T(), op1, *op1cc)

	// mark the operation as done.
	require.NoError(t.T(), o.DoneOperation(op1))

	// verify the operation and info.
	ifm, _, err := optimism.GetAllInfo(t.etcdTestCli)
	require.NoError(t.T(), err)
	require.Len(t.T(), ifm, 1)
	require.Len(t.T(), ifm[task], 1)
	require.Len(t.T(), ifm[task][source], 1)
	require.Len(t.T(), ifm[task][source][info1.UpSchema], 1)
	info1WithVer := info1
	info1WithVer.Version = 1
	info1WithVer.Revision = rev1
	require.Equal(t.T(), info1WithVer, ifm[task][source][info1.UpSchema][info1.UpTable])
	opc := op1c
	opc.Done = true
	opm, _, err := optimism.GetAllOperations(t.etcdTestCli)
	require.NoError(t.T(), err)
	require.Len(t.T(), opm, 1)
	require.Len(t.T(), opm[task], 1)
	require.Len(t.T(), opm[task][source], 1)
	require.Len(t.T(), opm[task][source][op1.UpSchema], 1)
	// Revision is in DoneOperation, skip this check
	opc.Revision = opm[task][source][op1.UpSchema][op1.UpTable].Revision
	require.Equal(t.T(), opc, opm[task][source][op1.UpSchema][op1.UpTable])

	// no info and operation in pending now.
	require.Nil(t.T(), o.PendingInfo())
	require.Nil(t.T(), o.PendingOperation())

	// handle `CREATE TABLE`.
	rev3, err := o.AddTable(infoCreate)
	require.NoError(t.T(), err)
	require.Greater(t.T(), rev3, rev2)

	// handle `DROP TABLE`.
	rev4, err := o.RemoveTable(infoDrop)
	require.NoError(t.T(), err)
	require.Greater(t.T(), rev4, rev3)
	ifm, _, err = optimism.GetAllInfo(t.etcdTestCli)
	require.NoError(t.T(), err)
	require.Nil(t.T(), ifm[task][source][infoDrop.UpSchema])
	ts = o.(*OptimistDM).tables
	require.Nil(t.T(), ts.Tables[infoCreate.DownSchema][infoCreate.DownTable][infoCreate.UpSchema])

	// put another info.
	rev5, err := o.PutInfo(info2)
	require.NoError(t.T(), err)
	require.NotNil(t.T(), o.PendingInfo())
	require.Equal(t.T(), info2, *o.PendingInfo())
	require.Nil(t.T(), o.PendingOperation())

	// put another lock operation.
	rev6, putted, err := optimism.PutOperation(t.etcdTestCli, false, op2, rev5)
	require.NoError(t.T(), err)
	require.Greater(t.T(), rev6, rev5)
	require.True(t.T(), putted)
	// wait for the lock operation.
	_, err = o.GetOperation(ctx, info2, rev5)
	require.NoError(t.T(), err)
	require.NotNil(t.T(), o.PendingOperation())
	op2.Revision = rev6
	require.Equal(t.T(), op2, *o.PendingOperation())

	// reset the optimist.
	o.Reset()
	require.Nil(t.T(), o.PendingInfo())
	require.Nil(t.T(), o.PendingOperation())
}

func (t *testOptimistSuite) TestOptimistEngine() {
	var (
		task         = "task-optimist"
		source       = "mysql-replicate-1"
		jobID        = "job"
		sourceTables = map[string]map[string]map[string]map[string]struct{}{
			"foo": {"bar": {
				"foo-1": {"bar-1": struct{}{}, "bar-2": struct{}{}},
				"foo-2": {"bar-3": struct{}{}, "bar-4": struct{}{}},
			}},
		}
		downSchema, downTable = "foo", "bar"
		ID                    = fmt.Sprintf("%s-`%s`.`%s`", task, downSchema, downTable)

		logger           = log.L()
		mockMessageAgent = &message.MockMessageAgent{}
		o                = NewOptimist(&logger, nil, mockMessageAgent, task, source, jobID)

		p              = parser.New()
		se             = mock.NewContext()
		tblID    int64 = 222
		DDLs1          = []string{"ALTER TABLE bar ADD COLUMN c1 TEXT"}
		DDLs2          = []string{"ALTER TABLE bar ADD COLUMN c1 DATETIME"}
		tiBefore       = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		tiAfter1       = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
		tiAfter2       = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 DATETIME)`)
		info1          = o.ConstructInfo("foo-1", "bar-1", downSchema, downTable, DDLs1, tiBefore, []*model.TableInfo{tiAfter1})
		op1            = optimism.NewOperation(ID, task, source, info1.UpSchema, info1.UpTable, DDLs1, optimism.ConflictNone, "", false, nil)
		info2          = o.ConstructInfo("foo-1", "bar-2", downSchema, downTable, DDLs2, tiBefore, []*model.TableInfo{tiAfter2})
		op2            = optimism.NewOperation(ID, task, source, info2.UpSchema, info2.UpTable, DDLs2, optimism.ConflictDetected, terror.ErrShardDDLOptimismTrySyncFail.Generate(ID, "conflict").Error(), false, nil)

		infoCreate = o.ConstructInfo("foo-new", "bar-new", downSchema, downTable,
			[]string{`CREATE TABLE bar (id INT PRIMARY KEY)`}, tiBefore, []*model.TableInfo{tiBefore}) // same table info.
		infoDrop = o.ConstructInfo("foo-new", "bar-new", downSchema, downTable,
			[]string{`DROP TABLE bar`}, nil, nil) // both table infos are nil.
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// init with some source tables.
	err := o.Init(sourceTables)
	require.NoError(t.T(), err)
	require.Len(t.T(), o.Tables(), 0)

	// no info and operation in pending.
	require.Nil(t.T(), o.PendingInfo())
	require.Nil(t.T(), o.PendingOperation())

	// put shard DDL info.
	mockMessageAgent.On("SendRequest", tmock.Anything, jobID, dmproto.CoordinateDDL, tmock.Anything).Return(&dmproto.CoordinateDDLResponse{DDLs: DDLs1, ConflictStage: optimism.ConflictNone}, nil).Once()
	rev1, err := o.PutInfo(info1)
	require.NoError(t.T(), err)
	require.Equal(t.T(), rev1, int64(0))

	// have info in pending.
	info1c := o.PendingInfo()
	require.NotNil(t.T(), info1c)
	require.Equal(t.T(), info1, *info1c)

	// get the lock operation.
	_, err = o.GetOperation(ctx, info1, rev1)
	require.NoError(t.T(), err)

	// have operation in pending.
	op1cc := o.PendingOperation()
	require.NotNil(t.T(), op1cc)
	require.Equal(t.T(), op1, *op1cc)

	// mark the operation as done.
	require.NoError(t.T(), o.DoneOperation(op1))

	// no info and operation in pending now.
	require.Nil(t.T(), o.PendingInfo())
	require.Nil(t.T(), o.PendingOperation())

	// handle `CREATE TABLE`.
	mockMessageAgent.On("SendRequest", tmock.Anything, jobID, dmproto.CoordinateDDL, tmock.Anything).Return(&dmproto.CoordinateDDLResponse{DDLs: infoCreate.DDLs, ConflictStage: optimism.ConflictNone}, nil).Once()
	rev2, err := o.AddTable(infoCreate)
	require.NoError(t.T(), err)
	require.Equal(t.T(), rev2, int64(0))

	// handle `DROP TABLE`.
	mockMessageAgent.On("SendRequest", tmock.Anything, jobID, dmproto.CoordinateDDL, tmock.Anything).Return(&dmproto.CoordinateDDLResponse{ConflictStage: optimism.ConflictNone}, nil).Once()
	rev3, err := o.RemoveTable(infoDrop)
	require.NoError(t.T(), err)
	require.Equal(t.T(), rev3, int64(0))

	// put another info.
	mockMessageAgent.On("SendRequest", tmock.Anything, jobID, dmproto.CoordinateDDL, tmock.Anything).Return(&dmproto.CoordinateDDLResponse{DDLs: DDLs2, ConflictStage: optimism.ConflictDetected, ErrorMsg: op2.ConflictMsg}, nil).Once()
	rev5, err := o.PutInfo(info2)
	require.NoError(t.T(), err)
	require.NotNil(t.T(), o.PendingInfo())
	require.Equal(t.T(), info2, *o.PendingInfo())

	// get the lock operation.
	_, err = o.GetOperation(ctx, info2, rev5)
	require.NoError(t.T(), err)
	require.NotNil(t.T(), o.PendingOperation())
	require.Equal(t.T(), op2, *o.PendingOperation())

	// reset the optimist.
	o.Reset()
	require.Nil(t.T(), o.PendingInfo())
	require.Nil(t.T(), o.PendingOperation())
	op, id := o.PendingRedirectOperation()
	require.Nil(t.T(), op)
	require.Equal(t.T(), id, "")
}
