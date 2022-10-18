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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/common"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/shardddl/pessimism"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

var etcdErrCompacted = v3rpc.ErrCompacted

const (
	noRestart          = iota // do nothing in rebuildPessimist, just keep testing
	restartOnly               // restart without building new instance. mock leader role transfer
	restartNewInstance        // restart with build a new instance. mock progress restore from failure
)

func TestPessimistSuite(t *testing.T) {
	suite.Run(t, new(testPessimistSuite))
}

type testPessimistSuite struct {
	suite.Suite
	mockCluster *integration.ClusterV3
	etcdTestCli *clientv3.Client
}

func (t *testPessimistSuite) SetupSuite() {
	require.NoError(t.T(), log.InitLogger(&log.Config{}))

	integration.BeforeTestExternal(t.T())
	t.mockCluster = integration.NewClusterV3(t.T(), &integration.ClusterConfig{Size: 1})
	t.etcdTestCli = t.mockCluster.RandClient()
}

func (t *testPessimistSuite) TearDownSuite() {
	t.mockCluster.Terminate(t.T())
}

func (t *testPessimistSuite) TearDownTest() {
	t.clearTestInfoOperation()
}

// clear keys in etcd test cluster.
func (t *testPessimistSuite) clearTestInfoOperation() {
	t.T().Helper()

	clearInfo := clientv3.OpDelete(common.ShardDDLPessimismInfoKeyAdapter.Path(), clientv3.WithPrefix())
	clearOp := clientv3.OpDelete(common.ShardDDLPessimismOperationKeyAdapter.Path(), clientv3.WithPrefix())
	_, err := t.etcdTestCli.Txn(context.Background()).Then(clearInfo, clearOp).Commit()
	require.NoError(t.T(), err)
}

func (t *testPessimistSuite) TestPessimist() {
	t.testPessimistProgress(noRestart)
	t.testPessimistProgress(restartOnly)
	t.testPessimistProgress(restartNewInstance)
}

func (t *testPessimistSuite) testPessimistProgress(restart int) {
	defer t.clearTestInfoOperation()

	var (
		watchTimeout  = 3 * time.Second
		task1         = "task-pessimist-1"
		task2         = "task-pessimist-2"
		source1       = "mysql-replica-1"
		source2       = "mysql-replica-2"
		source3       = "mysql-replica-3"
		schema, table = "foo", "bar"
		DDLs          = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		ID1           = fmt.Sprintf("%s-`%s`.`%s`", task1, schema, table)
		ID2           = fmt.Sprintf("%s-`%s`.`%s`", task2, schema, table)
		i11           = pessimism.NewInfo(task1, source1, schema, table, DDLs)
		i12           = pessimism.NewInfo(task1, source2, schema, table, DDLs)
		i21           = pessimism.NewInfo(task2, source1, schema, table, DDLs)
		i22           = pessimism.NewInfo(task2, source2, schema, table, DDLs)
		i23           = pessimism.NewInfo(task2, source3, schema, table, DDLs)

		sources = func(task string) []string {
			switch task {
			case task1:
				return []string{source1, source2}
			case task2:
				return []string{source1, source2, source3}
			default:
				t.T().Fatalf("unsupported task %s", task)
			}
			return []string{}
		}
		logger = log.L()
		p      = NewPessimist(&logger, sources)

		rebuildPessimist = func(ctx context.Context) {
			switch restart {
			case restartOnly:
				p.Close()
				require.NoError(t.T(), p.Start(ctx, t.etcdTestCli))
			case restartNewInstance:
				p.Close()
				p = NewPessimist(&logger, sources)
				require.NoError(t.T(), p.Start(ctx, t.etcdTestCli))
			}
		}
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// CASE 1: start without any previous kv and no etcd operation.
	require.NoError(t.T(), p.Start(ctx, t.etcdTestCli))
	require.Len(t.T(), p.Locks(), 0)
	p.Close()
	p.Close() // close multiple times.

	// CASE 2: start again without any previous kv.
	require.NoError(t.T(), p.Start(ctx, t.etcdTestCli))
	require.Len(t.T(), p.Locks(), 0)

	// PUT i11, will create a lock but not synced.
	_, err := pessimism.PutInfo(t.etcdTestCli, i11)
	require.NoError(t.T(), err)
	require.Eventually(t.T(), func() bool {
		return len(p.Locks()) == 1
	}, 3*time.Second, 100*time.Millisecond)
	require.Contains(t.T(), p.Locks(), ID1)
	synced, remain := p.Locks()[ID1].IsSynced()
	require.False(t.T(), synced)
	require.Equal(t.T(), 1, remain)
	// PUT i12, the lock will be synced, then an operation PUT for the owner will be triggered.
	rev1, err := pessimism.PutInfo(t.etcdTestCli, i12)
	require.NoError(t.T(), err)
	require.Eventually(t.T(), func() bool {
		synced, _ = p.Locks()[ID1].IsSynced()
		return synced
	}, 3*time.Second, 100*time.Millisecond)

	// wait exec operation for the owner become available.
	opCh := make(chan pessimism.Operation, 10)
	errCh := make(chan error, 10)
	ctx2, cancel2 := context.WithTimeout(ctx, watchTimeout)
	pessimism.WatchOperationPut(ctx2, t.etcdTestCli, task1, source1, rev1+1, opCh, errCh)
	cancel2()
	close(opCh)
	close(errCh)
	require.Equal(t.T(), 0, len(errCh))
	require.Equal(t.T(), 1, len(opCh))
	op11 := <-opCh
	require.True(t.T(), op11.Exec)
	require.False(t.T(), op11.Done)

	// mark exec operation for the owner as `done` (and delete the info).
	op11c := op11
	op11c.Done = true
	done, rev2, err := pessimism.PutOperationDeleteExistInfo(t.etcdTestCli, op11c, i11)
	require.NoError(t.T(), err)
	require.True(t.T(), done)
	require.Eventually(t.T(), func() bool {
		return p.Locks()[ID1].IsDone(source1)
	}, 3*time.Second, 100*time.Millisecond)

	// wait skip operation for the non-owner become available.
	opCh = make(chan pessimism.Operation, 10)
	errCh = make(chan error, 10)
	ctx2, cancel2 = context.WithTimeout(ctx, watchTimeout)
	pessimism.WatchOperationPut(ctx2, t.etcdTestCli, task1, source2, rev2+1, opCh, errCh)
	cancel2()
	close(opCh)
	close(errCh)
	require.Equal(t.T(), 0, len(errCh))
	require.Equal(t.T(), 1, len(opCh))
	op12 := <-opCh
	require.False(t.T(), op12.Exec)
	require.False(t.T(), op12.Done)

	// mark skip operation for the non-owner as `done` (and delete the info).
	// the lock should become resolved and deleted.
	op12c := op12
	op12c.Done = true
	done, _, err = pessimism.PutOperationDeleteExistInfo(t.etcdTestCli, op12c, i12)
	require.NoError(t.T(), err)
	require.True(t.T(), done)
	require.Eventually(t.T(), func() bool {
		_, ok := p.Locks()[ID1]
		return !ok
	}, 50*100*time.Millisecond, 100*time.Millisecond)
	require.Len(t.T(), p.Locks(), 0)
	require.Len(t.T(), p.ShowLocks("", nil), 0)

	// PUT i21, i22, this will create a lock.
	_, err = pessimism.PutInfo(t.etcdTestCli, i21)
	require.NoError(t.T(), err)
	_, err = pessimism.PutInfo(t.etcdTestCli, i22)
	require.NoError(t.T(), err)
	require.Eventually(t.T(), func() bool {
		lock := p.Locks()[ID2]
		if lock == nil {
			return false
		}
		_, remain = lock.IsSynced()
		return remain == 1
	}, 3*time.Second, 100*time.Millisecond)

	// CASE 3: start again with some previous shard DDL info and the lock is un-synced.
	rebuildPessimist(ctx)
	require.Len(t.T(), p.Locks(), 1)
	require.Contains(t.T(), p.Locks(), ID2)
	synced, remain = p.Locks()[ID2].IsSynced()
	require.False(t.T(), synced)
	require.Equal(t.T(), 1, remain)
	// check ShowLocks.
	expectedLock := []*pb.DDLLock{
		{
			ID:       ID2,
			Task:     i21.Task,
			Mode:     config.ShardPessimistic,
			Owner:    i21.Source,
			DDLs:     i21.DDLs,
			Synced:   []string{i21.Source, i22.Source},
			Unsynced: []string{i23.Source},
		},
	}
	require.Equal(t.T(), expectedLock, p.ShowLocks("", []string{}))
	require.Equal(t.T(), expectedLock, p.ShowLocks(i21.Task, []string{}))
	require.Equal(t.T(), expectedLock, p.ShowLocks("", []string{i21.Source}))
	require.Equal(t.T(), expectedLock, p.ShowLocks("", []string{i23.Source}))
	require.Equal(t.T(), expectedLock, p.ShowLocks("", []string{i22.Source, i23.Source}))
	require.Equal(t.T(), expectedLock, p.ShowLocks(i21.Task, []string{i22.Source, i23.Source}))
	require.Len(t.T(), p.ShowLocks("not-exist", []string{}), 0)
	require.Len(t.T(), p.ShowLocks("", []string{"not-exist"}), 0)

	// PUT i23, then the lock will become synced.
	rev3, err := pessimism.PutInfo(t.etcdTestCli, i23)
	require.NoError(t.T(), err)
	require.Eventually(t.T(), func() bool {
		synced, _ = p.Locks()[ID2].IsSynced()
		return synced
	}, 3*time.Second, 100*time.Millisecond)

	// wait exec operation for the owner become available.
	opCh = make(chan pessimism.Operation, 10)
	errCh = make(chan error, 10)
	ctx2, cancel2 = context.WithTimeout(ctx, watchTimeout)
	// both source1 and source2 have shard DDL info exist, and neither of them have operation exist.
	// we must ensure source1 always become the owner of the lock.
	pessimism.WatchOperationPut(ctx2, t.etcdTestCli, task2, source1, rev3+1, opCh, errCh)
	cancel2()
	close(opCh)
	close(errCh)
	require.Len(t.T(), errCh, 0)
	require.Len(t.T(), opCh, 1)
	op21 := <-opCh
	require.True(t.T(), op21.Exec)
	require.False(t.T(), op21.Done)

	// CASE 4: start again with some previous shard DDL info and non-`done` operation.
	rebuildPessimist(ctx)
	require.Len(t.T(), p.Locks(), 1)
	require.Contains(t.T(), p.Locks(), ID2)
	synced, _ = p.Locks()[ID2].IsSynced()
	require.True(t.T(), synced)
	require.False(t.T(), p.Locks()[ID2].IsDone(source1))

	// mark exec operation for the owner as `done` (and delete the info).
	op21c := op21
	op21c.Done = true
	done, _, err = pessimism.PutOperationDeleteExistInfo(t.etcdTestCli, op21c, i21)
	require.NoError(t.T(), err)
	require.True(t.T(), done)
	require.Eventually(t.T(), func() bool {
		return p.Locks()[ID2].IsDone(source1)
	}, 3*time.Second, 100*time.Millisecond)

	// CASE 5: start again with some previous shard DDL info and `done` operation for the owner.
	rebuildPessimist(ctx)
	require.Len(t.T(), p.Locks(), 1)
	require.Contains(t.T(), p.Locks(), ID2)
	synced, _ = p.Locks()[ID2].IsSynced()
	require.True(t.T(), synced)
	require.True(t.T(), p.Locks()[ID2].IsDone(source1))
	require.False(t.T(), p.Locks()[ID2].IsDone(source2))

	// mark exec operation for one non-owner as `done` (and delete the info).
	op22c := pessimism.NewOperation(ID2, task2, source2, DDLs, false, true)
	done, _, err = pessimism.PutOperationDeleteExistInfo(t.etcdTestCli, op22c, i22)
	require.NoError(t.T(), err)
	require.True(t.T(), done)
	require.Eventually(t.T(), func() bool {
		return p.Locks()[ID2].IsDone(source2)
	}, 3*time.Second, 100*time.Millisecond)

	// CASE 6: start again with some previous shard DDL info and `done` operation for the owner and non-owner.
	rebuildPessimist(ctx)
	require.Len(t.T(), p.Locks(), 1)
	require.Contains(t.T(), p.Locks(), ID2)
	synced, _ = p.Locks()[ID2].IsSynced()
	require.True(t.T(), synced)
	require.True(t.T(), p.Locks()[ID2].IsDone(source1))
	require.True(t.T(), p.Locks()[ID2].IsDone(source2))
	require.False(t.T(), p.Locks()[ID2].IsDone(source3))

	// mark skip operation for the non-owner as `done` (and delete the info).
	// the lock should become resolved and deleted.
	op23c := pessimism.NewOperation(ID2, task2, source3, DDLs, false, true)
	done, _, err = pessimism.PutOperationDeleteExistInfo(t.etcdTestCli, op23c, i23)
	require.NoError(t.T(), err)
	require.True(t.T(), done)
	require.Eventually(t.T(), func() bool {
		_, ok := p.Locks()[ID2]
		return !ok
	}, 3*time.Second, 100*time.Millisecond)
	require.Len(t.T(), p.Locks(), 0)

	// CASE 7: start again after all shard DDL locks have been resolved.
	rebuildPessimist(ctx)
	require.Len(t.T(), p.Locks(), 0)
	p.Close() // close the Pessimist.
}

func (t *testPessimistSuite) TestSourceReEntrant() {
	// sources (owner or non-owner) may be interrupted and re-run the sequence again.
	var (
		watchTimeout  = 3 * time.Second
		task          = "task-source-re-entrant"
		source1       = "mysql-replica-1"
		source2       = "mysql-replica-2"
		source3       = "mysql-replica-3"
		schema, table = "foo", "bar"
		DDLs          = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		ID            = fmt.Sprintf("%s-`%s`.`%s`", task, schema, table)
		i11           = pessimism.NewInfo(task, source1, schema, table, DDLs)
		i12           = pessimism.NewInfo(task, source2, schema, table, DDLs)
		i13           = pessimism.NewInfo(task, source3, schema, table, DDLs)

		sources = func(task string) []string {
			switch task {
			case task:
				return []string{source1, source2, source3}
			default:
				t.T().Fatalf("unsupported task %s", task)
			}
			return []string{}
		}
		logger = log.L()
		p      = NewPessimist(&logger, sources)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 0. start the pessimist.
	require.NoError(t.T(), p.Start(ctx, t.etcdTestCli))
	require.Len(t.T(), p.Locks(), 0)
	defer p.Close()

	// 1. PUT i11 and i12, will create a lock but not synced.
	_, err := pessimism.PutInfo(t.etcdTestCli, i11)
	require.NoError(t.T(), err)
	_, err = pessimism.PutInfo(t.etcdTestCli, i12)
	require.NoError(t.T(), err)
	require.Eventually(t.T(), func() bool {
		lock := p.Locks()[ID]
		if lock == nil {
			return false
		}
		_, remain := lock.IsSynced()
		return remain == 1
	}, 3*time.Second, 100*time.Millisecond)

	// 2. re-PUT i11, to simulate the re-entrant of the owner before the lock become synced.
	rev1, err := pessimism.PutInfo(t.etcdTestCli, i11)
	require.NoError(t.T(), err)

	// 3. re-PUT i12, to simulate the re-entrant of the non-owner before the lock become synced.
	rev2, err := pessimism.PutInfo(t.etcdTestCli, i12)
	require.NoError(t.T(), err)

	// 4. wait exec operation for the owner become available.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		opCh := make(chan pessimism.Operation, 10)
		errCh := make(chan error, 10)
		ctx2, cancel2 := context.WithTimeout(ctx, watchTimeout)
		pessimism.WatchOperationPut(ctx2, t.etcdTestCli, task, source1, rev1+1, opCh, errCh)
		cancel2()
		close(opCh)
		close(errCh)
		require.Len(t.T(), errCh, 0)
		require.Len(t.T(), opCh, 1)
		op := <-opCh
		require.True(t.T(), op.Exec)
		require.False(t.T(), op.Done)
	}()

	// 5. put i13, the lock will become synced, then an operation PUT for the owner will be triggered.
	_, err = pessimism.PutInfo(t.etcdTestCli, i13)
	require.NoError(t.T(), err)
	wg.Wait()

	// 6. re-PUT i11, to simulate the re-entrant of the owner after the lock become synced.
	rev1, err = pessimism.PutInfo(t.etcdTestCli, i11)
	require.NoError(t.T(), err)

	// 8. wait exec operation for the owner become available again (with new revision).
	opCh := make(chan pessimism.Operation, 10)
	errCh := make(chan error, 10)
	ctx2, cancel2 := context.WithTimeout(ctx, watchTimeout)
	pessimism.WatchOperationPut(ctx2, t.etcdTestCli, task, source1, rev1+1, opCh, errCh)
	cancel2()
	close(opCh)
	close(errCh)
	require.Len(t.T(), errCh, 0)
	require.Len(t.T(), opCh, 1)
	op11 := <-opCh
	require.True(t.T(), op11.Exec)
	require.False(t.T(), op11.Done)

	// 9. wait exec operation for the non-owner become available.
	wg.Add(1)
	go func() {
		defer wg.Done()
		opCh = make(chan pessimism.Operation, 10)
		errCh = make(chan error, 10)
		ctx2, cancel2 = context.WithTimeout(ctx, watchTimeout)
		pessimism.WatchOperationPut(ctx2, t.etcdTestCli, task, source2, rev2+1, opCh, errCh)
		cancel2()
		close(opCh)
		close(errCh)
		require.Len(t.T(), errCh, 0)
		require.Len(t.T(), opCh, 1)
		op := <-opCh
		require.False(t.T(), op.Exec)
		require.False(t.T(), op.Done)
	}()

	// 10. mark exec operation for the owner as `done` (and delete the info).
	op11c := op11
	op11c.Done = true
	done, _, err := pessimism.PutOperationDeleteExistInfo(t.etcdTestCli, op11c, i11)
	require.NoError(t.T(), err)
	require.True(t.T(), done)
	require.Eventually(t.T(), func() bool {
		return p.Locks()[ID].IsDone(source1)
	}, 3*time.Second, 100*time.Millisecond)
	wg.Wait()

	// 11. re-PUT i12, to simulate the re-entrant of the non-owner after the lock become synced.
	rev2, err = pessimism.PutInfo(t.etcdTestCli, i12)
	require.NoError(t.T(), err)

	// 12. wait skip operation for the non-owner become available again (with new revision, without existing done).
	opCh = make(chan pessimism.Operation, 10)
	errCh = make(chan error, 10)
	ctx2, cancel2 = context.WithTimeout(ctx, watchTimeout)
	pessimism.WatchOperationPut(ctx2, t.etcdTestCli, task, source2, rev2+1, opCh, errCh)
	cancel2()
	close(opCh)
	close(errCh)
	require.Len(t.T(), errCh, 0)
	require.Len(t.T(), opCh, 1)
	op12 := <-opCh
	require.False(t.T(), op12.Exec)
	require.False(t.T(), op12.Done)

	// 13. mark skip operation for the non-owner as `done` (and delete the info).
	op12c := op12
	op12c.Done = true
	done, _, err = pessimism.PutOperationDeleteExistInfo(t.etcdTestCli, op12c, i12)
	require.NoError(t.T(), err)
	require.True(t.T(), done)
	require.Eventually(t.T(), func() bool {
		return p.Locks()[ID].IsDone(source2)
	}, 3*time.Second, 100*time.Millisecond)

	// 14. re-PUT i13, to simulate the re-entrant of the owner after the lock become synced.
	rev3, err := pessimism.PutInfo(t.etcdTestCli, i13)
	require.NoError(t.T(), err)

	// 15. wait skip operation for the non-owner become available again (with new revision, with existing done).
	opCh = make(chan pessimism.Operation, 10)
	errCh = make(chan error, 10)
	ctx2, cancel2 = context.WithTimeout(ctx, watchTimeout)
	pessimism.WatchOperationPut(ctx2, t.etcdTestCli, task, source3, rev3+1, opCh, errCh)
	cancel2()
	close(opCh)
	close(errCh)
	require.Len(t.T(), errCh, 0)
	require.Len(t.T(), opCh, 1)
	op13 := <-opCh
	require.False(t.T(), op13.Exec)
	require.False(t.T(), op13.Done)

	// 16. mark skip operation for the non-owner as `done` (and delete the info).
	// the lock should become resolved now.
	op13c := op13
	op13c.Done = true
	done, _, err = pessimism.PutOperationDeleteExistInfo(t.etcdTestCli, op13c, i13)
	require.NoError(t.T(), err)
	require.True(t.T(), done)
	require.Eventually(t.T(), func() bool {
		_, ok := p.Locks()[ID]
		return !ok
	}, 3*time.Second, 100*time.Millisecond)
	t.noLockExist(p)
}

func (t *testPessimistSuite) TestUnlockSourceMissBeforeSynced() {
	// some sources may be deleted (miss) before the lock become synced.

	oriUnlockWaitOwnerInterval := unlockWaitInterval
	unlockWaitInterval = 100 * time.Millisecond
	defer func() {
		unlockWaitInterval = oriUnlockWaitOwnerInterval
	}()

	var (
		watchTimeout  = 3 * time.Second
		task          = "task-unlock-source-lack-before-synced"
		source1       = "mysql-replica-1"
		source2       = "mysql-replica-2"
		source3       = "mysql-replica-3"
		schema, table = "foo", "bar"
		DDLs          = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		ID            = fmt.Sprintf("%s-`%s`.`%s`", task, schema, table)
		i11           = pessimism.NewInfo(task, source1, schema, table, DDLs)
		i12           = pessimism.NewInfo(task, source2, schema, table, DDLs)

		sources = func(task string) []string {
			switch task {
			case task:
				return []string{source1, source2, source3}
			default:
				t.T().Fatalf("unsupported task %s", task)
			}
			return []string{}
		}
		logger = log.L()
		p      = NewPessimist(&logger, sources)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 0. start the pessimist.
	require.True(t.T(), terror.ErrMasterPessimistNotStarted.Equal(p.UnlockLock(ctx, ID, "", false)))
	require.NoError(t.T(), p.Start(ctx, t.etcdTestCli))
	require.Len(t.T(), p.Locks(), 0)
	defer p.Close()

	// no lock need to be unlock now.
	require.True(t.T(), terror.ErrMasterLockNotFound.Equal(p.UnlockLock(ctx, ID, "", false)))

	// 1. PUT i11 & i12, will create a lock but now synced.
	// not PUT info for source3 to simulate the deletion of it.
	_, err := pessimism.PutInfo(t.etcdTestCli, i11)
	require.NoError(t.T(), err)
	rev1, err := pessimism.PutInfo(t.etcdTestCli, i12)
	require.NoError(t.T(), err)
	require.Eventually(t.T(), func() bool {
		if len(p.Locks()) != 1 {
			return false
		}
		_, remain := p.Locks()[ID].IsSynced()
		return remain == 1
	}, 3*time.Second, 100*time.Millisecond)
	require.Contains(t.T(), p.Locks(), ID)
	synced, _ := p.Locks()[ID].IsSynced()
	require.False(t.T(), synced)
	ready := p.Locks()[ID].Ready()
	require.Len(t.T(), ready, 3)
	require.True(t.T(), ready[source1])
	require.True(t.T(), ready[source2])
	require.False(t.T(), ready[source3])

	// 2. try to unlock the lock manually, but the owner has not done the operation.
	// this will put `exec` operation for the done.
	require.True(t.T(), terror.ErrMasterOwnerExecDDL.Equal(p.UnlockLock(ctx, ID, "", false)))

	// 3. try to unlock the lock manually, and the owner done the operation.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		// put done for the owner.
		t.putDoneForSource(ctx, task, source1, i11, true, rev1+1, watchTimeout)
	}()
	go func() {
		defer wg.Done()
		// put done for the synced `source2`, no need to put done for the un-synced `source3`.
		t.putDoneForSource(ctx, task, source2, i12, false, rev1+1, watchTimeout)
	}()
	require.NoError(t.T(), p.UnlockLock(ctx, ID, "", false))
	wg.Wait()

	// 4. the lock should be removed now.
	t.noLockExist(p)
}

func (t *testPessimistSuite) TestUnlockSourceInterrupt() {
	// operations may be done but not be deleted, and then interrupted.

	oriUnlockWaitOwnerInterval := unlockWaitInterval
	unlockWaitInterval = 100 * time.Millisecond
	defer func() {
		unlockWaitInterval = oriUnlockWaitOwnerInterval
	}()

	var (
		watchTimeout  = 3 * time.Second
		task          = "task-unlock-source-interrupt"
		source1       = "mysql-replica-1"
		source2       = "mysql-replica-2"
		schema, table = "foo", "bar"
		DDLs          = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		ID            = fmt.Sprintf("%s-`%s`.`%s`", task, schema, table)
		i11           = pessimism.NewInfo(task, source1, schema, table, DDLs)
		i12           = pessimism.NewInfo(task, source2, schema, table, DDLs)

		sources = func(task string) []string {
			switch task {
			case task:
				return []string{source1, source2}
			default:
				t.T().Fatalf("unsupported task %s", task)
			}
			return []string{}
		}
		logger = log.L()
		p      = NewPessimist(&logger, sources)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 0. start the pessimist.
	require.NoError(t.T(), p.Start(ctx, t.etcdTestCli))
	require.Len(t.T(), p.Locks(), 0)
	defer p.Close()

	// CASE 1: owner interrupted.
	// 1. PUT i11 & i12, will create a lock and synced.
	rev1, err := pessimism.PutInfo(t.etcdTestCli, i11)
	require.NoError(t.T(), err)
	_, err = pessimism.PutInfo(t.etcdTestCli, i12)
	require.NoError(t.T(), err)
	require.Eventually(t.T(), func() bool {
		if len(p.Locks()) != 1 {
			return false
		}
		synced, remain := p.Locks()[ID].IsSynced()
		return synced && remain == 0
	}, 3*time.Second, 100*time.Millisecond)
	require.Contains(t.T(), p.Locks(), ID)
	ready := p.Locks()[ID].Ready()
	require.Len(t.T(), ready, 2)
	require.True(t.T(), ready[source1])
	require.True(t.T(), ready[source2])

	// 2. watch until get not-done operation for the owner.
	opCh := make(chan pessimism.Operation, 10)
	errCh := make(chan error, 10)
	ctx2, cancel2 := context.WithTimeout(ctx, watchTimeout)
	pessimism.WatchOperationPut(ctx2, t.etcdTestCli, task, "", rev1+1, opCh, errCh)
	cancel2()
	close(opCh)
	close(errCh)
	require.Len(t.T(), errCh, 0)
	require.Len(t.T(), opCh, 1)
	op := <-opCh
	require.Equal(t.T(), source1, op.Source)
	require.True(t.T(), op.Exec)
	require.False(t.T(), op.Done)
	require.False(t.T(), p.Locks()[ID].IsResolved())

	// 3. try to unlock the lock, but no `done` marked for the owner.
	require.True(t.T(), terror.ErrMasterOwnerExecDDL.Equal(p.UnlockLock(ctx, ID, "", false)))
	require.False(t.T(), p.Locks()[ID].IsResolved())

	// 4. force to remove the lock even no `done` marked for the owner.
	require.NoError(t.T(), p.UnlockLock(ctx, ID, "", true))
	t.noLockExist(p)

	// CASE 2: non-owner interrupted.
	// 1. PUT i11 & i12, will create a lock and synced.
	rev1, err = pessimism.PutInfo(t.etcdTestCli, i11)
	require.NoError(t.T(), err)
	_, err = pessimism.PutInfo(t.etcdTestCli, i12)
	require.NoError(t.T(), err)
	require.Eventually(t.T(), func() bool {
		if len(p.Locks()) != 1 {
			return false
		}
		synced, remain := p.Locks()[ID].IsSynced()
		return synced && remain == 0
	}, 3*time.Second, 100*time.Millisecond)
	require.Contains(t.T(), p.Locks(), ID)
	ready = p.Locks()[ID].Ready()
	require.Len(t.T(), ready, 2)
	require.True(t.T(), ready[source1])
	require.True(t.T(), ready[source2])

	// 2. putDone for the owner.
	t.putDoneForSource(ctx, task, source1, i11, true, rev1+1, watchTimeout)
	require.Eventually(t.T(), func() bool {
		return p.Locks()[ID].IsDone(source1)
	}, 3*time.Second, 100*time.Millisecond)
	require.False(t.T(), p.Locks()[ID].IsDone(source2))

	// 3. unlock the lock.
	require.NoError(t.T(), p.UnlockLock(ctx, ID, "", false))
	t.noLockExist(p)
}

func (t *testPessimistSuite) TestUnlockSourceOwnerRemoved() {
	// the owner may be deleted before the lock become synced.

	oriUnlockWaitOwnerInterval := unlockWaitInterval
	unlockWaitInterval = 100 * time.Millisecond
	defer func() {
		unlockWaitInterval = oriUnlockWaitOwnerInterval
	}()

	var (
		watchTimeout  = 3 * time.Second
		task          = "task-unlock-source-owner-removed"
		source1       = "mysql-replica-1"
		source2       = "mysql-replica-2"
		source3       = "mysql-replica-3"
		schema, table = "foo", "bar"
		DDLs          = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		ID            = fmt.Sprintf("%s-`%s`.`%s`", task, schema, table)
		i11           = pessimism.NewInfo(task, source1, schema, table, DDLs)
		i12           = pessimism.NewInfo(task, source2, schema, table, DDLs)

		sources = func(task string) []string {
			switch task {
			case task:
				return []string{source1, source2, source3}
			default:
				t.T().Fatalf("unsupported task %s", task)
			}
			return []string{}
		}
		logger = log.L()
		p      = NewPessimist(&logger, sources)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 0. start the pessimist.
	require.NoError(t.T(), p.Start(ctx, t.etcdTestCli))
	require.Len(t.T(), p.Locks(), 0)
	defer p.Close()

	// no lock need to be unlock now.
	require.True(t.T(), terror.ErrMasterLockNotFound.Equal(p.UnlockLock(ctx, ID, "", false)))

	// 1. PUT i11 & i12, will create a lock but now synced.
	_, err := pessimism.PutInfo(t.etcdTestCli, i11)
	require.NoError(t.T(), err)
	rev1, err := pessimism.PutInfo(t.etcdTestCli, i12)
	require.NoError(t.T(), err)
	require.Eventually(t.T(), func() bool {
		if len(p.Locks()) != 1 {
			return false
		}
		_, remain := p.Locks()[ID].IsSynced()
		return remain == 1
	}, 3*time.Second, 100*time.Millisecond)
	require.Contains(t.T(), p.Locks(), ID)
	synced, _ := p.Locks()[ID].IsSynced()
	require.False(t.T(), synced)
	ready := p.Locks()[ID].Ready()
	require.Len(t.T(), ready, 3)
	require.True(t.T(), ready[source1])
	require.True(t.T(), ready[source2])
	require.False(t.T(), ready[source3])

	// 2. try to unlock the lock with an un-synced replace owner.
	require.True(t.T(), terror.ErrMasterWorkerNotWaitLock.Equal(p.UnlockLock(ctx, ID, source3, false)))

	// 3. try to unlock the lock with a synced replace owner, but the replace owner has not done the operation.
	// this will put `exec` operation for the done.
	require.True(t.T(), terror.ErrMasterOwnerExecDDL.Equal(p.UnlockLock(ctx, ID, source2, false)))

	// 4. put done for the replace owner then can unlock the lock.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t.putDoneForSource(ctx, task, source2, i11, true, rev1+1, watchTimeout)
	}()
	require.NoError(t.T(), p.UnlockLock(ctx, ID, source2, false))
	wg.Wait()

	// 4. the lock should be removed now.
	t.noLockExist(p)
}

func (t *testPessimistSuite) TestMeetEtcdCompactError() {
	var (
		watchTimeout  = 3 * time.Second
		task1         = "task-pessimist-1"
		task2         = "task-pessimist-2"
		source1       = "mysql-replica-1"
		source2       = "mysql-replica-2"
		source3       = "mysql-replica-3"
		schema, table = "foo", "bar"
		DDLs          = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		ID1           = fmt.Sprintf("%s-`%s`.`%s`", task1, schema, table)
		i11           = pessimism.NewInfo(task1, source1, schema, table, DDLs)
		i12           = pessimism.NewInfo(task1, source2, schema, table, DDLs)
		op            = pessimism.NewOperation(ID1, task1, source1, DDLs, true, false)
		revCompacted  int64

		infoCh chan pessimism.Info
		opCh   chan pessimism.Operation
		errCh  chan error
		err    error

		sources = func(task string) []string {
			switch task {
			case task1:
				return []string{source1, source2}
			case task2:
				return []string{source1, source2, source3}
			default:
				t.T().Fatalf("unsupported task %s", task)
			}
			return []string{}
		}
		logger = log.L()
		p      = NewPessimist(&logger, sources)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.cli = t.etcdTestCli

	for i := 0; i <= 1; i++ {
		// i == 0, watch info is compacted; i == 1, watch operation is compacted
		// step 1: trigger an etcd compaction
		if i == 0 {
			revCompacted, err = pessimism.PutInfo(t.etcdTestCli, i11)
		} else {
			var putted bool
			revCompacted, putted, err = pessimism.PutOperations(t.etcdTestCli, false, op)
			require.True(t.T(), putted)
		}
		require.NoError(t.T(), err)
		if i == 0 {
			_, err = pessimism.DeleteInfosOperations(t.etcdTestCli, []pessimism.Info{i11}, []pessimism.Operation{})
		} else {
			_, err = pessimism.DeleteOperations(t.etcdTestCli, op)
		}
		require.NoError(t.T(), err)
		revThreshold, err := pessimism.PutInfo(t.etcdTestCli, i11)
		require.NoError(t.T(), err)
		_, err = t.etcdTestCli.Compact(ctx, revThreshold)
		require.NoError(t.T(), err)

		infoCh = make(chan pessimism.Info, 10)
		errCh = make(chan error, 10)
		ctx1, cancel1 := context.WithTimeout(ctx, time.Second)
		if i == 0 {
			pessimism.WatchInfoPut(ctx1, t.etcdTestCli, revCompacted, infoCh, errCh)
		} else {
			pessimism.WatchOperationPut(ctx1, t.etcdTestCli, "", "", revCompacted, opCh, errCh)
		}
		cancel1()
		select {
		case err = <-errCh:
			require.Equal(t.T(), etcdErrCompacted, err)
		case <-time.After(300 * time.Millisecond):
			t.T().Fatal("fail to get etcd error compacted")
		}

		// step 2: start running, i11 and i12 should be handled successfully
		ctx2, cancel2 := context.WithCancel(ctx)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			rev1, rev2 := revCompacted, revThreshold
			if i == 1 {
				rev1, rev2 = rev2, rev1
			}
			require.NoError(t.T(), p.run(ctx2, t.etcdTestCli, rev1, rev2))
		}()
		// PUT i11, will create a lock but not synced.
		require.Eventually(t.T(), func() bool {
			return len(p.Locks()) == 1
		}, 3*time.Second, 100*time.Millisecond)
		require.Contains(t.T(), p.Locks(), ID1)
		synced, remain := p.Locks()[ID1].IsSynced()
		require.False(t.T(), synced)
		require.Equal(t.T(), 1, remain)

		// PUT i12, the lock will be synced, then an operation PUT for the owner will be triggered.
		rev1, err := pessimism.PutInfo(t.etcdTestCli, i12)
		require.NoError(t.T(), err)
		require.Eventually(t.T(), func() bool {
			synced, _ = p.Locks()[ID1].IsSynced()
			return synced
		}, 3*time.Second, 100*time.Millisecond)

		// wait exec operation for the owner become available.
		opCh = make(chan pessimism.Operation, 10)
		errCh = make(chan error, 10)
		ctx3, cancel3 := context.WithTimeout(ctx, watchTimeout)
		pessimism.WatchOperationPut(ctx3, t.etcdTestCli, task1, source1, rev1+1, opCh, errCh)
		cancel3()
		close(opCh)
		close(errCh)
		require.Equal(t.T(), 0, len(errCh))
		require.Equal(t.T(), 1, len(opCh))
		op11 := <-opCh
		require.True(t.T(), op11.Exec)
		require.False(t.T(), op11.Done)

		// mark exec operation for the owner as `done` (and delete the info).
		op11c := op11
		op11c.Done = true
		done, rev2, err := pessimism.PutOperationDeleteExistInfo(t.etcdTestCli, op11c, i11)
		require.NoError(t.T(), err)
		require.True(t.T(), done)
		require.Eventually(t.T(), func() bool {
			return p.Locks()[ID1].IsDone(source1)
		}, 3*time.Second, 100*time.Millisecond)

		// wait skip operation for the non-owner become available.
		opCh = make(chan pessimism.Operation, 10)
		errCh = make(chan error, 10)
		ctx3, cancel3 = context.WithTimeout(ctx, watchTimeout)
		pessimism.WatchOperationPut(ctx3, t.etcdTestCli, task1, source2, rev2+1, opCh, errCh)
		cancel3()
		close(opCh)
		close(errCh)
		require.Equal(t.T(), 0, len(errCh))
		require.Equal(t.T(), 1, len(opCh))
		op12 := <-opCh
		require.False(t.T(), op12.Exec)
		require.False(t.T(), op12.Done)

		// mark skip operation for the non-owner as `done` (and delete the info).
		// the lock should become resolved and deleted.
		op12c := op12
		op12c.Done = true
		done, _, err = pessimism.PutOperationDeleteExistInfo(t.etcdTestCli, op12c, i12)
		require.NoError(t.T(), err)
		require.True(t.T(), done)
		require.Eventually(t.T(), func() bool {
			_, ok := p.Locks()[ID1]
			return !ok
		}, 5*time.Second, 100*time.Millisecond)
		require.Len(t.T(), p.Locks(), 0)

		cancel2()
		wg.Wait()
	}
}

func (t *testPessimistSuite) putDoneForSource(
	ctx context.Context, task, source string, info pessimism.Info, exec bool,
	watchRev int64, watchTimeout time.Duration,
) {
	t.T().Helper()
	var (
		wg            sync.WaitGroup
		opCh          = make(chan pessimism.Operation, 10)
		errCh         = make(chan error, 10)
		ctx2, cancel2 = context.WithTimeout(ctx, watchTimeout)
		doneErr       error
	)

	wg.Add(2)
	go func() {
		defer wg.Done()
		pessimism.WatchOperationPut(ctx2, t.etcdTestCli, task, source, watchRev, opCh, errCh)
		close(opCh)
		close(errCh)
	}()
	go func() {
		defer func() {
			cancel2()
			wg.Done()
		}()
		select {
		case <-ctx2.Done():
			doneErr = errors.New("wait for the operation of the source timeout")
		case op := <-opCh:
			// put `done` after received non-`done`.
			require.Equal(t.T(), exec, op.Exec)
			require.False(t.T(), op.Done)
			op.Done = true
			done, _, err := pessimism.PutOperationDeleteExistInfo(t.etcdTestCli, op, info)
			require.NoError(t.T(), err)
			require.True(t.T(), done)
		case err := <-errCh:
			doneErr = err
		}
	}()
	wg.Wait()
	require.NoError(t.T(), doneErr)
}

func (t *testPessimistSuite) noLockExist(p *Pessimist) {
	t.T().Helper()
	require.Len(t.T(), p.Locks(), 0)
	ifm, _, err := pessimism.GetAllInfo(t.etcdTestCli)
	require.NoError(t.T(), err)
	require.Len(t.T(), ifm, 0)
	opm, _, err := pessimism.GetAllOperations(t.etcdTestCli)
	require.NoError(t.T(), err)
	require.Len(t.T(), opm, 0)
}
