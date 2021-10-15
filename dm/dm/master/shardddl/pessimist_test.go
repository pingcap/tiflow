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

	. "github.com/pingcap/check"
	"go.etcd.io/etcd/clientv3"
	v3rpc "go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	"go.etcd.io/etcd/integration"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/shardddl/pessimism"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

var (
	etcdTestCli      *clientv3.Client
	etcdErrCompacted = v3rpc.ErrCompacted
	tt               *testing.T
)

const (
	noRestart          = iota // do nothing in rebuildPessimist, just keep testing
	restartOnly               // restart without building new instance. mock leader role transfer
	restartNewInstance        // restart with build a new instance. mock progress restore from failure
)

type testPessimist struct{}

var _ = Suite(&testPessimist{})

func TestShardDDL(t *testing.T) {
	err := log.InitLogger(&log.Config{})
	if err != nil {
		t.Fatal(err)
	}
	tt = t

	mockCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer mockCluster.Terminate(t)

	etcdTestCli = mockCluster.RandClient()

	TestingT(t)
}

// clear keys in etcd test cluster.
func clearTestInfoOperation(c *C) {
	clearInfo := clientv3.OpDelete(common.ShardDDLPessimismInfoKeyAdapter.Path(), clientv3.WithPrefix())
	clearOp := clientv3.OpDelete(common.ShardDDLPessimismOperationKeyAdapter.Path(), clientv3.WithPrefix())
	_, err := etcdTestCli.Txn(context.Background()).Then(clearInfo, clearOp).Commit()
	c.Assert(err, IsNil)
}

func (t *testPessimist) TestPessimist(c *C) {
	t.testPessimistProgress(c, noRestart)
	t.testPessimistProgress(c, restartOnly)
	t.testPessimistProgress(c, restartNewInstance)
}

func (t *testPessimist) testPessimistProgress(c *C, restart int) {
	defer clearTestInfoOperation(c)

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
				c.Fatalf("unsupported task %s", task)
			}
			return []string{}
		}
		logger = log.L()
		p      = NewPessimist(&logger, sources)

		rebuildPessimist = func(ctx context.Context) {
			switch restart {
			case restartOnly:
				p.Close()
				c.Assert(p.Start(ctx, etcdTestCli), IsNil)
			case restartNewInstance:
				p.Close()
				p = NewPessimist(&logger, sources)
				c.Assert(p.Start(ctx, etcdTestCli), IsNil)
			}
		}
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// CASE 1: start without any previous kv and no etcd operation.
	c.Assert(p.Start(ctx, etcdTestCli), IsNil)
	c.Assert(p.Locks(), HasLen, 0)
	p.Close()
	p.Close() // close multiple times.

	// CASE 2: start again without any previous kv.
	c.Assert(p.Start(ctx, etcdTestCli), IsNil)
	c.Assert(p.Locks(), HasLen, 0)

	// PUT i11, will create a lock but not synced.
	_, err := pessimism.PutInfo(etcdTestCli, i11)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return len(p.Locks()) == 1
	}), IsTrue)
	c.Assert(p.Locks(), HasKey, ID1)
	synced, remain := p.Locks()[ID1].IsSynced()
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)

	// PUT i12, the lock will be synced, then an operation PUT for the owner will be triggered.
	rev1, err := pessimism.PutInfo(etcdTestCli, i12)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		synced, _ = p.Locks()[ID1].IsSynced()
		return synced
	}), IsTrue)

	// wait exec operation for the owner become available.
	opCh := make(chan pessimism.Operation, 10)
	errCh := make(chan error, 10)
	ctx2, cancel2 := context.WithTimeout(ctx, watchTimeout)
	pessimism.WatchOperationPut(ctx2, etcdTestCli, task1, source1, rev1+1, opCh, errCh)
	cancel2()
	close(opCh)
	close(errCh)
	c.Assert(len(errCh), Equals, 0)
	c.Assert(len(opCh), Equals, 1)
	op11 := <-opCh
	c.Assert(op11.Exec, IsTrue)
	c.Assert(op11.Done, IsFalse)

	// mark exec operation for the owner as `done` (and delete the info).
	op11c := op11
	op11c.Done = true
	done, rev2, err := pessimism.PutOperationDeleteExistInfo(etcdTestCli, op11c, i11)
	c.Assert(err, IsNil)
	c.Assert(done, IsTrue)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return p.Locks()[ID1].IsDone(source1)
	}), IsTrue)

	// wait skip operation for the non-owner become available.
	opCh = make(chan pessimism.Operation, 10)
	errCh = make(chan error, 10)
	ctx2, cancel2 = context.WithTimeout(ctx, watchTimeout)
	pessimism.WatchOperationPut(ctx2, etcdTestCli, task1, source2, rev2+1, opCh, errCh)
	cancel2()
	close(opCh)
	close(errCh)
	c.Assert(len(errCh), Equals, 0)
	c.Assert(len(opCh), Equals, 1)
	op12 := <-opCh
	c.Assert(op12.Exec, IsFalse)
	c.Assert(op12.Done, IsFalse)

	// mark skip operation for the non-owner as `done` (and delete the info).
	// the lock should become resolved and deleted.
	op12c := op12
	op12c.Done = true
	done, _, err = pessimism.PutOperationDeleteExistInfo(etcdTestCli, op12c, i12)
	c.Assert(err, IsNil)
	c.Assert(done, IsTrue)
	c.Assert(utils.WaitSomething(50, 100*time.Millisecond, func() bool {
		_, ok := p.Locks()[ID1]
		return !ok
	}), IsTrue)
	c.Assert(p.Locks(), HasLen, 0)
	c.Assert(p.ShowLocks("", nil), HasLen, 0)

	// PUT i21, i22, this will create a lock.
	_, err = pessimism.PutInfo(etcdTestCli, i21)
	c.Assert(err, IsNil)
	_, err = pessimism.PutInfo(etcdTestCli, i22)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		lock := p.Locks()[ID2]
		if lock == nil {
			return false
		}
		_, remain = lock.IsSynced()
		return remain == 1
	}), IsTrue)

	// CASE 3: start again with some previous shard DDL info and the lock is un-synced.
	rebuildPessimist(ctx)
	c.Assert(p.Locks(), HasLen, 1)
	c.Assert(p.Locks(), HasKey, ID2)
	synced, remain = p.Locks()[ID2].IsSynced()
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)

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
	c.Assert(p.ShowLocks("", []string{}), DeepEquals, expectedLock)
	c.Assert(p.ShowLocks(i21.Task, []string{}), DeepEquals, expectedLock)
	c.Assert(p.ShowLocks("", []string{i21.Source}), DeepEquals, expectedLock)
	c.Assert(p.ShowLocks("", []string{i23.Source}), DeepEquals, expectedLock)
	c.Assert(p.ShowLocks("", []string{i22.Source, i23.Source}), DeepEquals, expectedLock)
	c.Assert(p.ShowLocks(i21.Task, []string{i22.Source, i23.Source}), DeepEquals, expectedLock)
	c.Assert(p.ShowLocks("not-exist", []string{}), HasLen, 0)
	c.Assert(p.ShowLocks("", []string{"not-exist"}), HasLen, 0)

	// PUT i23, then the lock will become synced.
	rev3, err := pessimism.PutInfo(etcdTestCli, i23)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		synced, _ = p.Locks()[ID2].IsSynced()
		return synced
	}), IsTrue)

	// wait exec operation for the owner become available.
	opCh = make(chan pessimism.Operation, 10)
	errCh = make(chan error, 10)
	ctx2, cancel2 = context.WithTimeout(ctx, watchTimeout)
	// both source1 and source2 have shard DDL info exist, and neither of them have operation exist.
	// we must ensure source1 always become the owner of the lock.
	pessimism.WatchOperationPut(ctx2, etcdTestCli, task2, source1, rev3+1, opCh, errCh)
	cancel2()
	close(opCh)
	close(errCh)
	c.Assert(len(errCh), Equals, 0)
	c.Assert(len(opCh), Equals, 1)
	op21 := <-opCh
	c.Assert(op21.Exec, IsTrue)
	c.Assert(op21.Done, IsFalse)

	// CASE 4: start again with some previous shard DDL info and non-`done` operation.
	rebuildPessimist(ctx)
	c.Assert(p.Locks(), HasLen, 1)
	c.Assert(p.Locks(), HasKey, ID2)
	synced, _ = p.Locks()[ID2].IsSynced()
	c.Assert(synced, IsTrue)
	c.Assert(p.Locks()[ID2].IsDone(source1), IsFalse)

	// mark exec operation for the owner as `done` (and delete the info).
	op21c := op21
	op21c.Done = true
	done, _, err = pessimism.PutOperationDeleteExistInfo(etcdTestCli, op21c, i21)
	c.Assert(err, IsNil)
	c.Assert(done, IsTrue)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return p.Locks()[ID2].IsDone(source1)
	}), IsTrue)

	// CASE 5: start again with some previous shard DDL info and `done` operation for the owner.
	rebuildPessimist(ctx)
	c.Assert(p.Locks(), HasLen, 1)
	c.Assert(p.Locks(), HasKey, ID2)
	synced, _ = p.Locks()[ID2].IsSynced()
	c.Assert(synced, IsTrue)
	c.Assert(p.Locks()[ID2].IsDone(source1), IsTrue)
	c.Assert(p.Locks()[ID2].IsDone(source2), IsFalse)

	// mark exec operation for one non-owner as `done` (and delete the info).
	op22c := pessimism.NewOperation(ID2, task2, source2, DDLs, false, true)
	done, _, err = pessimism.PutOperationDeleteExistInfo(etcdTestCli, op22c, i22)
	c.Assert(err, IsNil)
	c.Assert(done, IsTrue)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return p.Locks()[ID2].IsDone(source2)
	}), IsTrue)

	// CASE 6: start again with some previous shard DDL info and `done` operation for the owner and non-owner.
	rebuildPessimist(ctx)
	c.Assert(p.Locks(), HasLen, 1)
	c.Assert(p.Locks(), HasKey, ID2)
	synced, _ = p.Locks()[ID2].IsSynced()
	c.Assert(synced, IsTrue)
	c.Assert(p.Locks()[ID2].IsDone(source1), IsTrue)
	c.Assert(p.Locks()[ID2].IsDone(source2), IsTrue)
	c.Assert(p.Locks()[ID2].IsDone(source3), IsFalse)

	// mark skip operation for the non-owner as `done` (and delete the info).
	// the lock should become resolved and deleted.
	op23c := pessimism.NewOperation(ID2, task2, source3, DDLs, false, true)
	done, _, err = pessimism.PutOperationDeleteExistInfo(etcdTestCli, op23c, i23)
	c.Assert(err, IsNil)
	c.Assert(done, IsTrue)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		_, ok := p.Locks()[ID2]
		return !ok
	}), IsTrue)
	c.Assert(p.Locks(), HasLen, 0)

	// CASE 7: start again after all shard DDL locks have been resolved.
	rebuildPessimist(ctx)
	c.Assert(p.Locks(), HasLen, 0)
	p.Close() // close the Pessimist.
}

func (t *testPessimist) TestSourceReEntrant(c *C) {
	// sources (owner or non-owner) may be interrupted and re-run the sequence again.
	defer clearTestInfoOperation(c)

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
				c.Fatalf("unsupported task %s", task)
			}
			return []string{}
		}
		logger = log.L()
		p      = NewPessimist(&logger, sources)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 0. start the pessimist.
	c.Assert(p.Start(ctx, etcdTestCli), IsNil)
	c.Assert(p.Locks(), HasLen, 0)
	defer p.Close()

	// 1. PUT i11 and i12, will create a lock but not synced.
	_, err := pessimism.PutInfo(etcdTestCli, i11)
	c.Assert(err, IsNil)
	_, err = pessimism.PutInfo(etcdTestCli, i12)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		lock := p.Locks()[ID]
		if lock == nil {
			return false
		}
		_, remain := lock.IsSynced()
		return remain == 1
	}), IsTrue)

	// 2. re-PUT i11, to simulate the re-entrant of the owner before the lock become synced.
	rev1, err := pessimism.PutInfo(etcdTestCli, i11)
	c.Assert(err, IsNil)

	// 3. re-PUT i12, to simulate the re-entrant of the non-owner before the lock become synced.
	rev2, err := pessimism.PutInfo(etcdTestCli, i12)
	c.Assert(err, IsNil)

	// 4. wait exec operation for the owner become available.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		opCh := make(chan pessimism.Operation, 10)
		errCh := make(chan error, 10)
		ctx2, cancel2 := context.WithTimeout(ctx, watchTimeout)
		pessimism.WatchOperationPut(ctx2, etcdTestCli, task, source1, rev1+1, opCh, errCh)
		cancel2()
		close(opCh)
		close(errCh)
		c.Assert(len(errCh), Equals, 0)
		c.Assert(len(opCh), Equals, 1)
		op := <-opCh
		c.Assert(op.Exec, IsTrue)
		c.Assert(op.Done, IsFalse)
	}()

	// 5. put i13, the lock will become synced, then an operation PUT for the owner will be triggered.
	_, err = pessimism.PutInfo(etcdTestCli, i13)
	c.Assert(err, IsNil)
	wg.Wait()

	// 6. re-PUT i11, to simulate the re-entrant of the owner after the lock become synced.
	rev1, err = pessimism.PutInfo(etcdTestCli, i11)
	c.Assert(err, IsNil)

	// 8. wait exec operation for the owner become available again (with new revision).
	opCh := make(chan pessimism.Operation, 10)
	errCh := make(chan error, 10)
	ctx2, cancel2 := context.WithTimeout(ctx, watchTimeout)
	pessimism.WatchOperationPut(ctx2, etcdTestCli, task, source1, rev1+1, opCh, errCh)
	cancel2()
	close(opCh)
	close(errCh)
	c.Assert(len(errCh), Equals, 0)
	c.Assert(len(opCh), Equals, 1)
	op11 := <-opCh
	c.Assert(op11.Exec, IsTrue)
	c.Assert(op11.Done, IsFalse)

	// 9. wait exec operation for the non-owner become available.
	wg.Add(1)
	go func() {
		defer wg.Done()
		opCh = make(chan pessimism.Operation, 10)
		errCh = make(chan error, 10)
		ctx2, cancel2 = context.WithTimeout(ctx, watchTimeout)
		pessimism.WatchOperationPut(ctx2, etcdTestCli, task, source2, rev2+1, opCh, errCh)
		cancel2()
		close(opCh)
		close(errCh)
		c.Assert(len(errCh), Equals, 0)
		c.Assert(len(opCh), Equals, 1)
		op := <-opCh
		c.Assert(op.Exec, IsFalse)
		c.Assert(op.Done, IsFalse)
	}()

	// 10. mark exec operation for the owner as `done` (and delete the info).
	op11c := op11
	op11c.Done = true
	done, _, err := pessimism.PutOperationDeleteExistInfo(etcdTestCli, op11c, i11)
	c.Assert(err, IsNil)
	c.Assert(done, IsTrue)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return p.Locks()[ID].IsDone(source1)
	}), IsTrue)
	wg.Wait()

	// 11. re-PUT i12, to simulate the re-entrant of the non-owner after the lock become synced.
	rev2, err = pessimism.PutInfo(etcdTestCli, i12)
	c.Assert(err, IsNil)

	// 12. wait skip operation for the non-owner become available again (with new revision, without existing done).
	opCh = make(chan pessimism.Operation, 10)
	errCh = make(chan error, 10)
	ctx2, cancel2 = context.WithTimeout(ctx, watchTimeout)
	pessimism.WatchOperationPut(ctx2, etcdTestCli, task, source2, rev2+1, opCh, errCh)
	cancel2()
	close(opCh)
	close(errCh)
	c.Assert(len(errCh), Equals, 0)
	c.Assert(len(opCh), Equals, 1)
	op12 := <-opCh
	c.Assert(op12.Exec, IsFalse)
	c.Assert(op12.Done, IsFalse)

	// 13. mark skip operation for the non-owner as `done` (and delete the info).
	op12c := op12
	op12c.Done = true
	done, _, err = pessimism.PutOperationDeleteExistInfo(etcdTestCli, op12c, i12)
	c.Assert(err, IsNil)
	c.Assert(done, IsTrue)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return p.Locks()[ID].IsDone(source2)
	}), IsTrue)

	// 14. re-PUT i13, to simulate the re-entrant of the owner after the lock become synced.
	rev3, err := pessimism.PutInfo(etcdTestCli, i13)
	c.Assert(err, IsNil)

	// 15. wait skip operation for the non-owner become available again (with new revision, with existing done).
	opCh = make(chan pessimism.Operation, 10)
	errCh = make(chan error, 10)
	ctx2, cancel2 = context.WithTimeout(ctx, watchTimeout)
	pessimism.WatchOperationPut(ctx2, etcdTestCli, task, source3, rev3+1, opCh, errCh)
	cancel2()
	close(opCh)
	close(errCh)
	c.Assert(len(errCh), Equals, 0)
	c.Assert(len(opCh), Equals, 1)
	op13 := <-opCh
	c.Assert(op13.Exec, IsFalse)
	c.Assert(op13.Done, IsFalse)

	// 16. mark skip operation for the non-owner as `done` (and delete the info).
	// the lock should become resolved now.
	op13c := op13
	op13c.Done = true
	done, _, err = pessimism.PutOperationDeleteExistInfo(etcdTestCli, op13c, i13)
	c.Assert(err, IsNil)
	c.Assert(done, IsTrue)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		_, ok := p.Locks()[ID]
		return !ok
	}), IsTrue)
	t.noLockExist(c, p)
}

func (t *testPessimist) TestUnlockSourceMissBeforeSynced(c *C) {
	// some sources may be deleted (miss) before the lock become synced.
	defer clearTestInfoOperation(c)

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
				c.Fatalf("unsupported task %s", task)
			}
			return []string{}
		}
		logger = log.L()
		p      = NewPessimist(&logger, sources)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 0. start the pessimist.
	c.Assert(terror.ErrMasterPessimistNotStarted.Equal(p.UnlockLock(ctx, ID, "", false)), IsTrue)
	c.Assert(p.Start(ctx, etcdTestCli), IsNil)
	c.Assert(p.Locks(), HasLen, 0)
	defer p.Close()

	// no lock need to be unlock now.
	c.Assert(terror.ErrMasterLockNotFound.Equal(p.UnlockLock(ctx, ID, "", false)), IsTrue)

	// 1. PUT i11 & i12, will create a lock but now synced.
	// not PUT info for source3 to simulate the deletion of it.
	_, err := pessimism.PutInfo(etcdTestCli, i11)
	c.Assert(err, IsNil)
	rev1, err := pessimism.PutInfo(etcdTestCli, i12)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		if len(p.Locks()) != 1 {
			return false
		}
		_, remain := p.Locks()[ID].IsSynced()
		return remain == 1
	}), IsTrue)
	c.Assert(p.Locks(), HasKey, ID)
	synced, _ := p.Locks()[ID].IsSynced()
	c.Assert(synced, IsFalse)
	ready := p.Locks()[ID].Ready()
	c.Assert(ready, HasLen, 3)
	c.Assert(ready[source1], IsTrue)
	c.Assert(ready[source2], IsTrue)
	c.Assert(ready[source3], IsFalse)

	// 2. try to unlock the lock manually, but the owner has not done the operation.
	// this will put `exec` operation for the done.
	c.Assert(terror.ErrMasterOwnerExecDDL.Equal(p.UnlockLock(ctx, ID, "", false)), IsTrue)

	// 3. try to unlock the lock manually, and the owner done the operation.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		// put done for the owner.
		t.putDoneForSource(ctx, task, source1, i11, true, rev1+1, watchTimeout, c)
	}()
	go func() {
		defer wg.Done()
		// put done for the synced `source2`, no need to put done for the un-synced `source3`.
		t.putDoneForSource(ctx, task, source2, i12, false, rev1+1, watchTimeout, c)
	}()
	c.Assert(p.UnlockLock(ctx, ID, "", false), IsNil)
	wg.Wait()

	// 4. the lock should be removed now.
	t.noLockExist(c, p)
}

func (t *testPessimist) TestUnlockSourceInterrupt(c *C) {
	// operations may be done but not be deleted, and then interrupted.
	defer clearTestInfoOperation(c)

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
				c.Fatalf("unsupported task %s", task)
			}
			return []string{}
		}
		logger = log.L()
		p      = NewPessimist(&logger, sources)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 0. start the pessimist.
	c.Assert(p.Start(ctx, etcdTestCli), IsNil)
	c.Assert(p.Locks(), HasLen, 0)
	defer p.Close()

	// CASE 1: owner interrupted.
	// 1. PUT i11 & i12, will create a lock and synced.
	rev1, err := pessimism.PutInfo(etcdTestCli, i11)
	c.Assert(err, IsNil)
	_, err = pessimism.PutInfo(etcdTestCli, i12)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		if len(p.Locks()) != 1 {
			return false
		}
		synced, remain := p.Locks()[ID].IsSynced()
		return synced && remain == 0
	}), IsTrue)
	c.Assert(p.Locks(), HasKey, ID)
	ready := p.Locks()[ID].Ready()
	c.Assert(ready, HasLen, 2)
	c.Assert(ready[source1], IsTrue)
	c.Assert(ready[source2], IsTrue)

	// 2. watch until get not-done operation for the owner.
	opCh := make(chan pessimism.Operation, 10)
	errCh := make(chan error, 10)
	ctx2, cancel2 := context.WithTimeout(ctx, watchTimeout)
	pessimism.WatchOperationPut(ctx2, etcdTestCli, task, "", rev1+1, opCh, errCh)
	cancel2()
	close(opCh)
	close(errCh)
	c.Assert(len(errCh), Equals, 0)
	c.Assert(len(opCh), Equals, 1)
	op := <-opCh
	c.Assert(op.Source, Equals, source1)
	c.Assert(op.Exec, IsTrue)
	c.Assert(op.Done, IsFalse)
	c.Assert(p.Locks()[ID].IsResolved(), IsFalse)

	// 3. try to unlock the lock, but no `done` marked for the owner.
	c.Assert(terror.ErrMasterOwnerExecDDL.Equal(p.UnlockLock(ctx, ID, "", false)), IsTrue)
	c.Assert(p.Locks()[ID].IsResolved(), IsFalse)

	// 4. force to remove the lock even no `done` marked for the owner.
	c.Assert(p.UnlockLock(ctx, ID, "", true), IsNil)
	t.noLockExist(c, p)

	// CASE 2: non-owner interrupted.
	// 1. PUT i11 & i12, will create a lock and synced.
	rev1, err = pessimism.PutInfo(etcdTestCli, i11)
	c.Assert(err, IsNil)
	_, err = pessimism.PutInfo(etcdTestCli, i12)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		if len(p.Locks()) != 1 {
			return false
		}
		synced, remain := p.Locks()[ID].IsSynced()
		return synced && remain == 0
	}), IsTrue)
	c.Assert(p.Locks(), HasKey, ID)
	ready = p.Locks()[ID].Ready()
	c.Assert(ready, HasLen, 2)
	c.Assert(ready[source1], IsTrue)
	c.Assert(ready[source2], IsTrue)

	// 2. putDone for the owner.
	t.putDoneForSource(ctx, task, source1, i11, true, rev1+1, watchTimeout, c)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return p.Locks()[ID].IsDone(source1)
	}), IsTrue)
	c.Assert(p.Locks()[ID].IsDone(source2), IsFalse)

	// 3. unlock the lock.
	c.Assert(p.UnlockLock(ctx, ID, "", false), IsNil)
	t.noLockExist(c, p)
}

func (t *testPessimist) TestUnlockSourceOwnerRemoved(c *C) {
	// the owner may be deleted before the lock become synced.
	defer clearTestInfoOperation(c)

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
				c.Fatalf("unsupported task %s", task)
			}
			return []string{}
		}
		logger = log.L()
		p      = NewPessimist(&logger, sources)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 0. start the pessimist.
	c.Assert(p.Start(ctx, etcdTestCli), IsNil)
	c.Assert(p.Locks(), HasLen, 0)
	defer p.Close()

	// no lock need to be unlock now.
	c.Assert(terror.ErrMasterLockNotFound.Equal(p.UnlockLock(ctx, ID, "", false)), IsTrue)

	// 1. PUT i11 & i12, will create a lock but now synced.
	_, err := pessimism.PutInfo(etcdTestCli, i11)
	c.Assert(err, IsNil)
	rev1, err := pessimism.PutInfo(etcdTestCli, i12)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		if len(p.Locks()) != 1 {
			return false
		}
		_, remain := p.Locks()[ID].IsSynced()
		return remain == 1
	}), IsTrue)
	c.Assert(p.Locks(), HasKey, ID)
	synced, _ := p.Locks()[ID].IsSynced()
	c.Assert(synced, IsFalse)
	ready := p.Locks()[ID].Ready()
	c.Assert(ready, HasLen, 3)
	c.Assert(ready[source1], IsTrue)
	c.Assert(ready[source2], IsTrue)
	c.Assert(ready[source3], IsFalse)

	// 2. try to unlock the lock with an un-synced replace owner.
	c.Assert(terror.ErrMasterWorkerNotWaitLock.Equal(p.UnlockLock(ctx, ID, source3, false)), IsTrue)

	// 3. try to unlock the lock with a synced replace owner, but the replace owner has not done the operation.
	// this will put `exec` operation for the done.
	c.Assert(terror.ErrMasterOwnerExecDDL.Equal(p.UnlockLock(ctx, ID, source2, false)), IsTrue)

	// 4. put done for the replace owner then can unlock the lock.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t.putDoneForSource(ctx, task, source2, i11, true, rev1+1, watchTimeout, c)
	}()
	c.Assert(p.UnlockLock(ctx, ID, source2, false), IsNil)
	wg.Wait()

	// 4. the lock should be removed now.
	t.noLockExist(c, p)
}

func (t *testPessimist) TestMeetEtcdCompactError(c *C) {
	defer clearTestInfoOperation(c)

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
				c.Fatalf("unsupported task %s", task)
			}
			return []string{}
		}
		logger = log.L()
		p      = NewPessimist(&logger, sources)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.cli = etcdTestCli

	for i := 0; i <= 1; i++ {
		// i == 0, watch info is compacted; i == 1, watch operation is compacted
		// step 1: trigger an etcd compaction
		if i == 0 {
			revCompacted, err = pessimism.PutInfo(etcdTestCli, i11)
		} else {
			var putted bool
			revCompacted, putted, err = pessimism.PutOperations(etcdTestCli, false, op)
			c.Assert(putted, IsTrue)
		}
		c.Assert(err, IsNil)
		if i == 0 {
			_, err = pessimism.DeleteInfosOperations(etcdTestCli, []pessimism.Info{i11}, []pessimism.Operation{})
		} else {
			_, err = pessimism.DeleteOperations(etcdTestCli, op)
		}
		c.Assert(err, IsNil)
		revThreshold, err := pessimism.PutInfo(etcdTestCli, i11)
		c.Assert(err, IsNil)
		_, err = etcdTestCli.Compact(ctx, revThreshold)
		c.Assert(err, IsNil)

		infoCh = make(chan pessimism.Info, 10)
		errCh = make(chan error, 10)
		ctx1, cancel1 := context.WithTimeout(ctx, time.Second)
		if i == 0 {
			pessimism.WatchInfoPut(ctx1, etcdTestCli, revCompacted, infoCh, errCh)
		} else {
			pessimism.WatchOperationPut(ctx1, etcdTestCli, "", "", revCompacted, opCh, errCh)
		}
		cancel1()
		select {
		case err = <-errCh:
			c.Assert(err, Equals, etcdErrCompacted)
		case <-time.After(300 * time.Millisecond):
			c.Fatal("fail to get etcd error compacted")
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
			c.Assert(p.run(ctx2, etcdTestCli, rev1, rev2), IsNil)
		}()
		// PUT i11, will create a lock but not synced.
		c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
			return len(p.Locks()) == 1
		}), IsTrue)
		c.Assert(p.Locks(), HasKey, ID1)
		synced, remain := p.Locks()[ID1].IsSynced()
		c.Assert(synced, IsFalse)
		c.Assert(remain, Equals, 1)

		// PUT i12, the lock will be synced, then an operation PUT for the owner will be triggered.
		rev1, err := pessimism.PutInfo(etcdTestCli, i12)
		c.Assert(err, IsNil)
		c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
			synced, _ = p.Locks()[ID1].IsSynced()
			return synced
		}), IsTrue)

		// wait exec operation for the owner become available.
		opCh = make(chan pessimism.Operation, 10)
		errCh = make(chan error, 10)
		ctx3, cancel3 := context.WithTimeout(ctx, watchTimeout)
		pessimism.WatchOperationPut(ctx3, etcdTestCli, task1, source1, rev1+1, opCh, errCh)
		cancel3()
		close(opCh)
		close(errCh)
		c.Assert(len(errCh), Equals, 0)
		c.Assert(len(opCh), Equals, 1)
		op11 := <-opCh
		c.Assert(op11.Exec, IsTrue)
		c.Assert(op11.Done, IsFalse)

		// mark exec operation for the owner as `done` (and delete the info).
		op11c := op11
		op11c.Done = true
		done, rev2, err := pessimism.PutOperationDeleteExistInfo(etcdTestCli, op11c, i11)
		c.Assert(err, IsNil)
		c.Assert(done, IsTrue)
		c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
			return p.Locks()[ID1].IsDone(source1)
		}), IsTrue)

		// wait skip operation for the non-owner become available.
		opCh = make(chan pessimism.Operation, 10)
		errCh = make(chan error, 10)
		ctx3, cancel3 = context.WithTimeout(ctx, watchTimeout)
		pessimism.WatchOperationPut(ctx3, etcdTestCli, task1, source2, rev2+1, opCh, errCh)
		cancel3()
		close(opCh)
		close(errCh)
		c.Assert(len(errCh), Equals, 0)
		c.Assert(len(opCh), Equals, 1)
		op12 := <-opCh
		c.Assert(op12.Exec, IsFalse)
		c.Assert(op12.Done, IsFalse)

		// mark skip operation for the non-owner as `done` (and delete the info).
		// the lock should become resolved and deleted.
		op12c := op12
		op12c.Done = true
		done, _, err = pessimism.PutOperationDeleteExistInfo(etcdTestCli, op12c, i12)
		c.Assert(err, IsNil)
		c.Assert(done, IsTrue)
		c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
			_, ok := p.Locks()[ID1]
			return !ok
		}), IsTrue)
		c.Assert(p.Locks(), HasLen, 0)

		cancel2()
		wg.Wait()
	}
}

func (t *testPessimist) putDoneForSource(
	ctx context.Context, task, source string, info pessimism.Info, exec bool,
	watchRev int64, watchTimeout time.Duration, c *C) {
	var (
		wg            sync.WaitGroup
		opCh          = make(chan pessimism.Operation, 10)
		errCh         = make(chan error, 10)
		ctx2, cancel2 = context.WithTimeout(ctx, watchTimeout)
	)

	wg.Add(2)
	go func() {
		defer wg.Done()
		pessimism.WatchOperationPut(ctx2, etcdTestCli, task, source, watchRev, opCh, errCh)
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
			c.Fatal("wait for the operation of the source timeout")
		case op := <-opCh:
			// put `done` after received non-`done`.
			c.Assert(op.Exec, Equals, exec)
			c.Assert(op.Done, IsFalse)
			op.Done = true
			done, _, err := pessimism.PutOperationDeleteExistInfo(etcdTestCli, op, info)
			c.Assert(err, IsNil)
			c.Assert(done, IsTrue)
		case err := <-errCh:
			c.Fatal(err)
		}
	}()
	wg.Wait()
}

func (t *testPessimist) noLockExist(c *C, p *Pessimist) {
	c.Assert(p.Locks(), HasLen, 0)
	ifm, _, err := pessimism.GetAllInfo(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(ifm, HasLen, 0)
	opm, _, err := pessimism.GetAllOperations(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(opm, HasLen, 0)
}
