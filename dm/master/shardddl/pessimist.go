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
	"sort"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/master/metrics"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/shardddl/pessimism"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var (
	// variables to control the behavior of waiting for the operation to be done for `UnlockLock`.
	unlockWaitInterval = time.Second
	unlockWaitNum      = 10
)

// Pessimist used to coordinate the shard DDL migration in pessimism mode.
type Pessimist struct {
	mu sync.Mutex

	logger log.Logger

	closed bool
	cancel context.CancelFunc
	wg     sync.WaitGroup

	cli *clientv3.Client
	lk  *pessimism.LockKeeper

	// taskSources used to get all sources relative to the given task.
	taskSources func(task string) []string

	infoOpMu sync.Mutex
}

// NewPessimist creates a new Pessimist instance.
func NewPessimist(pLogger *log.Logger, taskSources func(task string) []string) *Pessimist {
	return &Pessimist{
		logger:      pLogger.WithFields(zap.String("component", "shard DDL pessimist")),
		closed:      true, // mark as closed before started.
		lk:          pessimism.NewLockKeeper(),
		taskSources: taskSources,
	}
}

// Start starts the shard DDL coordination in pessimism mode.
// NOTE: for logic errors, it should start without returning errors (but report via metrics or log) so that the user can fix them.
func (p *Pessimist) Start(pCtx context.Context, etcdCli *clientv3.Client) error {
	p.logger.Info("the shard DDL pessimist is starting")

	p.mu.Lock()
	defer p.mu.Unlock()

	p.cli = etcdCli // p.cli should be set before watching and recover locks because these operations need p.cli
	rev1, rev2, err := p.buildLocks(etcdCli)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(pCtx)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		// TODO: handle fatal error from run
		//nolint:errcheck
		p.run(ctx, etcdCli, rev1, rev2)
	}()

	p.closed = false // started now.
	p.cancel = cancel
	p.logger.Info("the shard DDL pessimist has started")
	return nil
}

func (p *Pessimist) run(ctx context.Context, etcdCli *clientv3.Client, rev1, rev2 int64) error {
	for {
		err := p.watchInfoOperation(ctx, etcdCli, rev1, rev2)
		if etcdutil.IsRetryableError(err) {
			retryNum := 1
			succeed := false
			for !succeed {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(500 * time.Millisecond):
					rev1, rev2, err = p.buildLocks(etcdCli)
					if err != nil {
						log.L().Error("resetWorkerEv is failed, will retry later", zap.Error(err), zap.Int("retryNum", retryNum))
					} else {
						succeed = true
					}
				}
				retryNum++
			}
		} else {
			if err != nil {
				log.L().Error("pessimist is failed and will quit now", zap.Error(err))
			} else {
				log.L().Info("pessimist will quit now")
			}
			return err
		}
	}
}

func (p *Pessimist) buildLocks(etcdCli *clientv3.Client) (int64, int64, error) {
	p.lk.Clear() // clear all previous locks to support re-Start.

	// get the history shard DDL info.
	// for the sequence of coordinate a shard DDL lock, see `/pkg/shardddl/pessimism/doc.go`.
	ifm, rev1, err := pessimism.GetAllInfo(etcdCli)
	if err != nil {
		return 0, 0, err
	}
	p.logger.Info("get history shard DDL info", zap.Reflect("info", ifm), zap.Int64("revision", rev1))

	// get the history shard DDL lock operation.
	// the newly operations after this GET will be received through the WATCH with `rev2`,
	// and call `Lock.MarkDone` multiple times is fine.
	opm, rev2, err := pessimism.GetAllOperations(etcdCli)
	if err != nil {
		return 0, 0, err
	}
	p.logger.Info("get history shard DDL lock operation", zap.Reflect("operation", opm), zap.Int64("revision", rev2))

	// recover the shard DDL lock based on history shard DDL info & lock operation.
	err = p.recoverLocks(ifm, opm)
	if err != nil {
		// only log the error, and don't return it to forbid the startup of the DM-master leader.
		// then these unexpected locks can be handled by the user.
		p.logger.Error("fail to recover locks", log.ShortError(err))
	}
	return rev1, rev2, nil
}

func (p *Pessimist) watchInfoOperation(pCtx context.Context, etcdCli *clientv3.Client, rev1, rev2 int64) error {
	ctx, cancel := context.WithCancel(pCtx)
	var wg sync.WaitGroup
	defer func() {
		cancel()
		wg.Wait()
	}()

	// watch for the shard DDL info and handle them.
	infoCh := make(chan pessimism.Info, 10)
	errCh := make(chan error, 10)
	wg.Add(2)
	go func() {
		defer func() {
			wg.Done()
			close(infoCh)
		}()
		pessimism.WatchInfoPut(ctx, etcdCli, rev1+1, infoCh, errCh)
	}()
	go func() {
		defer wg.Done()
		p.handleInfoPut(ctx, infoCh)
	}()

	// watch for the shard DDL lock operation and handle them.
	opCh := make(chan pessimism.Operation, 10)
	wg.Add(2)
	go func() {
		defer func() {
			wg.Done()
			close(opCh)
		}()
		pessimism.WatchOperationPut(ctx, etcdCli, "", "", rev2+1, opCh, errCh)
	}()
	go func() {
		defer wg.Done()
		p.handleOperationPut(ctx, opCh)
	}()

	select {
	case err := <-errCh:
		return err
	case <-pCtx.Done():
		return nil
	}
}

// Close closes the Pessimist instance.
func (p *Pessimist) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	if p.cancel != nil {
		p.cancel()
		p.cancel = nil
	}

	p.wg.Wait()
	p.closed = true // closed now.
	p.logger.Info("the shard DDL pessimist has closed")
}

// Locks return all shard DDL locks current exist.
func (p *Pessimist) Locks() map[string]*pessimism.Lock {
	return p.lk.Locks()
}

// ShowLocks is used by `show-ddl-locks` command.
func (p *Pessimist) ShowLocks(task string, sources []string) []*pb.DDLLock {
	locks := p.lk.Locks()
	ret := make([]*pb.DDLLock, 0, len(locks))
	for _, lock := range locks {
		if task != "" && task != lock.Task {
			continue // specify task but mismatch
		}
		ready := lock.Ready()
		if len(sources) > 0 {
			for _, worker := range sources {
				if _, ok := ready[worker]; ok {
					goto FOUND // if any source matched, show lock for it.
				}
			}
			continue // specify workers but mismatch
		}
	FOUND:
		l := &pb.DDLLock{
			ID:       lock.ID,
			Task:     lock.Task,
			Mode:     config.ShardPessimistic,
			Owner:    lock.Owner,
			DDLs:     lock.DDLs,
			Synced:   make([]string, 0, len(ready)),
			Unsynced: make([]string, 0, len(ready)),
		}
		for worker, synced := range ready {
			if synced {
				l.Synced = append(l.Synced, worker)
			} else {
				l.Unsynced = append(l.Unsynced, worker)
			}
		}
		sort.Strings(l.Synced)
		sort.Strings(l.Unsynced)
		ret = append(ret, l)
	}
	return ret
}

// UnlockLock unlocks a shard DDL lock manually when using `unlock-ddl-lock` command.
// ID: the shard DDL lock ID.
// replaceOwner: the new owner used to replace the original DDL for executing DDL to downstream.
//
//	if the original owner is still exist, we should NOT specify any replaceOwner.
//
// forceRemove: whether force to remove the DDL lock even fail to unlock it (for the owner).
//
//	if specified forceRemove and then fail to unlock, we may need to use `BreakLock` later.
//
// NOTE: this function has side effects, if it failed, some status can't revert anymore.
// NOTE: this function should not be called if the lock is still in automatic resolving.
func (p *Pessimist) UnlockLock(ctx context.Context, id, replaceOwner string, forceRemove bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return terror.ErrMasterPessimistNotStarted.Generate()
	}
	// 1. find the lock.
	lock := p.lk.FindLock(id)
	if lock == nil {
		return terror.ErrMasterLockNotFound.Generate(id)
	}

	// 2. check whether has resolved before (this often should not happen).
	if lock.IsResolved() {
		err := p.removeLock(lock)
		if err != nil {
			return err
		}
		return terror.ErrMasterLockIsResolving.Generatef("the lock %s has been resolved before", id)
	}

	// 3. find out synced & un-synced sources.
	ready := lock.Ready()
	synced := make([]string, 0, len(ready))
	unsynced := make([]string, 0, len(ready))
	for source, isSynced := range ready {
		if isSynced {
			synced = append(synced, source)
		} else {
			unsynced = append(unsynced, source)
		}
	}
	sort.Strings(synced)
	sort.Strings(unsynced)
	p.logger.Warn("some sources are still not synced before unlock the lock",
		zap.Strings("un-synced", unsynced), zap.Strings("synced", synced))

	// 4. check whether the owner has synced (and it must be synced if using `UnlockLock`).
	// if no source synced yet, we should choose to use `BreakLock` instead.
	owner := lock.Owner
	if replaceOwner != "" {
		p.logger.Warn("replace the owner of the lock", zap.String("lock", id),
			zap.String("original owner", owner), zap.String("new owner", replaceOwner))
		owner = replaceOwner
	}
	if isSynced, ok := ready[owner]; !ok || !isSynced {
		return terror.ErrMasterWorkerNotWaitLock.Generatef(
			"owner %s is not waiting for a lock, but sources %v are waiting for the lock", owner, synced)
	}

	// 5. force to mark the lock as synced.
	lock.ForceSynced()
	var revertLockSync bool // revert lock's sync status if the operation for the owner is not done.
	defer func() {
		if revertLockSync {
			lock.RevertSynced(unsynced)
			p.logger.Warn("revert some sources stage to un-synced", zap.Strings("sources", unsynced))
		}
	}()

	// 6. put `exec` operation for the owner, and wait for the owner to be done.
	done, err := p.waitOwnerToBeDone(ctx, lock, owner)
	if err != nil {
		revertLockSync = true
		return err
	} else if !done && !forceRemove { // if `forceRemove==true`, we still try to complete following steps.
		revertLockSync = true
		return terror.ErrMasterOwnerExecDDL.Generatef(
			"the owner %s of the lock %s has not done the operation", owner, id)
	}

	// 7. put `skip` operations for other sources, and wait for them to be done.
	// NOTE: we don't put operations for un-synced sources,
	// because they should be not waiting for these operations.
	done, err = p.waitNonOwnerToBeDone(ctx, lock, owner, synced)
	if err != nil {
		p.logger.Error("the owner has done the exec operation, but fail to wait for some other sources done the skip operation, the lock is still removed",
			zap.String("lock", id), zap.Bool("force remove", forceRemove), zap.String("owner", owner),
			zap.Strings("un-synced", unsynced), zap.Strings("synced", synced), zap.Error(err))
	} else if !done {
		p.logger.Error("the owner has done the exec operation, but some other sources have not done the skip operation, the lock is still removed",
			zap.String("lock", id), zap.Bool("force remove", forceRemove), zap.String("owner", owner),
			zap.Strings("un-synced", unsynced), zap.Strings("synced", synced))
	}

	// 8. remove or clear shard DDL lock and info.
	p.lk.RemoveLock(id)
	err2 := p.deleteInfosOps(lock)

	switch {
	case err != nil && err2 != nil:
		return terror.ErrMasterPartWorkerExecDDLFail.AnnotateDelegate(
			err, "fail to wait for non-owner sources %v to skip the shard DDL and delete shard DDL infos and operations, %s", unsynced, err2.Error())
	case err != nil:
		return terror.ErrMasterPartWorkerExecDDLFail.Delegate(err, "fail to wait for non-owner sources to skip the shard DDL")
	case err2 != nil:
		return terror.ErrMasterPartWorkerExecDDLFail.Delegate(err2, "fail to delete shard DDL infos and operations")
	}
	return nil
}

// RemoveMetaData removes meta data for a specified task
// NOTE: this function can only be used when the specified task is not running.
func (p *Pessimist) RemoveMetaData(task string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return terror.ErrMasterPessimistNotStarted.Generate()
	}

	infos, ops, _, err := pessimism.GetInfosOperationsByTask(p.cli, task)
	if err != nil {
		return err
	}
	for _, info := range infos {
		p.lk.RemoveLockByInfo(info)
	}
	for _, op := range ops {
		p.lk.RemoveLock(op.ID)
	}

	// clear meta data in etcd
	_, err = pessimism.DeleteInfosOperationsByTask(p.cli, task)
	return err
}

// recoverLocks recovers shard DDL locks based on shard DDL info and shard DDL lock operation.
func (p *Pessimist) recoverLocks(ifm map[string]map[string]pessimism.Info, opm map[string]map[string]pessimism.Operation) error {
	// construct locks based on the shard DDL info.
	for task, ifs := range ifm {
		sources := p.taskSources(task)
		// if no operation exists for the lock, we let the smallest (lexicographical order) source as the owner of the lock.
		// if any operation exists for the lock, we let the source with `exec=true` as the owner of the lock (the logic is below).
		for _, info := range pessimismInfoMapToSlice(ifs) {
			_, _, _, err := p.lk.TrySync(info, sources)
			if err != nil {
				return err
			}
		}
	}

	// update locks based on the lock operation.
	for _, ops := range opm {
		for source, op := range ops {
			lock := p.lk.FindLock(op.ID)
			if lock == nil {
				p.logger.Warn("no shard DDL lock exists for the operation", zap.Stringer("operation", op))
				continue
			}

			// if any operation exists, the lock must have been synced.
			lock.ForceSynced()

			if op.Done {
				lock.MarkDone(source)
			}
			if op.Exec {
				// restore the role of `owner` based on `exec` operation.
				// This is needed because `TrySync` can only set `owner` for the first call of the lock.
				p.logger.Info("restore the role of owner for the shard DDL lock", zap.String("lock", op.ID), zap.String("from", lock.Owner), zap.String("to", op.Source))
				lock.Owner = op.Source
			}
		}
	}

	// try to handle locks.
	for _, lock := range p.lk.Locks() {
		synced, remain := lock.IsSynced()
		if !synced {
			p.logger.Info("restored an un-synced shard DDL lock", zap.String("lock", lock.ID), zap.Int("remain", remain))
			continue
		}
		err := p.handleLock(lock.ID, "")
		if err != nil {
			return err
		}
	}

	return nil
}

// handleInfoPut handles the shard DDL lock info PUTed.
func (p *Pessimist) handleInfoPut(ctx context.Context, infoCh <-chan pessimism.Info) {
	for {
		select {
		case <-ctx.Done():
			return
		case info, ok := <-infoCh:
			if !ok {
				return
			}
			p.logger.Info("receive a shard DDL info", zap.Stringer("info", info))

			p.infoOpMu.Lock()
			lockID, synced, remain, err := p.lk.TrySync(info, p.taskSources(info.Task))
			if err != nil {
				p.logger.Error("fail to try sync shard DDL lock", zap.Stringer("info", info), log.ShortError(err))
				// currently, only DDL mismatch will cause error
				metrics.ReportDDLError(info.Task, metrics.InfoErrSyncLock)
				p.infoOpMu.Unlock()
				continue
			} else if !synced {
				p.logger.Info("the shard DDL lock has not synced", zap.String("lock", lockID), zap.Int("remain", remain))
				p.infoOpMu.Unlock()
				continue
			}
			p.logger.Info("the shard DDL lock has synced", zap.String("lock", lockID))

			err = p.handleLock(lockID, info.Source)
			if err != nil {
				p.logger.Error("fail to handle the shard DDL lock", zap.String("lock", lockID), log.ShortError(err))
				metrics.ReportDDLError(info.Task, metrics.InfoErrHandleLock)
			}
			p.infoOpMu.Unlock()
		}
	}
}

// handleOperationPut handles the shard DDL lock operations PUTed.
func (p *Pessimist) handleOperationPut(ctx context.Context, opCh <-chan pessimism.Operation) {
	for {
		select {
		case <-ctx.Done():
			return
		case op, ok := <-opCh:
			if !ok {
				return
			}
			p.logger.Info("receive a shard DDL lock operation", zap.Stringer("operation", op))
			if !op.Done {
				p.logger.Info("the shard DDL lock operation has not done", zap.Stringer("operation", op))
				continue
			}

			p.infoOpMu.Lock()
			lock := p.lk.FindLock(op.ID)
			if lock == nil {
				p.logger.Warn("no lock for the shard DDL lock operation exist", zap.Stringer("operation", op))
				p.infoOpMu.Unlock()
				continue
			} else if synced, _ := lock.IsSynced(); !synced {
				// this should not happen in normal case.
				p.logger.Warn("the lock for the shard DDL lock operation has not synced", zap.Stringer("operation", op))
				metrics.ReportDDLError(op.Task, metrics.OpErrLockUnSynced)
				p.infoOpMu.Unlock()
				continue
			}

			// update the `done` status of the lock and check whether is resolved.
			lock.MarkDone(op.Source)
			if lock.IsResolved() {
				p.logger.Info("the lock for the shard DDL lock operation has been resolved", zap.Stringer("operation", op))
				// remove all operations for this shard DDL lock.
				err := p.removeLock(lock)
				if err != nil {
					p.logger.Error("fail to delete the shard DDL lock operations", zap.String("lock", lock.ID), log.ShortError(err))
					metrics.ReportDDLError(op.Task, metrics.OpErrRemoveLock)
				}
				p.logger.Info("the lock info for the shard DDL lock operation has been cleared", zap.Stringer("operation", op))
				p.infoOpMu.Unlock()
				continue
			}

			// one of the non-owner dm-worker instance has done the operation,
			// still need to wait for more `done` from other non-owner dm-worker instances.
			if op.Source != lock.Owner {
				p.logger.Info("the shard DDL lock operation of a non-owner has done", zap.Stringer("operation", op), zap.String("owner", lock.Owner))
				p.infoOpMu.Unlock()
				continue
			}

			// the owner has done the operation, put `skip` operation for non-owner dm-worker instances.
			// no need to `skipDone`, all of them should be not done just after the owner has done.
			err := p.putOpsForNonOwner(lock, "", false)
			if err != nil {
				p.logger.Error("fail to put skip shard DDL lock operations for non-owner", zap.String("lock", lock.ID), log.ShortError(err))
				metrics.ReportDDLError(op.Task, metrics.OpErrPutNonOwnerOp)
			}
			p.infoOpMu.Unlock()
		}
	}
}

// handleLock handles a single shard DDL lock.
// if source is not empty, it means the function is triggered by an Info with the source,
// this is often called when the source re-PUTed again after an interrupt.
func (p *Pessimist) handleLock(lockID, source string) error {
	lock := p.lk.FindLock(lockID)
	if lock == nil {
		return nil
	}
	if synced, _ := lock.IsSynced(); !synced {
		return nil // do not handle un-synced lock now.
	}

	// check whether the lock has resolved.
	if lock.IsResolved() {
		// remove all operations for this shard DDL lock.
		// this is to handle the case where dm-master exit before deleting operations for them.
		err := p.removeLock(lock)
		if err != nil {
			return err
		}
		return nil
	}

	// check whether the owner has done.
	if lock.IsDone(lock.Owner) {
		// try to put the skip operation for non-owner dm-worker instances,
		// this is to handle the case where dm-master exit before putting operations for them.
		// use `skipDone` to avoid overwriting any existing operations.
		err := p.putOpsForNonOwner(lock, source, true)
		if err != nil {
			return err
		}
		return nil
	}

	// put `exec=true` for the owner and skip it if already existing.
	return p.putOpForOwner(lock, lock.Owner, true)
}

// putOpForOwner PUTs the shard DDL lock operation for the owner into etcd.
func (p *Pessimist) putOpForOwner(lock *pessimism.Lock, owner string, skipDone bool) error {
	op := pessimism.NewOperation(lock.ID, lock.Task, owner, lock.DDLs, true, false)
	rev, succ, err := pessimism.PutOperations(p.cli, skipDone, op)
	if err != nil {
		return err
	}
	p.logger.Info("put exec shard DDL lock operation for the owner", zap.String("lock", lock.ID), zap.String("owner", lock.Owner), zap.Bool("already done", !succ), zap.Int64("revision", rev))
	return nil
}

// putOpsForNonOwner PUTs shard DDL lock operations for non-owner dm-worker instances into etcd.
func (p *Pessimist) putOpsForNonOwner(lock *pessimism.Lock, onlySource string, skipDone bool) error {
	var sources []string
	if onlySource != "" {
		sources = append(sources, onlySource)
	} else {
		for source := range lock.Ready() {
			if source != lock.Owner {
				sources = append(sources, source)
			}
		}
	}

	ops := make([]pessimism.Operation, 0, len(sources))
	for _, source := range sources {
		ops = append(ops, pessimism.NewOperation(lock.ID, lock.Task, source, lock.DDLs, false, false))
	}

	rev, succ, err := pessimism.PutOperations(p.cli, skipDone, ops...)
	if err != nil {
		return err
	}
	p.logger.Info("put skip shard DDL lock operations for non-owner", zap.String("lock", lock.ID), zap.Strings("non-owner", sources), zap.Bool("already done", !succ), zap.Int64("revision", rev))
	return nil
}

// removeLock removes the lock in memory and its information in etcd.
func (p *Pessimist) removeLock(lock *pessimism.Lock) error {
	// remove all operations for this shard DDL lock.
	if err := p.deleteOps(lock); err != nil {
		return err
	}

	failpoint.Inject("SleepWhenRemoveLock", func(val failpoint.Value) {
		t := val.(int)
		log.L().Info("wait new ddl info putted into etcd in pessimistic",
			zap.String("failpoint", "SleepWhenRemoveLock"),
			zap.Int("max wait second", t))

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		timer := time.NewTimer(time.Duration(t) * time.Second)
		defer timer.Stop()
	OUTER:
		for {
			select {
			case <-timer.C:
				log.L().Info("failed to wait new DDL info", zap.Int("wait second", t))
				break OUTER
			case <-ticker.C:
				// manually check etcd
				infos, _, err := pessimism.GetAllInfo(p.cli)
				if err == nil {
					if _, ok := infos[lock.Task]; ok {
						log.L().Info("found new DDL info")
						break OUTER
					}
				}
			}
		}
	})
	p.lk.RemoveLock(lock.ID)
	return nil
}

// deleteOps DELETEs shard DDL lock operations relative to the lock.
func (p *Pessimist) deleteOps(lock *pessimism.Lock) error {
	ready := lock.Ready()
	ops := make([]pessimism.Operation, 0, len(ready))
	for source := range ready {
		// When deleting operations, we do not verify the value of the operation now,
		// so simply set `exec=false` and `done=true`.
		ops = append(ops, pessimism.NewOperation(lock.ID, lock.Task, source, lock.DDLs, false, true))
	}
	rev, err := pessimism.DeleteOperations(p.cli, ops...)
	if err != nil {
		return err
	}
	p.logger.Info("delete shard DDL lock operations", zap.String("lock", lock.ID), zap.Int64("revision", rev))
	return err
}

// deleteInfos DELETEs shard DDL lock infos and operations relative to the lock.
func (p *Pessimist) deleteInfosOps(lock *pessimism.Lock) error {
	ready := lock.Ready()
	infos := make([]pessimism.Info, 0, len(ready))
	for source := range lock.Ready() {
		// NOTE: we rely one the `schema` and `table` not used in `DeleteInfosOperations`.
		infos = append(infos, pessimism.NewInfo(lock.Task, source, "", "", lock.DDLs))
	}
	ops := make([]pessimism.Operation, 0, len(ready))
	for source := range ready {
		// When deleting operations, we do not verify the value of the operation now,
		// so simply set `exec=false` and `done=true`.
		ops = append(ops, pessimism.NewOperation(lock.ID, lock.Task, source, lock.DDLs, false, true))
	}

	rev, err := pessimism.DeleteInfosOperations(p.cli, infos, ops)
	if err != nil {
		return err
	}
	p.logger.Info("delete shard DDL infos and operations", zap.String("lock", lock.ID), zap.Int64("revision", rev))
	return nil
}

// waitOwnerToBeDone waits for the owner of the lock to be done for the `exec` operation.
func (p *Pessimist) waitOwnerToBeDone(ctx context.Context, lock *pessimism.Lock, owner string) (bool, error) {
	if lock.IsDone(owner) {
		p.logger.Info("the owner of the lock has been done before",
			zap.String("owner", owner), zap.String("lock", lock.ID))
		return true, nil // done before.
	}

	// put the `exec` operation.
	err := p.putOpForOwner(lock, owner, true)
	if err != nil {
		return false, err
	}

	// wait for the owner done the operation.
	for retryNum := 1; retryNum <= unlockWaitNum; retryNum++ {
		select {
		case <-ctx.Done():
			return lock.IsDone(owner), ctx.Err()
		case <-time.After(unlockWaitInterval):
		}
		if lock.IsDone(owner) {
			break
		}
		p.logger.Info("retry to wait for the owner done the operation",
			zap.String("owner", owner), zap.String("lock", lock.ID), zap.Int("retry", retryNum))
	}

	return lock.IsDone(owner), nil
}

// waitNonOwnerToBeDone waits for the non-owner sources of the lock to be done for the `skip` operations.
func (p *Pessimist) waitNonOwnerToBeDone(ctx context.Context, lock *pessimism.Lock, owner string, sources []string) (bool, error) {
	// check whether some sources need to wait.
	if len(sources) == 0 {
		p.logger.Info("no non-owner sources need to wait for the operations", zap.String("lock", lock.ID))
		return true, nil
	}
	waitSources := make([]string, 0, len(sources)-1)
	for _, source := range sources {
		if source != owner {
			waitSources = append(waitSources, source)
		}
	}
	if len(waitSources) == 0 {
		p.logger.Info("no non-owner sources need to wait for the operations", zap.String("lock", lock.ID))
		return true, nil
	}

	// check whether already done before.
	allDone := func() bool {
		for _, source := range waitSources {
			if !lock.IsDone(source) {
				return false
			}
		}
		return true
	}
	if allDone() {
		p.logger.Info("non-owner sources of the lock have been done before",
			zap.String("lock", lock.ID), zap.Strings("sources", waitSources))
		return true, nil
	}

	// put `skip` operations.
	// NOTE: the auto triggered `putOpsForNonOwner` in `handleOperationPut` by the done operation of the owner
	// may put `skip` operations for all non-owner sources, but in order to support `replace owner`,
	// we still put `skip` operations for waitSources one more time with `skipDone=true`.
	ops := make([]pessimism.Operation, 0, len(waitSources))
	for _, source := range waitSources {
		ops = append(ops, pessimism.NewOperation(lock.ID, lock.Task, source, lock.DDLs, false, false))
	}
	rev, succ, err := pessimism.PutOperations(p.cli, true, ops...)
	if err != nil {
		return false, err
	}
	p.logger.Info("put skip shard DDL lock operations for non-owner", zap.String("lock", lock.ID), zap.Strings("non-owner", waitSources), zap.Bool("already done", !succ), zap.Int64("revision", rev))

	// wait sources done the operations.
	for retryNum := 1; retryNum <= unlockWaitNum; retryNum++ {
		var ctxDone bool
		select {
		case <-ctx.Done():
			ctxDone = true
		case <-time.After(unlockWaitInterval):
		}
		if ctxDone || allDone() {
			break
		}
		p.logger.Info("retry to wait for non-owner sources done the operation",
			zap.String("lock", lock.ID), zap.Strings("sources", waitSources), zap.Int("retry", retryNum))
	}

	return allDone(), nil
}
