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
	"sort"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/util/dbutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/common"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/master/metrics"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/shardddl/optimism"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

// Optimist is used to coordinate the shard DDL migration in optimism mode.
type Optimist struct {
	mu sync.Mutex

	logger log.Logger

	closed bool
	cancel context.CancelFunc
	wg     sync.WaitGroup

	cli *clientv3.Client
	lk  *optimism.LockKeeper
	tk  *optimism.TableKeeper
}

// NewOptimist creates a new Optimist instance.
func NewOptimist(pLogger *log.Logger, getDownstreamMetaFunc func(string) (*config.DBConfig, string)) *Optimist {
	return &Optimist{
		logger: pLogger.WithFields(zap.String("component", "shard DDL optimist")),
		closed: true,
		lk:     optimism.NewLockKeeper(getDownstreamMetaFunc),
		tk:     optimism.NewTableKeeper(),
	}
}

// Start starts the shard DDL coordination in optimism mode.
// NOTE: for logic errors, it should start without returning errors (but report via metrics or log) so that the user can fix them.
func (o *Optimist) Start(pCtx context.Context, etcdCli *clientv3.Client) error {
	o.logger.Info("the shard DDL optimist is starting")

	o.mu.Lock()
	defer o.mu.Unlock()

	o.cli = etcdCli // o.cli should be set before watching and recover locks because these operations need o.cli

	revSource, revInfo, revOperation, err := o.rebuildLocks()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(pCtx)

	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		// TODO: handle fatal error from run
		//nolint:errcheck
		o.run(ctx, revSource, revInfo, revOperation)
	}()

	o.closed = false // started now, no error will interrupt the start process.
	o.cancel = cancel
	o.logger.Info("the shard DDL optimist has started")
	return nil
}

// Close closes the Optimist instance.
func (o *Optimist) Close() {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return
	}

	if o.cancel != nil {
		o.cancel()
		o.cancel = nil
	}

	o.closed = true // closed now.
	o.mu.Unlock()
	// unlock before wg.Wait() to avoid deadlock because other goroutines acquire the lock.
	// such as https://github.com/pingcap/tiflow/blob/92fc4c4/dm/dm/master/shardddl/optimist.go#L686
	o.wg.Wait()
	o.logger.Info("the shard DDL optimist has closed")
}

// Locks return all shard DDL locks current exist.
func (o *Optimist) Locks() map[string]*optimism.Lock {
	return o.lk.Locks()
}

// ShowLocks is used by `show-ddl-locks` command.
func (o *Optimist) ShowLocks(task string, sources []string) ([]*pb.DDLLock, error) {
	locks := o.lk.Locks()
	ret := make([]*pb.DDLLock, 0, len(locks))
	var ifm map[string]map[string]map[string]map[string]optimism.Info
	opm, _, err := optimism.GetAllOperations(o.cli)
	if err == nil {
		ifm, _, err = optimism.GetAllInfo(o.cli)
	}
	for _, lock := range locks {
		if task != "" && task != lock.Task {
			continue // specify task but mismatch
		}
		ready := lock.Ready()
		if len(sources) > 0 {
			for _, source := range sources {
				if _, ok := ready[source]; ok {
					goto FOUND // if any source matched, show lock for it.
				}
			}
			continue // specify sources but mismath
		}
	FOUND:
		var (
			owners    []string
			ddlGroups [][]string
		)

		appendOwnerDDLs := func(opmss map[string]map[string]optimism.Operation, source string) {
			for schema, opmsst := range opmss {
				for table, op := range opmsst {
					if op.ConflictStage != optimism.ConflictDetected {
						continue
					}
					if _, ok := ifm[lock.Task]; !ok {
						continue
					}
					if _, ok := ifm[lock.Task][source]; !ok {
						continue
					}
					if _, ok := ifm[lock.Task][source][schema]; !ok {
						continue
					}
					if info, ok := ifm[lock.Task][source][schema][table]; ok {
						owners = append(owners, utils.GenDDLLockID(source, schema, table))
						ddlGroups = append(ddlGroups, info.DDLs)
					}
				}
			}
		}
		if opms, ok := opm[lock.Task]; ok {
			if len(sources) > 0 {
				for _, source := range sources {
					if opmss, ok := opms[source]; ok {
						appendOwnerDDLs(opmss, source)
					}
				}
			} else {
				for source, opmss := range opms {
					appendOwnerDDLs(opmss, source)
				}
			}
		}
		lockSynced := make([]string, 0, len(ready))
		lockUnsynced := make([]string, 0, len(ready))
		for source, schemaTables := range ready {
			for schema, tables := range schemaTables {
				for table, synced := range tables {
					if synced {
						lockSynced = append(lockSynced, fmt.Sprintf("%s-%s", source, dbutil.TableName(schema, table)))
					} else {
						lockUnsynced = append(lockUnsynced, fmt.Sprintf("%s-%s", source, dbutil.TableName(schema, table)))
					}
				}
			}
		}
		sort.Strings(lockSynced)
		sort.Strings(lockUnsynced)

		if len(owners) == 0 {
			owners = append(owners, "")
			ddlGroups = append(ddlGroups, nil)
		}
		for i, owner := range owners {
			ret = append(ret, &pb.DDLLock{
				ID:       lock.ID,
				Task:     lock.Task,
				Mode:     config.ShardOptimistic,
				Owner:    owner,
				DDLs:     ddlGroups[i],
				Synced:   lockSynced,
				Unsynced: lockUnsynced,
			})
		}
	}
	return ret, err
}

// UnlockLock unlocks a shard DDL lock manually only when using `unlock-ddl-lock` command.
// ID: the shard DDL lock ID.
// source, upstreamSchema, upstreamTable: reveal the upstream table's info which we need to skip/exec
// action: whether to skip/exec the blocking DDLs for the specified upstream table
// NOTE: this function has side effects, if it failed, some status can't revert anymore.
// NOTE: this function should not be called if the lock is still in automatic resolving.
func (o *Optimist) UnlockLock(ctx context.Context, id, source, upstreamSchema, upstreamTable string, action pb.UnlockDDLLockOp) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return terror.ErrMasterOptimistNotStarted.Generate()
	}
	task := utils.ExtractTaskFromLockID(id)
	// 1. find the lock.
	lock := o.lk.FindLock(id)
	if lock == nil {
		return terror.ErrMasterLockNotFound.Generate(id)
	}

	// 2. check whether has resolved before (this often should not happen).
	if lock.IsResolved() {
		_, err := o.removeLock(lock)
		return err
	}

	// 3. find out related info & operation
	infos, ops, _, err := optimism.GetInfosOperationsByTask(o.cli, task)
	if err != nil {
		return terror.ErrMasterLockIsResolving.Generatef("fail to get info and operation for task %s", task)
	}
	l := 0
	for i, info := range infos {
		if info.Task == task && info.Source == source && info.UpSchema == upstreamSchema && info.UpTable == upstreamTable {
			infos[l] = infos[i]
			l++
		}
	}
	// TODO: change this condition after unlock ddl supports unlock several tables at one time
	if l != 1 {
		return terror.ErrMasterLockIsResolving.Generatef("fail to find related info for lock %s", id)
	}
	infos = infos[:l]

	l = 0
	for j, op := range ops {
		if op.Task == task && op.Source == source && op.UpSchema == upstreamSchema && op.UpTable == upstreamTable {
			// TODO: adjust waiting for redirect conflict status
			if op.ConflictStage != optimism.ConflictDetected {
				return terror.ErrMasterLockIsResolving.Generatef("lock %s is in %s status, not conflicted", id, op.ConflictStage)
			}
			ops[l] = ops[j]
			l++
		}
	}
	// TODO: change this condition after unlock ddl supports unlock several tables at one time
	if l != 1 {
		return terror.ErrMasterLockIsResolving.Generatef("fail to find related operation for lock %s", id)
	}
	ops = ops[:l]

	// 4. rewrite operation.DDLs to skip/exec DDLs
	switch action {
	case pb.UnlockDDLLockOp_ExecLock:
		ops[0].DDLs = infos[0].DDLs
	case pb.UnlockDDLLockOp_SkipLock:
		ops[0].DDLs = ops[0].DDLs[:0]
	}
	ops[0].ConflictStage = optimism.ConflictUnlocked

	// 5. put operation into etcd for workers to execute
	rev, succ, err := optimism.PutOperation(o.cli, false, ops[0], ops[0].Revision+1)
	if err != nil {
		return err
	}
	if action == pb.UnlockDDLLockOp_ExecLock {
		lock.UpdateTableAfterUnlock(infos[0])
	}
	o.logger.Info("put shard DDL lock operation", zap.String("lock", id),
		zap.Stringer("operation", ops[0]), zap.Bool("already exist", !succ), zap.Int64("revision", rev))
	return nil
}

// RemoveMetaDataWithTask removes meta data for a specified task
// NOTE: this function can only be used when the specified task is not running.
// This function only be used when --remove-meta or stop-task
// NOTE: For stop-task, we still delete drop columns in etcd though user may restart the task again later.
func (o *Optimist) RemoveMetaDataWithTask(task string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return terror.ErrMasterOptimistNotStarted.Generate()
	}

	lockIDSet := make(map[string]struct{})

	infos, ops, _, err := optimism.GetInfosOperationsByTask(o.cli, task)
	if err != nil {
		return err
	}
	for _, info := range infos {
		o.lk.RemoveLockByInfo(info)
		lockIDSet[utils.GenDDLLockID(info.Task, info.DownSchema, info.DownTable)] = struct{}{}
	}
	for _, op := range ops {
		o.lk.RemoveLock(op.ID)
	}

	o.lk.RemoveDownstreamMeta(task)
	o.tk.RemoveTableByTask(task)

	// clear meta data in etcd
	_, err = optimism.DeleteInfosOperationsTablesByTask(o.cli, task, lockIDSet)
	return err
}

// RemoveMetaDataWithTaskAndSources removes meta data for a specified task and sources
// NOTE: this function can only be used when the specified task for source is not running.
func (o *Optimist) RemoveMetaDataWithTaskAndSources(task string, sources ...string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return terror.ErrMasterOptimistNotStarted.Generate()
	}

	dropColumns := make(map[string][]string)

	// gets all locks for this task
	locks := o.lk.FindLocksByTask(task)
	for _, lock := range locks {
		// remove table by sources for related lock
		cols := lock.TryRemoveTableBySources(sources)
		dropColumns[lock.ID] = cols
		o.logger.Debug("the tables removed from the lock", zap.String("task", task), zap.Strings("sources", sources))
		if !lock.HasTables() {
			o.lk.RemoveLock(lock.ID)
		}
	}

	o.lk.RemoveDownstreamMeta(task)
	// remove source table in table keeper
	o.tk.RemoveTableByTaskAndSources(task, sources)
	o.logger.Debug("the tables removed from the table keeper", zap.String("task", task), zap.Strings("source", sources))
	// clear meta data in etcd
	_, err := optimism.DeleteInfosOperationsTablesByTaskAndSource(o.cli, task, sources, dropColumns)
	return err
}

// run runs jobs in the background.
func (o *Optimist) run(ctx context.Context, revSource, revInfo, revOperation int64) error {
	for {
		err := o.watchSourceInfoOperation(ctx, revSource, revInfo, revOperation)
		if etcdutil.IsRetryableError(err) {
			retryNum := 0
			for {
				retryNum++
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(500 * time.Millisecond):
					revSource, revInfo, revOperation, err = o.rebuildLocks()
					if err != nil {
						o.logger.Error("fail to rebuild shard DDL lock, will retry",
							zap.Int("retryNum", retryNum), zap.Error(err))
						continue
					}
				}
				break
			}
		} else {
			if err != nil {
				o.logger.Error("non-retryable error occurred, optimist will quite now", zap.Error(err))
			}
			return err
		}
	}
}

// rebuildLocks rebuilds shard DDL locks from etcd persistent data.
func (o *Optimist) rebuildLocks() (revSource, revInfo, revOperation int64, err error) {
	o.lk.Clear() // clear all previous locks to support re-Start.

	// get the history & initial source tables.
	stm, revSource, err := optimism.GetAllSourceTables(o.cli)
	if err != nil {
		return 0, 0, 0, err
	}
	// we do not log `stm`, `ifm` and `opm` now, because they may too long in optimism mode.
	o.logger.Info("get history initial source tables", zap.Int64("revision", revSource))
	o.tk.Init(stm) // re-initialize again with valid tables.

	// get the history shard DDL info.
	ifm, revInfo, err := optimism.GetAllInfo(o.cli)
	if err != nil {
		return 0, 0, 0, err
	}
	o.logger.Info("get history shard DDL info", zap.Int64("revision", revInfo))

	// get the history shard DDL lock operation.
	// the newly operations after this GET will be received through the WATCH with `revOperation+1`,
	opm, revOperation, err := optimism.GetAllOperations(o.cli)
	if err != nil {
		return 0, 0, 0, err
	}
	o.logger.Info("get history shard DDL lock operation", zap.Int64("revision", revOperation))

	colm, _, err := optimism.GetAllDroppedColumns(o.cli)
	if err != nil {
		// only log the error, and don't return it to forbid the startup of the DM-master leader.
		// then these unexpected columns can be handled by the user.
		o.logger.Error("fail to recover colms", log.ShortError(err))
	}
	o.lk.SetDropColumns(colm)

	// recover the shard DDL lock based on history shard DDL info & lock operation.
	err = o.recoverLocks(ifm, opm)
	if err != nil {
		// only log the error, and don't return it to forbid the startup of the DM-master leader.
		// then these unexpected locks can be handled by the user.
		o.logger.Error("fail to recover locks", log.ShortError(err))
	}
	o.lk.SetDropColumns(nil)

	return revSource, revInfo, revOperation, nil
}

// sortInfos sort all infos by revision.
func sortInfos(ifm map[string]map[string]map[string]map[string]optimism.Info) []optimism.Info {
	infos := make([]optimism.Info, 0, len(ifm))

	for _, ifTask := range ifm {
		for _, ifSource := range ifTask {
			for _, ifSchema := range ifSource {
				for _, info := range ifSchema {
					infos = append(infos, info)
				}
			}
		}
	}

	// sort according to the Revision
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].Revision < infos[j].Revision
	})
	return infos
}

// recoverLocks recovers shard DDL locks based on shard DDL info and shard DDL lock operation.
func (o *Optimist) recoverLocks(
	ifm map[string]map[string]map[string]map[string]optimism.Info,
	opm map[string]map[string]map[string]map[string]optimism.Operation,
) error {
	// sort infos by revision
	infos := sortInfos(ifm)
	var firstErr error
	setFirstErr := func(err error) {
		if firstErr == nil && err != nil {
			firstErr = err
		}
	}

	for _, info := range infos {
		if info.IsDeleted {
			// TODO: handle drop table
			continue
		}
		if !o.tk.SourceTableExist(info.Task, info.Source, info.UpSchema, info.UpTable, info.DownSchema, info.DownTable) {
			continue
		}
		// never mark the lock operation from `done` to `not-done` when recovering.
		err := o.handleInfo(info, true)
		if err != nil {
			o.logger.Error("fail to handle info while recovering locks", zap.Error(err))
			setFirstErr(err)
		}
	}

	// update the done status of the lock.
	for _, opTask := range opm {
		for _, opSource := range opTask {
			for _, opSchema := range opSource {
				for _, op := range opSchema {
					lock := o.lk.FindLock(op.ID)
					if lock == nil {
						o.logger.Warn("lock for the operation not found", zap.Stringer("operation", op))
						continue
					}
					if op.Done {
						lock.TryMarkDone(op.Source, op.UpSchema, op.UpTable)
						err := lock.DeleteColumnsByOp(op)
						if err != nil {
							o.logger.Error("fail to update lock columns", zap.Error(err))
						}
						// should remove resolved lock or it will be kept until next DDL
						if lock.IsResolved() {
							o.removeLockOptional(op, lock)
						}
					}
				}
			}
		}
	}
	return firstErr
}

// watchSourceInfoOperation watches the etcd operation for source tables, shard DDL infos and shard DDL operations.
func (o *Optimist) watchSourceInfoOperation(
	pCtx context.Context, revSource, revInfo, revOperation int64,
) error {
	ctx, cancel := context.WithCancel(pCtx)
	var wg sync.WaitGroup
	defer func() {
		cancel()
		wg.Wait()
	}()

	errCh := make(chan error, 10)

	// watch for source tables and handle them.
	sourceCh := make(chan optimism.SourceTables, 10)
	wg.Add(2)
	go func() {
		defer func() {
			wg.Done()
			close(sourceCh)
		}()
		optimism.WatchSourceTables(ctx, o.cli, revSource+1, sourceCh, errCh)
	}()
	go func() {
		defer wg.Done()
		o.handleSourceTables(ctx, sourceCh)
	}()

	// watch for the shard DDL info and handle them.
	infoCh := make(chan optimism.Info, 10)
	wg.Add(2)
	go func() {
		defer func() {
			wg.Done()
			close(infoCh)
		}()
		optimism.WatchInfo(ctx, o.cli, revInfo+1, infoCh, errCh)
	}()
	go func() {
		defer wg.Done()
		o.handleInfoPut(ctx, infoCh)
	}()

	// watch for the shard DDL lock operation and handle them.
	opCh := make(chan optimism.Operation, 10)
	wg.Add(2)
	go func() {
		defer func() {
			wg.Done()
			close(opCh)
		}()
		optimism.WatchOperationPut(ctx, o.cli, "", "", "", "", revOperation+1, opCh, errCh)
	}()
	go func() {
		defer wg.Done()
		o.handleOperationPut(ctx, opCh)
	}()

	select {
	case err := <-errCh:
		return err
	case <-pCtx.Done():
		return nil
	}
}

// handleSourceTables handles PUT and DELETE for source tables.
func (o *Optimist) handleSourceTables(ctx context.Context, sourceCh <-chan optimism.SourceTables) {
	for {
		select {
		case <-ctx.Done():
			return
		case st, ok := <-sourceCh:
			if !ok {
				return
			}
			o.mu.Lock()
			addedTable, droppedTable := o.tk.Update(st)
			// handle create table
			for routeTable := range addedTable {
				lock := o.lk.FindLock(utils.GenDDLLockID(st.Task, routeTable.DownSchema, routeTable.DownTable))
				if lock != nil {
					lock.AddTable(st.Source, routeTable.UpSchema, routeTable.UpTable, true)
				}
			}
			// handle drop table
			for routeTable := range droppedTable {
				lock := o.lk.FindLock(utils.GenDDLLockID(st.Task, routeTable.DownSchema, routeTable.DownTable))
				if lock != nil {
					cols := lock.TryRemoveTable(st.Source, routeTable.UpSchema, routeTable.UpTable)
					if !lock.HasTables() {
						o.lk.RemoveLock(lock.ID)
					}
					_, err := optimism.DeleteInfosOperationsTablesByTable(o.cli, st.Task, st.Source, routeTable.UpSchema, routeTable.UpTable, lock.ID, cols)
					if err != nil {
						o.logger.Error("failed to delete etcd meta data for table", zap.String("lockID", lock.ID), zap.String("schema", routeTable.UpSchema), zap.String("table", routeTable.UpTable))
					}
				}
			}
			o.mu.Unlock()
		}
	}
}

// handleInfoPut handles PUT and DELETE for the shard DDL info.
func (o *Optimist) handleInfoPut(ctx context.Context, infoCh <-chan optimism.Info) {
	for {
		select {
		case <-ctx.Done():
			return
		case info, ok := <-infoCh:
			if !ok {
				return
			}
			o.logger.Info("receive a shard DDL info", zap.Stringer("info", info), zap.Bool("is deleted", info.IsDeleted))

			if info.IsDeleted {
				// this often happen after the lock resolved.
				continue
			}

			// avoid new ddl added while previous ddl resolved and remove lock
			// change lock granularity if needed
			o.mu.Lock()
			// put operation for the table. we don't set `skipDone=true` now,
			// because in optimism mode, one table may execute/done multiple DDLs but other tables may do nothing.
			_ = o.handleInfo(info, false)
			o.mu.Unlock()
		}
	}
}

func (o *Optimist) handleInfo(info optimism.Info, skipDone bool) error {
	added := o.tk.AddTable(info.Task, info.Source, info.UpSchema, info.UpTable, info.DownSchema, info.DownTable)
	o.logger.Debug("a table added for info", zap.Bool("added", added), zap.String("info", info.ShortString()))

	tts := o.tk.FindTables(info.Task, info.DownSchema, info.DownTable)
	if tts == nil {
		// WATCH for SourceTables may fall behind WATCH for Info although PUT earlier,
		// so we try to get SourceTables again.
		// NOTE: check SourceTables for `info.Source` if needed later.
		stm, _, err := optimism.GetAllSourceTables(o.cli)
		if err != nil {
			o.logger.Error("fail to get source tables", log.ShortError(err))
		} else if tts2 := optimism.TargetTablesForTask(info.Task, info.DownSchema, info.DownTable, stm); tts2 != nil {
			tts = tts2
		}
	}
	err := o.handleLock(info, tts, skipDone)
	if err != nil {
		o.logger.Error("fail to handle the shard DDL lock", zap.String("info", info.ShortString()), log.ShortError(err))
		metrics.ReportDDLError(info.Task, metrics.InfoErrHandleLock)
	}
	return err
}

// handleOperationPut handles PUT for the shard DDL lock operations.
func (o *Optimist) handleOperationPut(ctx context.Context, opCh <-chan optimism.Operation) {
	for {
		select {
		case <-ctx.Done():
			return
		case op, ok := <-opCh:
			if !ok {
				return
			}
			o.logger.Info("receive a shard DDL lock operation", zap.Stringer("operation", op))
			if !op.Done {
				o.logger.Info("the shard DDL lock operation has not done", zap.Stringer("operation", op))
				continue
			}

			// avoid new ddl added while previous ddl resolved and remove lock
			// change lock granularity if needed
			o.mu.Lock()
			o.handleOperation(op)
			o.mu.Unlock()
		}
	}
}

func (o *Optimist) handleOperation(op optimism.Operation) {
	lock := o.lk.FindLock(op.ID)
	if lock == nil {
		o.logger.Warn("no lock for the shard DDL lock operation exist", zap.Stringer("operation", op))
		return
	}

	err := lock.DeleteColumnsByOp(op)
	if err != nil {
		o.logger.Error("fail to update lock columns", zap.Error(err))
	}
	// in optimistic mode, we always try to mark a table as done after received the `done` status of the DDLs operation.
	// NOTE: even all tables have done their previous DDLs operations, the lock may still not resolved,
	// because these tables may have different schemas.
	done := lock.TryMarkDone(op.Source, op.UpSchema, op.UpTable)
	o.logger.Info("mark operation for a table as done", zap.Bool("done", done), zap.Stringer("operation", op))
	if !lock.IsResolved() {
		o.logger.Info("the lock is still not resolved", zap.Stringer("operation", op))
		return
	}
	o.removeLockOptional(op, lock)
}

func (o *Optimist) removeLockOptional(op optimism.Operation, lock *optimism.Lock) {
	// the lock has done, remove the lock.
	o.logger.Info("the lock for the shard DDL lock operation has been resolved", zap.Stringer("operation", op))
	deleted, err := o.removeLock(lock)
	if err != nil {
		o.logger.Error("fail to delete the shard DDL infos and lock operations", zap.String("lock", lock.ID), log.ShortError(err))
		metrics.ReportDDLError(op.Task, metrics.OpErrRemoveLock)
	}
	if deleted {
		o.logger.Info("the shard DDL infos and lock operations have been cleared", zap.Stringer("operation", op))
	}
}

// handleLock handles a single shard DDL lock.
func (o *Optimist) handleLock(info optimism.Info, tts []optimism.TargetTable, skipDone bool) error {
	var (
		cfStage = optimism.ConflictNone
		cfMsg   = ""
	)

	lockID, newDDLs, cols, err := o.lk.TrySync(o.cli, info, tts)
	switch {
	case info.IgnoreConflict:
		o.logger.Warn("error occur when trying to sync for shard DDL info, this often means shard DDL conflict detected",
			zap.String("lock", lockID), zap.String("info", info.ShortString()), zap.Bool("is deleted", info.IsDeleted), log.ShortError(err))
	case err != nil:
		switch {
		case terror.ErrShardDDLOptimismNeedSkipAndRedirect.Equal(err):
			cfStage = optimism.ConflictSkipWaitRedirect
			cfMsg = err.Error()
			o.logger.Warn("Please make sure all sharding tables execute this DDL in order", log.ShortError(err))
		case terror.ErrShardDDLOptimismTrySyncFail.Equal(err):
			cfStage = optimism.ConflictDetected
			cfMsg = err.Error()
			o.logger.Warn("conflict occur when trying to sync for shard DDL info, this often means shard DDL conflict detected",
				zap.String("lock", lockID), zap.String("info", info.ShortString()), zap.Bool("is deleted", info.IsDeleted), log.ShortError(err))
		default:
			cfStage = optimism.ConflictError // we treat any errors returned from `TrySync` as conflict detected now.
			cfMsg = err.Error()
			o.logger.Warn("error occur when trying to sync for shard DDL info, this often means shard DDL error happened",
				zap.String("lock", lockID), zap.String("info", info.ShortString()), zap.Bool("is deleted", info.IsDeleted), log.ShortError(err))
		}
	default:
		o.logger.Info("the shard DDL lock returned some DDLs",
			zap.String("lock", lockID), zap.Strings("ddls", newDDLs), zap.Strings("cols", cols), zap.String("info", info.ShortString()), zap.Bool("is deleted", info.IsDeleted))
	}

	lock := o.lk.FindLock(lockID)
	if lock == nil {
		// should not happen
		return terror.ErrMasterLockNotFound.Generate(lockID)
	}

	// check whether the lock has resolved.
	if lock.IsResolved() {
		// remove all operations for this shard DDL lock.
		// this is to handle the case where dm-master exit before deleting operations for them.
		_, err = o.removeLock(lock)
		if err != nil {
			return err
		}
		return nil
	}

	if info.IgnoreConflict {
		return nil
	}

	op := optimism.NewOperation(lockID, lock.Task, info.Source, info.UpSchema, info.UpTable, newDDLs, cfStage, cfMsg, false, cols)
	rev, succ, err := optimism.PutOperation(o.cli, skipDone, op, info.Revision)
	if err != nil {
		return err
	}
	o.logger.Info("put shard DDL lock operation", zap.String("lock", lockID),
		zap.Stringer("operation", op), zap.Bool("already exist", !succ), zap.Int64("revision", rev))
	return nil
}

// removeLock removes the lock in memory and its information in etcd.
func (o *Optimist) removeLock(lock *optimism.Lock) (bool, error) {
	failpoint.Inject("SleepWhenRemoveLock", func(val failpoint.Value) {
		t := val.(int)
		log.L().Info("wait new ddl info putted into etcd in optimistic",
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
				cmps := make([]clientv3.Cmp, 0)
				for source, schemaTables := range lock.Ready() {
					for schema, tables := range schemaTables {
						for table := range tables {
							info := optimism.NewInfo(lock.Task, source, schema, table, lock.DownSchema, lock.DownTable, nil, nil, nil)
							info.Version = lock.GetVersion(source, schema, table)
							key := common.ShardDDLOptimismInfoKeyAdapter.Encode(info.Task, info.Source, info.UpSchema, info.UpTable)
							cmps = append(cmps, clientv3.Compare(clientv3.Version(key), "<", info.Version+1))
						}
					}
				}
				resp, _, err := etcdutil.DoTxnWithRepeatable(o.cli, etcdutil.FullOpFunc(cmps, nil, nil))
				if err == nil && !resp.Succeeded {
					log.L().Info("found new DDL info")
					break OUTER
				}
			}
		}
	})
	deleted, err := o.deleteInfosOps(lock)
	if err != nil {
		return deleted, err
	}
	if !deleted {
		return false, nil
	}
	o.lk.RemoveLock(lock.ID)
	metrics.ReportDDLPending(lock.Task, metrics.DDLPendingSynced, metrics.DDLPendingNone)
	return true, nil
}

// deleteInfosOps DELETEs shard DDL lock info and operations.
func (o *Optimist) deleteInfosOps(lock *optimism.Lock) (bool, error) {
	infos := make([]optimism.Info, 0)
	ops := make([]optimism.Operation, 0)
	for source, schemaTables := range lock.Ready() {
		for schema, tables := range schemaTables {
			for table := range tables {
				// NOTE: we rely on only `task`, `source`, `upSchema`, `upTable` and `Version` used for deletion.
				info := optimism.NewInfo(lock.Task, source, schema, table, lock.DownSchema, lock.DownTable, nil, nil, nil)
				info.Version = lock.GetVersion(source, schema, table)
				infos = append(infos, info)
				ops = append(ops, optimism.NewOperation(lock.ID, lock.Task, source, schema, table, nil, optimism.ConflictNone, "", false, nil))
			}
		}
	}
	// NOTE: we rely on only `task`, `downSchema`, and `downTable` used for deletion.
	rev, deleted, err := optimism.DeleteInfosOperationsColumns(o.cli, infos, ops, lock.ID)
	if err != nil {
		return deleted, err
	}
	if deleted {
		o.logger.Info("delete shard DDL infos and lock operations", zap.String("lock", lock.ID), zap.Int64("revision", rev))
	} else {
		o.logger.Info("fail to delete shard DDL infos and lock operations", zap.String("lock", lock.ID), zap.Int64("revision", rev))
	}
	return deleted, nil
}
