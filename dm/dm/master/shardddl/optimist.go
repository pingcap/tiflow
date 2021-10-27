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
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/schemacmp"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/master/metrics"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/shardddl/optimism"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
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
func NewOptimist(pLogger *log.Logger) *Optimist {
	return &Optimist{
		logger: pLogger.WithFields(zap.String("component", "shard DDL optimist")),
		closed: true,
		lk:     optimism.NewLockKeeper(),
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
	defer o.mu.Unlock()

	if o.closed {
		return
	}

	if o.cancel != nil {
		o.cancel()
		o.cancel = nil
	}

	o.wg.Wait()
	o.closed = true // closed now.
	o.logger.Info("the shard DDL optimist has closed")
}

// Locks return all shard DDL locks current exist.
func (o *Optimist) Locks() map[string]*optimism.Lock {
	return o.lk.Locks()
}

// ShowLocks is used by `show-ddl-locks` command.
func (o *Optimist) ShowLocks(task string, sources []string) []*pb.DDLLock {
	locks := o.lk.Locks()
	ret := make([]*pb.DDLLock, 0, len(locks))
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
		l := &pb.DDLLock{
			ID:       lock.ID,
			Task:     lock.Task,
			Mode:     config.ShardOptimistic,
			Owner:    "",  // N/A for the optimistic mode
			DDLs:     nil, // N/A for the optimistic mode
			Synced:   make([]string, 0, len(ready)),
			Unsynced: make([]string, 0, len(ready)),
		}
		for source, schemaTables := range ready {
			for schema, tables := range schemaTables {
				for table, synced := range tables {
					if synced {
						l.Synced = append(l.Synced, fmt.Sprintf("%s-%s", source, dbutil.TableName(schema, table)))
					} else {
						l.Unsynced = append(l.Unsynced, fmt.Sprintf("%s-%s", source, dbutil.TableName(schema, table)))
					}
				}
			}
		}
		sort.Strings(l.Synced)
		sort.Strings(l.Unsynced)
		ret = append(ret, l)
	}
	return ret
}

// RemoveMetaData removes meta data for a specified task
// NOTE: this function can only be used when the specified task is not running.
func (o *Optimist) RemoveMetaData(task string) error {
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

	o.tk.RemoveTableByTask(task)

	// clear meta data in etcd
	_, err = optimism.DeleteInfosOperationsTablesSchemasByTask(o.cli, task, lockIDSet)
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

	initSchemas, revInitSchemas, err := optimism.GetAllInitSchemas(o.cli)
	if err != nil {
		return 0, 0, 0, err
	}
	o.logger.Info("get all init schemas", zap.Int64("revision", revInitSchemas))

	colm, _, err := optimism.GetAllDroppedColumns(o.cli)
	if err != nil {
		// only log the error, and don't return it to forbid the startup of the DM-master leader.
		// then these unexpected columns can be handled by the user.
		o.logger.Error("fail to recover colms", log.ShortError(err))
	}

	// recover the shard DDL lock based on history shard DDL info & lock operation.
	err = o.recoverLocks(ifm, opm, colm, initSchemas)
	if err != nil {
		// only log the error, and don't return it to forbid the startup of the DM-master leader.
		// then these unexpected locks can be handled by the user.
		o.logger.Error("fail to recover locks", log.ShortError(err))
	}

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

// buildLockJoinedAndTTS build joined table and target table slice for lock by history infos.
func (o *Optimist) buildLockJoinedAndTTS(
	ifm map[string]map[string]map[string]map[string]optimism.Info,
	initSchemas map[string]map[string]map[string]optimism.InitSchema) (
	map[string]schemacmp.Table, map[string][]optimism.TargetTable,
	map[string]map[string]map[string]map[string]schemacmp.Table) {
	type infoKey struct {
		lockID   string
		source   string
		upSchema string
		upTable  string
	}
	infos := make(map[infoKey]optimism.Info)
	lockTTS := make(map[string][]optimism.TargetTable)
	for _, taskInfos := range ifm {
		for _, sourceInfos := range taskInfos {
			for _, schemaInfos := range sourceInfos {
				for _, info := range schemaInfos {
					lockID := utils.GenDDLLockID(info.Task, info.DownSchema, info.DownTable)
					if _, ok := lockTTS[lockID]; !ok {
						lockTTS[lockID] = o.tk.FindTables(info.Task, info.DownSchema, info.DownTable)
					}
					infos[infoKey{lockID, info.Source, info.UpSchema, info.UpTable}] = info
				}
			}
		}
	}

	lockJoined := make(map[string]schemacmp.Table)
	missTable := make(map[string]map[string]map[string]map[string]schemacmp.Table)
	for lockID, tts := range lockTTS {
		for _, tt := range tts {
			for upSchema, tables := range tt.UpTables {
				for upTable := range tables {
					var table schemacmp.Table
					if info, ok := infos[infoKey{lockID, tt.Source, upSchema, upTable}]; ok && info.TableInfoBefore != nil {
						table = schemacmp.Encode(info.TableInfoBefore)
					} else if initSchema, ok := initSchemas[tt.Task][tt.DownSchema][tt.DownTable]; ok {
						// If there is no optimism.Info for a upstream table, it indicates the table structure
						// hasn't been changed since last removeLock. So the init schema should be its table info.
						table = schemacmp.Encode(initSchema.TableInfo)
						if _, ok := missTable[lockID]; !ok {
							missTable[lockID] = make(map[string]map[string]map[string]schemacmp.Table)
						}
						if _, ok := missTable[lockID][tt.Source]; !ok {
							missTable[lockID][tt.Source] = make(map[string]map[string]schemacmp.Table)
						}
						if _, ok := missTable[lockID][tt.Source][upSchema]; !ok {
							missTable[lockID][tt.Source][upSchema] = make(map[string]schemacmp.Table)
						}
						missTable[lockID][tt.Source][upSchema][upTable] = table
					} else {
						o.logger.Error(
							"can not find table info for upstream table",
							zap.String("source", tt.Source),
							zap.String("up-schema", upSchema),
							zap.String("up-table", upTable),
						)
						continue
					}
					if joined, ok := lockJoined[lockID]; !ok {
						lockJoined[lockID] = table
					} else {
						newJoined, err := joined.Join(table)
						// ignore error, will report it in TrySync later
						if err != nil {
							o.logger.Error(fmt.Sprintf("fail to join table info %s with %s, lockID: %s in recover lock", joined, newJoined, lockID), log.ShortError(err))
						} else {
							lockJoined[lockID] = newJoined
						}
					}
				}
			}
		}
	}
	return lockJoined, lockTTS, missTable
}

// recoverLocks recovers shard DDL locks based on shard DDL info and shard DDL lock operation.
func (o *Optimist) recoverLocks(
	ifm map[string]map[string]map[string]map[string]optimism.Info,
	opm map[string]map[string]map[string]map[string]optimism.Operation,
	colm map[string]map[string]map[string]map[string]map[string]optimism.DropColumnStage,
	initSchemas map[string]map[string]map[string]optimism.InitSchema) error {
	// construct joined table based on the shard DDL info.
	o.logger.Info("build lock joined and tts")
	lockJoined, lockTTS, missTable := o.buildLockJoinedAndTTS(ifm, initSchemas)
	// build lock and restore table info
	o.logger.Info("rebuild locks and tables")
	o.lk.RebuildLocksAndTables(o.cli, ifm, colm, lockJoined, lockTTS, missTable)
	// sort infos by revision
	infos := sortInfos(ifm)
	var firstErr error
	setFirstErr := func(err error) {
		if firstErr == nil && err != nil {
			firstErr = err
		}
	}

	for _, info := range infos {
		// never mark the lock operation from `done` to `not-done` when recovering.
		err := o.handleInfo(info, true)
		if err != nil {
			o.logger.Error("fail to handle info while recovering locks", zap.Error(err))
			setFirstErr(err)
			continue
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
							continue
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
	pCtx context.Context, revSource, revInfo, revOperation int64) error {
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
			updated := o.tk.Update(st)
			o.logger.Info("receive source tables", zap.Stringer("source tables", st),
				zap.Bool("is deleted", st.IsDeleted), zap.Bool("updated", updated))
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

			// avoid new ddl added while previous ddl resolved and remove lock
			// change lock granularity if needed
			o.mu.Lock()
			if info.IsDeleted {
				lock := o.lk.FindLockByInfo(info)
				if lock == nil {
					// this often happen after the lock resolved.
					o.logger.Debug("lock for info not found", zap.String("info", info.ShortString()))
					o.mu.Unlock()
					continue
				}
				// handle `DROP TABLE`, need to remove the table schema from the lock,
				// and remove the table name from table keeper.
				removed := lock.TryRemoveTable(info.Source, info.UpSchema, info.UpTable)
				o.logger.Debug("the table name remove from the table keeper", zap.Bool("removed", removed), zap.String("info", info.ShortString()))
				removed = o.tk.RemoveTable(info.Task, info.Source, info.UpSchema, info.UpTable, info.DownSchema, info.DownTable)
				o.logger.Debug("a table removed for info from the lock", zap.Bool("removed", removed), zap.String("info", info.ShortString()))
				o.mu.Unlock()
				continue
			}

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
			lock := o.lk.FindLock(op.ID)
			if lock == nil {
				o.logger.Warn("no lock for the shard DDL lock operation exist", zap.Stringer("operation", op))
				o.mu.Unlock()
				continue
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
				o.mu.Unlock()
				continue
			}

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
			o.mu.Unlock()
		}
	}
}

// handleLock handles a single shard DDL lock.
func (o *Optimist) handleLock(info optimism.Info, tts []optimism.TargetTable, skipDone bool) error {
	cfStage := optimism.ConflictNone
	cfMsg := ""
	lockID, newDDLs, cols, err := o.lk.TrySync(o.cli, info, tts)
	switch {
	case info.IgnoreConflict:
		o.logger.Warn("error occur when trying to sync for shard DDL info, this often means shard DDL conflict detected",
			zap.String("lock", lockID), zap.String("info", info.ShortString()), zap.Bool("is deleted", info.IsDeleted), log.ShortError(err))
	case err != nil:
		cfStage = optimism.ConflictDetected // we treat any errors returned from `TrySync` as conflict detected now.
		cfMsg = err.Error()
		o.logger.Warn("error occur when trying to sync for shard DDL info, this often means shard DDL conflict detected",
			zap.String("lock", lockID), zap.String("info", info.ShortString()), zap.Bool("is deleted", info.IsDeleted), log.ShortError(err))
	default:
		o.logger.Info("the shard DDL lock returned some DDLs",
			zap.String("lock", lockID), zap.Strings("ddls", newDDLs), zap.Strings("cols", cols), zap.String("info", info.ShortString()), zap.Bool("is deleted", info.IsDeleted))

		// try to record the init schema before applied the DDL to the downstream.
		initSchema := optimism.NewInitSchema(info.Task, info.DownSchema, info.DownTable, info.TableInfoBefore)
		rev, putted, err2 := optimism.PutInitSchemaIfNotExist(o.cli, initSchema)
		switch {
		case err2 != nil:
			return err2
		case putted:
			o.logger.Info("recorded the initial schema", zap.String("info", info.ShortString()))
		default:
			o.logger.Debug("skip to record the initial schema", zap.String("info", info.ShortString()), zap.Int64("revision", rev))
		}
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
		log.L().Info("wait new ddl info putted into etcd",
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
				resp, _, err := etcdutil.DoOpsInOneCmpsTxnWithRetry(o.cli, cmps, nil, nil)
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
	initSchema := optimism.NewInitSchema(lock.Task, lock.DownSchema, lock.DownTable, nil)
	rev, deleted, err := optimism.DeleteInfosOperationsSchemaColumn(o.cli, infos, ops, initSchema)
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
