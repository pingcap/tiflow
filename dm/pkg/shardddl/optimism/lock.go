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

package optimism

import (
	"fmt"
	"sync"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb-tools/pkg/schemacmp"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/master/metrics"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

// DropColumnStage represents whether drop column done for a sharding table.
type DropColumnStage int

const (
	// DropNotDone represents master haven't received done for the col.
	DropNotDone DropColumnStage = iota
	// DropPartiallyDone represents master receive done for the col.
	DropPartiallyDone
	// DropDone represents master receive done and ddl for the col(executed in downstream).
	DropDone
)

// Lock represents the shard DDL lock in memory.
// This information does not need to be persistent, and can be re-constructed from the shard DDL info.
type Lock struct {
	mu sync.RWMutex

	cli *clientv3.Client

	ID   string // lock's ID
	Task string // lock's corresponding task name

	DownSchema string // downstream schema name
	DownTable  string // downstream table name

	// current joined info.
	joined schemacmp.Table
	// per-table's table info,
	// upstream source ID -> upstream schema name -> upstream table name -> table info.
	// if all of them are the same, then we call the lock `synced`.
	tables map[string]map[string]map[string]schemacmp.Table

	synced bool

	// whether DDLs operations have done (execute the shard DDL) to the downstream.
	// if all of them have done and have the same schema, then we call the lock `resolved`.
	// in optimistic mode, one table should only send a new table info (and DDLs) after the old one has done,
	// so we can set `done` to `false` when received a table info (and need the table to done some DDLs),
	// and mark `done` to `true` after received the done status of the DDLs operation.
	done map[string]map[string]map[string]bool

	// upstream source ID -> upstream schema name -> upstream table name -> info version.
	versions map[string]map[string]map[string]int64

	// record the partially dropped columns
	// column name -> source -> upSchema -> upTable -> int
	columns map[string]map[string]map[string]map[string]DropColumnStage
}

// NewLock creates a new Lock instance.
// NOTE: we MUST give the initial table info when creating the lock now.
func NewLock(cli *clientv3.Client, id, task, downSchema, downTable string, joined schemacmp.Table, tts []TargetTable) *Lock {
	l := &Lock{
		cli:        cli,
		ID:         id,
		Task:       task,
		DownSchema: downSchema,
		DownTable:  downTable,
		joined:     joined,
		tables:     make(map[string]map[string]map[string]schemacmp.Table),
		done:       make(map[string]map[string]map[string]bool),
		synced:     true,
		versions:   make(map[string]map[string]map[string]int64),
		columns:    make(map[string]map[string]map[string]map[string]DropColumnStage),
	}
	l.addTables(tts)
	metrics.ReportDDLPending(task, metrics.DDLPendingNone, metrics.DDLPendingSynced)

	return l
}

// TrySync tries to sync the lock, re-entrant.
// new upstream sources may join when the DDL lock is in syncing,
// so we need to merge these new sources.
// NOTE: now, any error returned, we treat it as conflict detected.
// NOTE: now, DDLs (not empty) returned when resolved the conflict, but in fact these DDLs should not be replicated to the downstream.
// NOTE: now, `TrySync` can detect and resolve conflicts in both of the following modes:
//   - non-intrusive: update the schema of non-conflict tables to match the conflict tables.
//                      data from conflict tables are non-intrusive.
//   - intrusive: revert the schema of the conflict tables to match the non-conflict tables.
//                  data from conflict tables are intrusive.
// TODO: but both of these modes are difficult to be implemented in DM-worker now, try to do that later.
// for non-intrusive, a broadcast mechanism needed to notify conflict tables after the conflict has resolved, or even a block mechanism needed.
// for intrusive, a DML prune or transform mechanism needed for two different schemas (before and after the conflict resolved).
func (l *Lock) TrySync(info Info, tts []TargetTable) (newDDLs []string, cols []string, err error) {
	var (
		callerSource   = info.Source
		callerSchema   = info.UpSchema
		callerTable    = info.UpTable
		ddls           = info.DDLs
		emptyCols      = []string{}
		newTIs         = info.TableInfosAfter
		infoVersion    = info.Version
		ignoreConflict = info.IgnoreConflict
		oldSynced      = l.synced
		emptyDDLs      = []string{}
	)
	l.mu.Lock()
	defer func() {
		if len(newDDLs) > 0 {
			// revert the `done` status if need to wait for the new operation to be done.
			// Now, we wait for the new operation to be done if any DDLs returned.
			l.tryRevertDone(callerSource, callerSchema, callerTable)
		}
		l.mu.Unlock()
	}()

	defer func() {
		_, remain := l.syncStatus()
		l.synced = remain == 0
		if oldSynced != l.synced {
			if oldSynced {
				metrics.ReportDDLPending(l.Task, metrics.DDLPendingSynced, metrics.DDLPendingUnSynced)
			} else {
				metrics.ReportDDLPending(l.Task, metrics.DDLPendingUnSynced, metrics.DDLPendingSynced)
			}
		}
	}()

	// joinTable join other tables
	joinTable := func(joined schemacmp.Table) (schemacmp.Table, error) {
		for source, schemaTables := range l.tables {
			for schema, tables := range schemaTables {
				for table, ti := range tables {
					if source != callerSource || schema != callerSchema || table != callerTable {
						newJoined, err2 := joined.Join(ti)
						if err2 != nil {
							// NOTE: conflict detected.
							return newJoined, terror.ErrShardDDLOptimismTrySyncFail.Delegate(
								err2, l.ID, fmt.Sprintf("fail to join table info %s with %s", joined, ti))
						}
						joined = newJoined
					}
				}
			}
		}
		return joined, nil
	}

	// should not happen
	if len(ddls) != len(newTIs) || len(newTIs) == 0 {
		return ddls, emptyCols, terror.ErrMasterInconsistentOptimisticDDLsAndInfo.Generate(len(ddls), len(newTIs))
	}

	// should not happen
	if info.TableInfoBefore == nil {
		return ddls, emptyCols, terror.ErrMasterOptimisticTableInfoBeforeNotExist.Generate(ddls)
	}
	// handle the case where <callerSource, callerSchema, callerTable>
	// is not in old source tables and current new source tables.
	// duplicate append is not a problem.
	tts = append(tts, newTargetTable(l.Task, callerSource, l.DownSchema, l.DownTable,
		map[string]map[string]struct{}{callerSchema: {callerTable: struct{}{}}}))
	// add any new source tables.
	l.addTables(tts)
	if val, ok := l.versions[callerSource][callerSchema][callerTable]; !ok || val < infoVersion {
		l.versions[callerSource][callerSchema][callerTable] = infoVersion
	}

	lastTableInfo := schemacmp.Encode(newTIs[len(newTIs)-1])
	defer func() {
		// only update table info if no error or ignore conflict
		if ignoreConflict || err == nil {
			log.L().Info("update table info", zap.String("lock", l.ID), zap.String("source", callerSource), zap.String("schema", callerSchema), zap.String("table", callerTable),
				zap.Stringer("from", l.tables[callerSource][callerSchema][callerTable]), zap.Stringer("to", lastTableInfo), zap.Strings("ddls", ddls))
			l.tables[callerSource][callerSchema][callerTable] = lastTableInfo
		}
	}()

	prevTable := schemacmp.Encode(info.TableInfoBefore)
	// if preTable not equal table in master, we always use preTable
	// this often happens when an info TrySync twice, e.g. worker restart/resume task
	if cmp, err2 := prevTable.Compare(l.tables[callerSource][callerSchema][callerTable]); err2 != nil || cmp != 0 {
		log.L().Warn("table-info-before not equal table saved in master", zap.Stringer("master-table", l.tables[callerSource][callerSchema][callerTable]), zap.Stringer("table-info-before", prevTable))
		l.tables[callerSource][callerSchema][callerTable] = prevTable
		prevJoined, err2 := joinTable(prevTable)
		if err2 != nil {
			return emptyDDLs, emptyCols, err2
		}
		l.joined = prevJoined
	}
	oldJoined := l.joined

	lastJoined, err := joinTable(lastTableInfo)
	if err != nil {
		return emptyDDLs, emptyCols, err
	}

	defer func() {
		if err == nil {
			// update the current joined table info, it should be logged in `if cmp != 0` block below.
			l.joined = lastJoined
		}
	}()

	newDDLs = []string{}
	cols = []string{}
	nextTable := prevTable
	newJoined := oldJoined

	// join and compare every new table info
	for idx, newTI := range newTIs {
		prevTable = nextTable
		oldJoined = newJoined
		nextTable = schemacmp.Encode(newTI)
		// special case: check whether DDLs making the schema become part of larger and another part of smaller.
		if _, err = prevTable.Compare(nextTable); err != nil {
			return emptyDDLs, emptyCols, terror.ErrShardDDLOptimismTrySyncFail.Delegate(
				err, l.ID, fmt.Sprintf("there will be conflicts if DDLs %s are applied to the downstream. old table info: %s, new table info: %s", ddls, prevTable, nextTable))
		}

		// special case: if the DDL does not affect the schema at all, assume it is
		// idempotent and just execute the DDL directly.
		// if any real conflicts after joined exist, they will be detected by the following steps.
		// this often happens when executing `CREATE TABLE` statement
		var cmp int
		if cmp, err = nextTable.Compare(oldJoined); err == nil && cmp == 0 {
			if col, err2 := GetColumnName(l.ID, ddls[idx], ast.AlterTableAddColumns); err2 != nil {
				return newDDLs, cols, err2
			} else if len(col) > 0 && l.IsDroppedColumn(info.Source, info.UpSchema, info.UpTable, col) {
				return newDDLs, cols, terror.ErrShardDDLOptimismTrySyncFail.Generate(
					l.ID, fmt.Sprintf("add column %s that wasn't fully dropped in downstream. ddl: %s", col, ddls[idx]))
			}
			newDDLs = append(newDDLs, ddls[idx])
			continue
		}

		// try to join tables.
		newJoined, err = joinTable(nextTable)
		if err != nil {
			return emptyDDLs, emptyCols, err
		}

		cmp, err = oldJoined.Compare(newJoined)
		// FIXME: Compute DDLs through schema diff instead of propagating DDLs directly.
		// and now we MUST ensure different sources execute same DDLs to the downstream multiple times is safe.
		if err != nil {
			// resolving conflict in non-intrusive mode.
			log.L().Warn("resolving conflict", zap.String("lock", l.ID), zap.String("source", callerSource), zap.String("schema", callerSchema), zap.String("table", callerTable),
				zap.Stringer("joined-from", oldJoined), zap.Stringer("joined-to", newJoined), zap.Strings("ddls", ddls))
			return ddls, cols, nil
		}
		if cmp != 0 {
			// < 0: the joined schema become larger after applied these DDLs.
			//      this often happens when executing `ADD COLUMN` for the FIRST table.
			// > 0: the joined schema become smaller after applied these DDLs.
			//      this often happens when executing `DROP COLUMN` for the LAST table.
			// for these two cases, we should execute the DDLs to the downstream to update the schema.
			log.L().Info("joined table info changed", zap.String("lock", l.ID), zap.Int("cmp", cmp), zap.Stringer("from", oldJoined), zap.Stringer("to", newJoined),
				zap.String("source", callerSource), zap.String("schema", callerSchema), zap.String("table", callerTable), zap.Strings("ddls", ddls))
			if cmp < 0 {
				// check for add column with a larger field len
				if col, err2 := AddDifferentFieldLenColumns(l.ID, ddls[idx], oldJoined, newJoined); err2 != nil {
					return ddls, cols, err2
				} else if len(col) > 0 && l.IsDroppedColumn(info.Source, info.UpSchema, info.UpTable, col) {
					return ddls, cols, terror.ErrShardDDLOptimismTrySyncFail.Generate(
						l.ID, fmt.Sprintf("add column %s that wasn't fully dropped in downstream. ddl: %s", col, ddls[idx]))
				}
			} else {
				if col, err2 := GetColumnName(l.ID, ddls[idx], ast.AlterTableDropColumn); err2 != nil {
					return ddls, cols, err2
				} else if len(col) > 0 {
					err = l.AddDroppedColumn(info, col)
					if err != nil {
						log.L().Error("fail to add dropped column info in etcd", zap.Error(err))
						return ddls, cols, terror.ErrShardDDLOptimismTrySyncFail.Generate(l.ID, "fail to add dropped column info in etcd")
					}
					cols = append(cols, col)
				}
			}
			newDDLs = append(newDDLs, ddls[idx])
			continue
		}

		// NOTE: now, different DM-workers do not wait for each other when executing DDL/DML,
		// when coordinating the shard DDL between multiple DM-worker instances,
		// a possible sequences:
		//   1. DM-worker-A do this `trySync` and DM-master let it to `ADD COLUMN`.
		//   2. DM-worker-B do this `trySync` again.
		//   3. DM-worker-B replicate DML to the downstream.
		//   4. DM-worker-A replicate `ADD COLUMN` to the downstream.
		// in order to support DML from DM-worker-B matches the downstream schema,
		// two strategies exist:
		//   A. DM-worker-B waits for DM-worker-A to finish the replication of the DDL before replicating DML.
		//   B. DM-worker-B also replicates the DDL before replicating DML,
		//      but this MUST ensure we can tolerate replicating the DDL multiple times.
		// for `DROP COLUMN` or other DDL which makes the schema become smaller,
		// this is not a problem because all DML with larger schema should already replicated to the downstream,
		// and any DML with smaller schema can fit both the larger or smaller schema.
		// To make it easy to implement, we will temporarily choose strategy-B.

		cmp, _ = prevTable.Compare(nextTable) // we have checked `err` returned above.
		if cmp < 0 {
			// check for add column with a smaller field len
			if col, err2 := AddDifferentFieldLenColumns(l.ID, ddls[idx], nextTable, newJoined); err2 != nil {
				return ddls, cols, err2
			} else if len(col) > 0 && l.IsDroppedColumn(info.Source, info.UpSchema, info.UpTable, col) {
				return ddls, cols, terror.ErrShardDDLOptimismTrySyncFail.Generate(
					l.ID, fmt.Sprintf("add column %s that wasn't fully dropped in downstream. ddl: %s", col, ddls[idx]))
			}
			// let every table to replicate the DDL.
			newDDLs = append(newDDLs, ddls[idx])
			continue
		} else if cmp > 0 {
			if col, err2 := GetColumnName(l.ID, ddls[idx], ast.AlterTableDropColumn); err2 != nil {
				return ddls, cols, err2
			} else if len(col) > 0 {
				err = l.AddDroppedColumn(info, col)
				if err != nil {
					log.L().Error("fail to add dropped column info in etcd", zap.Error(err))
					return ddls, cols, terror.ErrShardDDLOptimismTrySyncFail.Generate(l.ID, "fail to add dropped column info in etcd")
				}
				cols = append(cols, col)
			}
			// last shard table won't go here
			continue
		}

		// compare the current table's info with joined info.
		cmp, err = nextTable.Compare(newJoined)
		if err != nil {
			return emptyDDLs, emptyCols, terror.ErrShardDDLOptimismTrySyncFail.Delegate(
				err, l.ID, "can't compare table info (new table info) %s with (new joined table info) %s", nextTable, newJoined) // NOTE: this should not happen.
		}
		if cmp < 0 {
			// no need to replicate DDLs, because has a larger joined schema (in the downstream).
			// FIXME: if the previous tables reached the joined schema has not replicated to the downstream,
			// now, they should re-try until replicated successfully, try to implement better strategy later.
			continue
		}
		log.L().Warn("new table info >= new joined table info", zap.Stringer("table info", nextTable), zap.Stringer("joined table info", newJoined))
		return ddls, cols, nil // NOTE: this should not happen.
	}

	return newDDLs, cols, nil
}

// TryRemoveTable tries to remove a table in the lock.
// it returns whether the table has been removed.
// TODO: it does NOT try to rebuild the joined schema after the table removed now.
// try to support this if needed later.
// NOTE: if no table exists in the lock after removed the table,
// it's the caller's responsibility to decide whether remove the lock or not.
func (l *Lock) TryRemoveTable(source, schema, table string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.tables[source]; !ok {
		return false
	}
	if _, ok := l.tables[source][schema]; !ok {
		return false
	}

	ti, ok := l.tables[source][schema][table]
	if !ok {
		return false
	}

	delete(l.tables[source][schema], table)
	_, remain := l.syncStatus()
	l.synced = remain == 0
	delete(l.done[source][schema], table)
	delete(l.versions[source][schema], table)
	log.L().Info("table removed from the lock", zap.String("lock", l.ID),
		zap.String("source", source), zap.String("schema", schema), zap.String("table", table),
		zap.Stringer("table info", ti))
	return true
}

// IsSynced returns whether the lock has synced.
// In the optimistic mode, we call it `synced` if table info of all tables are the same,
// and we define `remain` as the table count which have different table info with the joined one,
// e.g. for `ADD COLUMN`, it's the table count which have not added the column,
// for `DROP COLUMN`, it's the table count which have dropped the column.
func (l *Lock) IsSynced() (bool, int) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	_, remain := l.syncStatus()
	return remain == 0, remain
}

// Ready returns the source tables' sync status (whether they are ready).
// we define `ready` if the table's info is the same with the joined one,
// e.g for `ADD COLUMN`, it's true if it has added the column,
// for `DROP COLUMN`, it's true if it has not dropped the column.
func (l *Lock) Ready() map[string]map[string]map[string]bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	ready, _ := l.syncStatus()
	return ready
}

// Joined returns the joined table info.
func (l *Lock) Joined() schemacmp.Table {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.joined
}

// TryMarkDone tries to mark the operation of the source table as done.
// it returns whether marked done.
// NOTE: this method can always mark a existing table as done,
// so the caller of this method should ensure that the table has done the DDLs operation.
// NOTE: a done table may revert to not-done if new table schema received and new DDLs operation need to be done.
func (l *Lock) TryMarkDone(source, schema, table string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.done[source]; !ok {
		return false
	}
	if _, ok := l.done[source][schema]; !ok {
		return false
	}
	if _, ok := l.done[source][schema][table]; !ok {
		return false
	}

	// always mark it as `true` now.
	l.done[source][schema][table] = true
	return true
}

// IsDone returns whether the operation of the source table has done.
func (l *Lock) IsDone(source, schema, table string) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if _, ok := l.done[source]; !ok {
		return false
	}
	if _, ok := l.done[source][schema]; !ok {
		return false
	}
	if _, ok := l.done[source][schema][table]; !ok {
		return false
	}
	return l.done[source][schema][table]
}

// IsResolved returns whether the lock has resolved.
// return true if all tables have the same schema and all DDLs operations have done.
func (l *Lock) IsResolved() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// whether all tables have the same schema.
	if _, remain := l.syncStatus(); remain != 0 {
		return false
	}

	// whether all tables have done DDLs operations.
	for _, schemaTables := range l.done {
		for _, tables := range schemaTables {
			for _, done := range tables {
				if !done {
					return false
				}
			}
		}
	}
	return true
}

// syncedStatus returns the current tables' sync status (<Ready, remain>).
func (l *Lock) syncStatus() (map[string]map[string]map[string]bool, int) {
	ready := make(map[string]map[string]map[string]bool)
	remain := 0
	for source, schemaTables := range l.tables {
		if _, ok := ready[source]; !ok {
			ready[source] = make(map[string]map[string]bool)
		}
		for schema, tables := range schemaTables {
			if _, ok := ready[source][schema]; !ok {
				ready[source][schema] = make(map[string]bool)
			}
			for table, ti := range tables {
				if cmp, err := l.joined.Compare(ti); err == nil && cmp == 0 {
					ready[source][schema][table] = true
				} else {
					ready[source][schema][table] = false
					remain++
				}
			}
		}
	}
	return ready, remain
}

// tryRevertDone tries to revert the done status when the table's schema changed.
func (l *Lock) tryRevertDone(source, schema, table string) {
	if _, ok := l.done[source]; !ok {
		return
	}
	if _, ok := l.done[source][schema]; !ok {
		return
	}
	if _, ok := l.done[source][schema][table]; !ok {
		return
	}
	l.done[source][schema][table] = false
}

// addTables adds any not-existing tables into the lock.
func (l *Lock) addTables(tts []TargetTable) {
	for _, tt := range tts {
		if _, ok := l.tables[tt.Source]; !ok {
			l.tables[tt.Source] = make(map[string]map[string]schemacmp.Table)
			l.done[tt.Source] = make(map[string]map[string]bool)
			l.versions[tt.Source] = make(map[string]map[string]int64)
		}
		for schema, tables := range tt.UpTables {
			if _, ok := l.tables[tt.Source][schema]; !ok {
				l.tables[tt.Source][schema] = make(map[string]schemacmp.Table)
				l.done[tt.Source][schema] = make(map[string]bool)
				l.versions[tt.Source][schema] = make(map[string]int64)
			}
			for table := range tables {
				if _, ok := l.tables[tt.Source][schema][table]; !ok {
					l.tables[tt.Source][schema][table] = l.joined
					l.done[tt.Source][schema][table] = false
					l.versions[tt.Source][schema][table] = 0
					log.L().Info("table added to the lock", zap.String("lock", l.ID),
						zap.String("source", tt.Source), zap.String("schema", schema), zap.String("table", table),
						zap.Stringer("table info", l.joined))
				}
			}
		}
	}
}

// GetVersion return version of info in lock.
func (l *Lock) GetVersion(source string, schema string, table string) int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.versions[source][schema][table]
}

// IsDroppedColumn checks whether this column is a partially dropped column for this lock.
func (l *Lock) IsDroppedColumn(source, upSchema, upTable, col string) bool {
	if _, ok := l.columns[col]; !ok {
		return false
	}
	if _, ok := l.columns[col][source]; !ok {
		return false
	}
	if _, ok := l.columns[col][source][upSchema]; !ok {
		return false
	}
	if _, ok := l.columns[col][source][upSchema][upTable]; !ok {
		return false
	}
	return true
}

// AddDroppedColumn adds a dropped column name in both etcd and lock's column map.
func (l *Lock) AddDroppedColumn(info Info, col string) error {
	source, upSchema, upTable := info.Source, info.UpSchema, info.UpTable
	if l.IsDroppedColumn(source, upSchema, upTable, col) {
		return nil
	}
	log.L().Info("add partially dropped columns", zap.String("column", col), zap.String("info", info.ShortString()))

	_, _, err := PutDroppedColumn(l.cli, genDDLLockID(info), col, info.Source, info.UpSchema, info.UpTable, DropNotDone)
	if err != nil {
		return err
	}

	if _, ok := l.columns[col]; !ok {
		l.columns[col] = make(map[string]map[string]map[string]DropColumnStage)
	}
	if _, ok := l.columns[col][source]; !ok {
		l.columns[col][source] = make(map[string]map[string]DropColumnStage)
	}
	if _, ok := l.columns[col][source][upSchema]; !ok {
		l.columns[col][source][upSchema] = make(map[string]DropColumnStage)
	}
	l.columns[col][source][upSchema][upTable] = DropNotDone
	return nil
}

// DeleteColumnsByOp deletes the partially dropped columns that extracted from operation.
// We can not remove columns from the partially dropped columns map unless:
// this column is dropped in the downstream database,
// all the upstream source done the delete column operation
// that is to say, columns all done.
func (l *Lock) DeleteColumnsByOp(op Operation) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	doneCols := make(map[string]struct{}, len(op.DDLs))
	for _, ddl := range op.DDLs {
		col, err := GetColumnName(l.ID, ddl, ast.AlterTableDropColumn)
		if err != nil {
			return err
		}
		if len(col) > 0 {
			doneCols[col] = struct{}{}
		}
	}

	colsToDelete := make([]string, 0, len(op.Cols))
	for _, col := range op.Cols {
		done := DropPartiallyDone
		if l.IsDroppedColumn(op.Source, op.UpSchema, op.UpTable, col) {
			if _, ok := doneCols[col]; ok {
				done = DropDone
			}
			// mark col PartiallyDone/Done
			_, _, err := PutDroppedColumn(l.cli, op.ID, col, op.Source, op.UpSchema, op.UpTable, done)
			if err != nil {
				log.L().Error("cannot put drop column to etcd", log.ShortError(err))
				return err
			}
			l.columns[col][op.Source][op.UpSchema][op.UpTable] = done
		}

		allDone := true
		dropDone := false
	OUTER:
		for _, schemaCols := range l.columns[col] {
			for _, tableCols := range schemaCols {
				for _, done := range tableCols {
					if done == DropDone {
						dropDone = true
					}
					if done == DropNotDone {
						allDone = false
						break OUTER
					}
				}
			}
		}
		if allDone && dropDone {
			colsToDelete = append(colsToDelete, col)
		}
	}

	if len(colsToDelete) > 0 {
		log.L().Info("delete partially dropped columns",
			zap.String("lockID", l.ID), zap.Strings("columns", colsToDelete))

		_, _, err := DeleteDroppedColumns(l.cli, op.ID, colsToDelete...)
		if err != nil {
			return err
		}

		for _, col := range colsToDelete {
			delete(l.columns, col)
		}
	}

	return nil
}

// TableExist check whether table exists.
func (l *Lock) TableExist(source, schema, table string) bool {
	if _, ok := l.tables[source]; !ok {
		return false
	}
	if _, ok := l.tables[source][schema]; !ok {
		return false
	}
	if _, ok := l.tables[source][schema][table]; !ok {
		return false
	}
	return true
}

// AddDifferentFieldLenColumns checks whether dm adds columns with different field lengths.
func AddDifferentFieldLenColumns(lockID, ddl string, oldJoined, newJoined schemacmp.Table) (string, error) {
	col, err := GetColumnName(lockID, ddl, ast.AlterTableAddColumns)
	if err != nil {
		return col, err
	}
	if len(col) > 0 {
		oldJoinedCols := schemacmp.DecodeColumnFieldTypes(oldJoined)
		newJoinedCols := schemacmp.DecodeColumnFieldTypes(newJoined)
		oldCol, ok1 := oldJoinedCols[col]
		newCol, ok2 := newJoinedCols[col]
		if ok1 && ok2 && newCol.Flen != oldCol.Flen {
			return col, terror.ErrShardDDLOptimismTrySyncFail.Generate(
				lockID, fmt.Sprintf("add columns with different field lengths."+
					"ddl: %s, origLen: %d, newLen: %d", ddl, oldCol.Flen, newCol.Flen))
		}
	}
	return col, nil
}

// GetColumnName checks whether dm adds/drops a column, and return this column's name.
func GetColumnName(lockID, ddl string, tp ast.AlterTableType) (string, error) {
	if stmt, err := parser.New().ParseOneStmt(ddl, "", ""); err != nil {
		return "", terror.ErrShardDDLOptimismTrySyncFail.Delegate(
			err, lockID, fmt.Sprintf("fail to parse ddl %s", ddl))
	} else if v, ok := stmt.(*ast.AlterTableStmt); ok && len(v.Specs) > 0 {
		spec := v.Specs[0]
		if spec.Tp == tp {
			switch spec.Tp {
			case ast.AlterTableAddColumns:
				if len(spec.NewColumns) > 0 {
					return spec.NewColumns[0].Name.Name.O, nil
				}
			case ast.AlterTableDropColumn:
				if spec.OldColumnName != nil {
					return spec.OldColumnName.Name.O, nil
				}
			}
		}
	}
	return "", nil
}
