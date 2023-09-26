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
	"bytes"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/dbutil"
	"github.com/pingcap/tidb/util/schemacmp"
	"github.com/pingcap/tiflow/dm/master/metrics"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/cputil"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/net/context"
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

type tableType int

const (
	// normalTables represents upstream table info record in checkpoint.
	normalTables tableType = iota
	// conflictTables represents upstream table info after executing conflict DDL.
	conflictTables
	// finalTables combines normalTables and conflcitTables,
	// which represents all upstream table infos after executing all conflict DDLs.
	finalTables
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

	// first prevTable when a lock created
	// only use when fetchTableInfo return an error.
	initTable schemacmp.Table
	// per-table's table info,
	// upstream source ID -> upstream schema name -> upstream table name -> table info.
	// if all of them are the same, then we call the lock `synced`.
	tables map[string]map[string]map[string]schemacmp.Table
	// conflictTables is used for conflict DDL coordination
	// upstream source ID -> upstream schema name -> upstream table name -> table info.
	conflictTables map[string]map[string]map[string]schemacmp.Table
	// finalTables combine tables and conflcitTables
	// it represents final state of all tables
	// upstream source ID -> upstream schema name -> upstream table name -> table info.
	finalTables map[string]map[string]map[string]schemacmp.Table

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

	downstreamMeta *DownstreamMeta
}

// NewLock creates a new Lock instance.
func NewLock(cli *clientv3.Client, id, task, downSchema, downTable string, initTable schemacmp.Table, tts []TargetTable, downstreamMeta *DownstreamMeta) *Lock {
	l := &Lock{
		cli:            cli,
		ID:             id,
		Task:           task,
		DownSchema:     downSchema,
		DownTable:      downTable,
		initTable:      initTable,
		tables:         make(map[string]map[string]map[string]schemacmp.Table),
		conflictTables: make(map[string]map[string]map[string]schemacmp.Table),
		finalTables:    make(map[string]map[string]map[string]schemacmp.Table),
		done:           make(map[string]map[string]map[string]bool),
		synced:         true,
		versions:       make(map[string]map[string]map[string]int64),
		columns:        make(map[string]map[string]map[string]map[string]DropColumnStage),
		downstreamMeta: downstreamMeta,
	}
	l.addTables(tts)
	metrics.ReportDDLPending(task, metrics.DDLPendingNone, metrics.DDLPendingSynced)
	return l
}

// FetchTableInfos fetch all table infos for a lock.
func (l *Lock) FetchTableInfos(task, source, schema, table string) (*model.TableInfo, error) {
	if l.downstreamMeta == nil {
		return nil, terror.ErrMasterOptimisticDownstreamMetaNotFound.Generate(task)
	}

	db, err := conn.DefaultDBProvider.Apply(l.downstreamMeta.dbConfig)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), dbutil.DefaultTimeout)
	defer cancel()

	query := `SELECT table_info FROM ` + dbutil.TableName(l.downstreamMeta.meta, cputil.SyncerCheckpoint(task)) + ` WHERE id = ? AND cp_schema = ? AND cp_table = ?`
	row := db.DB.QueryRowContext(ctx, query, source, schema, table)
	if row.Err() != nil {
		return nil, terror.ErrDBExecuteFailed.Delegate(row.Err(), query)
	}
	var tiBytes []byte
	if err := row.Scan(&tiBytes); err != nil {
		return nil, terror.ErrDBExecuteFailed.Delegate(err, query)
	}
	var ti *model.TableInfo
	if bytes.Equal(tiBytes, []byte("null")) {
		log.L().Warn("null table info", zap.String("query", query), zap.String("source", source), zap.String("schema", schema), zap.String("table", table))
		return nil, terror.ErrMasterOptimisticDownstreamMetaNotFound.Generate(task)
	}
	if err := json.Unmarshal(tiBytes, &ti); err != nil {
		return nil, err
	}
	return ti, nil
}

// TrySync tries to sync the lock, re-entrant.
// new upstream sources may join when the DDL lock is in syncing,
// so we need to merge these new sources.
// NOTE: now, any error returned, we treat it as conflict detected.
// NOTE: now, DDLs (not empty) returned when resolved the conflict, but in fact these DDLs should not be replicated to the downstream.
// NOTE: now, `TrySync` can detect and resolve conflicts in both of the following modes:
//   - non-intrusive: update the schema of non-conflict tables to match the conflict tables.
//     data from conflict tables are non-intrusive.
//   - intrusive: revert the schema of the conflict tables to match the non-conflict tables.
//     data from conflict tables are intrusive.
//
// TODO: but both of these modes are difficult to be implemented in DM-worker now, try to do that later.
// for non-intrusive, a broadcast mechanism needed to notify conflict tables after the conflict has resolved, or even a block mechanism needed.
// for intrusive, a DML prune or transform mechanism needed for two different schemas (before and after the conflict resolved).
func (l *Lock) TrySync(info Info, tts []TargetTable) (newDDLs []string, cols []string, err error) {
	var (
		callerSource   = info.Source
		callerSchema   = info.UpSchema
		callerTable    = info.UpTable
		ddls           = info.DDLs
		emptyDDLs      = []string{}
		emptyCols      = []string{}
		newTIs         = info.TableInfosAfter
		infoVersion    = info.Version
		ignoreConflict = info.IgnoreConflict
		oldSynced      = l.synced
	)
	l.mu.Lock()
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
		if len(newDDLs) > 0 || (err != nil && (terror.ErrShardDDLOptimismNeedSkipAndRedirect.Equal(err) ||
			terror.ErrShardDDLOptimismTrySyncFail.Equal(err))) {
			// revert the `done` status if need to wait for the new operation to be done.
			// Now, we wait for the new operation to be done if any DDLs returned.
			l.tryRevertDone(callerSource, callerSchema, callerTable)
		}
		l.mu.Unlock()
	}()

	// should not happen
	if len(ddls) != len(newTIs) || len(newTIs) == 0 {
		return emptyDDLs, emptyCols, terror.ErrMasterInconsistentOptimisticDDLsAndInfo.Generate(len(ddls), len(newTIs))
	}
	// should not happen
	if info.TableInfoBefore == nil {
		return emptyDDLs, emptyCols, terror.ErrMasterOptimisticTableInfoBeforeNotExist.Generate(ddls)
	}

	defer func() {
		if err == nil && len(cols) > 0 {
			err = l.AddDroppedColumns(callerSource, callerSchema, callerTable, cols)
		}
		// only update table info if no error or ignore conflict or conflict DDL
		if err != nil {
			var revertInfo schemacmp.Table
			switch {
			case ignoreConflict:
				// forcely set schema for --ignore-conflict
				revertInfo = schemacmp.Encode(newTIs[len(newTIs)-1])
			case terror.ErrShardDDLOptimismNeedSkipAndRedirect.Equal(err):
				return
			default:
				revertInfo = schemacmp.Encode(info.TableInfoBefore)
			}
			l.tables[callerSource][callerSchema][callerTable] = revertInfo
			l.finalTables[callerSource][callerSchema][callerTable] = revertInfo
			l.removeConflictTable(callerSource, callerSchema, callerTable)
		}
	}()

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

	newDDLs = []string{}
	cols = []string{}
	prevTable := schemacmp.Encode(info.TableInfoBefore)
	// join and compare every new table info
	for idx, ti := range newTIs {
		postTable := schemacmp.Encode(ti)
		schemaChanged, conflictStage := l.trySyncForOneDDL(callerSource, callerSchema, callerTable, prevTable, postTable)

		switch conflictStage {
		case ConflictDetected:
			return emptyDDLs, emptyCols, terror.ErrShardDDLOptimismTrySyncFail.Generate(l.ID, fmt.Sprintf("there will be conflicts if DDLs %s are applied to the downstream. old table info: %s, new table info: %s", ddls[idx], prevTable, postTable))
		case ConflictNone:
			if col, err := l.checkAddDropColumn(callerSource, callerSchema, callerTable, ddls[idx], prevTable, postTable, cols); err != nil {
				return emptyDDLs, emptyCols, err
			} else if len(col) != 0 {
				cols = append(cols, col)
			}
		case ConflictSkipWaitRedirect:
			return newDDLs, cols, terror.ErrShardDDLOptimismNeedSkipAndRedirect.Generate(l.ID, ddls[idx])
		case ConflictResolved:
			log.L().Info("all conflict DDL resolved", zap.String("DDL", ddls[idx]), zap.String("callerSource", callerSource),
				zap.String("callerSchema", callerSchema), zap.String("callerTable", callerTable))
		}

		if schemaChanged {
			newDDLs = append(newDDLs, ddls[idx])
		}
		prevTable = postTable
	}
	return newDDLs, cols, nil
}

// TryRemoveTable tries to remove a table in the lock.
// it returns whether the table has been removed.
// TODO: it does NOT try to rebuild the joined schema after the table removed now.
// try to support this if needed later.
// NOTE: if no table exists in the lock after removed the table,
// it's the caller's responsibility to decide whether remove the lock or not.
func (l *Lock) TryRemoveTable(source, schema, table string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.tables[source]; !ok {
		return nil
	}
	if _, ok := l.tables[source][schema]; !ok {
		return nil
	}

	ti, ok := l.tables[source][schema][table]
	if !ok {
		return nil
	}

	// delete drop columns
	dropColumns := make([]string, 0)
	for col, sourceColumns := range l.columns {
		if schemaColumns, ok := sourceColumns[source]; ok {
			if tableColumn, ok := schemaColumns[schema]; ok {
				if _, ok := tableColumn[table]; ok {
					dropColumns = append(dropColumns, col)
					delete(tableColumn, table)
					if len(tableColumn) == 0 {
						delete(schemaColumns, schema)
					}
				}
			}
		}
	}

	delete(l.tables[source][schema], table)
	delete(l.finalTables[source][schema], table)
	l.removeConflictTable(source, schema, table)
	_, remain := l.syncStatus()
	l.synced = remain == 0
	delete(l.done[source][schema], table)
	delete(l.versions[source][schema], table)
	log.L().Info("table removed from the lock", zap.String("lock", l.ID),
		zap.String("source", source), zap.String("schema", schema), zap.String("table", table),
		zap.Stringer("table info", ti))
	return dropColumns
}

// TryRemoveTable tries to remove tables in the lock by sources.
// return drop columns for later use.
func (l *Lock) TryRemoveTableBySources(sources []string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	// record drop columns for sources
	dropColumns := make([]string, 0)
	for col, sourceColumns := range l.columns {
		for _, source := range sources {
			if _, ok := sourceColumns[source]; ok {
				dropColumns = append(dropColumns, col)
				break
			}
		}
	}

	for _, source := range sources {
		if _, ok := l.tables[source]; !ok {
			continue
		}

		delete(l.tables, source)
		delete(l.finalTables, source)
		delete(l.conflictTables, source)
		_, remain := l.syncStatus()
		l.synced = remain == 0
		delete(l.done, source)
		delete(l.versions, source)
		for _, sourceColumns := range l.columns {
			delete(sourceColumns, source)
		}
		log.L().Info("tables removed from the lock", zap.String("lock", l.ID), zap.String("source", source))
	}
	return dropColumns
}

// HasTables check whether a lock has tables.
func (l *Lock) HasTables() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, schemas := range l.tables {
		for _, tables := range schemas {
			for range tables {
				return true
			}
		}
	}
	return false
}

// UpdateTableAfterUnlock updates table's schema info after unlock exec action.
func (l *Lock) UpdateTableAfterUnlock(info Info) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var ok bool
	if _, ok = l.tables[info.Source]; !ok {
		l.tables[info.Source] = make(map[string]map[string]schemacmp.Table)
	}
	if _, ok = l.tables[info.Source][info.UpSchema]; !ok {
		l.tables[info.Source][info.UpSchema] = make(map[string]schemacmp.Table)
	}
	l.tables[info.Source][info.UpSchema][info.UpTable] = schemacmp.Encode(info.TableInfosAfter[len(info.TableInfosAfter)-1])
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
func (l *Lock) Joined() (schemacmp.Table, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.joinNormalTables()
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
	joined, joinedErr := l.joinFinalTables()
	for source, schemaTables := range l.finalTables {
		if _, ok := ready[source]; !ok {
			ready[source] = make(map[string]map[string]bool)
		}
		for schema, tables := range schemaTables {
			if _, ok := ready[source][schema]; !ok {
				ready[source][schema] = make(map[string]bool)
			}
			for table, ti := range tables {
				if joinedErr == nil {
					if cmp, err := joined.Compare(ti); err == nil && cmp == 0 {
						ready[source][schema][table] = true
						continue
					}
				}
				ready[source][schema][table] = false
				remain++
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

// AddTable create a table in lock.
func (l *Lock) AddTable(source, schema, table string, needLock bool) {
	if needLock {
		l.mu.Lock()
		defer l.mu.Unlock()
	}
	if _, ok := l.tables[source]; !ok {
		l.tables[source] = make(map[string]map[string]schemacmp.Table)
		l.finalTables[source] = make(map[string]map[string]schemacmp.Table)
		l.done[source] = make(map[string]map[string]bool)
		l.versions[source] = make(map[string]map[string]int64)
	}
	if _, ok := l.tables[source][schema]; !ok {
		l.tables[source][schema] = make(map[string]schemacmp.Table)
		l.finalTables[source][schema] = make(map[string]schemacmp.Table)
		l.done[source][schema] = make(map[string]bool)
		l.versions[source][schema] = make(map[string]int64)
	}
	if _, ok := l.tables[source][schema][table]; !ok {
		ti, err := l.FetchTableInfos(l.Task, source, schema, table)
		if err != nil {
			log.L().Error("source table info not found, use init table info instead", zap.String("task", l.Task), zap.String("source", source), zap.String("schema", schema), zap.String("table", table), log.ShortError(err))
			l.tables[source][schema][table] = l.initTable
			l.finalTables[source][schema][table] = l.initTable
		} else {
			t := schemacmp.Encode(ti)
			log.L().Debug("get source table info", zap.String("task", l.Task), zap.String("source", source), zap.String("schema", schema), zap.String("table", table), zap.Stringer("info", t))
			l.tables[source][schema][table] = t
			l.finalTables[source][schema][table] = t
		}
		l.done[source][schema][table] = false
		l.versions[source][schema][table] = 0
		log.L().Info("table added to the lock", zap.String("lock", l.ID),
			zap.String("source", source), zap.String("schema", schema), zap.String("table", table),
			zap.Stringer("table info", l.initTable))
	}
}

// addTables adds any not-existing tables into the lock.
// For a new table, try to fetch table info from downstream.
func (l *Lock) addTables(tts []TargetTable) {
	for _, tt := range tts {
		for schema, tables := range tt.UpTables {
			for table := range tables {
				l.AddTable(tt.Source, schema, table, false)
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

// AddDroppedColumns adds a dropped column name in both etcd and lock's column map.
func (l *Lock) AddDroppedColumns(source, schema, table string, cols []string) error {
	newCols := make([]string, 0, len(cols))
	for _, col := range cols {
		if !l.IsDroppedColumn(source, schema, table, col) {
			newCols = append(newCols, col)
		}
	}
	log.L().Info("add partially dropped columns", zap.Strings("columns", newCols), zap.String("source", source), zap.String("schema", schema), zap.String("table", table))

	if len(newCols) > 0 {
		_, _, err := PutDroppedColumns(l.cli, l.ID, source, schema, table, newCols, DropNotDone)
		if err != nil {
			return err
		}
	}

	for _, col := range newCols {
		if _, ok := l.columns[col]; !ok {
			l.columns[col] = make(map[string]map[string]map[string]DropColumnStage)
		}
		if _, ok := l.columns[col][source]; !ok {
			l.columns[col][source] = make(map[string]map[string]DropColumnStage)
		}
		if _, ok := l.columns[col][source][schema]; !ok {
			l.columns[col][source][schema] = make(map[string]DropColumnStage)
		}
		l.columns[col][source][schema][table] = DropNotDone
	}
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
			_, _, err := PutDroppedColumns(l.cli, op.ID, op.Source, op.UpSchema, op.UpTable, []string{col}, done)
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
		if ok1 && ok2 && newCol.GetFlen() != oldCol.GetFlen() {
			return col, terror.ErrShardDDLOptimismAddNotFullyDroppedColumn.Generate(
				lockID, fmt.Sprintf("add columns with different field lengths. "+
					"ddl: %s, origLen: %d, newLen: %d", ddl, oldCol.GetFlen(), newCol.GetFlen()))
		}
	}
	return col, nil
}

// GetColumnName checks whether dm adds/drops a column, and return this column's name.
func GetColumnName(lockID, ddl string, tp ast.AlterTableType) (string, error) {
	if stmt, err := parser.New().ParseOneStmt(ddl, "", ""); err != nil {
		return "", terror.ErrShardDDLOptimismAddNotFullyDroppedColumn.Delegate(
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

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// checkAddDropColumn check for ALTER TABLE ADD/DROP COLUMN statement
// FOR ADD COLUMN, check whether add column with a different field or add a dropped column
// FOR DROP COLUMN, return the droped column.
func (l *Lock) checkAddDropColumn(source, schema, table string, ddl string, prevTable, postTable schemacmp.Table, newDropColumns []string) (string, error) {
	currTable := l.tables[source][schema][table]
	defer func() {
		l.tables[source][schema][table] = currTable
	}()

	l.tables[source][schema][table] = prevTable
	oldJoined, err := l.joinNormalTables()
	if err != nil {
		// nolint:nilerr
		return "", nil
	}

	l.tables[source][schema][table] = postTable
	newJoined, err := l.joinNormalTables()
	if err != nil {
		// nolint:nilerr
		return "", nil
	}

	cmp, err := oldJoined.Compare(newJoined)
	if err != nil {
		// nolint:nilerr
		return "", nil
	}

	if cmp <= 0 {
		if col, err2 := AddDifferentFieldLenColumns(l.ID, ddl, oldJoined, newJoined); err2 != nil {
			// check for add column with a larger field len
			return "", err2
		} else if _, err2 = AddDifferentFieldLenColumns(l.ID, ddl, postTable, newJoined); err2 != nil {
			// check for add column with a smaller field len
			return "", err2
		} else if len(col) > 0 && (l.IsDroppedColumn(source, schema, table, col) || contains(newDropColumns, col)) {
			return "", terror.ErrShardDDLOptimismAddNotFullyDroppedColumn.Generate(l.ID, fmt.Sprintf("add column %s that wasn't fully dropped in downstream. ddl: %s", col, ddl))
		}
	}

	if cmp >= 0 {
		if col, err2 := GetColumnName(l.ID, ddl, ast.AlterTableDropColumn); err2 != nil {
			return "", err2
		} else if len(col) > 0 {
			return col, nil
		}
	}
	return "", nil
}

// trySyncForOneDDL try sync for a DDL operation.
// e.g. `ALTER TABLE ADD COLUMN a, RENAME b TO c, DROP COLUMN d' will call this func three times.
// return whether joined table is changed and whether there is a conflict.
func (l *Lock) trySyncForOneDDL(source, schema, table string, prevTable, postTable schemacmp.Table) (schemaChanged bool, conflictStage ConflictStage) {
	// we only support resolve one conflict DDL per table,
	// so reset conflict table after receive new table info.
	l.removeConflictTable(source, schema, table)
	l.finalTables[source][schema][table] = l.tables[source][schema][table]

	// For idempotent DDL
	// this often happens when an info TrySync twice, e.g. worker restart/resume task
	idempotent := false
	if cmp, err := prevTable.Compare(l.tables[source][schema][table]); err != nil || cmp != 0 {
		if cmp, err := postTable.Compare(l.tables[source][schema][table]); err == nil && cmp == 0 {
			idempotent = true
		}
		log.L().Warn("prev-table not equal table saved in master", zap.Stringer("master-table", l.tables[source][schema][table]), zap.Stringer("prev-table", prevTable))
		l.tables[source][schema][table] = prevTable
		l.finalTables[source][schema][table] = prevTable
	}

	tableCmp, tableErr := prevTable.Compare(postTable)

	// Normal DDL
	if tableErr == nil {
		log.L().Info("receive a normal DDL", zap.String("source", source), zap.String("schema", schema), zap.String("table", table), zap.Stringer("prevTable", prevTable), zap.Stringer("postTable", postTable))
		oldJoined, oldErr := l.joinNormalTables()

		l.tables[source][schema][table] = postTable
		l.finalTables[source][schema][table] = postTable

		newJoined, newErr := l.joinNormalTables()
		// normal DDL can be sync if no error
		if newErr == nil {
			// if a normal DDL let all final tables become no conflict
			// return ConflictNone
			if len(l.conflictTables) > 0 && l.noConflictForFinalTables() {
				log.L().Info("all conflict resolved for the DDL", zap.String("source", source), zap.String("schema", schema), zap.String("table", table), zap.Stringer("prevTable", prevTable), zap.Stringer("postTable", postTable))
				err := l.redirectForConflictTables(source, schema, table)
				if err != nil {
					log.L().Error("failed to put redirect operation for conflict tables", log.ShortError(err))
					return false, ConflictDetected
				}
				l.resolveTables()
				return true, ConflictNone
			}

			if oldErr != nil {
				return true, ConflictNone
			}
			joinedCmp, joinedErr := oldJoined.Compare(newJoined)
			// special case: if the DDL does not affect the schema at all, assume it is
			// idempotent and just execute the DDL directly.
			// this often happens when executing `CREATE TABLE` statement
			cmp, err2 := postTable.Compare(oldJoined)

			// return schema changed in 3 cases
			// oldJoined != newJoined
			// postTable == oldJoined (CREATE TABLE)
			// prevTable < postTable
			// prevTable == postTable(Partition/Sequence)
			return (joinedErr != nil || joinedCmp != 0) || (err2 == nil && cmp == 0) || tableCmp <= 0, ConflictNone
		}
	}

	log.L().Info("found conflict for DDL", zap.String("source", source), zap.String("schema", schema), zap.String("table", table), zap.Stringer("prevTable", prevTable), zap.Stringer("postTable", postTable), log.ShortError(tableErr))

	if idempotent || l.noConflictWithOneNormalTable(source, schema, table, prevTable, postTable) {
		log.L().Info("directly return conflict DDL", zap.Bool("idempotent", idempotent), zap.String("source", source), zap.String("schema", schema), zap.String("table", table), zap.Stringer("prevTable", prevTable), zap.Stringer("postTable", postTable))
		l.tables[source][schema][table] = postTable
		l.finalTables[source][schema][table] = postTable
		return true, ConflictNone
	}

	// meet conflict DDL
	// revert tables and update conflictTables and finalTables
	l.tables[source][schema][table] = prevTable
	l.addConflictTable(source, schema, table, postTable)
	l.finalTables[source][schema][table] = postTable

	// if any conflict happened between conflict DDLs, return error
	// e.g. tb1: "ALTER TABLE RENAME a TO b", tb2: "ALTER TABLE RENAME c TO d"
	if !l.noConflictForConflictTables() {
		log.L().Error("conflict happened with other conflict tables", zap.String("source", source), zap.String("schema", schema), zap.String("table", table), zap.Stringer("prevTable", prevTable), zap.Stringer("postTable", postTable))
		return false, ConflictDetected
	}

	if l.noConflictForFinalTables() {
		log.L().Info("all conflict resolved for the DDL", zap.String("source", source), zap.String("schema", schema), zap.String("table", table), zap.Stringer("prevTable", prevTable), zap.Stringer("postTable", postTable))
		err := l.redirectForConflictTables(source, schema, table)
		if err != nil {
			log.L().Error("failed to put redirect operation for conflict tables", log.ShortError(err))
			return false, ConflictDetected
		}
		l.resolveTables()

		return true, ConflictNone
	}
	log.L().Info("conflict hasn't been resolved", zap.String("source", source), zap.String("schema", schema), zap.String("table", table), zap.Stringer("prevTable", prevTable), zap.Stringer("postTable", postTable))
	return false, ConflictSkipWaitRedirect
}

// joinTables join tables by tableType.
func (l *Lock) joinTables(tp tableType) (schemacmp.Table, error) {
	var (
		joined     schemacmp.Table
		allTables  map[string]map[string]map[string]schemacmp.Table
		firstTable = true
	)

	switch tp {
	case conflictTables:
		allTables = l.conflictTables
	case finalTables:
		allTables = l.finalTables
	default:
		allTables = l.tables
	}

	for source, schemaTables := range allTables {
		for schema, tables := range schemaTables {
			for table, ti := range tables {
				if firstTable {
					joined = ti
					firstTable = false
					continue
				}

				newJoined, err := joined.Join(ti)
				if err != nil {
					return newJoined, errors.Errorf("failed to join tables with %s.%s.%s, joined: %s, table: %s, root cause: %s", source, schema, table, joined.String(), ti.String(), err.Error())
				}
				joined = newJoined
			}
		}
	}

	return joined, nil
}

// Compare(joined,prev_tbx) == error
// For a conflict DDL make table become part of larger and another part of smaller,
// this function make sure all tables that need to be judged become part of smaller.
// e.g. `ALTER TABLE RENAME a TO b`, this function check whether all tables do not contain `a`.
// Prove:
//
//	Compare(joined,prev_tbk) == error
//
// => Joined ⊇ prev_tbk-{a}+{b} && Joined ⊅ prev_tbk
// => a ∉ Joined.
func (l *Lock) allTableSmaller(tp tableType) bool {
	var (
		joined schemacmp.Table
		err    error
	)
	switch tp {
	case conflictTables:
		joined, err = l.joinConflictTables()
	default:
		joined, err = l.joinFinalTables()
	}

	if err != nil {
		return false
	}

	for source, schemaTables := range l.conflictTables {
		for schema, tables := range schemaTables {
			for table := range tables {
				ti := l.tables[source][schema][table]

				if _, err = joined.Compare(ti); err == nil {
					return false
				}
			}
		}
	}
	return true
}

// Compare(Join(prev_tbx,tabley),post_tbx)>=0
// For a conflict DDL make table become part of larger and another part of smaller,
// this function make sure all the tables that need to be judged become part of larger.
// e.g `ALTER TABLE RENAME a TO b`, this function check whether all tables contain `b`.
// Prove:
//
//	Compare(Join(prev_tbx,tabley),post_tbx)>=0
//
// => Compare(Join(prev_tbk,tabley),prev_tbk-{a}+{b})>=0
// => Join(prev_tbk,tabley) ⊇ prev_tbk-{a}+{b}
// => b ∈ tabley.
func (l *Lock) allTableLarger(tp tableType) bool {
	var judgeTables map[string]map[string]map[string]schemacmp.Table

	switch tp {
	case normalTables:
		judgeTables = l.tables
	case conflictTables:
		judgeTables = l.conflictTables
	default:
		judgeTables = l.finalTables
	}

	for source, schemaTables := range l.conflictTables {
		for schema, tables := range schemaTables {
			for table, conflictTi := range tables {
				// for every conflict table's prev_table
				ti := l.tables[source][schema][table]

				// for every judge table
				for _, sTables := range judgeTables {
					for _, ts := range sTables {
						for _, finalTi := range ts {
							joined, err := ti.Join(finalTi)
							if err != nil {
								// modify column
								joined = finalTi
							}
							if cmp, err := joined.Compare(conflictTi); err != nil || cmp < 0 {
								return false
							}
						}
					}
				}
			}
		}
	}
	return true
}

func (l *Lock) joinNormalTables() (schemacmp.Table, error) {
	return l.joinTables(normalTables)
}

func (l *Lock) joinFinalTables() (schemacmp.Table, error) {
	return l.joinTables(finalTables)
}

func (l *Lock) joinConflictTables() (schemacmp.Table, error) {
	return l.joinTables(conflictTables)
}

func (l *Lock) allConflictTableSmaller() bool {
	return l.allTableSmaller(conflictTables)
}

func (l *Lock) allFinalTableSmaller() bool {
	return l.allTableSmaller(finalTables)
}

func (l *Lock) allConflictTableLarger() bool {
	return l.allTableLarger(conflictTables)
}

func (l *Lock) allFinalTableLarger() bool {
	return l.allTableLarger(finalTables)
}

// jude a conflict ddl is no conflict with at least one normal table.
func (l *Lock) noConflictWithOneNormalTable(callerSource, callerSchema, callerTable string, prevTable, postTable schemacmp.Table) bool {
	for source, schemaTables := range l.tables {
		for schema, tables := range schemaTables {
			for table, ti := range tables {
				if source == callerSource && schema == callerSchema && table == callerTable {
					continue
				}

				// judge joined no error
				joined, err := postTable.Join(ti)
				if err != nil {
					continue
				}

				// judge this normal table is smaller(same as allTableSmaller)
				if _, err = joined.Compare(prevTable); err == nil {
					continue
				}

				// judge this normal table is larger(same as allTableLarger)
				if joined, err = prevTable.Join(ti); err != nil {
					joined = ti
				}
				if cmp, err := joined.Compare(postTable); err != nil || cmp < 0 {
					continue
				}

				return true
			}
		}
	}
	return false
}

// judge whether all conflict tables has no conflict.
func (l *Lock) noConflictForConflictTables() bool {
	if _, err := l.joinConflictTables(); err != nil {
		return false
	}
	if !l.allConflictTableSmaller() {
		return false
	}
	if !l.allConflictTableLarger() {
		return false
	}
	return true
}

// judge whether all final tables has no conflict.
func (l *Lock) noConflictForFinalTables() bool {
	if _, err := l.joinFinalTables(); err != nil {
		return false
	}
	if !l.allFinalTableSmaller() {
		return false
	}
	if !l.allFinalTableLarger() {
		return false
	}
	return true
}

func (l *Lock) addConflictTable(source, schema, table string, ti schemacmp.Table) {
	if _, ok := l.conflictTables[source]; !ok {
		l.conflictTables[source] = make(map[string]map[string]schemacmp.Table)
	}
	if _, ok := l.conflictTables[source][schema]; !ok {
		l.conflictTables[source][schema] = make(map[string]schemacmp.Table)
	}
	l.conflictTables[source][schema][table] = ti
}

func (l *Lock) removeConflictTable(source, schema, table string) {
	if _, ok := l.conflictTables[source]; !ok {
		return
	}
	if _, ok := l.conflictTables[source][schema]; !ok {
		return
	}
	delete(l.conflictTables[source][schema], table)
	if len(l.conflictTables[source][schema]) == 0 {
		delete(l.conflictTables[source], schema)
	}
	if len(l.conflictTables[source]) == 0 {
		delete(l.conflictTables, source)
	}
}

// resolveTables reset conflictTables and copy tables from final tables.
func (l *Lock) resolveTables() {
	l.conflictTables = make(map[string]map[string]map[string]schemacmp.Table)
	for source, schemaTables := range l.finalTables {
		for schema, tables := range schemaTables {
			for table, ti := range tables {
				l.tables[source][schema][table] = ti
			}
		}
	}
}

// redirectForConflictTables put redirect Ops for all conflict tables.
func (l *Lock) redirectForConflictTables(callerSource, callerSchema, callerTable string) error {
	for source, schemaTables := range l.conflictTables {
		for schema, tables := range schemaTables {
			for table := range tables {
				if source == callerSource && schema == callerSchema && table == callerTable {
					// no redirect for caller table
					continue
				}
				op := NewOperation(l.ID, l.Task, source, schema, table, nil, ConflictResolved, "", false, nil)
				// TODO(GMHDBJD): put these operation in one transaction
				rev, succ, err := PutOperation(l.cli, false, op, 0)
				if err != nil {
					return err
				}
				log.L().Info("put redirect operation for conflict table", zap.String("lock", l.ID),
					zap.Stringer("operation", op), zap.Bool("succeed", !succ), zap.Int64("revision", rev))
			}
		}
	}
	return nil
}
