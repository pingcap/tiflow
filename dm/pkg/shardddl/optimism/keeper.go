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
	"sort"
	"sync"

	"github.com/pingcap/tidb-tools/pkg/schemacmp"
	"go.etcd.io/etcd/clientv3"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

// DownstreamMeta used to fetch table info from downstream.
type DownstreamMeta struct {
	dbConfig *config.DBConfig
	meta     string
}

// LockKeeper used to keep and handle DDL lock conveniently.
// The lock information do not need to be persistent, and can be re-constructed from the shard DDL info.
// But the drop columns should be persistent.
type LockKeeper struct {
	mu    sync.RWMutex
	locks map[string]*Lock // lockID -> Lock

	downstreamMetaMap     map[string]*DownstreamMeta
	getDownstreamMetaFunc func(string) (*config.DBConfig, string)
	// lockID -> column name -> source -> upSchema -> upTable -> int
	dropColumns map[string]map[string]map[string]map[string]map[string]DropColumnStage
}

// NewLockKeeper creates a new LockKeeper instance.
func NewLockKeeper(getDownstreamMetaFunc func(string) (*config.DBConfig, string)) *LockKeeper {
	return &LockKeeper{
		locks:                 make(map[string]*Lock),
		downstreamMetaMap:     make(map[string]*DownstreamMeta),
		getDownstreamMetaFunc: getDownstreamMetaFunc,
	}
}

// SetDropColumns set drop columns for lock keeper.
func (lk *LockKeeper) SetDropColumns(dropColumns map[string]map[string]map[string]map[string]map[string]DropColumnStage) {
	lk.dropColumns = dropColumns
}

// getDownstreamMeta gets and cached downstream meta.
func (lk *LockKeeper) getDownstreamMeta(task string) (*DownstreamMeta, error) {
	if downstreamMeta, ok := lk.downstreamMetaMap[task]; ok {
		return downstreamMeta, nil
	}

	dbConfig, meta := lk.getDownstreamMetaFunc(task)
	if dbConfig == nil {
		return nil, terror.ErrMasterOptimisticDownstreamMetaNotFound.Generate(task)
	}
	downstreamMeta := &DownstreamMeta{dbConfig: dbConfig, meta: meta}
	lk.downstreamMetaMap[task] = downstreamMeta
	return downstreamMeta, nil
}

// RemoveDownstreamMeta removes downstream mate by task.
func (lk *LockKeeper) RemoveDownstreamMeta(task string) {
	delete(lk.downstreamMetaMap, task)
}

// TrySync tries to sync the lock.
func (lk *LockKeeper) TrySync(cli *clientv3.Client, info Info, tts []TargetTable) (string, []string, []string, error) {
	var (
		lockID = genDDLLockID(info)
		l      *Lock
		ok     bool
	)

	lk.mu.Lock()
	defer lk.mu.Unlock()

	if info.TableInfoBefore == nil {
		return "", nil, nil, terror.ErrMasterOptimisticTableInfoBeforeNotExist.Generate(info.DDLs)
	}

	if l, ok = lk.locks[lockID]; !ok {
		downstreamMeta, err := lk.getDownstreamMeta(info.Task)
		if err != nil {
			log.L().Error("get downstream meta", log.ShortError(err))
		}

		lk.locks[lockID] = NewLock(cli, lockID, info.Task, info.DownSchema, info.DownTable, schemacmp.Encode(info.TableInfoBefore), tts, downstreamMeta)
		l = lk.locks[lockID]

		// set drop columns, only when recover locks
		if lk.dropColumns != nil {
			if cols, ok := lk.dropColumns[lockID]; ok {
				l.columns = cols
			}
		}
	}

	newDDLs, cols, err := l.TrySync(info, tts)
	return lockID, newDDLs, cols, err
}

// RemoveLock removes a lock.
func (lk *LockKeeper) RemoveLock(lockID string) bool {
	lk.mu.Lock()
	defer lk.mu.Unlock()

	_, ok := lk.locks[lockID]
	delete(lk.locks, lockID)
	return ok
}

// RemoveLockByInfo removes a lock.
func (lk *LockKeeper) RemoveLockByInfo(info Info) bool {
	lockID := genDDLLockID(info)
	return lk.RemoveLock(lockID)
}

// FindLock finds a lock.
func (lk *LockKeeper) FindLock(lockID string) *Lock {
	lk.mu.RLock()
	defer lk.mu.RUnlock()

	return lk.locks[lockID]
}

// FindLocksByTask finds locks by task.
func (lk *LockKeeper) FindLocksByTask(task string) []*Lock {
	lk.mu.RLock()
	defer lk.mu.RUnlock()

	locks := make([]*Lock, 0)
	for _, lock := range lk.locks {
		if lock.Task == task {
			locks = append(locks, lock)
		}
	}

	return locks
}

// FindLockByInfo finds a lock with a shard DDL info.
func (lk *LockKeeper) FindLockByInfo(info Info) *Lock {
	return lk.FindLock(genDDLLockID(info))
}

// Locks return a copy of all Locks.
func (lk *LockKeeper) Locks() map[string]*Lock {
	lk.mu.RLock()
	defer lk.mu.RUnlock()

	locks := make(map[string]*Lock, len(lk.locks))
	for k, v := range lk.locks {
		locks[k] = v
	}
	return locks
}

// Clear clears all Locks.
func (lk *LockKeeper) Clear() {
	lk.mu.Lock()
	defer lk.mu.Unlock()

	lk.locks = make(map[string]*Lock)
	lk.downstreamMetaMap = make(map[string]*DownstreamMeta)
}

// genDDLLockID generates DDL lock ID from its info.
func genDDLLockID(info Info) string {
	return utils.GenDDLLockID(info.Task, info.DownSchema, info.DownTable)
}

// TableKeeper used to keep initial tables for a task in optimism mode.
type TableKeeper struct {
	mu     sync.RWMutex
	tables map[string]map[string]SourceTables // task-name -> source-ID -> tables.
}

// NewTableKeeper creates a new TableKeeper instance.
func NewTableKeeper() *TableKeeper {
	return &TableKeeper{
		tables: make(map[string]map[string]SourceTables),
	}
}

// Init (re-)initializes the keeper with initial source tables.
func (tk *TableKeeper) Init(stm map[string]map[string]SourceTables) {
	tk.mu.Lock()
	defer tk.mu.Unlock()

	tk.tables = make(map[string]map[string]SourceTables)
	for task, sts := range stm {
		if _, ok := tk.tables[task]; !ok {
			tk.tables[task] = make(map[string]SourceTables)
		}
		for source, st := range sts {
			tk.tables[task][source] = st
		}
	}
}

// Update adds/updates tables into the keeper or removes tables from the keeper.
// it returns whether added/updated or removed.
func (tk *TableKeeper) Update(st SourceTables) bool {
	tk.mu.Lock()
	defer tk.mu.Unlock()

	if st.IsDeleted {
		if _, ok := tk.tables[st.Task]; !ok {
			return false
		}
		if _, ok := tk.tables[st.Task][st.Source]; !ok {
			return false
		}
		delete(tk.tables[st.Task], st.Source)
		return true
	}

	if _, ok := tk.tables[st.Task]; !ok {
		tk.tables[st.Task] = make(map[string]SourceTables)
	}
	tk.tables[st.Task][st.Source] = st
	return true
}

// AddTable adds a table into the source tables.
// it returns whether added (not exist before).
// NOTE: we only add for existing task now.
func (tk *TableKeeper) AddTable(task, source, upSchema, upTable, downSchema, downTable string) bool {
	tk.mu.Lock()
	defer tk.mu.Unlock()

	if _, ok := tk.tables[task]; !ok {
		return false
	}
	if _, ok := tk.tables[task][source]; !ok {
		tk.tables[task][source] = NewSourceTables(task, source)
	}
	st := tk.tables[task][source]
	added := st.AddTable(upSchema, upTable, downSchema, downTable)
	tk.tables[task][source] = st // assign the modified SourceTables.
	return added
}

// SourceTableExist check whether a source table exist.
func (tk *TableKeeper) SourceTableExist(task, source, upSchema, upTable, downSchema, downTable string) bool {
	tk.mu.Lock()
	defer tk.mu.Unlock()

	if _, ok := tk.tables[task]; !ok {
		return false
	}
	if _, ok := tk.tables[task][source]; !ok {
		return false
	}
	st := tk.tables[task][source]
	targetTable := st.TargetTable(downSchema, downTable)

	if targetTable.UpTables != nil {
		if tables, ok := targetTable.UpTables[upSchema]; ok {
			if _, ok2 := tables[upTable]; ok2 {
				return true
			}
		}
	}
	return false
}

// RemoveTable removes a table from the source tables.
// it returns whether removed (exit before).
func (tk *TableKeeper) RemoveTable(task, source, upSchema, upTable, downSchema, downTable string) bool {
	tk.mu.Lock()
	defer tk.mu.Unlock()

	if _, ok := tk.tables[task]; !ok {
		return false
	}
	if _, ok := tk.tables[task][source]; !ok {
		return false
	}
	st := tk.tables[task][source]
	removed := st.RemoveTable(upSchema, upTable, downSchema, downTable)
	tk.tables[task][source] = st // assign the modified SourceTables.
	return removed
}

// RemoveTableByTask removes tables from the source tables through task name.
// it returns whether removed (exit before).
func (tk *TableKeeper) RemoveTableByTask(task string) bool {
	tk.mu.Lock()
	defer tk.mu.Unlock()

	if _, ok := tk.tables[task]; !ok {
		return false
	}
	delete(tk.tables, task)
	return true
}

// RemoveTableByTaskAndSource removes tables from the source tables through task name and sources.
func (tk *TableKeeper) RemoveTableByTaskAndSources(task string, sources []string) {
	tk.mu.Lock()
	defer tk.mu.Unlock()

	if _, ok := tk.tables[task]; !ok {
		return
	}

	for _, source := range sources {
		delete(tk.tables[task], source)
	}
}

// FindTables finds source tables by task name and downstream table name.
func (tk *TableKeeper) FindTables(task, downSchema, downTable string) []TargetTable {
	tk.mu.RLock()
	defer tk.mu.RUnlock()

	stm, ok := tk.tables[task]
	if !ok || len(stm) == 0 {
		return nil
	}

	ret := make([]TargetTable, 0, len(stm))
	for _, st := range stm {
		if tt := st.TargetTable(downSchema, downTable); !tt.IsEmpty() {
			ret = append(ret, tt)
		}
	}

	sort.Sort(TargetTableSlice(ret))
	return ret
}

// TargetTablesForTask returns TargetTable list for a specified task and downstream table.
// stm: task name -> upstream source ID -> SourceTables.
func TargetTablesForTask(task, downSchema, downTable string, stm map[string]map[string]SourceTables) []TargetTable {
	sts, ok := stm[task]
	if !ok || len(sts) == 0 {
		return nil
	}

	ret := make([]TargetTable, 0, len(sts))
	for _, st := range sts {
		if tt := st.TargetTable(downSchema, downTable); !tt.IsEmpty() {
			ret = append(ret, tt)
		}
	}

	sort.Sort(TargetTableSlice(ret))
	return ret
}

// TargetTableSlice attaches the methods of Interface to []TargetTable,
// sorting in increasing order according to `Source` field.
type TargetTableSlice []TargetTable

// Len implements Sorter.Len.
func (t TargetTableSlice) Len() int {
	return len(t)
}

// Less implements Sorter.Less.
func (t TargetTableSlice) Less(i, j int) bool {
	return t[i].Source < t[j].Source
}

// Swap implements Sorter.Swap.
func (t TargetTableSlice) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}
