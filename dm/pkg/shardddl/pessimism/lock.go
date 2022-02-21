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

package pessimism

import (
	"sync"

	"github.com/pingcap/tiflow/dm/dm/master/metrics"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

// Lock represents the shard DDL lock in memory.
// This information does not need to be persistent, and can be re-constructed from the shard DDL info.
type Lock struct {
	mu sync.RWMutex

	ID     string   // lock's ID
	Task   string   // lock's corresponding task name
	Owner  string   // Owner's source ID (not DM-worker's name)
	DDLs   []string // DDL statements
	remain int      // remain count of sources needed to receive DDL info

	// whether the DDL info received from the source.
	// if all of them have been ready, then we call the lock `synced`.
	ready map[string]bool

	// whether the operations have done (exec/skip the shard DDL).
	// if all of them have done, then we call the lock `resolved`.
	done map[string]bool
}

// NewLock creates a new Lock instance.
func NewLock(id, task, owner string, ddls, sources []string) *Lock {
	l := &Lock{
		ID:     id,
		Task:   task,
		Owner:  owner,
		DDLs:   ddls,
		remain: len(sources),
		ready:  make(map[string]bool),
		done:   make(map[string]bool),
	}
	for _, s := range sources {
		l.ready[s] = false
		l.done[s] = false
	}
	metrics.ReportDDLPending(task, metrics.DDLPendingNone, metrics.DDLPendingUnSynced)

	return l
}

// TrySync tries to sync the lock, does decrease on remain, re-entrant.
// new upstream sources may join when the DDL lock is in syncing,
// so we need to merge these new sources.
func (l *Lock) TrySync(caller string, ddls, sources []string) (bool, int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// check DDL statement first.
	if !utils.CompareShardingDDLs(ddls, l.DDLs) {
		return l.remain <= 0, l.remain, terror.ErrMasterShardingDDLDiff.Generate(l.DDLs, ddls)
	}

	// try to merge any newly joined sources.
	for _, s := range sources {
		if _, ok := l.ready[s]; !ok {
			l.remain++
			l.ready[s] = false
			l.done[s] = false // mark as not-done for newly joined sources.
		}
	}

	// only `sync` once.
	if synced, ok := l.ready[caller]; ok && !synced {
		l.remain--
		l.ready[caller] = true
		if l.remain == 0 {
			metrics.ReportDDLPending(l.Task, metrics.DDLPendingUnSynced, metrics.DDLPendingSynced)
		}
	}

	return l.remain <= 0, l.remain, nil
}

// ForceSynced forces to mark the lock as synced.
func (l *Lock) ForceSynced() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.remain > 0 {
		metrics.ReportDDLPending(l.Task, metrics.DDLPendingUnSynced, metrics.DDLPendingSynced)
	}
	for source := range l.ready {
		l.ready[source] = true
	}
	l.remain = 0
}

// RevertSynced reverts the synced stage of the sources.
func (l *Lock) RevertSynced(sources []string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.remain == 0 && len(sources) > 0 {
		metrics.ReportDDLPending(l.Task, metrics.DDLPendingSynced, metrics.DDLPendingUnSynced)
	}
	for _, source := range sources {
		if synced, ok := l.ready[source]; ok && synced {
			l.ready[source] = false
			l.remain++
		}
	}
}

// IsSynced returns whether the lock has synced.
func (l *Lock) IsSynced() (bool, int) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.remain <= 0, l.remain
}

// Ready returns the sources sync status or whether they are ready.
func (l *Lock) Ready() map[string]bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	ret := make(map[string]bool, len(l.ready))
	for k, v := range l.ready {
		ret[k] = v
	}
	return ret
}

// MarkDone marks the operation of the source as done.
// NOTE: we do not support revert the `done` after marked now.
func (l *Lock) MarkDone(source string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.done[source]; !ok {
		return // do not add it if not exists.
	}
	l.done[source] = true
}

// IsDone returns whether the operation has done.
func (l *Lock) IsDone(source string) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.done[source]
}

// IsResolved returns whether the lock has resolved (all operations have done).
func (l *Lock) IsResolved() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	for _, done := range l.done {
		if !done {
			return false
		}
	}
	return true
}
