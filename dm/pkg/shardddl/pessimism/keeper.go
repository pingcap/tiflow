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
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

// LockKeeper used to keep and handle DDL lock conveniently.
// The lock information do not need to be persistent, and can be re-constructed from the shard DDL info.
type LockKeeper struct {
	mu    sync.RWMutex
	locks map[string]*Lock // lockID -> Lock
}

// NewLockKeeper creates a new LockKeeper instance.
func NewLockKeeper() *LockKeeper {
	return &LockKeeper{
		locks: make(map[string]*Lock),
	}
}

// TrySync tries to sync the lock.
func (lk *LockKeeper) TrySync(info Info, sources []string) (string, bool, int, error) {
	var (
		lockID = genDDLLockID(info)
		l      *Lock
		ok     bool
	)

	lk.mu.Lock()
	defer lk.mu.Unlock()

	if l, ok = lk.locks[lockID]; !ok {
		lk.locks[lockID] = NewLock(lockID, info.Task, info.Source, info.DDLs, sources)
		l = lk.locks[lockID]
	}

	synced, remain, err := l.TrySync(info.Source, info.DDLs, sources)
	return lockID, synced, remain, err
}

// RemoveLock removes a lock.
func (lk *LockKeeper) RemoveLock(lockID string) bool {
	lk.mu.Lock()
	defer lk.mu.Unlock()

	_, ok := lk.locks[lockID]
	delete(lk.locks, lockID)
	metrics.RemoveDDLPending(utils.ExtractTaskFromLockID(lockID))
	return ok
}

// RemoveLockByInfo removes a lock through given info.
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
}

// genDDLLockID generates DDL lock ID from its info.
func genDDLLockID(info Info) string {
	return utils.GenDDLLockID(info.Task, info.Schema, info.Table)
}
