// Copyright 2022 PingCAP, Inc.
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

package tablesink

import (
	"sync"

	"github.com/emirpasic/gods/maps/linkedhashmap"
	"github.com/pingcap/tiflow/cdc/model"
)

// progressTracker is used to track the progress of the table sink.
type progressTracker struct {
	// This lock for both pendingCommitTs and lastMinCommitTs.
	lock sync.Mutex
	// pendingCommitTs is used to store the pending commits ts.
	// The key is the key of the commitTs + batchID.
	// The value is the row or txn count.
	// Since the data in TableSink is sequential,
	// we only need to maintain an insertion order.
	pendingCommitTs *linkedhashmap.Map
	// lastMinCommitTs is used to store the last min commits ts.
	// It is used to indicate the progress of the table sink.
	lastMinCommitTs model.ResolvedTs
}

// newProgressTracker is used to create a new progress tracker.
// The last min commit ts is set to 0.
// It means that the table sink has not started yet.
// nolint:deadcode
func newProgressTracker() *progressTracker {
	return &progressTracker{
		pendingCommitTs: linkedhashmap.New(),
		// It means the start of the table.
		lastMinCommitTs: model.NewResolvedTs(0),
	}
}

// add is used to add the pending commit ts.
func (r *progressTracker) add(commitTs uint64, resolvedTs model.ResolvedTs) {
	r.lock.Lock()
	defer r.lock.Unlock()
	key := commitTs + resolvedTs.BatchID
	count, exists := r.pendingCommitTs.Get(commitTs + resolvedTs.BatchID)
	if exists {
		r.pendingCommitTs.Put(key, count.(uint64)+1)
	} else {
		r.pendingCommitTs.Put(key, 1)
	}
}

// remove is used to remove the pending resolved ts.
// If we are deleting the smallest row or txn,
// that means we can advance the progress,
// and we will update lastMinCommitTs.
func (r *progressTracker) remove(commitTs uint64, resolvedTs model.ResolvedTs) {
	r.lock.Lock()
	defer r.lock.Unlock()
	iterator := r.pendingCommitTs.Iterator()
	if !iterator.First() {
		panic("pendingCommitTs is empty")
	}
	key := commitTs + resolvedTs.BatchID
	// Is the smallest commitTs?
	if iterator.Key() == key {
		count := iterator.Value().(uint64)
		count--
		if count == 0 {
			// It means we need to advance the min ts.
			// We need to store the last min ts.
			r.lastMinCommitTs = model.ResolvedTs{
				Ts:      commitTs,
				BatchID: resolvedTs.BatchID,
				Mode:    resolvedTs.Mode,
			}
			r.pendingCommitTs.Remove(key)
		} else {
			r.pendingCommitTs.Put(key, count)
		}
		return
	}

	if count, exists := r.pendingCommitTs.Get(key); exists {
		r.pendingCommitTs.Put(key, count.(uint64)-1)
	} else {
		panic("can not find the key")
	}
}

// minTs returns the last min resolved ts.
// This means that all data prior to this point has already been processed.
// It can be considered as CheckpointTs of the table.
func (r *progressTracker) minTs() model.ResolvedTs {
	r.lock.Lock()
	defer r.lock.Unlock()

	return r.lastMinCommitTs
}
