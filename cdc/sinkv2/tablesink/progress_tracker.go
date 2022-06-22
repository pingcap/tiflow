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
	// This lock for both pendingResolvedTs and lastMinResolvedTs.
	lock sync.Mutex
	// pendingResolvedTs is used to store the pending resolved ts.
	// The key is the key of the row or txn.
	// The value is the resolved ts.
	// Since the data in TableSink is sequential,
	// we only need to maintain an insertion order.
	pendingResolvedTs *linkedhashmap.Map
	// lastMinResolvedTs is used to store the last min resolved ts.
	// It is used to indicate the progress of the table sink.
	lastMinResolvedTs model.ResolvedTs
}

// newProgressTracker is used to create a new progress tracker.
// The last min resolved ts is set to 0.
// It means that the table sink has not started yet.
// nolint:deadcode
func newProgressTracker() *progressTracker {
	return &progressTracker{
		pendingResolvedTs: linkedhashmap.New(),
		// It means the start of the table.
		lastMinResolvedTs: model.NewResolvedTs(0),
	}
}

// add is used to add the pending resolved ts.
func (r *progressTracker) add(key any, resolvedTs model.ResolvedTs) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.pendingResolvedTs.Put(key, resolvedTs)
}

// remove is used to remove the pending resolved ts.
// If we are deleting the smallest row or txn,
// that means we can advance the progress,
// and we will update lastMinResolvedTs.
func (r *progressTracker) remove(key any) {
	r.lock.Lock()
	defer r.lock.Unlock()
	iterator := r.pendingResolvedTs.Iterator()
	if iterator.First() {
		// Is the smallest row or txn?
		if iterator.Key() == key {
			// It means we need to advance the min ts.
			// We need to store the last min ts.
			r.lastMinResolvedTs = iterator.Value().(model.ResolvedTs)
		}
		r.pendingResolvedTs.Remove(key)
	}
}

// minTs returns the last min resolved ts.
// This means that all data prior to this point has already been processed.
// It can be considered as CheckpointTs of the table.
func (r *progressTracker) minTs() model.ResolvedTs {
	r.lock.Lock()
	defer r.lock.Unlock()

	return r.lastMinResolvedTs
}
