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
	// This lock for both pendingEventAndResolvedTs and lastMinCommitTs.
	lock sync.Mutex
	// pendingEventAndResolvedTs is used to store the pending events and resolved tss.
	// The key is the key of the event or the resolved ts.
	// The value is nil or the resolved ts.
	// Since the data in TableSink is sequential,
	// we only need to maintain an insertion order.
	pendingEventAndResolvedTs *linkedhashmap.Map
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
		pendingEventAndResolvedTs: linkedhashmap.New(),
		// It means the start of the table.
		lastMinCommitTs: model.NewResolvedTs(0),
	}
}

// addEvent is used to add the pending event.
func (r *progressTracker) addEvent(key uint64) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.pendingEventAndResolvedTs.Put(key, nil)
}

// addResolvedTs is used to add the pending resolved ts.
func (r *progressTracker) addResolvedTs(key uint64, resolvedTs model.ResolvedTs) {
	r.lock.Lock()
	defer r.lock.Unlock()
	// If no pending event and resolved ts,
	// we can directly advance the progress.
	if r.pendingEventAndResolvedTs.Empty() {
		r.lastMinCommitTs = resolvedTs
	}
	r.pendingEventAndResolvedTs.Put(key, resolvedTs)
}

// remove is used to remove the pending resolved ts.
// If we are deleting the smallest row or txn,
// that means we can advance the progress,
// and we will update lastMinCommitTs.
func (r *progressTracker) remove(key uint64) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.pendingEventAndResolvedTs.Remove(key)
	iterator := r.pendingEventAndResolvedTs.Iterator()
	// No need to update lastMinCommitTs
	// if there is no pending event and resolved ts.
	if !iterator.First() {
		return
	}

	// If the first element is resolved ts,
	// it means we can advance the progress.
	if iterator.Value() != nil {
		r.lastMinCommitTs = iterator.Value().(model.ResolvedTs)
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
