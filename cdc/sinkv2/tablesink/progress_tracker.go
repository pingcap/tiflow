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
	"context"
	"sync"
	"time"

	"github.com/emirpasic/gods/maps/linkedhashmap"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	// waitingInterval is the interval to wait for the all callbacks be called.
	// It used for close the table sink.
	waitingInterval = 100 * time.Millisecond
	// warnDuration is the duration to warn the progress tracker is not closed.
	warnDuration = 3 * time.Minute
)

// progressTracker is used to track the progress of the table sink.
// For example,
// We have txn1, txn2, resolvedTs2, txn3-1, txn3-2, resolvedTs3, resolvedTs4, resolvedTs5.
// txn3-1 and txn3-2 are in the same big txn.
// First txn1 and txn2 are written, then the progress can be updated to resolvedTs2.
// Then txn3-1 and txn3-2 are written, then the progress can be updated to resolvedTs3.
// Next, since no data is being written, we can update to resolvedTs5 in order.
type progressTracker struct {
	// tableID is the table ID of the table sink.
	tableID model.TableID
	// This lock for both pendingEventAndResolvedTs and lastMinResolvedTs.
	lock sync.Mutex
	// pendingEventAndResolvedTs is used to store the pending event keys and resolved tss.
	// The key is the key of the event or the resolved ts.
	// The value is nil or the resolved ts. **nil for event**.
	// Since the data in TableSink is sequential,
	// we only need to maintain an insertion order.
	pendingEventAndResolvedTs *linkedhashmap.Map
	// lastMinResolvedTs is used to store the last min resolved ts.
	// It is used to indicate the progress of the table sink.
	lastMinResolvedTs model.ResolvedTs
	// closed is used to indicate the progress tracker is closed.
	closed atomic.Bool
}

// newProgressTracker is used to create a new progress tracker.
// The last min resolved ts is set to 0.
// It means that the table sink has not started yet.
func newProgressTracker(tableID model.TableID) *progressTracker {
	return &progressTracker{
		tableID:                   tableID,
		pendingEventAndResolvedTs: linkedhashmap.New(),
		// It means the start of the table.
		// It's Ok to use 0 here.
		// Because sink node only update the checkpoint when it's growing.
		lastMinResolvedTs: model.NewResolvedTs(0),
	}
}

// addEvent is used to add the pending event key.
// Notice: must hold the lock.
func (r *progressTracker) addEvent(key uint64) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.pendingEventAndResolvedTs.Put(key, nil)
}

// addResolvedTs is used to add the pending resolved ts.
// Notice: must hold the lock.
func (r *progressTracker) addResolvedTs(key uint64, resolvedTs model.ResolvedTs) {
	r.lock.Lock()
	defer r.lock.Unlock()
	// If no pending event and resolved ts,
	// we can directly advance the progress.
	if r.pendingEventAndResolvedTs.Empty() {
		r.lastMinResolvedTs = resolvedTs
		return
	}
	r.pendingEventAndResolvedTs.Put(key, resolvedTs)
}

// remove is used to remove the pending resolved ts.
// If we are deleting the last value before resolved ts,
// that means we can advance the progress,
// and we will update lastMinResolvedTs.
// Notice: must hold the lock.
func (r *progressTracker) remove(key uint64) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.pendingEventAndResolvedTs.Remove(key)
	// We chose to use an iterator here instead of a for loop
	// because in most cases we will encounter some event before the last resolved ts.
	iterator := r.pendingEventAndResolvedTs.Iterator()
	var deleteKeys []any
	for iterator.Next() {
		// If the element is resolved ts,
		// it means we can advance the progress.
		if iterator.Value() != nil {
			// If the tracker is closed, we no longer need to track the progress.
			if !r.closed.Load() {
				r.lastMinResolvedTs = iterator.Value().(model.ResolvedTs)
			}
			deleteKeys = append(deleteKeys, iterator.Key())
		} else {
			// When we met the first event,
			// we couldn't advance anymore.
			break
		}
	}

	// Do not forget to remove the resolved ts.
	for _, key := range deleteKeys {
		r.pendingEventAndResolvedTs.Remove(key)
	}
}

// minTs returns the last min resolved ts.
// This means that all data prior to this point has already been processed.
// It can be considered as CheckpointTs of the table.
// Notice: must hold the lock.
func (r *progressTracker) minTs() model.ResolvedTs {
	r.lock.Lock()
	defer r.lock.Unlock()

	return r.lastMinResolvedTs
}

// trackingCount returns the number of pending events and resolved tss.
// Notice: must hold the lock.
func (r *progressTracker) trackingCount() int {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.pendingEventAndResolvedTs.Size()
}

// close is used to close the progress tracker.
func (r *progressTracker) close(ctx context.Context) error {
	r.closed.Store(true)

	blockTicker := time.NewTicker(warnDuration)
	defer blockTicker.Stop()
	// Used to block for loop for a while to prevent CPU spin.
	waitingTicker := time.NewTicker(waitingInterval)
	defer waitingTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-blockTicker.C:
			log.Warn("close processor doesn't return in time, may be stuck",
				zap.Int64("tableID", r.tableID),
				zap.Int("trackingCount", r.trackingCount()),
				zap.Any("lastMinResolvedTs", r.minTs()),
			)
		case <-waitingTicker.C:
			if r.trackingCount() == 0 {
				return nil
			}
		}
	}
}
