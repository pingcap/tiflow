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
	"math"
	"sync"
	stdatomic "sync/atomic"
	"time"

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

	// A progressTracker contains several internal fixed-length buffers.
	defaultBufferSize uint64 = 1024 * 1024
)

// A pendingResolvedTs is received by progressTracker but hasn't been flushed yet.
type pendingResolvedTs struct {
	offset     uint64
	resolvedTs model.ResolvedTs
}

// progressTracker is used to track the progress of the table sink.
//
// For example,
// We have txn1, txn2, resolvedTs2, txn3-1, txn3-2, resolvedTs3, resolvedTs4, resolvedTs5.
// txn3-1 and txn3-2 are in the same big txn.
// First txn1 and txn2 are written, then the progress can be updated to resolvedTs2.
// Then txn3-1 and txn3-2 are written, then the progress can be updated to resolvedTs3.
// Next, since no data is being written, we can update to resolvedTs5 in order.
//
// The core of the algorithm is `pendingEvents`, which is a bit map for all events.
// Every event is associated with a `eventID` which is a continuous number. `eventID`
// can be regarded as the event's offset in `pendingEvents`.
type progressTracker struct {
	// tableID is the table ID of the table sink.
	tableID model.TableID

	// Internal Buffer size.
	bufferSize uint64

	// closed is used to indicate the progress tracker is closed.
	closed atomic.Bool

	// Following fields are protected by `mu`.
	mu sync.Mutex

	// Used to generated the next eventID.
	nextEventID uint64

	// Every received event is a bit in `pendingEvents`.
	pendingEvents [][]uint64

	// When old events are flushed the buffer should be released.
	nextToReleasePos uint64

	// The position that the next event which should be check in `advance`.
	nextToResolvePos uint64

	resolvedTsCache []pendingResolvedTs

	lastMinResolvedTs model.ResolvedTs
}

// newProgressTracker is used to create a new progress tracker.
// The last min resolved ts is set to 0.
// It means that the table sink has not started yet.
func newProgressTracker(tableID model.TableID, bufferSize uint64) *progressTracker {
	if bufferSize%8 != 0 {
		panic("bufferSize must be align to 8 bytes")
	}

	return &progressTracker{
		tableID:    tableID,
		bufferSize: bufferSize / 8,
		// It means the start of the table.
		// It's Ok to use 0 here.
		// Because sink node only update the checkpoint when it's growing.
		lastMinResolvedTs: model.NewResolvedTs(0),
	}
}

// addEvent is used to add the pending event key. `postEventFlush` should be called
// when the event has been flushed.
func (r *progressTracker) addEvent() (postEventFlush func()) {
	r.mu.Lock()
	defer r.mu.Unlock()

	eventID := r.nextEventID
	bit := eventID % 64
	r.nextEventID += 1

	bufferCount := len(r.pendingEvents)
	if bufferCount == 0 || (uint64(len(r.pendingEvents[bufferCount-1])) == r.bufferSize && bit == 0) {
		buffer := make([]uint64, 0, r.bufferSize)
		r.pendingEvents = append(r.pendingEvents, buffer)
		bufferCount += 1
	}

	if bit == 0 {
		r.pendingEvents[bufferCount-1] = append(r.pendingEvents[bufferCount-1], 0)
	}
	lastBuffer := r.pendingEvents[bufferCount-1]

	postEventFlush = func() { stdatomic.AddUint64(&lastBuffer[len(lastBuffer)-1], 1<<bit) }
	return
}

// addResolvedTs is used to add the pending resolved ts.
func (r *progressTracker) addResolvedTs(resolvedTs model.ResolvedTs) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.nextEventID == 0 {
		r.lastMinResolvedTs = resolvedTs
		return
	}

	r.resolvedTsCache = append(r.resolvedTsCache, pendingResolvedTs{
		offset:     r.nextEventID - 1,
		resolvedTs: resolvedTs,
	})
}

// advance tries to move forward the tracker and returns the latest resolved timestamp.
func (r *progressTracker) advance() model.ResolvedTs {
	r.mu.Lock()
	defer r.mu.Unlock()

	// `pendingEvents` is like a 3-dimo bit array. To access a given bit in the array,
	// use `pendingEvents[idx1][idx2][idx3]`.
	offset := r.nextToResolvePos - r.nextToReleasePos
	idx1 := offset / (r.bufferSize * 64)
	idx2 := offset % (r.bufferSize * 64) / 64
	idx3 := offset % (r.bufferSize * 64) % 64

	for {
		if r.nextToResolvePos >= r.nextEventID {
			// All events are resolved.
			break
		}

		currBitMap := stdatomic.LoadUint64(&r.pendingEvents[idx1][idx2])
		if currBitMap == math.MaxUint64 {
			idx2 += 1
			if idx2 >= r.bufferSize {
				idx2 = 0
				idx1 += 1
			}
			r.nextToResolvePos += 64 - idx3
			idx3 = 0
		} else {
			for i := idx3; i < 64; i++ {
				if currBitMap&uint64(1<<i) == 0 {
					r.nextToResolvePos += i - idx3
					break
				}
			}
			break
		}
	}

	if r.nextToResolvePos > 0 {
		for len(r.resolvedTsCache) > 0 {
			cached := r.resolvedTsCache[0]
			if cached.offset <= r.nextToResolvePos-1 {
				r.lastMinResolvedTs = cached.resolvedTs
				r.resolvedTsCache = r.resolvedTsCache[1:]
				if len(r.resolvedTsCache) == 0 {
					r.resolvedTsCache = nil
				}
			} else {
				break
			}
		}
	}

	for r.nextToResolvePos-r.nextToReleasePos >= r.bufferSize*64 {
		r.nextToReleasePos += r.bufferSize * 64
		r.pendingEvents = r.pendingEvents[1:]
		if len(r.pendingEvents) == 0 {
			r.pendingEvents = nil
		}
	}

	return r.lastMinResolvedTs
}

// trackingCount returns the number of pending events and resolved tss.
// Notice: must hold the lock.
func (r *progressTracker) trackingCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return int(r.nextEventID - r.nextToResolvePos)
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
			log.Warn("Close process doesn't return in time, may be stuck",
				zap.Int64("tableID", r.tableID),
				zap.Int("trackingCount", r.trackingCount()),
				zap.Any("lastMinResolvedTs", r.advance()),
			)
		case <-waitingTicker.C:
			r.advance()
			if r.trackingCount() == 0 {
				return nil
			}
		}
	}
}
