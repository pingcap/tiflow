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
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"go.uber.org/zap"
)

const (
	// waitingInterval is the interval to wait for the all callbacks be called.
	// It used for closing the table sink.
	waitingInterval = 100 * time.Millisecond
	// warnDuration is the duration to warn the progress tracker is not closed.
	warnDuration = 30 * time.Second
	// A progressTracker contains several internal fixed-length buffers.
	// NOTICE: the buffer size must be aligned to 8 bytes.
	// It shouldn't be too large, otherwise it will consume too much memory.
	defaultBufferSize uint64 = 4096
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
	// span is the span of the table sink.
	span tablepb.Span

	// Internal Buffer size. Modified in tests only.
	bufferSize uint64

	// Following fields are protected by `mu`.
	mu sync.Mutex

	// frozen is used to indicate whether the progress tracker is frozen.
	// It means we do not advance anymore.
	frozen bool

	// Used to generate the next eventID.
	nextEventID uint64

	// Every received event is a bit in `pendingEvents`.
	pendingEvents [][]uint64

	// When old events are flushed the buffer should be released.
	nextToReleasePos uint64

	// The position that the next event which should be check in `advance`.
	nextToResolvePos uint64

	resolvedTsCache []pendingResolvedTs

	lastMinResolvedTs model.ResolvedTs

	lastCheckClosed atomic.Int64
}

// newProgressTracker is used to create a new progress tracker.
// The last min resolved ts is set to 0.
// It means that the table sink has not started yet.
func newProgressTracker(span tablepb.Span, bufferSize uint64) *progressTracker {
	if bufferSize%8 != 0 {
		panic("bufferSize must be align to 8 bytes")
	}

	return &progressTracker{
		span:       span,
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
		// If there is no buffer or the last one is full, we need to allocate a new one.
		buffer := make([]uint64, 0, r.bufferSize)
		r.pendingEvents = append(r.pendingEvents, buffer)
		bufferCount += 1
	}

	if bit == 0 {
		// If bit is 0 it means we need to append a new uint64 word for the event.
		r.pendingEvents[bufferCount-1] = append(r.pendingEvents[bufferCount-1], 0)
	}
	lastBuffer := r.pendingEvents[bufferCount-1]

	// Set the corresponding bit to 1.
	// For example, if the eventID is 3, the bit is 3 % 64 = 3.
	// 0000000000000000000000000000000000000000000000000000000000000000 ->
	// 0000000000000000000000000000000000000000000000000000000000001000
	// When we advance the progress, we can try to find the first 0 bit to indicate the progress.
	postEventFlush = func() { atomic.AddUint64(&lastBuffer[len(lastBuffer)-1], 1<<bit) }
	return
}

// addResolvedTs is used to add the pending resolved ts.
func (r *progressTracker) addResolvedTs(resolvedTs model.ResolvedTs) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// NOTICE: We should **NOT** update the `lastMinResolvedTs` when tracker is frozen.
	// So there is no need to try to append the resolved ts to `resolvedTsCache`.
	if r.frozen {
		return
	}

	// If there is no event or all events are flushed, we can update the resolved ts directly.
	if r.nextEventID == 0 || r.nextToResolvePos >= r.nextEventID {
		// Update the checkpoint ts.
		r.lastMinResolvedTs = resolvedTs
		return
	}

	// Sometimes, if there are no events for a long time and a lot of resolved ts are received,
	// we can update the last resolved ts directly.
	tsCacheLen := len(r.resolvedTsCache)
	if tsCacheLen > 0 {
		// The offset of the last resolved ts is the last event ID.
		// It means no event is adding. We can update the resolved ts directly.
		if r.resolvedTsCache[tsCacheLen-1].offset+1 == r.nextEventID {
			r.resolvedTsCache[tsCacheLen-1].resolvedTs = resolvedTs
			return
		}
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
	// The first index is used to access the buffer.
	// The second index is used to access the uint64 in the buffer.
	// The third index is used to access the bit in the uint64.
	offset := r.nextToResolvePos - r.nextToReleasePos
	idx1 := offset / (r.bufferSize * 64)
	idx2 := offset % (r.bufferSize * 64) / 64
	idx3 := offset % (r.bufferSize * 64) % 64

	for {
		if r.nextToResolvePos >= r.nextEventID {
			// All events are resolved.
			break
		}

		currBitMap := atomic.LoadUint64(&r.pendingEvents[idx1][idx2])
		if currBitMap == math.MaxUint64 {
			// Move to the next uint64 word (maybe in the next buffer).
			idx2 += 1
			if idx2 >= r.bufferSize {
				idx2 = 0
				idx1 += 1
			}
			r.nextToResolvePos += 64 - idx3
			idx3 = 0
		} else {
			// Try to find the first 0 bit in the word.
			for i := idx3; i < 64; i++ {
				if currBitMap&uint64(1<<i) == 0 {
					r.nextToResolvePos += i - idx3
					break
				}
			}
			break
		}
	}

	// Try to advance resolved timestamp based on `nextToResolvePos`.
	if r.nextToResolvePos > 0 {
		for len(r.resolvedTsCache) > 0 {
			cached := r.resolvedTsCache[0]
			if cached.offset <= r.nextToResolvePos-1 {
				// NOTICE: We should **NOT** update the `lastMinResolvedTs` when tracker is frozen.
				if !r.frozen {
					r.lastMinResolvedTs = cached.resolvedTs
				}
				r.resolvedTsCache = r.resolvedTsCache[1:]
				if len(r.resolvedTsCache) == 0 {
					r.resolvedTsCache = nil
				}
			} else {
				break
			}
		}
	}

	// If a buffer is finished, release it.
	for r.nextToResolvePos-r.nextToReleasePos >= r.bufferSize*64 {
		r.nextToReleasePos += r.bufferSize * 64
		// Use zero value to release the memory.
		r.pendingEvents[0] = nil
		r.pendingEvents = r.pendingEvents[1:]
		if len(r.pendingEvents) == 0 {
			r.pendingEvents = nil
		}
	}

	return r.lastMinResolvedTs
}

// trackingCount returns the number of pending events and resolved timestamps.
// Notice: must hold the lock.
func (r *progressTracker) trackingCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return int(r.nextEventID - r.nextToResolvePos)
}

// freezeProcess marks we do not advance checkpoint ts anymore.
func (r *progressTracker) freezeProcess() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.frozen {
		r.frozen = true
		r.lastCheckClosed.Store(time.Now().Unix())
	}
}

// close is used to close the progress tracker.
func (r *progressTracker) waitClosed(backendDead <-chan struct{}) {
	waitingTicker := time.NewTicker(waitingInterval)
	defer waitingTicker.Stop()
	for {
		select {
		case <-backendDead:
			r.advance()
			return
		case <-waitingTicker.C:
			if r.doCheckClosed() {
				return
			}
		}
	}
}

func (r *progressTracker) checkClosed(backendDead <-chan struct{}) bool {
	select {
	case <-backendDead:
		r.advance()
		return true
	default:
		return r.doCheckClosed()
	}
}

func (r *progressTracker) doCheckClosed() bool {
	resolvedTs := r.advance()
	trackingCount := r.trackingCount()
	if trackingCount == 0 {
		return true
	}

	now := time.Now().Unix()
	lastCheck := r.lastCheckClosed.Load()
	for now > lastCheck+int64(warnDuration.Seconds()) {
		if r.lastCheckClosed.CompareAndSwap(lastCheck, now) {
			log.Warn("Close table doesn't return in time, may be stuck",
				zap.Stringer("span", &r.span),
				zap.Int("trackingCount", trackingCount),
				zap.Any("lastMinResolvedTs", resolvedTs))
			break
		}
		lastCheck = r.lastCheckClosed.Load()
	}
	return false
}
