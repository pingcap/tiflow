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

package db

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sorter"
	"github.com/pingcap/tiflow/cdc/sorter/db/message"
	"github.com/pingcap/tiflow/cdc/sorter/encoding"
	"github.com/pingcap/tiflow/pkg/actor"
	actormsg "github.com/pingcap/tiflow/pkg/actor/message"
	"github.com/pingcap/tiflow/pkg/db"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// reader accepts out-of-order raw kv entries and output sorted entries
type reader struct {
	common
	stopped bool

	state pollState

	lastSentCommitTs   uint64
	lastSentResolvedTs uint64
	lastEvent          *model.PolymorphicEvent

	outputCh chan *model.PolymorphicEvent
	delete   deleteThrottle

	metricIterReadDuration    prometheus.Observer
	metricIterNextDuration    prometheus.Observer
	metricTotalEventsKV       prometheus.Counter
	metricTotalEventsResolved prometheus.Counter
}

var _ actor.Actor[message.Task] = (*reader)(nil)

func (r *reader) stats() sorter.Stats {
	return sorter.Stats{
		CheckpointTsEgress: atomic.LoadUint64(&r.lastSentCommitTs),
		ResolvedTsEgress:   atomic.LoadUint64(&r.lastSentResolvedTs),
	}
}

// setTaskDelete set delete range if there are too many events can be deleted or
// it has been a long time since last delete.
func (r *reader) setTaskDelete(task *message.Task, deleteKeys []message.Key) {
	if len(deleteKeys) <= 0 {
		return
	}

	totalDelete := r.delete.count
	if !r.delete.trigger(len(deleteKeys), time.Now()) {
		return
	}

	// Delete events that less than the last delete keys' commit ts.
	task.DeleteReq = &message.DeleteRequest{}
	task.DeleteReq.Range[0] = encoding.EncodeTsKey(r.uid, r.tableID, 0)
	task.DeleteReq.Range[1] = []byte(deleteKeys[len(deleteKeys)-1])
	task.DeleteReq.Count = totalDelete + len(deleteKeys)
}

// output nonblocking outputs an event. Caller should retry when it returns false.
func (r *reader) output(event *model.PolymorphicEvent) bool {
	if r.lastEvent == nil {
		r.lastEvent = event
	}
	if r.lastEvent.CRTs > event.CRTs {
		log.Panic("regression",
			zap.Any("lastEntry", r.lastEvent), zap.Any("event", event),
			zap.Uint64("regionID", event.RegionID()))
	}
	select {
	case r.outputCh <- event:
		r.lastEvent = event
		atomic.StoreUint64(&r.lastSentCommitTs, event.CRTs)
		return true
	default:
		return false
	}
}

// outputResolvedTs nonblocking outputs a resolved ts event.
func (r *reader) outputResolvedTs(rts model.Ts) {
	ok := r.output(model.NewResolvedPolymorphicEvent(0, rts))
	if ok {
		r.metricTotalEventsResolved.Inc()
		atomic.StoreUint64(&r.lastSentResolvedTs, rts)
	}
}

// outputBufferedResolvedEvents nonblocking output resolved events and
// resolved ts that are buffered in outputBuffer.
// It pops outputted events in the buffer and append their key to deleteKeys.
func (r *reader) outputBufferedResolvedEvents(buffer *outputBuffer) {
	hasRemainEvents := false
	// Index of remaining output events
	remainIdx := 0
	// Commit ts of the last outputted events.
	lastCommitTs := uint64(0)
	for idx := range buffer.resolvedEvents {
		event := buffer.resolvedEvents[idx]
		ok := r.output(event)
		if !ok {
			hasRemainEvents = true
			break
		}
		lastCommitTs = event.CRTs

		// Delete sent events.
		key := encoding.EncodeKey(r.uid, r.tableID, event)
		buffer.appendDeleteKey(message.Key(key))
		remainIdx = idx + 1
	}
	r.metricTotalEventsKV.Add(float64(remainIdx))
	// Remove outputted events.
	buffer.shiftResolvedEvents(remainIdx)

	// If all buffered resolved events are sent, send its resolved ts too.
	if lastCommitTs != 0 && !hasRemainEvents && !buffer.partialReadTxn {
		r.outputResolvedTs(lastCommitTs)
	}
}

// outputIterEvents nonblocking output resolved events that are buffered
// in db.
// It appends outputted events's key to outputBuffer deleteKeys to delete them
// later, and appends resolved events to outputBuffer resolvedEvents to send
// them later.
//
// It returns a new read position.
//
// Note: outputBuffer must be empty.
func (r *reader) outputIterEvents(
	iter db.Iterator, position readPosition, buffer *outputBuffer, resolvedTs uint64,
) (readPosition, error) {
	lenResolvedEvents, lenDeleteKeys := buffer.len()
	if lenDeleteKeys > 0 || lenResolvedEvents > 0 {
		log.Panic("buffer is not empty",
			zap.Int("deleteKeys", lenDeleteKeys),
			zap.Int("resolvedEvents", lenResolvedEvents))
	}

	// Commit ts of buffered resolved events.
	commitTs := uint64(0)
	start := time.Now()
	lastNext := start
	if position.iterHasRead {
		// We have read the last key/value, move the Next.
		iter.Next()
		r.metricIterNextDuration.Observe(time.Since(start).Seconds())
	} // else the last is not read, we need to skip calling Next and read again.
	hasReadNext := true
	hasNext := iter.Valid()
	for ; hasNext; hasNext = iter.Next() {
		now := time.Now()
		r.metricIterNextDuration.Observe(now.Sub(lastNext).Seconds())
		lastNext = now

		if iter.Error() != nil {
			return readPosition{}, errors.Trace(iter.Error())
		}
		event := new(model.PolymorphicEvent)
		_, err := r.serde.Unmarshal(event, iter.Value())
		if err != nil {
			return readPosition{}, errors.Trace(err)
		}
		if commitTs > event.CRTs || commitTs > resolvedTs {
			log.Panic("event commit ts regression",
				zap.Any("event", event), zap.Stringer("key", message.Key(iter.Key())),
				zap.Uint64("ts", commitTs), zap.Uint64("resolvedTs", resolvedTs))
		}

		if commitTs == 0 {
			commitTs = event.CRTs
		}
		// Read all resolved events that have the same commit ts.
		if commitTs == event.CRTs {
			ok := buffer.tryAppendResolvedEvent(event)
			if !ok {
				// append fails and buffer is full, we need to flush buffer to
				// prevent OOM.
				// It means we have not read value in to buffer after calling Next.
				hasReadNext = false
				break
			}
			continue
		}

		// Commit ts has changed, the new event belongs to a new txn group,
		// we need to output all buffered events before append the event.
		r.outputBufferedResolvedEvents(buffer)
		lenResolvedEvents, _ = buffer.len()
		if lenResolvedEvents > 0 {
			// Output blocked, skip append new event.
			// It means we have not read value in to buffer after calling Next.
			hasReadNext = false
			break
		}

		// Append new event to the buffer.
		commitTs = event.CRTs
		buffer.tryAppendResolvedEvent(event)
	}
	elapsed := time.Since(start)
	r.metricIterReadDuration.Observe(elapsed.Seconds())

	// When iter exhausts, buffer may never get a chance to output in the above
	// for loop. We retry output buffer again.
	r.outputBufferedResolvedEvents(buffer)

	// Try shrink buffer to release memory.
	buffer.maybeShrink()

	newPos := readPosition{
		iterHasRead: hasReadNext,
	}
	if !buffer.partialReadTxn {
		// All resolved events whose commit ts are less or equal to the commitTs
		// have read into buffer.
		newPos.exhaustedResolvedTs = commitTs
		if !hasNext {
			// Iter is exhausted, it means resolved events whose commit ts are
			// less or equal to the commitTs have read into buffer.
			if resolvedTs != 0 {
				newPos.exhaustedResolvedTs = resolvedTs
			}
		}
	} else {
		// Copy current iter key to position.
		newPos.partialTxnKey = append([]byte{}, iter.Key()...)
	}

	return newPos, nil
}

type readPosition struct {
	// A flag to mark whether the current position has been read.
	iterHasRead bool
	// partialTxnKey is set when a transaction is partially read.
	partialTxnKey []byte
	// All resolved events before the resolved ts are read into buffer.
	exhaustedResolvedTs uint64
}

func (r *readPosition) update(position readPosition) {
	if position.exhaustedResolvedTs > r.exhaustedResolvedTs {
		r.exhaustedResolvedTs = position.exhaustedResolvedTs
	}
	r.iterHasRead = position.iterHasRead
	r.partialTxnKey = position.partialTxnKey
}

type pollState struct {
	// Buffer for resolved events and to-be-deleted events.
	outputBuf *outputBuffer
	// The position of a reader.
	position readPosition

	// The maximum commit ts for all events.
	maxCommitTs uint64
	// The maximum commit ts for all resolved ts events.
	maxResolvedTs uint64

	// read data after `startTs`
	startTs uint64

	// ID and router of the reader itself.
	readerID     actor.ID
	readerRouter *actor.Router[message.Task]

	// Compactor actor ID.
	compactorID actor.ID
	// A scheduler that triggers db compaction to speed up Iterator.Seek().
	compact *CompactScheduler
	// A threshold of triggering db compaction.
	iterFirstSlowDuration time.Duration
	// A timestamp when iterator was created.
	// Iterator is released once it exceeds `iterMaxAliveDuration`.
	iterAliveTime        time.Time
	iterMaxAliveDuration time.Duration
	// A timestamp when we request an iterator.
	iterRequestTime time.Time
	// A channel for receiving iterator asynchronously.
	iterCh chan *message.LimitedIterator
	// An iterator for reading resolved events, up to the `iterResolvedTs`.
	iter           *message.LimitedIterator
	iterResolvedTs uint64

	metricIterRequest prometheus.Observer
	metricIterFirst   prometheus.Observer
	metricIterRelease prometheus.Observer
}

func (state *pollState) hasResolvedEvents() bool {
	// It has resolved events, if 1) it has buffer resolved events,
	lenResolvedEvents, _ := state.outputBuf.len()
	if lenResolvedEvents > 0 {
		return true
	}
	// or 2) there are some events that can be resolved.
	// -------|-----------------|-------------|-------> time
	// exhaustedResolvedTs
	//                     maxCommitTs
	//                                   maxResolvedTs
	// -------|-----------------|-------------|-------> time
	// exhaustedResolvedTs
	//                     maxResolvedTs
	//                                   maxCommitTs
	if state.position.exhaustedResolvedTs < state.maxCommitTs &&
		state.position.exhaustedResolvedTs < state.maxResolvedTs {
		return true
	}

	// Otherwise, there is no event can be resolved.
	// -------|-----------------|-------------|-------> time
	//   maxCommitTs
	//                 exhaustedResolvedTs
	//                                   maxResolvedTs
	return false
}

func (state *pollState) advanceMaxTs(maxCommitTs, maxResolvedTs uint64) {
	// The max commit ts of all received events.
	if maxCommitTs > state.maxCommitTs {
		state.maxCommitTs = maxCommitTs
	}
	// The max resolved ts of all received resolvedTs events.
	if maxResolvedTs > state.maxResolvedTs {
		state.maxResolvedTs = maxResolvedTs
	}
}

// tryGetIterator tries to get an iterator.
// When it returns a request, caller must send it.
// When it returns true, it means there is an iterator that can be used.
func (state *pollState) tryGetIterator(uid uint32, tableID uint64) (*message.IterRequest, bool) {
	if state.iter != nil && state.iterCh != nil {
		log.Panic("assert failed, there can only be one of iter or iterCh",
			zap.Any("iter", state.iter), zap.Uint64("tableID", tableID),
			zap.Uint32("uid", uid))
	}

	if state.iter != nil {
		return nil, true
	}

	if state.iterCh == nil {
		// We haven't sent request.
		iterCh := make(chan *message.LimitedIterator, 1)
		state.iterCh = iterCh
		state.iterRequestTime = time.Now()
		readerRouter := state.readerRouter
		readerID := state.readerID
		lowerBoundTs := atomic.LoadUint64(&state.startTs)
		if lowerBoundTs < state.position.exhaustedResolvedTs {
			lowerBoundTs = state.position.exhaustedResolvedTs
		}
		return &message.IterRequest{
			Range: [2][]byte{
				encoding.EncodeTsKey(uid, tableID, lowerBoundTs+1),
				encoding.EncodeTsKey(uid, tableID, state.maxResolvedTs+1),
			},
			ResolvedTs: state.maxResolvedTs,
			CRTsFilter: [2]uint64{state.position.exhaustedResolvedTs + 1, state.maxResolvedTs},
			IterCallback: func(iter *message.LimitedIterator) {
				iterCh <- iter
				close(iterCh)
				// Notify itself that iterator has acquired.
				_ = readerRouter.Send(readerID, actormsg.ValueMessage(
					message.Task{
						UID:     uid,
						TableID: tableID,
						ReadTs:  message.ReadTs{},
					}))
			},
		}, false
	}

	// Try to receive iterator.
	select {
	case iter := <-state.iterCh:
		// Iterator received, reset state.iterCh
		state.iterCh = nil
		state.iter = iter
		start := time.Now()
		requestDuration := start.Sub(state.iterRequestTime)
		state.metricIterRequest.Observe(requestDuration.Seconds())
		state.iterAliveTime = start
		state.iterResolvedTs = iter.ResolvedTs
		state.position.iterHasRead = false
		state.iter.Seek(state.position.partialTxnKey)
		duration := time.Since(start)
		state.metricIterFirst.Observe(duration.Seconds())
		if duration >= state.iterFirstSlowDuration {
			// Try trigger a compaction if Iterator.Seek is too slow.
			state.compact.tryScheduleCompact(state.compactorID, 0)
		}
		return nil, true
	default:
		// Iterator is not ready yet.
		return nil, false
	}
}

func (state *pollState) tryReleaseIterator(force bool) error {
	if state.iter == nil {
		return nil
	}
	now := time.Now()
	if !state.iter.Valid() || now.Sub(state.iterAliveTime) > state.iterMaxAliveDuration || force {
		err := state.iter.Release()
		if err != nil {
			return errors.Trace(err)
		}
		state.metricIterRelease.Observe(time.Since(now).Seconds())
		state.iter = nil
		state.position.iterHasRead = true

		if state.iterCh != nil {
			log.Panic("there must not be iterCh", zap.Any("iter", state.iter))
		}
	}

	return nil
}

// Poll receives ReadTs and send resolved events.
func (r *reader) Poll(ctx context.Context, msgs []actormsg.Message[message.Task]) (running bool) {
	for i := range msgs {
		switch msgs[i].Tp {
		case actormsg.TypeValue:
		case actormsg.TypeStop:
			r.reportError("receive stop message", nil)
			return false
		default:
			log.Panic("unexpected message", zap.Any("message", msgs[i]))
		}
		if msgs[i].Value.StartTs != 0 {
			atomic.StoreUint64(&r.state.startTs, msgs[i].Value.StartTs)
			continue
		}
		// Update the max commit ts and resolved ts of all received events.
		ts := msgs[i].Value.ReadTs
		r.state.advanceMaxTs(ts.MaxCommitTs, ts.MaxResolvedTs)

		// Test only message.
		if msgs[i].Value.Test != nil {
			time.Sleep(msgs[i].Value.Test.Sleep)
		}
	}

	// Length of buffered resolved events.
	lenResolvedEvents, _ := r.state.outputBuf.len()
	if lenResolvedEvents != 0 {
		// Try output buffered resolved events.
		r.outputBufferedResolvedEvents(r.state.outputBuf)
		lenResolvedEvents, _ = r.state.outputBuf.len()
	}
	// Build task for new events and delete sent keys.
	task := message.Task{UID: r.uid, TableID: r.tableID}
	r.setTaskDelete(&task, r.state.outputBuf.deleteKeys)
	// Reset buffer as delete keys are scheduled.
	r.state.outputBuf.resetDeleteKey()
	// Try shrink buffer to release memory.
	r.state.outputBuf.maybeShrink()

	// It can only read an iterator when
	// 1. No buffered resolved events, they must be sent before
	//    sending further resolved events from iterator.
	readIter := lenResolvedEvents == 0
	// 2. There are some events that can be resolved.
	readIter = readIter && r.state.hasResolvedEvents()
	if !readIter {
		// No buffered resolved events, try to send resolved ts.
		if !r.state.hasResolvedEvents() && r.state.maxResolvedTs != 0 {
			// To avoid ping-pong busy loop, we only send resolved ts
			// when it advances.
			if r.state.maxResolvedTs > r.lastSentResolvedTs {
				r.outputResolvedTs(r.state.maxResolvedTs)
			}
		}
		// Release iterator as we do not need to read.
		err := r.state.tryReleaseIterator(false)
		if err != nil {
			r.reportError("failed to release iterator", err)
			return false
		}
		// Send delete task to db.
		if task.DeleteReq != nil {
			err = r.dbRouter.SendB(ctx, r.dbActorID, actormsg.ValueMessage(task))
			if err != nil {
				r.reportError("failed to send delete request", err)
				return false
			}
		}
		return true
	}

	var hasIter bool
	task.IterReq, hasIter = r.state.tryGetIterator(r.uid, r.tableID)
	// Send delete/read task to db.
	err := r.dbRouter.SendB(ctx, r.dbActorID, actormsg.ValueMessage(task))
	if err != nil {
		r.reportError("failed to send delete request", err)
		return false
	}
	if !hasIter {
		// Skip read iterator if there is no iterator
		return true
	}

	// Read and send resolved events from iterator.
	position, err := r.outputIterEvents(
		r.state.iter, r.state.position, r.state.outputBuf, r.state.iterResolvedTs)
	if err != nil {
		r.reportError("failed to read iterator", err)
		return false
	}
	r.state.position.update(position)
	err = r.state.tryReleaseIterator(false)
	if err != nil {
		r.reportError("failed to release iterator", err)
		return false
	}
	return true
}

// OnClose releases reader resource.
func (r *reader) OnClose() {
	if r.stopped {
		return
	}
	r.stopped = true
	// Must release iterator before stopping, otherwise it leaks iterator.
	_ = r.state.tryReleaseIterator(true)
	r.common.closedWg.Done()
}
