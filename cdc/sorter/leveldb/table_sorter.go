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

package leveldb

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sorter/encoding"
	"github.com/pingcap/tiflow/cdc/sorter/leveldb/message"
	"github.com/pingcap/tiflow/pkg/actor"
	actormsg "github.com/pingcap/tiflow/pkg/actor/message"
	"github.com/pingcap/tiflow/pkg/db"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// reader accepts out-of-order raw kv entries and output sorted entries
type reader struct {
	common

	state pollState

	lastSentCommitTs   uint64
	lastSentResolvedTs uint64
	lastEvent          *model.PolymorphicEvent

	outputCh chan *model.PolymorphicEvent
	delete   deleteThrottle

	metricIterReadDuration prometheus.Observer
	metricIterNextDuration prometheus.Observer
}

var _ actor.Actor = (*reader)(nil)

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
		r.lastSentCommitTs = event.CRTs
		return true
	default:
		return false
	}
}

// outputResolvedTs nonblocking outputs a resolved ts event.
func (r *reader) outputResolvedTs(rts model.Ts) {
	ok := r.output(model.NewResolvedPolymorphicEvent(0, rts))
	if ok {
		r.lastSentResolvedTs = rts
	}
}

// outputBufferedResolvedEvents nonblocking output resolved events and
// resolved ts that are buffered in outputBuffer.
// It pops outputted events in the buffer and append their key to deleteKeys.
func (r *reader) outputBufferedResolvedEvents(
	buffer *outputBuffer, sendResolvedTsHint bool,
) {
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
	// Remove outputted events.
	buffer.shiftResolvedEvents(remainIdx)

	// If all buffered resolved events are sent, send its resolved ts too.
	if sendResolvedTsHint && lastCommitTs != 0 && !hasRemainEvents {
		r.outputResolvedTs(lastCommitTs)
	}
}

// outputIterEvents nonblocking output resolved events that are buffered
// in leveldb.
// It appends outputted events's key to outputBuffer deleteKeys to delete them
// later, and appends resolved events to outputBuffer resolvedEvents to send
// them later.
//
// It returns:
//   * a bool to indicate whether it has read the last Next or not.
//   * a uint64, if it is not 0, it means all resolved events before the ts
//     are outputted.
//   * an error if it occurs.
//
// Note: outputBuffer must be empty.
func (r *reader) outputIterEvents(
	iter db.Iterator, hasReadLastNext bool, buffer *outputBuffer,
	resolvedTs uint64,
) (bool, uint64, error) {
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
	if hasReadLastNext {
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
			return false, 0, errors.Trace(iter.Error())
		}
		event := new(model.PolymorphicEvent)
		_, err := r.serde.Unmarshal(event, iter.Value())
		if err != nil {
			return false, 0, errors.Trace(err)
		}
		if commitTs > event.CRTs || commitTs > resolvedTs {
			log.Panic("event commit ts regression",
				zap.Any("event", event), zap.Stringer("key", message.Key(iter.Key())),
				zap.Uint64("ts", commitTs), zap.Uint64("resolvedTs", resolvedTs))
		}

		if commitTs == 0 {
			commitTs = event.CRTs
		}
		// Group resolved events that has the same commit ts.
		if commitTs == event.CRTs {
			buffer.appendResolvedEvent(event)
			continue
		}
		// As a new event belongs to a new txn group, we need to output all
		// buffered events before append the event.
		r.outputBufferedResolvedEvents(buffer, true)
		lenResolvedEvents, _ = buffer.len()
		if lenResolvedEvents > 0 {
			// Output blocked, skip append new event.
			// This means we have not read Next.
			hasReadNext = false
			break
		}

		// Append new event to the buffer.
		commitTs = event.CRTs
		buffer.appendResolvedEvent(event)
	}
	elapsed := time.Since(start)
	r.metricIterReadDuration.Observe(elapsed.Seconds())

	// Try shrink buffer to release memory.
	buffer.maybeShrink()

	// Events have not been sent, buffer them and output them later.
	// Do not let outputBufferedResolvedEvents output resolved ts, instead we
	// output resolved ts here.
	sendResolvedTsHint := false
	r.outputBufferedResolvedEvents(buffer, sendResolvedTsHint)
	lenResolvedEvents, _ = buffer.len()

	// Skip output resolved ts if there is any buffered resolved event.
	if lenResolvedEvents != 0 {
		return hasReadNext, 0, nil
	}

	if !hasNext && resolvedTs != 0 {
		// Iter is exhausted and there is no resolved event (up to max
		// resolved ts), output max resolved ts and return an exhausted
		// resolved ts.
		r.outputResolvedTs(resolvedTs)
		return hasReadNext, resolvedTs, nil
	}
	if commitTs != 0 {
		// All buffered resolved events are outputted,
		// output last commit ts.
		r.outputResolvedTs(commitTs)
	}

	return hasReadNext, 0, nil
}

// TODO: inline the struct to reader.
type pollState struct {
	// Buffer for resolved events and to-be-deleted events.
	outputBuf *outputBuffer
	// The maximum commit ts for all events.
	maxCommitTs uint64
	// The maximum commit ts for all resolved ts events.
	maxResolvedTs uint64
	// All resolved events before the resolved ts are outputted.
	exhaustedResolvedTs uint64

	// ID and router of the reader itself.
	readerID     actor.ID
	readerRouter *actor.Router

	// Compactor actor ID.
	compactorID actor.ID
	// A scheduler that triggers db compaction to speed up Iterator.Seek().
	compact *CompactScheduler
	// A threshold of triggering db compaction.
	iterFirstSlowDuration time.Duration
	// A timestamp when iterator was created.
	// Iterator is released once it execced `iterMaxAliveDuration`.
	iterAliveTime        time.Time
	iterMaxAliveDuration time.Duration
	// A channel for receiving iterator asynchronously.
	iterCh chan *message.LimitedIterator
	// A iterator for reading resolved events, up to the `iterResolvedTs`.
	iter           *message.LimitedIterator
	iterResolvedTs uint64
	// A flag to mark whether the current position has been read.
	iterHasRead bool

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
	if state.exhaustedResolvedTs < state.maxCommitTs &&
		state.exhaustedResolvedTs < state.maxResolvedTs {
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
func (state *pollState) tryGetIterator(
	uid uint32, tableID uint64, lastSentResolvedTs, lastSentCommitTs uint64,
) (*message.IterRequest, bool) {
	if state.iter != nil && state.iterCh != nil {
		log.Panic("assert failed, there can only be one of iter or iterCh",
			zap.Any("iter", state.iter), zap.Uint64("tableID", tableID),
			zap.Uint32("uid", uid))
	}

	if state.iter != nil {
		return nil, true
	}

	if state.iterCh == nil {
		// We haven't send request.
		iterCh := make(chan *message.LimitedIterator, 1)
		state.iterCh = iterCh
		readerRouter := state.readerRouter
		readerID := state.readerID
		return &message.IterRequest{
			Range: [2][]byte{
				encoding.EncodeTsKey(uid, tableID, state.exhaustedResolvedTs+1),
				encoding.EncodeTsKey(uid, tableID, state.maxResolvedTs+1),
			},
			ResolvedTs: state.maxResolvedTs,
			IterCallback: func(iter *message.LimitedIterator) {
				iterCh <- iter
				close(iterCh)
				// Notify itself that iterator has acquired.
				_ = readerRouter.Send(readerID, actormsg.SorterMessage(
					message.Task{
						UID:     uid,
						TableID: tableID,
						ReadTs:  message.ReadTs{},
					}))
			},
		}, false
	}

	// Try receive iterator.
	select {
	case iter := <-state.iterCh:
		seekTs := lastSentCommitTs
		if lastSentResolvedTs >= lastSentCommitTs {
			seekTs = lastSentResolvedTs + 1
		}
		// Iterator received, reset state.iterCh
		state.iterCh = nil
		state.iter = iter
		start := time.Now()
		state.iterAliveTime = start
		state.iterResolvedTs = iter.ResolvedTs
		state.iterHasRead = false
		state.iter.Seek(encoding.EncodeTsKey(uid, tableID, seekTs))
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

func (state *pollState) tryReleaseIterator() error {
	if state.iter == nil {
		return nil
	}
	now := time.Now()
	if !state.iter.Valid() || now.Sub(state.iterAliveTime) > state.iterMaxAliveDuration {
		err := state.iter.Release()
		if err != nil {
			return errors.Trace(err)
		}
		state.metricIterRelease.Observe(time.Since(now).Seconds())
		state.iter = nil
		state.iterHasRead = true

		if state.iterCh != nil {
			log.Panic("there must not be iterCh", zap.Any("iter", state.iter))
		}
	}

	return nil
}

// Poll receives ReadTs and send resolved events.
func (r *reader) Poll(ctx context.Context, msgs []actormsg.Message) (running bool) {
	for i := range msgs {
		switch msgs[i].Tp {
		case actormsg.TypeSorterTask:
		case actormsg.TypeStop:
			return false
		default:
			log.Panic("unexpected message", zap.Any("message", msgs[i]))
		}
		// Update the max commit ts and resolved ts of all received events.
		ts := msgs[i].SorterTask.ReadTs
		r.state.advanceMaxTs(ts.MaxCommitTs, ts.MaxResolvedTs)
	}

	// Length of buffered resolved events.
	lenResolvedEvents, _ := r.state.outputBuf.len()
	if lenResolvedEvents != 0 {
		// No new received events, it means output channel is available.
		// output resolved events as much as possible.
		r.outputBufferedResolvedEvents(r.state.outputBuf, true)
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
		// No new events and no resolved events.
		if !r.state.hasResolvedEvents() && r.state.maxResolvedTs != 0 {
			// To avoid ping-pong busy loop, we only send resolved ts
			// when it advances.
			if r.state.maxResolvedTs > r.lastSentResolvedTs {
				r.outputResolvedTs(r.state.maxResolvedTs)
			}
		}
		// Release iterator as we does not need to read.
		err := r.state.tryReleaseIterator()
		if err != nil {
			r.reportError("failed to release iterator", err)
			return false
		}
		// Send delete task to leveldb.
		if task.DeleteReq != nil {
			err = r.dbRouter.SendB(ctx, r.dbActorID, actormsg.SorterMessage(task))
			if err != nil {
				r.reportError("failed to send delete request", err)
				return false
			}
		}
		return true
	}

	var hasIter bool
	task.IterReq, hasIter = r.state.tryGetIterator(
		r.uid, r.tableID, r.lastSentResolvedTs, r.lastSentCommitTs)
	// Send delete/read task to leveldb.
	err := r.dbRouter.SendB(ctx, r.dbActorID, actormsg.SorterMessage(task))
	if err != nil {
		r.reportError("failed to send delete request", err)
		return false
	}
	if !hasIter {
		// Skip read iterator if there is no iterator
		return true
	}

	// Read and send resolved events from iterator.
	hasReadNext, exhaustedResolvedTs, err := r.outputIterEvents(
		r.state.iter, r.state.iterHasRead, r.state.outputBuf, r.state.iterResolvedTs)
	if err != nil {
		r.reportError("failed to read iterator", err)
		return false
	}
	if exhaustedResolvedTs > r.state.exhaustedResolvedTs {
		r.state.exhaustedResolvedTs = exhaustedResolvedTs
	}
	r.state.iterHasRead = hasReadNext
	err = r.state.tryReleaseIterator()
	if err != nil {
		r.reportError("failed to release iterator", err)
		return false
	}
	return true
}
