// Copyright 2021 PingCAP, Inc.
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
	"math"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sorter"
	"github.com/pingcap/tiflow/cdc/sorter/encoding"
	"github.com/pingcap/tiflow/cdc/sorter/leveldb/message"
	"github.com/pingcap/tiflow/pkg/actor"
	actormsg "github.com/pingcap/tiflow/pkg/actor/message"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/db"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	// Capacity of db sorter input and output channels.
	sorterInputCap, sorterOutputCap = 64, 64
	// Max size of received event batch.
	batchReceiveEventSize = 32
)

var levelDBSorterIDAlloc uint32 = 0

func allocID() uint32 {
	return atomic.AddUint32(&levelDBSorterIDAlloc, 1)
}

// Sorter accepts out-of-order raw kv entries and output sorted entries
type Sorter struct {
	actorID actor.ID
	router  *actor.Router
	compact *CompactScheduler
	uid     uint32
	tableID uint64
	serde   *encoding.MsgPackGenSerde

	iterMaxAliveDuration  time.Duration
	iterFirstSlowDuration time.Duration

	lastSentResolvedTs uint64
	lastEvent          *model.PolymorphicEvent

	inputCh  chan *model.PolymorphicEvent
	outputCh chan *model.PolymorphicEvent

	closed int32

	metricTotalEventsKV         prometheus.Counter
	metricTotalEventsResolvedTs prometheus.Counter
	metricIterDuration          prometheus.ObserverVec
	metricIterReadDuration      prometheus.Observer
	metricIterNextDuration      prometheus.Observer
}

// NewSorter creates a new Sorter
func NewSorter(
	ctx context.Context, tableID int64, startTs uint64,
	router *actor.Router, actorID actor.ID, compact *CompactScheduler,
	cfg *config.DBConfig,
) *Sorter {
	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	metricIterDuration := sorterIterReadDurationHistogram.MustCurryWith(
		prometheus.Labels{"capture": captureAddr, "id": changefeedID})
	return &Sorter{
		actorID:            actorID,
		router:             router,
		compact:            compact,
		uid:                allocID(),
		tableID:            uint64(tableID),
		lastSentResolvedTs: startTs,
		serde:              &encoding.MsgPackGenSerde{},

		iterMaxAliveDuration:  time.Duration(cfg.IteratorMaxAliveDuration) * time.Millisecond,
		iterFirstSlowDuration: time.Duration(cfg.IteratorSlowReadDuration) * time.Millisecond,

		inputCh:  make(chan *model.PolymorphicEvent, sorterInputCap),
		outputCh: make(chan *model.PolymorphicEvent, sorterOutputCap),

		metricTotalEventsKV:         sorter.EventCount.WithLabelValues(captureAddr, changefeedID, "kv"),
		metricTotalEventsResolvedTs: sorter.EventCount.WithLabelValues(captureAddr, changefeedID, "resolved"),
		metricIterDuration:          metricIterDuration,
		metricIterReadDuration:      metricIterDuration.WithLabelValues("read"),
		metricIterNextDuration:      metricIterDuration.WithLabelValues("next"),
	}
}

func (ls *Sorter) waitInput(ctx context.Context) (*model.PolymorphicEvent, error) {
	select {
	case <-ctx.Done():
		return nil, errors.Trace(ctx.Err())
	case ev := <-ls.inputCh:
		return ev, nil
	}
}

func (ls *Sorter) waitInputOutput(
	ctx context.Context,
) (*model.PolymorphicEvent, error) {
	// A dummy event for detecting whether output is available.
	dummyEvent := model.NewResolvedPolymorphicEvent(0, 0)
	select {
	// Prefer receiving input events.
	case ev := <-ls.inputCh:
		return ev, nil
	default:
		select {
		case <-ctx.Done():
			return nil, errors.Trace(ctx.Err())
		case ev := <-ls.inputCh:
			return ev, nil
		case ls.outputCh <- dummyEvent:
			return nil, nil
		}
	}
}

// wait input or output becomes available.
// It returns
//   1) the max commit ts of received new events,
//   2) the max resolved ts of new resolvedTs events,
//   3) number of received new events,
//   4) error.
//
// If input is available, it batches newly received events.
// If output available, it sends a dummy resolved ts event and returns.
func (ls *Sorter) wait(
	ctx context.Context, waitOutput bool, events []*model.PolymorphicEvent,
) (uint64, uint64, int, error) {
	batchSize := len(events)
	if batchSize <= 0 {
		log.Panic("batch size must be larger than 0")
	}
	maxCommitTs, maxResolvedTs := uint64(0), uint64(0)
	inputCount, kvEventCount, resolvedEventCount := 0, 0, 0
	appendInputEvent := func(ev *model.PolymorphicEvent) {
		if ls.lastSentResolvedTs != 0 && ev.CRTs < ls.lastSentResolvedTs {
			// Since TiKV/Puller may send out of order or duplicated events,
			// we should not panic here.
			// Regression is not a common case, use warn level to rise our
			// attention.
			log.Warn("commit ts < resolved ts",
				zap.Uint64("lastSentResolvedTs", ls.lastSentResolvedTs),
				zap.Any("event", ev), zap.Uint64("regionID", ev.RegionID()))
			return
		}
		if ev.RawKV.OpType == model.OpTypeResolved {
			if maxResolvedTs < ev.CRTs {
				maxResolvedTs = ev.CRTs
			}
			resolvedEventCount++
		} else {
			if maxCommitTs < ev.CRTs {
				maxCommitTs = ev.CRTs
			}
			events[inputCount] = ev
			inputCount++
			kvEventCount++
		}
	}

	if waitOutput {
		// Wait intput and output.
		ev, err := ls.waitInputOutput(ctx)
		if err != nil {
			atomic.StoreInt32(&ls.closed, 1)
			close(ls.outputCh)
			return 0, 0, 0, errors.Trace(ctx.Err())
		}
		if ev == nil {
			// No input event and output is available.
			return maxCommitTs, maxResolvedTs, 0, nil
		}
		appendInputEvent(ev)
	} else {
		// Wait input only.
		ev, err := ls.waitInput(ctx)
		if err != nil {
			atomic.StoreInt32(&ls.closed, 1)
			close(ls.outputCh)
			return 0, 0, 0, errors.Trace(ctx.Err())
		}
		appendInputEvent(ev)
	}

	// Batch receive events
BATCH:
	for inputCount < batchSize {
		select {
		case ev := <-ls.inputCh:
			appendInputEvent(ev)
		default:
			break BATCH
		}
	}
	ls.metricTotalEventsKV.Add(float64(kvEventCount))
	ls.metricTotalEventsResolvedTs.Add(float64(resolvedEventCount))

	// Release buffered events to help GC reclaim memory.
	for i := inputCount; i < batchSize; i++ {
		events[i] = nil
	}
	return maxCommitTs, maxResolvedTs, inputCount, nil
}

// buildTask build a task for writing new events and delete outputted events.
func (ls *Sorter) buildTask(
	events []*model.PolymorphicEvent, deleteKeys []message.Key,
) (message.Task, error) {
	writes := make(map[message.Key][]byte)
	for i := range events {
		event := events[i]
		if event.RawKV.OpType == model.OpTypeResolved {
			continue
		}

		key := encoding.EncodeKey(ls.uid, ls.tableID, event)
		value := []byte{}
		var err error
		value, err = ls.serde.Marshal(event, value)
		if err != nil {
			return message.Task{}, errors.Trace(err)
		}
		writes[message.Key(key)] = value
	}

	// Delete keys of outputted resolved events.
	for i := range deleteKeys {
		writes[deleteKeys[i]] = []byte{}
	}

	return message.Task{
		UID:     ls.uid,
		TableID: ls.tableID,
		Events:  writes,
	}, nil
}

// output nonblocking outputs an event. Caller should retry when it returns false.
func (ls *Sorter) output(event *model.PolymorphicEvent) bool {
	if ls.lastEvent == nil {
		ls.lastEvent = event
	}
	if ls.lastEvent.CRTs > event.CRTs {
		log.Panic("regression",
			zap.Any("lastEntry", ls.lastEvent), zap.Any("event", event),
			zap.Uint64("regionID", event.RegionID()))
	}
	select {
	case ls.outputCh <- event:
		ls.lastEvent = event
		return true
	default:
		return false
	}
}

// outputResolvedTs nonblocking outputs a resolved ts event.
func (ls *Sorter) outputResolvedTs(rts model.Ts) {
	ok := ls.output(model.NewResolvedPolymorphicEvent(0, rts))
	if ok {
		ls.lastSentResolvedTs = rts
	}
}

// outputBufferedResolvedEvents nonblocking output resolved events and
// resolved ts that are buffered in outputBuffer.
// It pops outputted events in the buffer and append their key to deleteKeys.
func (ls *Sorter) outputBufferedResolvedEvents(
	buffer *outputBuffer, sendResolvedTsHint bool,
) {
	hasRemainEvents := false
	// Index of remaining output events
	remainIdx := 0
	// Commit ts of the last outputted events.
	lastCommitTs := uint64(0)
	for idx := range buffer.resolvedEvents {
		event := buffer.resolvedEvents[idx]
		ok := ls.output(event)
		if !ok {
			hasRemainEvents = true
			break
		}
		lastCommitTs = event.CRTs

		// Delete sent events.
		key := encoding.EncodeKey(ls.uid, ls.tableID, event)
		buffer.appendDeleteKey(message.Key(key))
		remainIdx = idx + 1
	}
	// Remove outputted events.
	buffer.shiftResolvedEvents(remainIdx)

	// If all buffered resolved events are sent, send its resolved ts too.
	if sendResolvedTsHint && lastCommitTs != 0 && !hasRemainEvents {
		ls.outputResolvedTs(lastCommitTs)
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
func (ls *Sorter) outputIterEvents(
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
		ls.metricIterNextDuration.Observe(time.Since(start).Seconds())
	} // else the last is not read, we need to skip calling Next and read again.
	hasReadNext := true
	hasNext := iter.Valid()
	for ; hasNext; hasNext = iter.Next() {
		now := time.Now()
		ls.metricIterNextDuration.Observe(now.Sub(lastNext).Seconds())
		lastNext = now

		if iter.Error() != nil {
			return false, 0, errors.Trace(iter.Error())
		}
		event := new(model.PolymorphicEvent)
		_, err := ls.serde.Unmarshal(event, iter.Value())
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
		ls.outputBufferedResolvedEvents(buffer, true)
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
	ls.metricIterReadDuration.Observe(elapsed.Seconds())

	// Try shrink buffer to release memory.
	buffer.maybeShrink()

	// Events have not been sent, buffer them and output them later.
	// Do not let outputBufferedResolvedEvents output resolved ts, instead we
	// output resolved ts here.
	sendResolvedTsHint := false
	ls.outputBufferedResolvedEvents(buffer, sendResolvedTsHint)
	lenResolvedEvents, _ = buffer.len()

	// Skip output resolved ts if there is any buffered resolved event.
	if lenResolvedEvents != 0 {
		return hasReadNext, 0, nil
	}

	if !hasNext && resolvedTs != 0 {
		// Iter is exhausted and there is no resolved event (up to max
		// resolved ts), output max resolved ts and return an exhausted
		// resolved ts.
		ls.outputResolvedTs(resolvedTs)
		return hasReadNext, resolvedTs, nil
	}
	if commitTs != 0 {
		// All buffered resolved events are outputted,
		// output last commit ts.
		ls.outputResolvedTs(commitTs)
	}

	return hasReadNext, 0, nil
}

type pollState struct {
	// Buffer for receiveing new events from AddEntry.
	eventsBuf []*model.PolymorphicEvent
	// Buffer for resolved events and to-be-deleted events.
	outputBuf *outputBuffer
	// The maximum commit ts for all events.
	maxCommitTs uint64
	// The maximum commit ts for all resolved ts events.
	maxResolvedTs uint64
	// All resolved events before the resolved ts are outputted.
	exhaustedResolvedTs uint64

	// Compactor actor ID.
	actorID actor.ID
	// A scheduler that triggers db compaction to speed up Iterator.First().
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
	uid uint32, tableID uint64,
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
		state.iterCh = make(chan *message.LimitedIterator, 1)
		return &message.IterRequest{
			Range: [2][]byte{
				encoding.EncodeTsKey(uid, tableID, 0),
				encoding.EncodeTsKey(uid, tableID, state.maxResolvedTs+1),
			},
			ResolvedTs: state.maxResolvedTs,
			IterCh:     state.iterCh,
		}, false
	}

	// Try receive iterator.
	select {
	case iter := <-state.iterCh:
		// Iterator received, reset state.iterCh
		state.iterCh = nil
		state.iter = iter
		start := time.Now()
		state.iterAliveTime = start
		state.iterResolvedTs = iter.ResolvedTs
		state.iterHasRead = false
		state.iter.First()
		duration := time.Since(start)
		state.metricIterFirst.Observe(duration.Seconds())
		if duration >= state.iterFirstSlowDuration {
			// Force trigger a compaction if Iterator.Fisrt is too slow.
			state.compact.maybeCompact(state.actorID, int(math.MaxInt32))
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

// poll receives new events and send resolved events asynchronously.
// TODO: Refactor into actor model, divide receive-send into two parts
//       to reduce complexity.
func (ls *Sorter) poll(ctx context.Context, state *pollState) error {
	// Wait input or output becomes available.
	waitOutput := state.hasResolvedEvents()
	// TODO: we should also wait state.iterCh, so that we can read and output
	//       resolved events ASAP.
	maxCommitTs, maxResolvedTs, n, err := ls.wait(ctx, waitOutput, state.eventsBuf)
	if err != nil {
		return errors.Trace(err)
	}
	// The max commit ts and resolved ts of all received events.
	state.advanceMaxTs(maxCommitTs, maxResolvedTs)
	// Length of buffered resolved events.
	lenResolvedEvents, _ := state.outputBuf.len()
	if n == 0 && lenResolvedEvents != 0 {
		// No new received events, it means output channel is available.
		// output resolved events as much as possible.
		ls.outputBufferedResolvedEvents(state.outputBuf, true)
		lenResolvedEvents, _ = state.outputBuf.len()
	}
	// New received events.
	newEvents := state.eventsBuf[:n]
	// Build task for new events and delete sent keys.
	task, err := ls.buildTask(newEvents, state.outputBuf.deleteKeys)
	if err != nil {
		return errors.Trace(err)
	}
	// Reset buffer as delete keys are scheduled.
	state.outputBuf.resetDeleteKey()
	// Try shrink buffer to release memory.
	state.outputBuf.maybeShrink()

	// It can only read an iterator when
	// 1. No buffered resolved events, they must be sent before
	//    sending further resolved events from iterator.
	readIter := lenResolvedEvents == 0
	// 2. There are some events that can be resolved.
	readIter = readIter && state.hasResolvedEvents()
	if !readIter {
		// No new events and no resolved events.
		if !state.hasResolvedEvents() && state.maxResolvedTs != 0 {
			ls.outputResolvedTs(state.maxResolvedTs)
		}
		// Release iterator as we does not need to read.
		err := state.tryReleaseIterator()
		if err != nil {
			return errors.Trace(err)
		}
		// Send write task to leveldb.
		return ls.router.SendB(ctx, ls.actorID, actormsg.SorterMessage(task))
	}

	var hasIter bool
	task.IterReq, hasIter = state.tryGetIterator(ls.uid, ls.tableID)
	// Send write/read task to leveldb.
	err = ls.router.SendB(ctx, ls.actorID, actormsg.SorterMessage(task))
	if err != nil {
		// Skip read iterator if send fails.
		return errors.Trace(err)
	}
	if !hasIter {
		// Skip read iterator if there is no iterator
		return nil
	}

	// Read and send resolved events from iterator.
	hasReadNext, exhaustedResolvedTs, err := ls.outputIterEvents(
		state.iter, state.iterHasRead, state.outputBuf, state.iterResolvedTs)
	if err != nil {
		return errors.Trace(err)
	}
	if exhaustedResolvedTs > state.exhaustedResolvedTs {
		state.exhaustedResolvedTs = exhaustedResolvedTs
	}
	state.iterHasRead = hasReadNext
	return state.tryReleaseIterator()
}

// Run runs Sorter
func (ls *Sorter) Run(ctx context.Context) error {
	state := &pollState{
		eventsBuf: make([]*model.PolymorphicEvent, batchReceiveEventSize),
		outputBuf: newOutputBuffer(batchReceiveEventSize),

		maxCommitTs:         uint64(0),
		maxResolvedTs:       uint64(0),
		exhaustedResolvedTs: uint64(0),

		actorID:               ls.actorID,
		compact:               ls.compact,
		iterFirstSlowDuration: ls.iterFirstSlowDuration,
		iterMaxAliveDuration:  ls.iterMaxAliveDuration,

		metricIterFirst:   ls.metricIterDuration.WithLabelValues("first"),
		metricIterRelease: ls.metricIterDuration.WithLabelValues("release"),
	}
	for {
		err := ls.poll(ctx, state)
		if err != nil {
			return errors.Trace(err)
		}
	}
}

// AddEntry adds an RawKVEntry to the EntryGroup
func (ls *Sorter) AddEntry(ctx context.Context, event *model.PolymorphicEvent) {
	if atomic.LoadInt32(&ls.closed) != 0 {
		return
	}
	select {
	case <-ctx.Done():
	case ls.inputCh <- event:
	}
}

// TryAddEntry tries to add an RawKVEntry to the EntryGroup
func (ls *Sorter) TryAddEntry(
	ctx context.Context, event *model.PolymorphicEvent,
) (bool, error) {
	if atomic.LoadInt32(&ls.closed) != 0 {
		return false, nil
	}
	select {
	case <-ctx.Done():
		return false, errors.Trace(ctx.Err())
	case ls.inputCh <- event:
		return true, nil
	default:
		return false, nil
	}
}

// Output returns the sorted raw kv output channel
func (ls *Sorter) Output() <-chan *model.PolymorphicEvent {
	return ls.outputCh
}

// CleanupTask returns a clean up task that delete sorter's data.
func (ls *Sorter) CleanupTask() actormsg.Message {
	return actormsg.SorterMessage(message.NewCleanupTask(ls.uid, ls.tableID))
}

// ActorID returns the actor ID the Sorter.
func (ls *Sorter) ActorID() actor.ID {
	return ls.actorID
}
