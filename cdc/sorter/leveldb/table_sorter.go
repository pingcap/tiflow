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
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sorter"
	"github.com/pingcap/ticdc/cdc/sorter/encoding"
	"github.com/pingcap/ticdc/cdc/sorter/leveldb/message"
	"github.com/pingcap/ticdc/pkg/actor"
	actormsg "github.com/pingcap/ticdc/pkg/actor/message"
	"github.com/pingcap/ticdc/pkg/db"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	// Capacity of leveldb sorter input and output channels.
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
	uid     uint32
	tableID uint64
	serde   *encoding.MsgPackGenSerde

	lastSentResolvedTs uint64
	lastEvent          *model.PolymorphicEvent

	inputCh  chan *model.PolymorphicEvent
	outputCh chan *model.PolymorphicEvent

	closed int32

	metricTotalEventsKV         prometheus.Counter
	metricTotalEventsResolvedTs prometheus.Counter
}

// NewLevelDBSorter creates a new LevelDBSorter
func NewLevelDBSorter(
	ctx context.Context, tableID int64, startTs uint64,
	router *actor.Router, actorID actor.ID,
) *Sorter {
	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	return &Sorter{
		actorID:            actorID,
		router:             router,
		uid:                allocID(),
		tableID:            uint64(tableID),
		lastSentResolvedTs: startTs,
		serde:              &encoding.MsgPackGenSerde{},

		inputCh:  make(chan *model.PolymorphicEvent, sorterInputCap),
		outputCh: make(chan *model.PolymorphicEvent, sorterOutputCap),

		metricTotalEventsKV:         sorter.EventCount.WithLabelValues(captureAddr, changefeedID, "kv"),
		metricTotalEventsResolvedTs: sorter.EventCount.WithLabelValues(captureAddr, changefeedID, "resolved"),
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
			log.Panic("commit ts < resolved ts",
				zap.Uint64("lastSentResolvedTs", ls.lastSentResolvedTs),
				zap.Any("event", ev), zap.Uint64("regionID", ev.RegionID()))
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

// asyncWrite writes events and delete keys asynchronously.
// It returns a channel to notify caller when write is done,
// if needSnap is true, caller receives a snapshot and reads all resolved
// events, up to the maxResolvedTs.
func (ls *Sorter) asyncWrite(
	ctx context.Context, events []*model.PolymorphicEvent, deleteKeys []message.Key, needSnap bool,
) (chan message.LimitedSnapshot, error) {
	// Write and sort events.
	tk := message.Task{
		UID:     ls.uid,
		TableID: ls.tableID,
		Events:  make(map[message.Key][]byte),
		SnapCh:  make(chan message.LimitedSnapshot, 1),
	}
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
			return nil, errors.Trace(err)
		}
		tk.Events[message.Key(key)] = value
	}

	// Delete keys of outputted resolved events.
	for i := range deleteKeys {
		tk.Events[deleteKeys[i]] = []byte{}
	}

	tk.NeedSnap = needSnap

	// Send write task to leveldb.
	err := ls.router.SendB(ctx, ls.actorID, actormsg.SorterMessage(tk))
	return tk.SnapCh, errors.Trace(err)
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
// later, and appends not-yet-send resolved events to outputBuffer resolvedEvents
// to send them later.
// outputBuffer must be empty.
func (ls *Sorter) outputIterEvents(
	iter db.Iterator, buffer *outputBuffer, maxResolvedTs uint64,
) (uint64, error) {
	lenResolvedEvents, lenDeleteKeys := buffer.len()
	if lenDeleteKeys > 0 || lenResolvedEvents > 0 {
		log.Panic("buffer is not empty",
			zap.Int("deleteKeys", lenDeleteKeys),
			zap.Int("resolvedEvents", lenResolvedEvents))
	}

	// The commit ts of buffered resolved events.
	lastCommitTs := uint64(0)
	iterHasNext := iter.Next()
	for ; iterHasNext; iterHasNext = iter.Next() {
		event := new(model.PolymorphicEvent)
		_, err := ls.serde.Unmarshal(event, iter.Value())
		if err != nil {
			return 0, errors.Trace(err)
		}
		if lastCommitTs > event.CRTs || lastCommitTs > maxResolvedTs {
			log.Panic("event commit ts is less than previous event or larger than resolved ts",
				zap.Any("event", event), zap.Stringer("key", message.Key(iter.Key())),
				zap.Uint64("ts", lastCommitTs), zap.Uint64("resolvedTs", maxResolvedTs))
		}

		if lastCommitTs == 0 {
			lastCommitTs = event.CRTs
		}
		// Group resolved events that has the same commit ts.
		if lastCommitTs == event.CRTs {
			buffer.appendResolvedEvent(event)
			continue
		}
		// Output buffered events. The current event belongs to a new group.
		ls.outputBufferedResolvedEvents(buffer, true)
		lenResolvedEvents, _ = buffer.len()
		if lenResolvedEvents > 0 {
			// Output blocked, break and free iterator.
			break
		}

		// Append new events to the buffer.
		lastCommitTs = event.CRTs
		buffer.appendResolvedEvent(event)
	}

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
		return 0, nil
	}

	if !iterHasNext && maxResolvedTs != 0 {
		// Iter is exhausted and there is no resolved event (up to max
		// resolved ts), output max resolved ts and return an exhausted
		// resolved ts.
		ls.outputResolvedTs(maxResolvedTs)
		return maxResolvedTs, nil
	}
	if lastCommitTs != 0 {
		// All buffered resolved events are outputted,
		// output last commit ts.
		ls.outputResolvedTs(lastCommitTs)
	}

	return 0, nil
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

func (ls *Sorter) poll(ctx context.Context, state *pollState) error {
	// Wait input or output becomes available.
	waitOutput := state.hasResolvedEvents()
	maxCommitTs, maxResolvedTs, n, err :=
		ls.wait(ctx, waitOutput, state.eventsBuf)
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

	// It can only acquire a snapshot when
	// 1. No buffered resolved events, they must be sent before
	//    sending further resolved events from snapshot.
	needSnap := lenResolvedEvents == 0
	// 2. There are some events that can be resolved.
	needSnap = needSnap && state.hasResolvedEvents()

	// Write new events and delete sent keys.
	snapCh, err :=
		ls.asyncWrite(ctx, newEvents, state.outputBuf.deleteKeys, needSnap)
	if err != nil {
		return errors.Trace(err)
	}
	// Reset buffer as delete keys are scheduled.
	state.outputBuf.resetDeleteKey()
	// Try shrink buffer to release memory.
	state.outputBuf.maybeShrink()

	if !needSnap {
		// No new events and no resolved events.
		if !state.hasResolvedEvents() && state.maxResolvedTs != 0 {
			ls.outputResolvedTs(state.maxResolvedTs)
		}
		return nil
	}

	// Wait for writing and deleting.
	var snap message.LimitedSnapshot
	var ok bool
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case snap, ok = <-snapCh:
	}
	if !ok && needSnap {
		return cerrors.ErrUnexpectedSnapshot.GenWithStackByArgs(ls.tableID)
	}
	iter := snap.Iterator(
		encoding.EncodeTsKey(ls.uid, ls.tableID, 0),
		encoding.EncodeTsKey(ls.uid, ls.tableID, state.maxResolvedTs+1),
	)
	defer func() {
		// Release iter and snap should be fast, otherwise it may block
		// the funcation.
		if err := iter.Release(); err != nil {
			log.Error("iterator release error",
				zap.Error(err), zap.Uint64("tableID", ls.tableID))
		}
		if err := snap.Release(); err != nil {
			log.Error("snapshot release error",
				zap.Error(err), zap.Uint64("tableID", ls.tableID))
		}
	}()

	// Read and send resolved events from iterator.
	exhaustedResolvedTs, err :=
		ls.outputIterEvents(iter, state.outputBuf, state.maxResolvedTs)
	if err != nil {
		return errors.Trace(err)
	}
	if exhaustedResolvedTs > state.exhaustedResolvedTs {
		state.exhaustedResolvedTs = exhaustedResolvedTs
	}

	return nil
}

// Run runs LevelDBSorter
func (ls *Sorter) Run(ctx context.Context) error {
	state := &pollState{
		eventsBuf: make([]*model.PolymorphicEvent, batchReceiveEventSize),
		outputBuf: newOutputBuffer(batchReceiveEventSize),

		maxCommitTs:         uint64(0),
		maxResolvedTs:       uint64(0),
		exhaustedResolvedTs: uint64(0),
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
