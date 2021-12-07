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
	"encoding/hex"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sorter/encoding"
	"github.com/pingcap/ticdc/cdc/sorter/leveldb/message"
	"github.com/pingcap/ticdc/pkg/actor"
	actormsg "github.com/pingcap/ticdc/pkg/actor/message"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/db"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

func newTestLeveldbSorter(
	ctx context.Context, capacity int,
) (*Sorter, actor.Mailbox) {
	id := actor.ID(1)
	router := actor.NewRouter("teet")
	mb := actor.NewMailbox(1, capacity)
	router.InsertMailbox4Test(id, mb)
	ls := NewLevelDBSorter(ctx, 1, 1, router, id)
	return ls, mb
}

func TestWaitInput(t *testing.T) {
	t.Parallel()
	// Make sure input capacity is larger than batch size in order to test
	// batch behavior.
	require.Greater(t, sorterInputCap, batchReceiveEventSize)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	capacity := 8
	require.Greater(t, batchReceiveEventSize, capacity)
	ls, _ := newTestLeveldbSorter(ctx, capacity)
	// Nonbuffered channel is unavailable during the test.
	ls.outputCh = make(chan *model.PolymorphicEvent)

	expectedEvents := make([]*model.PolymorphicEvent, batchReceiveEventSize)
	for i := range expectedEvents {
		expectedEvents[i] = model.NewPolymorphicEvent(
			&model.RawKVEntry{CRTs: ls.lastSentResolvedTs, RegionID: uint64(i)})
	}

	eventsBuf := make([]*model.PolymorphicEvent, batchReceiveEventSize)

	// Test message count <= batchReceiveEventSize.
	for i := 1; i <= batchReceiveEventSize; i++ {
		for j := 0; j < i; j++ {
			ls.inputCh <- model.NewPolymorphicEvent(
				&model.RawKVEntry{CRTs: ls.lastSentResolvedTs, RegionID: uint64(j)})
		}
		cts, rts, n, err := ls.wait(ctx, false, eventsBuf)
		require.Nil(t, err)
		require.Equal(t, i, n)
		require.EqualValues(t, 0, rts)
		require.EqualValues(t, ls.lastSentResolvedTs, cts)
		require.EqualValues(t, expectedEvents[:n], eventsBuf[:n])
	}

	// Test message count > batchReceiveEventSize
	for i := batchReceiveEventSize + 1; i <= sorterInputCap; i++ {
		expectedEvents1 := make([]*model.PolymorphicEvent, i)
		for j := 0; j < i; j++ {
			ls.inputCh <- model.NewPolymorphicEvent(
				&model.RawKVEntry{CRTs: ls.lastSentResolvedTs, RegionID: uint64(j)})
			expectedEvents1[j] = model.NewPolymorphicEvent(
				&model.RawKVEntry{CRTs: ls.lastSentResolvedTs, RegionID: uint64(j)})
		}

		quotient, remainder := i/batchReceiveEventSize, i%batchReceiveEventSize
		for q := 0; q < quotient; q++ {
			cts, rts, n, err := ls.wait(ctx, false, eventsBuf)
			require.Nil(t, err)
			require.Equal(t, batchReceiveEventSize, n)
			require.EqualValues(t, 0, rts)
			require.EqualValues(t, ls.lastSentResolvedTs, cts)
			start, end := q*batchReceiveEventSize, q*batchReceiveEventSize+n
			require.EqualValues(t, expectedEvents1[start:end], eventsBuf[:n],
				"%d, %d, %d, %d", i, quotient, remainder, n)
		}
		if remainder != 0 {
			cts, rts, n, err := ls.wait(ctx, false, eventsBuf)
			require.Nil(t, err)
			require.Equal(t, remainder, n)
			require.EqualValues(t, 0, rts)
			require.EqualValues(t, ls.lastSentResolvedTs, cts)
			start, end := quotient*batchReceiveEventSize, quotient*batchReceiveEventSize+n
			require.EqualValues(t, expectedEvents1[start:end], eventsBuf[:n],
				"%d, %d, %d, %d", i, quotient, remainder, n)
		}
	}

	// Test returned max resolved ts of new resolvedts events.
	// Send batchReceiveEventSize/3 resolved events
	for i := 1; i <= batchReceiveEventSize/3; i++ {
		ls.inputCh <- model.NewResolvedPolymorphicEvent(0, uint64(i))
	}
	// Send batchReceiveEventSize/3 events
	for i := 0; i < batchReceiveEventSize/3; i++ {
		ls.inputCh <- model.NewPolymorphicEvent(
			&model.RawKVEntry{CRTs: ls.lastSentResolvedTs, RegionID: uint64(i)})
	}
	_, rts, n, err := ls.wait(ctx, false, eventsBuf)
	require.Nil(t, err)
	require.EqualValues(t, batchReceiveEventSize/3, n)
	require.EqualValues(t, batchReceiveEventSize/3, rts)
	require.EqualValues(t, expectedEvents[:n], eventsBuf[:n])

	// Test returned max commit ts of new events
	// Send batchReceiveEventSize/2 events
	for i := 1; i <= batchReceiveEventSize/2; i++ {
		ls.inputCh <- model.NewPolymorphicEvent(
			&model.RawKVEntry{CRTs: uint64(i), RegionID: uint64(i)})
	}
	cts, rts, n, err := ls.wait(ctx, false, eventsBuf)
	require.Nil(t, err)
	require.EqualValues(t, batchReceiveEventSize/2, n)
	require.EqualValues(t, batchReceiveEventSize/2, cts)
	require.EqualValues(t, 0, rts)

	// Test input block on empty message.
	dctx, dcancel := context.WithDeadline(ctx, time.Now().Add(100*time.Millisecond))
	defer dcancel()
	cts, rts, n, err = ls.wait(dctx, false, eventsBuf)
	require.Regexp(t, err, "context deadline exceeded")
	require.Equal(t, 0, n)
	require.EqualValues(t, 0, cts)
	require.EqualValues(t, 0, rts)
}

func TestWaitOutput(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	capacity := 4
	require.Greater(t, batchReceiveEventSize, capacity)
	ls, _ := newTestLeveldbSorter(ctx, capacity)

	eventsBuf := make([]*model.PolymorphicEvent, batchReceiveEventSize)

	waitOutput := true
	// It sends a dummy event if there is no buffered event.
	cts, rts, n, err := ls.wait(ctx, waitOutput, eventsBuf)
	require.Nil(t, err)
	require.EqualValues(t, 0, n)
	require.EqualValues(t, 0, cts)
	require.EqualValues(t, 0, rts)
	require.EqualValues(t,
		model.NewResolvedPolymorphicEvent(0, 0), <-ls.outputCh)

	// Test wait block when output channel is unavailable.
	ls.outputCh = make(chan *model.PolymorphicEvent)
	dctx, dcancel := context.WithDeadline(ctx, time.Now().Add(100*time.Millisecond))
	defer dcancel()
	cts, rts, n, err = ls.wait(dctx, waitOutput, eventsBuf)
	require.Regexp(t, err, "context deadline exceeded")
	require.Equal(t, 0, n)
	require.EqualValues(t, 0, cts)
	require.EqualValues(t, 0, rts)
}

func TestAsyncWrite(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	capacity := 4
	require.Greater(t, batchReceiveEventSize, capacity)
	ls, mb := newTestLeveldbSorter(ctx, capacity)

	cases := []struct {
		events     []*model.PolymorphicEvent
		deleteKeys []message.Key
		needSnap   bool
	}{
		// Empty write and delete.
		{
			events:     []*model.PolymorphicEvent{},
			deleteKeys: []message.Key{},
			needSnap:   false,
		},
		// Write one event
		{
			events: []*model.PolymorphicEvent{
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 1}),
			},
			deleteKeys: []message.Key{},
			needSnap:   false,
		},
		// Write one event and delete one key.
		{
			events: []*model.PolymorphicEvent{
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 1}),
			},
			deleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID,
					model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 2}))),
			},
			needSnap: false,
		},
		// Write one event and delete one key and need iter.
		{
			events: []*model.PolymorphicEvent{
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 1}),
			},
			deleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID,
					model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 2}))),
			},
			needSnap: true,
		},
		// Write one event and a resolved ts event.
		{
			events: []*model.PolymorphicEvent{
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 1}),
				model.NewResolvedPolymorphicEvent(0, 3),
			},
			deleteKeys: []message.Key{},
			needSnap:   false,
		},
		// Write two events and a resolved ts event, delete one key.
		{
			events: []*model.PolymorphicEvent{
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 4}),
				model.NewResolvedPolymorphicEvent(0, 5),
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 6}),
			},
			deleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID,
					model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 1}))),
			},
			needSnap: false,
		},
	}
	for i, cs := range cases {
		events, deleteKeys, needSnap := cs.events, cs.deleteKeys, cs.needSnap
		ch, err := ls.asyncWrite(ctx, events, deleteKeys, needSnap)
		require.Nil(t, err, "case #%d, %v", i, cs)
		require.NotNil(t, ch, "case #%d, %v", i, cs)
		task, ok := mb.Receive()
		require.True(t, ok, "case #%d, %v", i, cs)
		require.EqualValues(t, actormsg.TypeSorterTask, task.Tp)

		expectedEvents := make(map[message.Key][]uint8)
		for _, ev := range events {
			if ev.RawKV.OpType == model.OpTypeResolved {
				// Leveldb sorter does not write resolved ts events.
				continue
			}
			value, err := ls.serde.Marshal(ev, []byte{})
			require.Nil(t, err, "case #%d, %v", i, cs)
			key := message.Key(encoding.EncodeKey(ls.uid, ls.tableID, ev))
			expectedEvents[key] = value
		}
		for _, key := range deleteKeys {
			expectedEvents[key] = []byte{}
		}
		require.EqualValues(t, message.Task{
			UID:                ls.uid,
			TableID:            ls.tableID,
			Events:             expectedEvents,
			SnapCh:             ch,
			NeedSnap:           needSnap,
			Cleanup:            false,
			CleanupRatelimited: false,
		}, task.SorterTask, "case #%d, %v", i, cs)
	}
}

func TestOutput(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	capacity := 4
	ls, _ := newTestLeveldbSorter(ctx, capacity)

	ls.outputCh = make(chan *model.PolymorphicEvent, 1)
	ok := ls.output(&model.PolymorphicEvent{CRTs: 1})
	require.True(t, ok)
	require.EqualValues(t, &model.PolymorphicEvent{CRTs: 1}, ls.lastEvent)
	ok = ls.output(&model.PolymorphicEvent{CRTs: 1})
	require.False(t, ok)
	ls.outputResolvedTs(2)
	require.EqualValues(t, 1, ls.lastSentResolvedTs)

	<-ls.outputCh
	ls.outputResolvedTs(2)
	require.EqualValues(t, 2, ls.lastSentResolvedTs)

	<-ls.outputCh
	ok = ls.output(&model.PolymorphicEvent{CRTs: 3})
	require.True(t, ok)
}

func TestOutputBufferedResolvedEvents(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	capacity := 4
	ls, _ := newTestLeveldbSorter(ctx, capacity)

	buf := newOutputBuffer(capacity)

	cases := []struct {
		outputChCap             int
		inputEvents             []*model.PolymorphicEvent
		inputDeleteKeys         []message.Key
		inputSendResolvedTsHint bool

		expectEvents     []*model.PolymorphicEvent
		expectDeleteKeys []message.Key
		expectOutputs    []*model.PolymorphicEvent
	}{
		// Empty buffer.
		{
			outputChCap:             1,
			inputEvents:             []*model.PolymorphicEvent{},
			inputDeleteKeys:         []message.Key{},
			inputSendResolvedTsHint: true,

			expectEvents:     []*model.PolymorphicEvent{},
			expectDeleteKeys: []message.Key{},
			expectOutputs:    []*model.PolymorphicEvent{},
		},
		// Output one event, delete one event.
		{
			outputChCap: 2,
			inputEvents: []*model.PolymorphicEvent{
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 1}),
			},
			inputDeleteKeys:         []message.Key{},
			inputSendResolvedTsHint: true,

			expectEvents: []*model.PolymorphicEvent{},
			expectDeleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID,
					model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 1}))),
			},
			expectOutputs: []*model.PolymorphicEvent{
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 1}),
				// All inputEvent are sent, it also outputs a resolved ts event.
				model.NewResolvedPolymorphicEvent(0, 1),
			},
		},
		// Delete one event.
		{
			outputChCap: 2,
			inputEvents: []*model.PolymorphicEvent{},
			inputDeleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID,
					model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 1}))),
			},
			inputSendResolvedTsHint: true,

			expectEvents: []*model.PolymorphicEvent{},
			expectDeleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID,
					model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 1}))),
			},
			expectOutputs: []*model.PolymorphicEvent{},
		},
		// Output one event, delete two event.
		{
			outputChCap: 2,
			inputEvents: []*model.PolymorphicEvent{
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 2}),
			},
			inputDeleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID,
					model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 1}))),
			},
			inputSendResolvedTsHint: true,

			expectEvents: []*model.PolymorphicEvent{},
			expectDeleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID,
					model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 1}))),
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID,
					model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 2}))),
			},
			expectOutputs: []*model.PolymorphicEvent{
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 2}),
				// All inputEvent are sent, it also outputs a resolved ts event.
				model.NewResolvedPolymorphicEvent(0, 2),
			},
		},
		// Output two events, left one event.
		{
			outputChCap: 2,
			inputEvents: []*model.PolymorphicEvent{
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 3, RegionID: 1}),
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 3, RegionID: 2}),
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 3, RegionID: 3}),
			},
			inputDeleteKeys:         []message.Key{},
			inputSendResolvedTsHint: true,

			expectEvents: []*model.PolymorphicEvent{
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 3, RegionID: 3}),
			},
			expectDeleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID,
					model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 3, RegionID: 1}))),
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID,
					model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 3, RegionID: 2}))),
			},
			expectOutputs: []*model.PolymorphicEvent{
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 3, RegionID: 1}),
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 3, RegionID: 2}),
				// No resolved ts event because not all events are sent.
			},
		},
		// Output zero event, left two events.
		{
			outputChCap: 0,
			inputEvents: []*model.PolymorphicEvent{
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 4, RegionID: 1}),
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 4, RegionID: 2}),
			},
			inputDeleteKeys:         []message.Key{},
			inputSendResolvedTsHint: true,

			expectEvents: []*model.PolymorphicEvent{
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 4, RegionID: 1}),
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 4, RegionID: 2}),
			},
			expectDeleteKeys: []message.Key{},
			expectOutputs:    []*model.PolymorphicEvent{},
		},
	}

	for i, cs := range cases {
		ls.outputCh = make(chan *model.PolymorphicEvent, cs.outputChCap)
		buf.resolvedEvents = append([]*model.PolymorphicEvent{}, cs.inputEvents...)
		buf.deleteKeys = append([]message.Key{}, cs.inputDeleteKeys...)

		ls.outputBufferedResolvedEvents(buf, cs.inputSendResolvedTsHint)
		require.EqualValues(t, cs.expectDeleteKeys, buf.deleteKeys, "case #%d, %v", i, cs)
		require.EqualValues(t, cs.expectEvents, buf.resolvedEvents, "case #%d, %v", i, cs)

		outputEvents := []*model.PolymorphicEvent{}
	RECV:
		for {
			select {
			case ev := <-ls.outputCh:
				outputEvents = append(outputEvents, ev)
			default:
				break RECV
			}
		}
		require.EqualValues(t, cs.expectOutputs, outputEvents, "case #%d, %v", i, cs)
	}
}

func newTestEvent(crts, startTs uint64, key int) *model.PolymorphicEvent {
	return model.NewPolymorphicEvent(&model.RawKVEntry{
		OpType:  model.OpTypePut,
		Key:     []byte{byte(key)},
		StartTs: startTs,
		CRTs:    crts,
	})
}

func prepareTxnData(
	t *testing.T, ls *Sorter, txnCount, txnSize int,
) db.DB {
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	db, err := db.OpenLevelDB(context.Background(), 1, t.TempDir(), cfg)
	require.Nil(t, err)
	wb := db.Batch(0)
	for i := 1; i < txnCount+1; i++ { // txns.
		for j := 0; j < txnSize; j++ { // events.
			event := newTestEvent(uint64(i)+1, uint64(i), j)
			key := encoding.EncodeKey(ls.uid, ls.tableID, event)
			value, err := ls.serde.Marshal(event, []byte{})
			require.Nil(t, err)
			t.Logf("key: %s, value: %s\n", message.Key(key), hex.EncodeToString(value))
			wb.Put(key, value)
		}
	}
	require.Nil(t, wb.Commit())
	return db
}

func receiveOutputEvents(
	outputCh chan *model.PolymorphicEvent,
) []*model.PolymorphicEvent {
	outputEvents := []*model.PolymorphicEvent{}
RECV:
	for {
		select {
		case ev := <-outputCh:
			outputEvents = append(outputEvents, ev)
		default:
			break RECV
		}
	}
	return outputEvents
}

func TestOutputIterEvents(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	capacity := 4
	ls, _ := newTestLeveldbSorter(ctx, capacity)

	// Prepare data, 3 txns, 3 events for each.
	// CRTs 2, StartTs 1, keys (0|1|2)
	// CRTs 3, StartTs 2, keys (0|1|2)
	// CRTs 4, StartTs 3, keys (0|1|2)
	// CRTs 5, StartTs 4, keys (0|1|2)
	// CRTs 6, StartTs 4, keys (0|1|2)
	db := prepareTxnData(t, ls, 5, 3)
	_ = db

	cases := []struct {
		outputChCap   int
		maxResolvedTs uint64

		expectEvents       []*model.PolymorphicEvent
		expectDeleteKeys   []message.Key
		expectOutputs      []*model.PolymorphicEvent
		expectExhaustedRTs uint64
	}{
		// Empty resolved event.
		{
			outputChCap:   1,
			maxResolvedTs: 0,

			expectEvents:       []*model.PolymorphicEvent{},
			expectDeleteKeys:   []message.Key{},
			expectOutputs:      []*model.PolymorphicEvent{},
			expectExhaustedRTs: 0,
		},
		// Nonblocking output three events and one resolved ts.
		{
			outputChCap:   4,
			maxResolvedTs: 2, // CRTs 2 has 3 events.

			expectEvents: []*model.PolymorphicEvent{},
			expectDeleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID, newTestEvent(2, 1, 0))),
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID, newTestEvent(2, 1, 1))),
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID, newTestEvent(2, 1, 2))),
			},
			expectOutputs: []*model.PolymorphicEvent{
				newTestEvent(2, 1, 0),
				newTestEvent(2, 1, 1),
				newTestEvent(2, 1, 2),
				// No buffered resolved events, it outputs a resolved ts event.
				model.NewResolvedPolymorphicEvent(0, 2),
			},
			expectExhaustedRTs: 2, // Iter is exhausted and no buffered resolved events.
		},
		// Blocking output two events of CRTs 3.
		{
			outputChCap:   2,
			maxResolvedTs: 3, // CRTs 3 has 3 events.

			expectEvents: []*model.PolymorphicEvent{newTestEvent(3, 2, 2)},
			expectDeleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID, newTestEvent(3, 2, 0))),
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID, newTestEvent(3, 2, 1))),
			},
			expectOutputs: []*model.PolymorphicEvent{
				newTestEvent(3, 2, 0),
				newTestEvent(3, 2, 1),
			},
			expectExhaustedRTs: 0, // There are buffered resolved events.
		},
		// Output remaining event of CRTs 3.
		{
			outputChCap:   3,
			maxResolvedTs: 3, // CRTs 3 has 1 events.

			expectEvents: []*model.PolymorphicEvent{},
			expectDeleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID, newTestEvent(3, 2, 2))),
			},
			expectOutputs: []*model.PolymorphicEvent{
				newTestEvent(3, 2, 2),
				model.NewResolvedPolymorphicEvent(0, 3),
			},
			expectExhaustedRTs: 3, // Iter is exhausted and no buffered resolved events.
		},
		// Resolved ts covers all resolved events,
		// blocking output events of CRTs 4 (3 events) and 5 (1 event).
		{
			outputChCap:   5,
			maxResolvedTs: 7,

			expectEvents: []*model.PolymorphicEvent{
				newTestEvent(5, 4, 1),
				newTestEvent(5, 4, 2),
			},
			expectDeleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID, newTestEvent(4, 3, 0))),
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID, newTestEvent(4, 3, 1))),
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID, newTestEvent(4, 3, 2))),
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID, newTestEvent(5, 4, 0))),
			},
			expectOutputs: []*model.PolymorphicEvent{
				newTestEvent(4, 3, 0),
				newTestEvent(4, 3, 1),
				newTestEvent(4, 3, 2),
				model.NewResolvedPolymorphicEvent(0, 4),
				newTestEvent(5, 4, 0),
			},
			expectExhaustedRTs: 0, // Iter is not exhausted.
		},

		// Resolved ts covers all resolved events,
		// nonblocking output all events.
		{
			outputChCap:   7,
			maxResolvedTs: 7,

			expectEvents: []*model.PolymorphicEvent{},
			expectDeleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID, newTestEvent(5, 4, 1))),
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID, newTestEvent(5, 4, 2))),
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID, newTestEvent(6, 5, 0))),
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID, newTestEvent(6, 5, 1))),
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID, newTestEvent(6, 5, 2))),
			},
			expectOutputs: []*model.PolymorphicEvent{
				newTestEvent(5, 4, 1),
				newTestEvent(5, 4, 2),
				model.NewResolvedPolymorphicEvent(0, 5),
				newTestEvent(6, 5, 0),
				newTestEvent(6, 5, 1),
				newTestEvent(6, 5, 2),
				model.NewResolvedPolymorphicEvent(0, 7),
			},
			expectExhaustedRTs: 7, // Iter is exhausted and no buffered resolved events.
		},
	}

	for i, cs := range cases {
		ls.outputCh = make(chan *model.PolymorphicEvent, cs.outputChCap)
		buf := newOutputBuffer(capacity)

		snap, err := db.Snapshot()
		require.Nil(t, err, "case #%d, %v", i, cs)
		iter := snap.Iterator(
			encoding.EncodeTsKey(ls.uid, ls.tableID, 0),
			encoding.EncodeTsKey(ls.uid, ls.tableID, cs.maxResolvedTs+1))
		exhaustedRTs, err := ls.outputIterEvents(iter, buf, cs.maxResolvedTs)
		require.Nil(t, err, "case #%d, %v", i, cs)
		require.EqualValues(t, cs.expectExhaustedRTs, exhaustedRTs, "case #%d, %v", i, cs)
		require.EqualValues(t, cs.expectDeleteKeys, buf.deleteKeys, "case #%d, %v", i, cs)
		require.EqualValues(t, cs.expectEvents, buf.resolvedEvents, "case #%d, %v", i, cs)
		outputEvents := receiveOutputEvents(ls.outputCh)
		require.EqualValues(t, cs.expectOutputs, outputEvents, "case #%d, %v", i, cs)

		wb := db.Batch(0)
		for _, key := range cs.expectDeleteKeys {
			wb.Delete([]byte(key))
		}
		require.Nil(t, wb.Commit())
	}

	require.Nil(t, db.Close())
}

func TestPoll(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	capacity := 4
	ls, mb := newTestLeveldbSorter(ctx, capacity)

	// Prepare data, 3 txns, 3 events for each.
	// CRTs 2, StartTs 1, keys (0|1|2)
	// CRTs 3, StartTs 2, keys (0|1|2)
	// CRTs 4, StartTs 3, keys (0|1|2)
	// CRTs 5, StartTs 4, keys (0|1|2)
	// CRTs 6, StartTs 4, keys (0|1|2)
	db := prepareTxnData(t, ls, 5, 3)
	sema := semaphore.NewWeighted(1)

	cases := []struct {
		inputEvents []*model.PolymorphicEvent
		inputSnap   func() *message.LimitedSnapshot
		state       *pollState

		expectEvents        []*model.PolymorphicEvent
		expectDeleteKeys    []message.Key
		expectOutputs       []*model.PolymorphicEvent
		expectMaxCommitTs   uint64
		expectMaxResolvedTs uint64
		expectExhaustedRTs  uint64
	}{
		{
			inputEvents: []*model.PolymorphicEvent{
				model.NewResolvedPolymorphicEvent(0, 1),
			},
			state: &pollState{
				eventsBuf: make([]*model.PolymorphicEvent, 1),
				outputBuf: newOutputBuffer(1),
			},
			inputSnap: func() *message.LimitedSnapshot { return nil },

			expectEvents:     []*model.PolymorphicEvent{},
			expectDeleteKeys: []message.Key{},
			// It is initialized to 1 in the test.
			expectOutputs:       []*model.PolymorphicEvent{model.NewResolvedPolymorphicEvent(0, 1)},
			expectMaxCommitTs:   0,
			expectMaxResolvedTs: 1,
			expectExhaustedRTs:  0,
		},
		// maxCommitTs and maxResolvedTs must advance according to inputs.
		// And exhaustedResolvedTs must advance if there is no resolved event.
		{
			inputEvents: []*model.PolymorphicEvent{
				newTestEvent(3, 2, 1), // crts 3, startts 2
				model.NewResolvedPolymorphicEvent(0, 2),
			},
			state: &pollState{
				eventsBuf: make([]*model.PolymorphicEvent, 2),
				outputBuf: newOutputBuffer(1),
			},
			// An empty snapshot.
			inputSnap: newEmptySnapshot(ctx, t, sema),

			expectEvents:     []*model.PolymorphicEvent{},
			expectDeleteKeys: []message.Key{},
			expectOutputs: []*model.PolymorphicEvent{
				model.NewResolvedPolymorphicEvent(0, 2),
			},
			expectMaxCommitTs:   3,
			expectMaxResolvedTs: 2,
			expectExhaustedRTs:  2,
		},
		// exhaustedResolvedTs must advance if all resolved events are outputed.
		// Output: CRTs 2, StartTs 1, keys (0|1|2)
		{
			inputEvents: []*model.PolymorphicEvent{
				newTestEvent(3, 2, 1), // crts 3, startts 2
				model.NewResolvedPolymorphicEvent(0, 2),
			},
			state: &pollState{
				eventsBuf: make([]*model.PolymorphicEvent, 2),
				outputBuf: newOutputBuffer(1),
			},
			inputSnap: newSnapshot(ctx, t, db, sema),

			expectEvents: []*model.PolymorphicEvent{},
			expectDeleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID, newTestEvent(2, 1, 0))),
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID, newTestEvent(2, 1, 1))),
				message.Key(encoding.EncodeKey(ls.uid, ls.tableID, newTestEvent(2, 1, 2))),
			},
			expectOutputs: []*model.PolymorphicEvent{
				newTestEvent(2, 1, 0),
				newTestEvent(2, 1, 1),
				newTestEvent(2, 1, 2),
				model.NewResolvedPolymorphicEvent(0, 2),
			},
			expectMaxCommitTs:   3,
			expectMaxResolvedTs: 2,
			expectExhaustedRTs:  2,
		},
		// maxResolvedTs must advance even if there is only resolved ts event.
		{
			inputEvents: []*model.PolymorphicEvent{
				model.NewResolvedPolymorphicEvent(0, 3),
			},
			state: &pollState{
				eventsBuf:           make([]*model.PolymorphicEvent, 2),
				outputBuf:           newOutputBuffer(1),
				maxCommitTs:         2,
				exhaustedResolvedTs: 2,
			},
			inputSnap: func() *message.LimitedSnapshot { return nil },

			expectEvents:     []*model.PolymorphicEvent{},
			expectDeleteKeys: []message.Key{},
			expectOutputs: []*model.PolymorphicEvent{
				model.NewResolvedPolymorphicEvent(0, 3),
			},
			expectMaxCommitTs:   2,
			expectMaxResolvedTs: 3,
			expectExhaustedRTs:  2,
		},
		// Batch output buffered resolved events
		{
			inputEvents: []*model.PolymorphicEvent{},
			state: &pollState{
				eventsBuf: make([]*model.PolymorphicEvent, 2),
				outputBuf: &outputBuffer{
					deleteKeys: make([]message.Key, 0, 2),
					resolvedEvents: []*model.PolymorphicEvent{
						model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 4}),
						model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 4}),
						model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 4}),
					},
					advisedCapacity: 2,
				},
			},
			inputSnap: func() *message.LimitedSnapshot { return nil },

			expectEvents:     []*model.PolymorphicEvent{},
			expectDeleteKeys: []message.Key{},
			expectOutputs: []*model.PolymorphicEvent{
				model.NewResolvedPolymorphicEvent(0, 0), // A dummy events.
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 4}),
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 4}),
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 4}),
				model.NewResolvedPolymorphicEvent(0, 4),
			},
			expectMaxCommitTs:   0,
			expectMaxResolvedTs: 0,
			expectExhaustedRTs:  0,
		},
	}

	for i, cs := range cases {
		_, state := i, cs.state
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { handleTask(mb, cs.inputSnap(), &wg) }()
		for i := range cs.inputEvents {
			ls.AddEntry(ctx, cs.inputEvents[i])
		}
		t.Logf("test case #%d, %v", i, cs)
		err := ls.poll(ctx, state)
		require.Nil(t, err)
		require.EqualValues(t, cs.expectEvents, state.outputBuf.resolvedEvents, "case #%d, %v", i, cs)
		require.EqualValues(t, cs.expectDeleteKeys, state.outputBuf.deleteKeys, "case #%d, %v", i, cs)
		require.EqualValues(t, cs.expectMaxCommitTs, state.maxCommitTs, "case #%d, %v", i, cs)
		require.EqualValues(t, cs.expectMaxResolvedTs, state.maxResolvedTs, "case #%d, %v", i, cs)
		require.EqualValues(t, cs.expectExhaustedRTs, state.exhaustedResolvedTs, "case #%d, %v", i, cs)
		outputEvents := receiveOutputEvents(ls.outputCh)
		require.EqualValues(t, cs.expectOutputs, outputEvents, "case #%d, %v", i, cs)
		wg.Wait()
	}

	require.Nil(t, db.Close())
}

func handleTask(mb actor.Mailbox, iter *message.LimitedSnapshot, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		time.Sleep(20 * time.Millisecond)
		task, ok := mb.Receive()
		if !ok {
			continue
		}
		if iter != nil {
			task.SorterTask.SnapCh <- *iter
		}
		close(task.SorterTask.SnapCh)
		return
	}
}

func newSnapshot(
	ctx context.Context, t *testing.T, db db.DB,
	sema *semaphore.Weighted,
) func() *message.LimitedSnapshot {
	return func() *message.LimitedSnapshot {
		require.Nil(t, sema.Acquire(ctx, 1))
		snap, err := db.Snapshot()
		require.Nil(t, err)
		return &message.LimitedSnapshot{
			Snapshot: snap,
			Sema:     sema,
		}
	}
}

type emptySnapshot struct {
	db.Snapshot
}

func (e emptySnapshot) Iterator(_, _ []byte) db.Iterator { return emptyIterator{} }
func (e emptySnapshot) Release() error                   { return nil }

type emptyIterator struct {
	db.Iterator
}

func (emptyIterator) Valid() bool      { return false }
func (emptyIterator) First() bool      { return false }
func (emptyIterator) Seek([]byte) bool { return false }
func (emptyIterator) Next() bool       { return false }
func (emptyIterator) Key() []byte      { return nil }
func (emptyIterator) Value() []byte    { return nil }
func (emptyIterator) Error() error     { return nil }
func (emptyIterator) Release() error   { return nil }

func newEmptySnapshot(
	ctx context.Context, t *testing.T, sema *semaphore.Weighted,
) func() *message.LimitedSnapshot {
	return func() *message.LimitedSnapshot {
		require.Nil(t, sema.Acquire(ctx, 1))
		return &message.LimitedSnapshot{
			Snapshot: emptySnapshot{},
			Sema:     sema,
		}
	}
}

func TestTryAddEntry(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	capacity := 1
	ls, _ := newTestLeveldbSorter(ctx, capacity)

	resolvedTs1 := model.NewResolvedPolymorphicEvent(0, 1)
	sent, err := ls.TryAddEntry(ctx, resolvedTs1)
	require.True(t, sent)
	require.Nil(t, err)
	require.EqualValues(t, resolvedTs1, <-ls.inputCh)

	ls.inputCh = make(chan *model.PolymorphicEvent)
	sent, err = ls.TryAddEntry(ctx, resolvedTs1)
	require.False(t, sent)
	require.Nil(t, err)
}
