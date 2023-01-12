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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sorter"
	"github.com/pingcap/tiflow/cdc/sorter/encoding"
	"github.com/pingcap/tiflow/cdc/sorter/leveldb/message"
	"github.com/pingcap/tiflow/pkg/actor"
	actormsg "github.com/pingcap/tiflow/pkg/actor/message"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/db"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

func newTestReader() *reader {
	metricIterDuration := sorterIterReadDurationHistogram.MustCurryWith(
		prometheus.Labels{
			"namespace": "default",
			"id":        "test",
		})

	metricOutputKV := sorter.OutputEventCount.
		WithLabelValues("default", "test", "kv")
	metricOutputResolved := sorter.InputEventCount.
		WithLabelValues("default", "test", "resolved")
	return &reader{
		common: common{
			dbActorID: 1,
			// dbRouter:  dbRouter,
			uid:     2,
			tableID: uint64(3),
			serde:   &encoding.MsgPackGenSerde{},
		},
		state: pollState{
			metricIterFirst:   metricIterDuration.WithLabelValues("first"),
			metricIterRelease: metricIterDuration.WithLabelValues("release"),
		},
		metricIterReadDuration:    metricIterDuration.WithLabelValues("read"),
		metricIterNextDuration:    metricIterDuration.WithLabelValues("next"),
		metricTotalEventsKV:       metricOutputKV,
		metricTotalEventsResolved: metricOutputResolved,
	}
}

func TestReaderSetTaskDelete(t *testing.T) {
	t.Parallel()

	r := newTestReader()
	r.delete = deleteThrottle{
		countThreshold: 2,
		period:         2 * time.Second,
	}

	cases := []struct {
		deleteKeys   []message.Key
		sleep        time.Duration
		expectDelete *message.DeleteRequest
	}{
		// Empty delete does not set delete.
		{
			deleteKeys: []message.Key{},
		},
		// 1 delete key does not set delete.
		{
			deleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(r.uid, r.tableID,
					model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 2}))),
			},
		},
		// One more delete key sets delete.
		{
			deleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(r.uid, r.tableID,
					model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 1}))),
			},
			expectDelete: &message.DeleteRequest{
				Count: 2,
				Range: [2][]byte{
					encoding.EncodeTsKey(r.uid, r.tableID, 0),
					encoding.EncodeKey(r.uid, r.tableID,
						model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 1})),
				},
			},
		},
		// Waiting long period sets delete.
		{
			deleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(r.uid, r.tableID,
					model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 3}))),
			},
			sleep: 4 * time.Second,
			expectDelete: &message.DeleteRequest{
				Count: 1,
				Range: [2][]byte{
					encoding.EncodeTsKey(r.uid, r.tableID, 0),
					encoding.EncodeKey(r.uid, r.tableID,
						model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 3})),
				},
			},
		},
	}
	for i, cs := range cases {
		if cs.sleep != 0 {
			time.Sleep(cs.sleep)
		}
		deleteKeys := cs.deleteKeys
		task := &message.Task{}
		r.setTaskDelete(task, deleteKeys)

		require.EqualValues(t, &message.Task{
			DeleteReq: cs.expectDelete,
		}, task, "case #%d, %v", i, cs)
	}
}

func TestReaderOutput(t *testing.T) {
	t.Parallel()

	r := newTestReader()

	r.outputCh = make(chan *model.PolymorphicEvent, 1)
	ok := r.output(&model.PolymorphicEvent{CRTs: 1})
	require.True(t, ok)
	require.EqualValues(t, &model.PolymorphicEvent{CRTs: 1}, r.lastEvent)
	ok = r.output(&model.PolymorphicEvent{CRTs: 1})
	require.False(t, ok)
	r.outputResolvedTs(2)
	require.EqualValues(t, 1, r.lastSentCommitTs)

	<-r.outputCh
	r.outputResolvedTs(2)
	require.EqualValues(t, 2, r.lastSentResolvedTs)

	<-r.outputCh
	ok = r.output(&model.PolymorphicEvent{CRTs: 3})
	require.True(t, ok)
}

func TestReaderOutputBufferedResolvedEvents(t *testing.T) {
	t.Parallel()

	capacity := 4
	r := newTestReader()

	buf := newOutputBuffer(capacity)

	cases := []struct {
		outputChCap     int
		inputEvents     []*model.PolymorphicEvent
		inputDeleteKeys []message.Key

		expectEvents     []*model.PolymorphicEvent
		expectDeleteKeys []message.Key
		expectOutputs    []*model.PolymorphicEvent
	}{
		// Empty buffer.
		{
			outputChCap:     1,
			inputEvents:     []*model.PolymorphicEvent{},
			inputDeleteKeys: []message.Key{},

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
			inputDeleteKeys: []message.Key{},

			expectEvents: []*model.PolymorphicEvent{},
			expectDeleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(r.uid, r.tableID,
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
				message.Key(encoding.EncodeKey(r.uid, r.tableID,
					model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 1}))),
			},

			expectEvents: []*model.PolymorphicEvent{},
			expectDeleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(r.uid, r.tableID,
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
				message.Key(encoding.EncodeKey(r.uid, r.tableID,
					model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 1}))),
			},

			expectEvents: []*model.PolymorphicEvent{},
			expectDeleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(r.uid, r.tableID,
					model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 1}))),
				message.Key(encoding.EncodeKey(r.uid, r.tableID,
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
			inputDeleteKeys: []message.Key{},

			expectEvents: []*model.PolymorphicEvent{
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 3, RegionID: 3}),
			},
			expectDeleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(r.uid, r.tableID,
					model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 3, RegionID: 1}))),
				message.Key(encoding.EncodeKey(r.uid, r.tableID,
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
			inputDeleteKeys: []message.Key{},

			expectEvents: []*model.PolymorphicEvent{
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 4, RegionID: 1}),
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 4, RegionID: 2}),
			},
			expectDeleteKeys: []message.Key{},
			expectOutputs:    []*model.PolymorphicEvent{},
		},
	}

	for i, cs := range cases {
		r.outputCh = make(chan *model.PolymorphicEvent, cs.outputChCap)
		buf.resolvedEvents = append([]*model.PolymorphicEvent{}, cs.inputEvents...)
		buf.deleteKeys = append([]message.Key{}, cs.inputDeleteKeys...)

		r.outputBufferedResolvedEvents(buf)
		require.EqualValues(t, cs.expectDeleteKeys, buf.deleteKeys, "case #%d, %v", i, cs)
		require.EqualValues(t, cs.expectEvents, buf.resolvedEvents, "case #%d, %v", i, cs)

		outputEvents := []*model.PolymorphicEvent{}
	RECV:
		for {
			select {
			case ev := <-r.outputCh:
				outputEvents = append(outputEvents, ev)
			default:
				break RECV
			}
		}
		require.EqualValues(t, cs.expectOutputs, outputEvents, "case #%d, %v", i, cs)
	}
}

func prepareTxnData(
	t *testing.T, r *reader, txnCount, txnSize int,
) db.DB {
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	db, err := db.OpenPebble(context.Background(), 1, t.TempDir(), 0, cfg)
	require.Nil(t, err)
	wb := db.Batch(0)
	for i := 1; i < txnCount+1; i++ { // txns.
		for j := 0; j < txnSize; j++ { // events.
			event := newTestEvent(uint64(i)+2, uint64(i), j)
			key := encoding.EncodeKey(r.uid, r.tableID, event)
			value, err := r.serde.Marshal(event, []byte{})
			require.Nil(t, err)
			t.Logf("key: %s, value: %s\n", message.Key(key), hex.EncodeToString(value))
			wb.Put(key, value)
		}
	}
	require.Nil(t, wb.Commit())
	return db
}

func TestReaderOutputIterEvents(t *testing.T) {
	t.Parallel()

	capacity := 4
	r := newTestReader()

	// Prepare data, 3 txns, 3 events for each.
	// CRTs 3, StartTs 1, keys (0|1|2)
	// CRTs 4, StartTs 2, keys (0|1|2)
	// CRTs 5, StartTs 3, keys (0|1|2)
	// CRTs 6, StartTs 4, keys (0|1|2)
	// CRTs 7, StartTs 4, keys (0|1|2)
	db := prepareTxnData(t, r, 5, 3)

	cases := []struct {
		outputChCap   int
		maxResolvedTs uint64
		hasReadNext   bool

		expectEvents       []*model.PolymorphicEvent
		expectDeleteKeys   []message.Key
		expectOutputs      []*model.PolymorphicEvent
		expectExhaustedRTs uint64
		expectHasReadNext  bool
	}{
		// Empty resolved event.
		{
			outputChCap:   1,
			maxResolvedTs: 0,

			expectEvents:       []*model.PolymorphicEvent{},
			expectDeleteKeys:   []message.Key{},
			expectOutputs:      []*model.PolymorphicEvent{},
			expectExhaustedRTs: 0,
			expectHasReadNext:  true,
		},
		// Nonblocking output three events.
		{
			outputChCap:   3,
			maxResolvedTs: 3, // CRTs 3 has 3 events.

			expectEvents: []*model.PolymorphicEvent{},
			expectDeleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(r.uid, r.tableID, newTestEvent(3, 1, 0))),
				message.Key(encoding.EncodeKey(r.uid, r.tableID, newTestEvent(3, 1, 1))),
				message.Key(encoding.EncodeKey(r.uid, r.tableID, newTestEvent(3, 1, 2))),
			},
			expectOutputs: []*model.PolymorphicEvent{
				newTestEvent(3, 1, 0),
				newTestEvent(3, 1, 1),
				newTestEvent(3, 1, 2),
			},
			expectExhaustedRTs: 3, // Iter is exhausted and no buffered resolved events.
			expectHasReadNext:  true,
		},
		// Blocking output two events of CRTs 4.
		{
			outputChCap:   2,
			maxResolvedTs: 4, // CRTs 4 has 3 events.

			expectEvents: []*model.PolymorphicEvent{newTestEvent(4, 2, 2)},
			expectDeleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(r.uid, r.tableID, newTestEvent(4, 2, 0))),
				message.Key(encoding.EncodeKey(r.uid, r.tableID, newTestEvent(4, 2, 1))),
			},
			expectOutputs: []*model.PolymorphicEvent{
				newTestEvent(4, 2, 0),
				newTestEvent(4, 2, 1),
			},
			// Events of CRTs 4 have been read and buffered.
			expectExhaustedRTs: 4,
			expectHasReadNext:  true,
		},
		// Output remaining event of CRTs 4.
		{
			outputChCap:   3,
			maxResolvedTs: 4, // CRTs 4 has 1 events.

			expectEvents: []*model.PolymorphicEvent{},
			expectDeleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(r.uid, r.tableID, newTestEvent(4, 2, 2))),
			},
			expectOutputs: []*model.PolymorphicEvent{
				newTestEvent(4, 2, 2),
				model.NewResolvedPolymorphicEvent(0, 4),
			},
			expectExhaustedRTs: 4, // Iter is exhausted and no buffered resolved events.
			expectHasReadNext:  true,
		},
		// Resolved ts covers all resolved events,
		// blocking output events of CRTs 5 (3 events) and 6 (1 event).
		{
			outputChCap:   5,
			maxResolvedTs: 7,

			expectEvents: []*model.PolymorphicEvent{
				newTestEvent(6, 4, 1),
				newTestEvent(6, 4, 2),
			},
			expectDeleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(r.uid, r.tableID, newTestEvent(5, 3, 0))),
				message.Key(encoding.EncodeKey(r.uid, r.tableID, newTestEvent(5, 3, 1))),
				message.Key(encoding.EncodeKey(r.uid, r.tableID, newTestEvent(5, 3, 2))),
				message.Key(encoding.EncodeKey(r.uid, r.tableID, newTestEvent(6, 4, 0))),
			},
			expectOutputs: []*model.PolymorphicEvent{
				newTestEvent(5, 3, 0),
				newTestEvent(5, 3, 1),
				newTestEvent(5, 3, 2),
				model.NewResolvedPolymorphicEvent(0, 5),
				newTestEvent(6, 4, 0),
			},
			// Iter is not exhausted, but all events with commit ts 6 have been
			// read into buffer.
			expectExhaustedRTs: 6,
			expectHasReadNext:  false, // (6, 4, 1) is neither output nor buffered.
		},
		// Resolved ts covers all resolved events, nonblocking output all events.
		{
			outputChCap:   7,
			maxResolvedTs: 7,

			expectEvents: []*model.PolymorphicEvent{},
			expectDeleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(r.uid, r.tableID, newTestEvent(6, 4, 1))),
				message.Key(encoding.EncodeKey(r.uid, r.tableID, newTestEvent(6, 4, 2))),
				message.Key(encoding.EncodeKey(r.uid, r.tableID, newTestEvent(7, 5, 0))),
				message.Key(encoding.EncodeKey(r.uid, r.tableID, newTestEvent(7, 5, 1))),
				message.Key(encoding.EncodeKey(r.uid, r.tableID, newTestEvent(7, 5, 2))),
			},
			expectOutputs: []*model.PolymorphicEvent{
				newTestEvent(6, 4, 1),
				newTestEvent(6, 4, 2),
				model.NewResolvedPolymorphicEvent(0, 6),
				newTestEvent(7, 5, 0),
				newTestEvent(7, 5, 1),
				newTestEvent(7, 5, 2),
				model.NewResolvedPolymorphicEvent(0, 7),
			},
			expectExhaustedRTs: 7, // Iter is exhausted and no buffered resolved events.
			expectHasReadNext:  true,
		},
		// All resolved events outputted, as resolved ts continues advance,
		// exhausted resolved ts advances too.
		{
			outputChCap:   1,
			maxResolvedTs: 8,

			expectEvents:       []*model.PolymorphicEvent{},
			expectDeleteKeys:   []message.Key{},
			expectOutputs:      []*model.PolymorphicEvent{},
			expectExhaustedRTs: 8,
			expectHasReadNext:  true,
		},
	}

	for i, cs := range cases {
		r.outputCh = make(chan *model.PolymorphicEvent, cs.outputChCap)
		buf := newOutputBuffer(capacity)

		iter := db.Iterator(
			encoding.EncodeTsKey(r.uid, r.tableID, 0),
			encoding.EncodeTsKey(r.uid, r.tableID, cs.maxResolvedTs+1))
		iter.Seek([]byte{})
		require.Nil(t, iter.Error(), "case #%d, %v", i, cs)
		pos, err := r.outputIterEvents(
			iter, readPosition{iterHasRead: cs.hasReadNext}, buf, cs.maxResolvedTs)
		require.Nil(t, err, "case #%d, %v", i, cs)
		require.EqualValues(
			t, cs.expectExhaustedRTs, pos.exhaustedResolvedTs, "case #%d, %v", i, cs)
		for _, k := range buf.deleteKeys {
			fmt.Printf("%s\n", k)
		}
		require.EqualValues(t, cs.expectDeleteKeys, buf.deleteKeys, "case #%d, %v", i, cs)
		require.EqualValues(t, cs.expectEvents, buf.resolvedEvents, "case #%d, %v", i, cs)
		require.EqualValues(t, cs.expectHasReadNext, pos.iterHasRead, "case #%d, %v", i, cs)
		outputEvents := receiveOutputEvents(r.outputCh)
		require.EqualValues(t, cs.expectOutputs, outputEvents, "case #%d, %v", i, cs)

		wb := db.Batch(0)
		for _, key := range cs.expectDeleteKeys {
			wb.Delete([]byte(key))
		}
		require.Nil(t, wb.Commit())
		require.Nil(t, iter.Release())
	}

	require.Nil(t, db.Close())
}

func TestReaderStateIterator(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := newTestReader()
	// Prepare data, 1 txn.
	db := prepareTxnData(t, r, 1, 1)
	sema := semaphore.NewWeighted(1)
	router := actor.NewRouter[message.Task](t.Name())
	compactMb := actor.NewMailbox[message.Task](1, 1)
	router.InsertMailbox4Test(compactMb.ID(), compactMb)
	readerMb := actor.NewMailbox[message.Task](2, 1)
	router.InsertMailbox4Test(readerMb.ID(), readerMb)
	state := r.state
	state.readerID = readerMb.ID()
	state.readerRouter = router
	state.compactorID = compactMb.ID()
	state.iterFirstSlowDuration = 100 * time.Second
	state.compact = NewCompactScheduler(router)
	state.iterMaxAliveDuration = 100 * time.Millisecond

	// First get returns a request.
	req, ok := state.tryGetIterator(1, 1)
	require.False(t, ok)
	require.NotNil(t, req)

	// Still wait for iterator response.
	req1, ok := state.tryGetIterator(1, 1)
	require.False(t, ok)
	require.Nil(t, req1)

	// Send iterator.
	require.Nil(t, sema.Acquire(ctx, 1))
	req.IterCallback(&message.LimitedIterator{
		Iterator: db.Iterator([]byte{}, []byte{}),
		Sema:     sema,
	})
	// Must notify reader
	_, ok = readerMb.Receive()
	require.True(t, ok)
	// Get iterator successfully.
	req2, ok := state.tryGetIterator(1, 1)
	require.True(t, ok)
	require.Nil(t, req2)
	// Get iterator successfully again.
	req2, ok = state.tryGetIterator(1, 1)
	require.True(t, ok)
	require.Nil(t, req2)

	// Release an invalid iterator.
	require.False(t, state.iter.Valid())
	require.Nil(t, state.tryReleaseIterator(false))
	require.Nil(t, state.iter)

	// Release an outdated iterator.
	require.Nil(t, sema.Acquire(ctx, 1))
	state.iter = &message.LimitedIterator{
		Iterator: db.Iterator([]byte{}, []byte{0xff}),
		Sema:     sema,
	}
	require.True(t, state.iter.Seek([]byte{}))
	state.iterAliveTime = time.Now()
	time.Sleep(2 * state.iterMaxAliveDuration)
	require.Nil(t, state.tryReleaseIterator(false))
	require.Nil(t, state.iter)

	// Release empty iterator.
	require.Nil(t, state.tryReleaseIterator(false))

	// Slow first must send a compaction task.
	req3, ok := state.tryGetIterator(1, 1)
	require.False(t, ok)
	require.NotNil(t, req3)
	require.Nil(t, sema.Acquire(ctx, 1))
	req3.IterCallback(&message.LimitedIterator{
		Iterator: db.Iterator([]byte{}, []byte{}),
		Sema:     sema,
	})
	// Must notify reader
	_, ok = readerMb.Receive()
	require.True(t, ok)
	// No compaction task yet.
	_, ok = compactMb.Receive()
	require.False(t, ok)
	// Always slow.
	state.iterFirstSlowDuration = time.Duration(0)
	_, ok = state.tryGetIterator(1, 1)
	require.True(t, ok)
	require.NotNil(t, state.iter)
	// Must recv a compaction task.
	_, ok = compactMb.Receive()
	require.True(t, ok)
	// Release iterator.
	time.Sleep(2 * state.iterMaxAliveDuration)
	require.Nil(t, state.tryReleaseIterator(false))
	require.Nil(t, state.iter)

	require.Nil(t, db.Close())
}

func TestReaderPoll(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	capacity := 4
	router := actor.NewRouter[message.Task](t.Name())
	sema := semaphore.NewWeighted(1)
	dbMb := actor.NewMailbox[message.Task](1, capacity)
	router.InsertMailbox4Test(dbMb.ID(), dbMb)
	readerMb := actor.NewMailbox[message.Task](2, capacity)
	router.InsertMailbox4Test(readerMb.ID(), readerMb)
	r := newTestReader()
	r.common.dbRouter = router
	r.common.dbActorID = dbMb.ID()
	r.outputCh = make(chan *model.PolymorphicEvent, sorterOutputCap)
	tableID := r.tableID

	// Prepare data, 3 txns, 3 events for each.
	// CRTs 3, StartTs 1, keys (0|1|2)
	// CRTs 4, StartTs 2, keys (0|1|2)
	db := prepareTxnData(t, r, 2, 3)

	// We need to poll twice to read resolved events, so we need a slice of
	// two cases.
	cases := [][]struct {
		inputReadTs message.ReadTs
		inputIter   func([2][]byte) *message.LimitedIterator
		state       pollState
		releaseIter bool

		expectEvents        []*model.PolymorphicEvent
		expectDeleteKeys    []message.Key
		expectOutputs       []*model.PolymorphicEvent
		expectPartialTxnKey []byte
		expectMaxCommitTs   uint64
		expectMaxResolvedTs uint64
		expectExhaustedRTs  uint64
	}{
		{{ // The first poll
			inputReadTs: message.ReadTs{MaxResolvedTs: 1},
			state: pollState{
				outputBuf: newOutputBuffer(1),
			},
			inputIter: nil,

			expectEvents:        []*model.PolymorphicEvent{},
			expectDeleteKeys:    []message.Key{},
			expectOutputs:       []*model.PolymorphicEvent{model.NewResolvedPolymorphicEvent(0, 1)},
			expectMaxCommitTs:   0,
			expectMaxResolvedTs: 1,
			expectExhaustedRTs:  0,
		}, { // The second poll
			inputReadTs: message.ReadTs{MaxResolvedTs: 1},
			// state is inherited from the first poll.
			inputIter: nil, // no need to make an iterator.

			expectEvents:     []*model.PolymorphicEvent{},
			expectDeleteKeys: []message.Key{},
			// Does not output the same resolved ts twice.
			expectOutputs:       []*model.PolymorphicEvent{},
			expectMaxCommitTs:   0,
			expectMaxResolvedTs: 1,
			expectExhaustedRTs:  0,
		}},
		// maxCommitTs and maxResolvedTs must advance according to inputs.
		// And exhaustedResolvedTs must advance if there is no resolved event.
		{{ // The first poll
			inputReadTs: message.ReadTs{MaxResolvedTs: 2, MaxCommitTs: 3},
			state: pollState{
				outputBuf: newOutputBuffer(1),
			},
			// An empty iterator.
			inputIter: newEmptyIterator(ctx, t, db, sema),

			expectEvents:        []*model.PolymorphicEvent{},
			expectDeleteKeys:    []message.Key{},
			expectOutputs:       []*model.PolymorphicEvent{},
			expectMaxCommitTs:   3,
			expectMaxResolvedTs: 2,
			expectExhaustedRTs:  0,
		}, { // The second poll
			inputReadTs: message.ReadTs{MaxResolvedTs: 2},
			// state is inherited from the first poll.
			inputIter: nil, // no need to make an iterator.

			expectEvents:        []*model.PolymorphicEvent{},
			expectDeleteKeys:    []message.Key{},
			expectOutputs:       []*model.PolymorphicEvent{},
			expectMaxCommitTs:   3,
			expectMaxResolvedTs: 2,
			// exhaustedResolvedTs must advance if there is no resolved event.
			expectExhaustedRTs: 2,
		}},
		// exhaustedResolvedTs must not advance if a txn is partially read.
		// Output: CRTs 3, StartTs 1, keys (0|1|2)
		{{ // The first poll
			inputReadTs: message.ReadTs{MaxResolvedTs: 3, MaxCommitTs: 3},
			state: pollState{
				// A smaller buffer so that it can not hold all txn events.
				outputBuf: newOutputBuffer(1),
			},
			inputIter: newIterator(ctx, t, db, sema),

			expectEvents:        []*model.PolymorphicEvent{},
			expectDeleteKeys:    []message.Key{},
			expectOutputs:       []*model.PolymorphicEvent{},
			expectMaxCommitTs:   3,
			expectMaxResolvedTs: 3,
			expectExhaustedRTs:  0,
		}, { // The second poll
			inputReadTs: message.ReadTs{MaxResolvedTs: 3},
			// state is inherited from the first poll.
			inputIter: nil, // no need to make an iterator.

			expectEvents: []*model.PolymorphicEvent{},
			expectDeleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(r.uid, tableID, newTestEvent(3, 1, 0))),
			},
			expectOutputs: []*model.PolymorphicEvent{
				newTestEvent(3, 1, 0),
			},
			expectPartialTxnKey: encoding.EncodeKey(r.uid, tableID, newTestEvent(3, 1, 1)),
			expectMaxCommitTs:   3,
			expectMaxResolvedTs: 3,
			// exhaustedResolvedTs must not advance if a txn is partially read.
			expectExhaustedRTs: 0,
		}, { // The third poll
			inputReadTs: message.ReadTs{MaxResolvedTs: 3},
			// state is inherited from the first poll.
			inputIter: nil, // no need to make an iterator.

			expectEvents: []*model.PolymorphicEvent{},
			expectDeleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(r.uid, tableID, newTestEvent(3, 1, 1))),
			},
			expectOutputs: []*model.PolymorphicEvent{
				newTestEvent(3, 1, 1),
			},
			expectPartialTxnKey: encoding.EncodeKey(r.uid, tableID, newTestEvent(3, 1, 2)),
			expectMaxCommitTs:   3,
			expectMaxResolvedTs: 3,
			// exhaustedResolvedTs must not advance if a txn is partially read.
			expectExhaustedRTs: 0,
		}, { // The fourth poll, mock releasing iterator during read.
			inputReadTs: message.ReadTs{MaxResolvedTs: 3},
			// Release iterator to make reader request iter again.
			releaseIter: true,
			inputIter:   newIterator(ctx, t, db, sema),

			expectEvents:        []*model.PolymorphicEvent{},
			expectDeleteKeys:    []message.Key{},
			expectOutputs:       []*model.PolymorphicEvent{},
			expectPartialTxnKey: encoding.EncodeKey(r.uid, tableID, newTestEvent(3, 1, 2)),
			expectMaxCommitTs:   3,
			expectMaxResolvedTs: 3,
			// exhaustedResolvedTs must advance if a txn is completely read.
			expectExhaustedRTs: 0,
		}, { // The fifth poll, all events read.
			inputReadTs: message.ReadTs{MaxResolvedTs: 3},
			// state is inherited from the fourth poll.
			inputIter: nil,

			expectEvents: []*model.PolymorphicEvent{},
			expectDeleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(r.uid, tableID, newTestEvent(3, 1, 2))),
			},
			expectOutputs: []*model.PolymorphicEvent{
				newTestEvent(3, 1, 2),
				model.NewResolvedPolymorphicEvent(0, 3),
			},
			expectMaxCommitTs:   3,
			expectMaxResolvedTs: 3,
			// exhaustedResolvedTs must advance if a txn is completely read.
			expectExhaustedRTs: 3,
		}},
		// exhaustedResolvedTs must advance if all resolved events are outputted.
		// Output: CRTs 3, StartTs 1, keys (0|1|2)
		{{ // The first poll
			inputReadTs: message.ReadTs{MaxResolvedTs: 3, MaxCommitTs: 3},
			state: pollState{
				outputBuf: newOutputBuffer(3),
			},
			inputIter: newIterator(ctx, t, db, sema),

			expectEvents:        []*model.PolymorphicEvent{},
			expectDeleteKeys:    []message.Key{},
			expectOutputs:       []*model.PolymorphicEvent{},
			expectMaxCommitTs:   3,
			expectMaxResolvedTs: 3,
			expectExhaustedRTs:  0,
		}, { // The second poll
			inputReadTs: message.ReadTs{MaxResolvedTs: 3},
			// state is inherited from the first poll.
			inputIter: nil, // no need to make an iterator.

			expectEvents: []*model.PolymorphicEvent{},
			expectDeleteKeys: []message.Key{
				message.Key(encoding.EncodeKey(r.uid, tableID, newTestEvent(3, 1, 0))),
				message.Key(encoding.EncodeKey(r.uid, tableID, newTestEvent(3, 1, 1))),
				message.Key(encoding.EncodeKey(r.uid, tableID, newTestEvent(3, 1, 2))),
			},
			expectOutputs: []*model.PolymorphicEvent{
				newTestEvent(3, 1, 0),
				newTestEvent(3, 1, 1),
				newTestEvent(3, 1, 2),
				model.NewResolvedPolymorphicEvent(0, 3),
			},
			expectMaxCommitTs:   3,
			expectMaxResolvedTs: 3,
			// exhaustedResolvedTs must advance if there is no resolved event.
			expectExhaustedRTs: 3,
		}},
		// Batch output buffered resolved events
		{{ // The first poll
			inputReadTs: message.ReadTs{},
			state: pollState{
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
			inputIter: nil,

			expectEvents:     []*model.PolymorphicEvent{},
			expectDeleteKeys: []message.Key{},
			expectOutputs: []*model.PolymorphicEvent{
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 4}),
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 4}),
				model.NewPolymorphicEvent(&model.RawKVEntry{CRTs: 4}),
				model.NewResolvedPolymorphicEvent(0, 4),
			},
			expectMaxCommitTs:   0,
			expectMaxResolvedTs: 0,
			expectExhaustedRTs:  0,
		}, { // The second poll
			inputReadTs: message.ReadTs{MaxResolvedTs: 4},
			// state is inherited from the first poll.
			inputIter: nil, // no need to make an iterator.

			expectEvents:     []*model.PolymorphicEvent{},
			expectDeleteKeys: []message.Key{},
			// Does not output the same resolved ts twice.
			expectOutputs:       []*model.PolymorphicEvent{},
			expectMaxCommitTs:   0,
			expectMaxResolvedTs: 4,
			expectExhaustedRTs:  0,
		}},
	}

	metricIterDuration := sorterIterReadDurationHistogram.MustCurryWith(
		prometheus.Labels{
			"namespace": "default",
			"id":        t.Name(),
		})
	for i, css := range cases {
		r.state = css[0].state
		r.state.readerRouter = router
		r.state.readerID = readerMb.ID()
		r.state.iterFirstSlowDuration = 100 * time.Second
		r.state.iterMaxAliveDuration = 100 * time.Second
		// Do not send delete range.
		r.delete.countThreshold = 1000
		r.delete.period = 100 * time.Second
		r.state.metricIterFirst = metricIterDuration.WithLabelValues("first")
		r.state.metricIterRelease = metricIterDuration.WithLabelValues("release")
		for j, cs := range css {
			t.Logf("test case #%d[%d], %v", i, j, cs)
			if cs.releaseIter {
				require.Nil(t, r.state.tryReleaseIterator(true))
			}
			msg := actormsg.ValueMessage(message.Task{ReadTs: cs.inputReadTs})
			require.True(t, r.Poll(ctx, []actormsg.Message[message.Task]{msg}))
			require.EqualValues(
				t, cs.expectEvents, r.state.outputBuf.resolvedEvents,
				"case #%d[%d], %v", i, j, cs)
			require.EqualValues(
				t, cs.expectDeleteKeys, r.state.outputBuf.deleteKeys,
				"case #%d[%d], %v", i, j, cs)
			require.EqualValues(
				t, cs.expectMaxCommitTs, r.state.maxCommitTs,
				"case #%d[%d], %v", i, j, cs)
			require.EqualValues(
				t, cs.expectMaxResolvedTs, r.state.maxResolvedTs,
				"case #%d[%d], %v", i, j, cs)
			require.EqualValues(
				t, cs.expectExhaustedRTs, r.state.position.exhaustedResolvedTs,
				"case #%d[%d], %v", i, j, cs)
			require.EqualValues(
				t, cs.expectPartialTxnKey, r.state.position.partialTxnKey,
				"case #%d[%d], %v", i, j, cs)
			outputEvents := receiveOutputEvents(r.outputCh)
			require.EqualValues(
				t, cs.expectOutputs, outputEvents,
				"case #%d[%d], %v", i, j, cs)

			select {
			case err := <-r.errCh:
				require.Fail(t, "must not receive error", err)
			default:
			}
			task, ok := dbMb.Receive()
			if !ok {
				// No task, so there must be nil inputIter.
				require.Nil(t, cs.inputIter, "case #%d[%d], %v", i, j, cs)
				continue
			}
			handleTask(t, task, cs.inputIter, readerMb)
		}
		if r.state.iter != nil {
			require.Nil(t, r.state.iter.Release())
		}
	}

	require.Nil(t, db.Close())
}

func handleTask(
	t *testing.T, task actormsg.Message[message.Task],
	iterFn func(rg [2][]byte) *message.LimitedIterator, readerMb actor.Mailbox[message.Task],
) {
	if task.Value.IterReq == nil || iterFn == nil {
		return
	}
	iter := iterFn(task.Value.IterReq.Range)
	if iter != nil {
		iter.ResolvedTs = task.Value.IterReq.ResolvedTs
		task.Value.IterReq.IterCallback(iter)
		// Must notify reader
		_, ok := readerMb.Receive()
		require.True(t, ok)
	}
}

func newIterator(
	ctx context.Context, t *testing.T, db db.DB, sema *semaphore.Weighted,
) func(rg [2][]byte) *message.LimitedIterator {
	return func(rg [2][]byte) *message.LimitedIterator {
		require.Nil(t, sema.Acquire(ctx, 1))
		t.Logf("newIterator %s %s\n", message.Key(rg[0]), message.Key(rg[1]))
		return &message.LimitedIterator{
			Iterator: db.Iterator(rg[0], rg[1]),
			Sema:     sema,
		}
	}
}

func newEmptyIterator(
	ctx context.Context, t *testing.T, db db.DB, sema *semaphore.Weighted,
) func(rg [2][]byte) *message.LimitedIterator {
	return func(rg [2][]byte) *message.LimitedIterator {
		require.Nil(t, sema.Acquire(ctx, 1))
		return &message.LimitedIterator{
			Iterator: db.Iterator([]byte{}, []byte{}),
			Sema:     sema,
		}
	}
}
