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
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sorter"
	"github.com/pingcap/tiflow/cdc/sorter/encoding"
	"github.com/pingcap/tiflow/cdc/sorter/leveldb/message"
	"github.com/pingcap/tiflow/pkg/actor"
	actormsg "github.com/pingcap/tiflow/pkg/actor/message"
	"github.com/stretchr/testify/require"
)

func newTestWriter(
	c common, readerRouter *actor.Router[message.Task], readerActorID actor.ID,
) *writer {
	return &writer{
		common:        c,
		readerRouter:  readerRouter,
		readerActorID: readerActorID,

		metricTotalEventsKV: sorter.OutputEventCount.
			WithLabelValues("default", "test", "kv"),
		metricTotalEventsResolved: sorter.OutputEventCount.
			WithLabelValues("default", "test", "resolved"),
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

func TestWriterPoll(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	capacity := 4
	router := actor.NewRouter[message.Task](t.Name())
	readerID := actor.ID(1)
	readerMB := actor.NewMailbox[message.Task](readerID, capacity)
	router.InsertMailbox4Test(readerID, readerMB)
	dbID := actor.ID(2)
	dbMB := actor.NewMailbox[message.Task](dbID, capacity)
	router.InsertMailbox4Test(dbID, dbMB)
	c := common{dbActorID: dbID, dbRouter: router}
	writer := newTestWriter(c, router, readerID)

	// We need to poll twice to read resolved events, so we need a slice of
	// two cases.
	cases := []struct {
		inputEvents []*model.PolymorphicEvent

		expectWrites        [][]byte
		expectMaxCommitTs   uint64
		expectMaxResolvedTs uint64
	}{{
		// Only resoved ts events.
		inputEvents: []*model.PolymorphicEvent{
			model.NewResolvedPolymorphicEvent(0, 1),
			model.NewResolvedPolymorphicEvent(0, 2),
		},

		expectWrites:        [][]byte{},
		expectMaxCommitTs:   0,
		expectMaxResolvedTs: 2,
	}, {
		// Only rawkv events.
		inputEvents: []*model.PolymorphicEvent{
			newTestEvent(3, 1, 0), // crts 3, startts 2, key 0
			newTestEvent(3, 1, 1), // crts 3, startts 2, key 1
		},

		expectWrites: [][]byte{
			encoding.EncodeKey(c.uid, c.tableID, newTestEvent(3, 1, 0)),
			encoding.EncodeKey(c.uid, c.tableID, newTestEvent(3, 1, 1)),
		},
		expectMaxCommitTs:   3,
		expectMaxResolvedTs: 2,
	}, {
		// Mix rawkv events and resolved ts events.
		inputEvents: []*model.PolymorphicEvent{
			newTestEvent(4, 2, 0), // crts 4, startts 2
			model.NewResolvedPolymorphicEvent(0, 3),
			newTestEvent(6, 3, 0), // crts 6, startts 3
			model.NewResolvedPolymorphicEvent(0, 3),
		},

		expectWrites: [][]byte{
			encoding.EncodeKey(c.uid, c.tableID, newTestEvent(4, 2, 0)),
			encoding.EncodeKey(c.uid, c.tableID, newTestEvent(6, 3, 0)),
		},
		expectMaxCommitTs:   6,
		expectMaxResolvedTs: 3,
	}, {
		// Duplicate commit events.
		inputEvents: []*model.PolymorphicEvent{
			newTestEvent(6, 3, 0), // crts 6, startts 3
			newTestEvent(6, 3, 0), // crts 6, startts 3
		},

		expectWrites: [][]byte{
			encoding.EncodeKey(c.uid, c.tableID, newTestEvent(6, 3, 0)),
		},
		expectMaxCommitTs:   6,
		expectMaxResolvedTs: 3,
	}, {
		// Commit ts regress and bounce.
		inputEvents: []*model.PolymorphicEvent{
			newTestEvent(4, 3, 0), // crts 4, startts 3
			newTestEvent(5, 3, 0), // crts 5, startts 3
			newTestEvent(4, 2, 0), // crts 4, startts 3
		},

		expectWrites: [][]byte{
			encoding.EncodeKey(c.uid, c.tableID, newTestEvent(4, 3, 0)),
			encoding.EncodeKey(c.uid, c.tableID, newTestEvent(5, 3, 0)),
			encoding.EncodeKey(c.uid, c.tableID, newTestEvent(4, 2, 0)),
		},
		expectMaxCommitTs:   6,
		expectMaxResolvedTs: 3,
	}, {
		// Resolved ts regress. It should not happen, but we test it anyway.
		inputEvents: []*model.PolymorphicEvent{
			model.NewResolvedPolymorphicEvent(0, 2),
		},

		expectWrites:        [][]byte{},
		expectMaxCommitTs:   6,
		expectMaxResolvedTs: 3,
	}}

	for i, cs := range cases {
		msgs := make([]actormsg.Message[message.Task], 0, len(cs.inputEvents))
		for i := range cs.inputEvents {
			msgs = append(msgs, actormsg.ValueMessage(message.Task{
				InputEvent: cs.inputEvents[i],
			}))
		}
		t.Logf("test case #%d, %v", i, cs)
		require.True(t, writer.Poll(ctx, msgs), "case #%d, %v", i, cs)
		if len(cs.expectWrites) != 0 {
			msg, ok := dbMB.Receive()
			require.True(t, ok, "case #%d, %v", i, cs)
			writeReq := msg.Value.WriteReq
			require.EqualValues(t, len(cs.expectWrites), len(writeReq))
			for _, k := range cs.expectWrites {
				_, ok := writeReq[message.Key(k)]
				require.True(t, ok, "case #%d, %v, %v, %v", i, cs, writeReq)
			}
		} else {
			_, ok := dbMB.Receive()
			require.False(t, ok, "case #%d, %v", i, cs)
		}
		msg, ok := readerMB.Receive()
		require.True(t, ok, "case #%d, %v", i, cs)
		require.EqualValues(t,
			cs.expectMaxCommitTs, msg.Value.ReadTs.MaxCommitTs,
			"case #%d, %v", i, cs)
		require.EqualValues(t,
			cs.expectMaxResolvedTs, msg.Value.ReadTs.MaxResolvedTs,
			"case #%d, %v", i, cs)
	}

	// writer should stop once it receives Stop message.
	msg := actormsg.StopMessage[message.Task]()
	require.False(t, writer.Poll(ctx, []actormsg.Message[message.Task]{msg}))
}
