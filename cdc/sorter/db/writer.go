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

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/sorter"
	"github.com/pingcap/tiflow/cdc/sorter/db/message"
	"github.com/pingcap/tiflow/cdc/sorter/encoding"
	"github.com/pingcap/tiflow/pkg/actor"
	actormsg "github.com/pingcap/tiflow/pkg/actor/message"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// writer is a thin shim that batches, translates events into key-value pairs
// and writes to db.
type writer struct {
	common
	stopped bool

	readerRouter  *actor.Router[message.Task]
	readerActorID actor.ID

	maxResolvedTs uint64
	maxCommitTs   uint64

	metricTotalEventsKV       prometheus.Counter
	metricTotalEventsResolved prometheus.Counter
}

var _ actor.Actor[message.Task] = (*writer)(nil)

func (w *writer) Poll(ctx context.Context, msgs []actormsg.Message[message.Task]) (running bool) {
	kvEventCount, resolvedEventCount := 0, 0
	maxCommitTs, maxResolvedTs := uint64(0), uint64(0)
	writes := make(map[message.Key][]byte)
	for i := range msgs {
		switch msgs[i].Tp {
		case actormsg.TypeValue:
		case actormsg.TypeStop:
			return false
		default:
			log.Panic("unexpected message", zap.Any("message", msgs[i]))
		}

		ev := msgs[i].Value.InputEvent
		if ev.IsResolved() {
			if maxResolvedTs < ev.CRTs {
				maxResolvedTs = ev.CRTs
			}
			resolvedEventCount++
			continue
		}
		if maxCommitTs < ev.CRTs {
			maxCommitTs = ev.CRTs
		}
		kvEventCount++

		key := encoding.EncodeKey(w.uid, w.tableID, ev)
		value := []byte{}
		var err error
		value, err = w.serde.Marshal(ev, value)
		if err != nil {
			log.Panic("failed to marshal events", zap.Error(err))
		}
		writes[message.Key(key)] = value
	}
	w.metricTotalEventsKV.Add(float64(kvEventCount))
	w.metricTotalEventsResolved.Add(float64(resolvedEventCount))
	if atomic.LoadUint64(&w.maxCommitTs) < maxCommitTs {
		atomic.StoreUint64(&w.maxCommitTs, maxCommitTs)
	}
	if atomic.LoadUint64(&w.maxResolvedTs) < maxResolvedTs {
		atomic.StoreUint64(&w.maxResolvedTs, maxResolvedTs)
	}

	if len(writes) != 0 {
		// Send write task to db.
		task := message.Task{UID: w.uid, TableID: w.tableID, WriteReq: writes}
		err := w.dbRouter.SendB(ctx, w.dbActorID, actormsg.ValueMessage(task))
		if err != nil {
			w.reportError("failed to send write request", err)
			return false
		}
	}

	if w.maxResolvedTs == 0 {
		// Resolved ts has not advanced yet, skip notify reader.
		return true
	}
	// Notify reader that there is something to read.
	//
	// It's ok to notify reader immediately without waiting writes done,
	// because reader will see these writes:
	//   1. reader/writer send tasks to the same db, so tasks are ordered.
	//   2. ReadTs will trigger reader to take iterator from db,
	//      it happens after writer send writes to db.
	//   3. Before db takes iterator, it flushes all buffered writes.
	msg := actormsg.ValueMessage(message.Task{
		UID:     w.uid,
		TableID: w.tableID,
		ReadTs: message.ReadTs{
			// The maxCommitTs and maxResolvedTs must be sent together,
			// otherwise reader may output resolved ts wrongly.
			// As reader employs maxCommitTs and maxResolvedTs to skip taking
			// iterators when maxResolvedTs > maxCommitTs and
			// exhaustedResolvedTs >= maxCommitTs.
			//
			// If maxCommitTs and maxResolvedTs are sent separately,
			// data in (exhaustedResolvedTs, actual maxCommitTs] is lost:
			//        --------------------------------------------->
			// writer:                          ^ actual maxCommitTs
			// reader:  ^ maxCommitTs  ^ exhaustedResolvedTs   ^ maxResolvedTs
			MaxCommitTs:   atomic.LoadUint64(&w.maxCommitTs),
			MaxResolvedTs: atomic.LoadUint64(&w.maxResolvedTs),
		},
	})
	// It's ok if send fails, as resolved ts events are received periodically.
	_ = w.readerRouter.Send(w.readerActorID, msg)
	return true
}

// OnClose releases writer resource.
func (w *writer) OnClose() {
	if w.stopped {
		return
	}
	w.stopped = true
	w.common.closedWg.Done()
}

func (w *writer) stats() sorter.Stats {
	maxCommitTs := atomic.LoadUint64(&w.maxCommitTs)
	maxResolvedTs := atomic.LoadUint64(&w.maxResolvedTs)
	if maxCommitTs < maxResolvedTs {
		// In case, there is no write for the table,
		// we use maxResolvedTs as maxCommitTs to make the stats meaningful.
		maxCommitTs = maxResolvedTs
	}
	return sorter.Stats{
		CheckpointTsIngress: maxCommitTs,
		ResolvedTsIngress:   maxResolvedTs,
	}
}
