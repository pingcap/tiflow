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

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sorter/encoding"
	"github.com/pingcap/tiflow/cdc/sorter/leveldb/message"
	"github.com/pingcap/tiflow/pkg/actor"
	actormsg "github.com/pingcap/tiflow/pkg/actor/message"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// writer is a thin shim that batches, translates events into key vaule pairs
// and writes to leveldb.
type writer struct {
	common

	readerRouter  *actor.Router
	readerActorID actor.ID

	metricTotalEventsKV         prometheus.Counter
	metricTotalEventsResolvedTs prometheus.Counter
}

var _ actor.Actor = (*writer)(nil)

func (w *writer) Poll(ctx context.Context, msgs []actormsg.Message) (running bool) {
	maxCommitTs, maxResolvedTs := uint64(0), uint64(0)
	kvEventCount, resolvedEventCount := 0, 0
	writes := make(map[message.Key][]byte)
	for i := range msgs {
		switch msgs[i].Tp {
		case actormsg.TypeSorterTask:
		case actormsg.TypeStop:
			return false
		default:
			log.Panic("unexpected message", zap.Any("message", msgs[i]))
		}

		ev := msgs[i].SorterTask.InputEvent
		if ev.RawKV.OpType == model.OpTypeResolved {
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
	w.metricTotalEventsResolvedTs.Add(float64(resolvedEventCount))

	if len(writes) != 0 {
		// Send write task to leveldb.
		task := message.Task{UID: w.uid, TableID: w.tableID, WriteReq: writes}
		err := w.dbRouter.SendB(ctx, w.dbActorID, actormsg.SorterMessage(task))
		if err != nil {
			w.reportError("failed to send write request", err)
			return false
		}
	}

	// Notify readers that there is something to read.
	msg := actormsg.SorterMessage(message.Task{
		UID:     w.uid,
		TableID: w.tableID,
		ReadTs: message.ReadTs{
			MaxCommitTs:   maxCommitTs,
			MaxResolvedTs: maxResolvedTs,
		},
	})
	// It's ok if send fails, as resolved ts events are received periodically.
	_ = w.readerRouter.Send(w.readerActorID, msg)
	return true
}
