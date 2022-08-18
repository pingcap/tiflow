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

package txn

import (
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/backends"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/pkg/causality"
)

const (
	defaultConflictDetectorSlots int64 = 1024 * 1024
)

// Assert EventSink[E event.TableEvent] implementation
var _ eventsink.EventSink[*model.SingleTableTxn] = (*sink)(nil)

// sink is the sink for SingleTableTxn.
type sink struct {
	conflictDetector *causality.ConflictDetector[*worker, *txnEvent]
	workers          []*worker
}

func newSink(backends []backends.Backend, errCh chan<- error, conflictDetectorSlots int64) sink {
	workers := make([]*worker, 0, len(backends))
	for i, backend := range backends {
		w := newWorker(i, backend, errCh)
		w.runBackgroundLoop()
		workers = append(workers, w)
	}
	detector := causality.NewConflictDetector[*worker, *txnEvent](workers, conflictDetectorSlots)
	return sink{conflictDetector: detector, workers: workers}
}

// WriteEvents writes events to the sink.
func (s *sink) WriteEvents(rows ...*eventsink.TxnCallbackableEvent) (err error) {
	for _, row := range rows {
		err = s.conflictDetector.Add(newTxnEvent(row))
		if err != nil {
			return
		}
	}
	return
}

// Close closes the sink. It won't wait for all pending items backend handled.
func (s *sink) Close() error {
	s.conflictDetector.Close()
	for _, w := range s.workers {
		w.Close()
	}
	return nil
}
