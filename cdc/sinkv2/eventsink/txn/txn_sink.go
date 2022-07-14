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
	"sync"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/pkg/causality"
)

// Assert EventSink[E event.TableEvent] implementation
var _ eventsink.EventSink[*model.SingleTableTxn] = (*Sink)(nil)

// Sink is the sink for SingleTableTxn.
type Sink struct {
	conflictDetector *causality.ConflictDetector[worker, txnEvent]
    workers []worker
}

func NewSink() Sink {
	var workers []worker = make([]worker, 0, 1)
	workers = append(workers, &mysqlWorker{})
	return Sink{
		conflictDetector: causality.NewConflictDetector[worker, txnEvent](workers, 1024),
        workers: workers,
	}
}

// WriteEvents writes events to the sink.
func (s *Sink) WriteEvents(rows ...*eventsink.TxnCallbackableEvent) {
	for _, row := range rows {
		s.conflictDetector.Add(txnEvent{row})
	}
}

// Close closes the sink. It won't wait for all pending items be handled.
func (s *Sink) Close() error {
	for _, w := range s.workers {
		w.Close()
	}
	s.conflictDetector.Close()
    return nil
}

type txnEvent struct {
	*eventsink.TxnCallbackableEvent
}

// ConflictKeys implements causality.txnEvent interface.
func (e txnEvent) ConflictKeys() []int64 {
	return nil
}

type worker interface {
	// Add is inherited from causality.worker.
	Add(txn txnEvent, unlock func())

	Close()

    Run()
}

/////////////////

type mysqlWorker struct {
    txnCh chan txnWithNotifier
    stopped chan struct{}
    wg sync.WaitGroup
}

type txnWithNotifier struct {
    txnEvent
    wantMore func()
}

func newMySQLWorker() *mysqlWorker {
    return &mysqlWorker {
        txnCh: make(chan txnWithNotifier, 1),
        stopped: make(chan struct{}, 1),
    }
}

func (w *mysqlWorker) Add(txn txnEvent, unlock func()) {
    w.txnCh <- txnWithNotifier{txn, unlock}
}

func (w *mysqlWorker) Close() {
    w.stopped <- struct {}{}
    w.wg.Wait()
}

func (w *mysqlWorker) Run() {
    /*****************
    var (
		toExecRows []*model.RowChangedEvent
		replicaID  uint64
		txnNum     int
        metricBucketSize prometheus.Counter
    )

	flushRows := func() error {
		if len(toExecRows) == 0 {
			return nil
		}
		err := w.execDMLs(ctx, toExecRows, replicaID, w.bucket)
		if err != nil {
			txnNum = 0
			return err
		}
		toExecRows = toExecRows[:0]
		w.metricBucketSize.Add(float64(txnNum))
		txnNum = 0
		return nil
	}

    for {
        select {
        case <-w.stopped:
            return
        case txn:= <-w.txnCh:
            txn.wantMore()
            event := txn.txnEvent
            if txn.Event.FinishWg != nil {

            }
        }
    }
    *****************/
}
