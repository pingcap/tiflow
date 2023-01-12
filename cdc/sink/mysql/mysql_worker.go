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

package mysql

import (
	"context"
	"runtime"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/notify"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type mysqlSinkWorker struct {
	txnCh            chan *model.SingleTableTxn
	maxTxnRow        int
	bucket           int
	execDMLs         func(context.Context, []*model.SingleTableTxn, uint64, int) error
	metricBucketSize prometheus.Counter
	receiver         *notify.Receiver
	closedCh         chan struct{}
	hasError         atomic.Bool
}

func newMySQLSinkWorker(
	maxTxnRow int,
	bucket int,
	metricBucketSize prometheus.Counter,
	receiver *notify.Receiver,
	execDMLs func(context.Context, []*model.SingleTableTxn, uint64, int) error,
) *mysqlSinkWorker {
	return &mysqlSinkWorker{
		txnCh:            make(chan *model.SingleTableTxn, 1024),
		maxTxnRow:        maxTxnRow,
		bucket:           bucket,
		metricBucketSize: metricBucketSize,
		execDMLs:         execDMLs,
		receiver:         receiver,
		closedCh:         make(chan struct{}),
	}
}

func (w *mysqlSinkWorker) appendTxn(ctx context.Context, txn *model.SingleTableTxn) {
	if txn == nil {
		return
	}
	select {
	case <-ctx.Done():
	case w.txnCh <- txn:
	}
}

func (w *mysqlSinkWorker) appendFinishTxn(wg *sync.WaitGroup) {
	// since worker will always fetch txns from txnCh, we don't need to worry the
	// txnCh full and send is blocked.
	wg.Add(1)
	w.txnCh <- &model.SingleTableTxn{
		FinishWg: wg,
	}
}

func (w *mysqlSinkWorker) isNormal() bool {
	return !w.hasError.Load()
}

func (w *mysqlSinkWorker) run(ctx context.Context) (err error) {
	var (
		toExecTxns []*model.SingleTableTxn
		replicaID  uint64
		txnNum     int
		rowNum     int
	)

	// mark FinishWg before worker exits, all data txns can be omitted.
	defer func() {
		for {
			select {
			case txn := <-w.txnCh:
				if txn.FinishWg != nil {
					txn.FinishWg.Done()
				}
			default:
				return
			}
		}
	}()

	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			err = cerror.ErrMySQLWorkerPanic.GenWithStack("mysql sink concurrent execute panic, stack: %v", string(buf))
			log.Error("mysql sink worker panic", zap.Reflect("r", r), zap.Stack("stacktrace"))
		}
	}()

	flushRows := func() error {
		if len(toExecTxns) == 0 {
			return nil
		}
		err := w.execDMLs(ctx, toExecTxns, replicaID, w.bucket)
		if err != nil {
			txnNum = 0
			return err
		}
		toExecTxns = toExecTxns[:0]
		w.metricBucketSize.Add(float64(txnNum))
		txnNum = 0
		rowNum = 0
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case txn := <-w.txnCh:
			if txn == nil {
				return errors.Trace(flushRows())
			}
			if txn.FinishWg != nil {
				if err := flushRows(); err != nil {
					w.hasError.Store(true)
					txn.FinishWg.Done()
					return errors.Trace(err)
				}
				txn.FinishWg.Done()
				continue
			}
			if txn.ReplicaID != replicaID || rowNum+len(txn.Rows) > w.maxTxnRow {
				if err := flushRows(); err != nil {
					w.hasError.Store(true)
					txnNum++
					return errors.Trace(err)
				}
			}
			replicaID = txn.ReplicaID
			toExecTxns = append(toExecTxns, txn)
			rowNum += len(txn.Rows)
			txnNum++
		case <-w.receiver.C:
			if err := flushRows(); err != nil {
				w.hasError.Store(true)
				return errors.Trace(err)
			}
		}
	}
}

// cleanup waits for notification from closedCh and consumes all txns from txnCh.
// The exit sequence is
// 1. producer(sink.flushRowChangedEvents goroutine) of txnCh exits
// 2. goroutine in 1 sends notification to closedCh of each sink worker
// 3. each sink worker receives the notification from closedCh and mark FinishWg as Done
func (w *mysqlSinkWorker) cleanup() {
	for {
		select {
		case txn := <-w.txnCh:
			if txn.FinishWg != nil {
				txn.FinishWg.Done()
			}
		case <-w.closedCh:
			// We are the consumer.
			// By returning here, it means we return only if
			// the producer has returned.
			return
		}
	}
}

func (w *mysqlSinkWorker) close() {
	// Close the channel to provide idempotent reads.
	close(w.closedCh)
}
