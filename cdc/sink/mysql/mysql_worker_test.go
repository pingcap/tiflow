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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/pkg/notify"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestMysqlSinkWorker(t *testing.T) {
	t.Parallel()
	tbl := &model.TableName{
		Schema:      "test",
		Table:       "user",
		TableID:     1,
		IsPartition: false,
	}
	testCases := []struct {
		txns               []*model.SingleTableTxn
		expectedOutputRows [][]*model.RowChangedEvent
		maxTxnRow          int
	}{
		{
			txns:      []*model.SingleTableTxn{},
			maxTxnRow: 1,
		},
		{
			txns: []*model.SingleTableTxn{
				{
					Table:    tbl,
					CommitTs: 1,
					Rows:     []*model.RowChangedEvent{{CommitTs: 1}},
				},
			},
			expectedOutputRows: [][]*model.RowChangedEvent{{{CommitTs: 1}}},
			maxTxnRow:          1,
		},
		{
			txns: []*model.SingleTableTxn{
				{
					Table:    tbl,
					CommitTs: 1,
					Rows:     []*model.RowChangedEvent{{CommitTs: 1}, {CommitTs: 1}, {CommitTs: 1}},
				},
			},
			expectedOutputRows: [][]*model.RowChangedEvent{
				{{CommitTs: 1}, {CommitTs: 1}, {CommitTs: 1}},
			},
			maxTxnRow: 2,
		},
		{
			txns: []*model.SingleTableTxn{
				{
					Table:    tbl,
					CommitTs: 1,
					Rows:     []*model.RowChangedEvent{{CommitTs: 1}, {CommitTs: 1}},
				},
				{
					Table:    tbl,
					CommitTs: 2,
					Rows:     []*model.RowChangedEvent{{CommitTs: 2}},
				},
				{
					Table:    tbl,
					CommitTs: 3,
					Rows:     []*model.RowChangedEvent{{CommitTs: 3}, {CommitTs: 3}},
				},
			},
			expectedOutputRows: [][]*model.RowChangedEvent{
				{{CommitTs: 1}, {CommitTs: 1}, {CommitTs: 2}},
				{{CommitTs: 3}, {CommitTs: 3}},
			},
			maxTxnRow: 4,
		},
		{
			txns: []*model.SingleTableTxn{
				{
					Table:    tbl,
					CommitTs: 1,
					Rows:     []*model.RowChangedEvent{{CommitTs: 1}},
				},
				{
					Table:    tbl,
					CommitTs: 2,
					Rows:     []*model.RowChangedEvent{{CommitTs: 2}},
				},
				{
					Table:    tbl,
					CommitTs: 3,
					Rows:     []*model.RowChangedEvent{{CommitTs: 3}},
				},
			},
			expectedOutputRows: [][]*model.RowChangedEvent{
				{{CommitTs: 1}, {CommitTs: 2}},
				{{CommitTs: 3}},
			},
			maxTxnRow: 2,
		},
		{
			txns: []*model.SingleTableTxn{
				{
					Table:    tbl,
					CommitTs: 1,
					Rows:     []*model.RowChangedEvent{{CommitTs: 1}},
				},
				{
					Table:    tbl,
					CommitTs: 2,
					Rows:     []*model.RowChangedEvent{{CommitTs: 2}, {CommitTs: 2}, {CommitTs: 2}},
				},
				{
					Table:    tbl,
					CommitTs: 3,
					Rows:     []*model.RowChangedEvent{{CommitTs: 3}},
				},
				{
					Table:    tbl,
					CommitTs: 4,
					Rows:     []*model.RowChangedEvent{{CommitTs: 4}},
				},
			},
			expectedOutputRows: [][]*model.RowChangedEvent{
				{{CommitTs: 1}},
				{{CommitTs: 2}, {CommitTs: 2}, {CommitTs: 2}},
				{{CommitTs: 3}, {CommitTs: 4}},
			},
			maxTxnRow: 3,
		},
	}
	ctx := context.Background()

	notifier := new(notify.Notifier)
	for i, tc := range testCases {
		cctx, cancel := context.WithCancel(ctx)
		var outputRows [][]*model.RowChangedEvent
		receiver, err := notifier.NewReceiver(-1)
		require.Nil(t, err)
		w := newMySQLSinkWorker(tc.maxTxnRow, 1,
			metrics.BucketSizeCounter.
				WithLabelValues("default", "changefeed", "1"),
			receiver,
			func(ctx context.Context, events []*model.RowChangedEvent, bucket int) error {
				rows := make([]*model.RowChangedEvent, len(events))
				copy(rows, events)
				outputRows = append(outputRows, rows)
				return nil
			})
		errg, cctx := errgroup.WithContext(cctx)
		errg.Go(func() error {
			return w.run(cctx)
		})
		for _, txn := range tc.txns {
			w.appendTxn(cctx, txn)
		}
		var wg sync.WaitGroup
		w.appendFinishTxn(&wg)
		// ensure all txns are fetched from txn channel in sink worker
		time.Sleep(time.Millisecond * 100)
		notifier.Notify()
		wg.Wait()
		cancel()
		require.Equal(t, context.Canceled, errors.Cause(errg.Wait()))
		require.Equal(t, tc.expectedOutputRows, outputRows,
			fmt.Sprintf("case %v, %s, %s", i, spew.Sdump(outputRows), spew.Sdump(tc.expectedOutputRows)))
	}
}

func TestMySQLSinkWorkerExitWithError(t *testing.T) {
	t.Parallel()
	tbl := &model.TableName{
		Schema:      "test",
		Table:       "user",
		TableID:     1,
		IsPartition: false,
	}
	txns1 := []*model.SingleTableTxn{
		{
			Table:    tbl,
			CommitTs: 1,
			Rows:     []*model.RowChangedEvent{{CommitTs: 1}},
		},
		{
			Table:    tbl,
			CommitTs: 2,
			Rows:     []*model.RowChangedEvent{{CommitTs: 2}},
		},
		{
			Table:    tbl,
			CommitTs: 3,
			Rows:     []*model.RowChangedEvent{{CommitTs: 3}},
		},
		{
			Table:    tbl,
			CommitTs: 4,
			Rows:     []*model.RowChangedEvent{{CommitTs: 4}},
		},
	}
	txns2 := []*model.SingleTableTxn{
		{
			Table:    tbl,
			CommitTs: 5,
			Rows:     []*model.RowChangedEvent{{CommitTs: 5}},
		},
		{
			Table:    tbl,
			CommitTs: 6,
			Rows:     []*model.RowChangedEvent{{CommitTs: 6}},
		},
	}
	maxTxnRow := 1
	ctx := context.Background()

	errExecFailed := errors.New("sink worker exec failed")
	notifier := new(notify.Notifier)
	cctx, cancel := context.WithCancel(ctx)
	receiver, err := notifier.NewReceiver(-1)
	require.Nil(t, err)
	w := newMySQLSinkWorker(maxTxnRow, 1, /*bucket*/
		metrics.
			BucketSizeCounter.WithLabelValues("default", "changefeed", "1"),
		receiver,
		func(ctx context.Context, events []*model.RowChangedEvent, bucket int) error {
			return errExecFailed
		})
	errg, cctx := errgroup.WithContext(cctx)
	errg.Go(func() error {
		return w.run(cctx)
	})
	// txn in txns1 will be sent to worker txnCh
	for _, txn := range txns1 {
		w.appendTxn(cctx, txn)
	}

	// simulate notify sink worker to flush existing txns
	var wg sync.WaitGroup
	w.appendFinishTxn(&wg)
	time.Sleep(time.Millisecond * 100)
	// txn in txn2 will be blocked since the worker has exited
	for _, txn := range txns2 {
		w.appendTxn(cctx, txn)
	}
	notifier.Notify()

	errg.Go(func() error {
		w.cleanup()
		return nil
	})

	// the flush notification wait group should be done
	wg.Wait()

	// simulate sink shutdown and send closed singal to sink worker
	close(w.closedCh)

	cancel()
	require.Equal(t, errExecFailed, errg.Wait())
}

func TestMySQLSinkWorkerExitCleanup(t *testing.T) {
	t.Parallel()
	tbl := &model.TableName{
		Schema:      "test",
		Table:       "user",
		TableID:     1,
		IsPartition: false,
	}
	txns1 := []*model.SingleTableTxn{
		{
			Table:    tbl,
			CommitTs: 1,
			Rows:     []*model.RowChangedEvent{{CommitTs: 1}},
		},
		{
			Table:    tbl,
			CommitTs: 2,
			Rows:     []*model.RowChangedEvent{{CommitTs: 2}},
		},
	}
	txns2 := []*model.SingleTableTxn{
		{
			Table:    tbl,
			CommitTs: 5,
			Rows:     []*model.RowChangedEvent{{CommitTs: 5}},
		},
	}

	maxTxnRow := 1
	ctx := context.Background()

	errExecFailed := errors.New("sink worker exec failed")
	notifier := new(notify.Notifier)
	cctx, cancel := context.WithCancel(ctx)
	receiver, err := notifier.NewReceiver(-1)
	require.Nil(t, err)
	w := newMySQLSinkWorker(maxTxnRow, 1, /*bucket*/
		metrics.
			BucketSizeCounter.WithLabelValues("default", "changefeed", "1"),
		receiver,
		func(ctx context.Context, events []*model.RowChangedEvent, bucket int) error {
			return errExecFailed
		})
	errg, cctx := errgroup.WithContext(cctx)
	errg.Go(func() error {
		err := w.run(cctx)
		return err
	})
	for _, txn := range txns1 {
		w.appendTxn(cctx, txn)
	}

	// sleep to let txns flushed by tick
	time.Sleep(time.Millisecond * 100)

	// simulate more txns are sent to txnCh after the sink worker run has exited
	for _, txn := range txns2 {
		w.appendTxn(cctx, txn)
	}
	var wg sync.WaitGroup
	w.appendFinishTxn(&wg)
	notifier.Notify()

	errg.Go(func() error {
		w.cleanup()
		return nil
	})

	// the flush notification wait group should be done
	wg.Wait()

	// simulate sink shutdown and send closed singal to sink worker
	close(w.closedCh)
	cancel()
	require.Equal(t, errExecFailed, errg.Wait())
}
