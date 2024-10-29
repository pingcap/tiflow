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

package tests

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/causality"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type conflictTestDriver struct {
	workers          []*workerForTest
	conflictDetector *causality.ConflictDetector[*txnForTest]
	generator        workloadGenerator

	pendingCount atomic.Int64
}

//nolint:unparam
func newConflictTestDriver(
	numWorkers int, numSlots int, workload workloadGenerator,
) *conflictTestDriver {
	detector := causality.NewConflictDetector[*txnForTest](uint64(numSlots), causality.TxnCacheOption{
		Count:         numWorkers,
		Size:          1024,
		BlockStrategy: causality.BlockStrategyWaitAvailable,
	})

	workers := make([]*workerForTest, 0, numWorkers)
	for i := 0; i < numWorkers; i++ {
		id := int64(i)
		workers = append(workers, newWorkerForTest(detector.GetOutChByCacheID(id)))
	}
	return &conflictTestDriver{
		workers:          workers,
		conflictDetector: detector,
		generator:        workload,
	}
}

func (d *conflictTestDriver) WithExecFunc(fn func(txn *txnForTest) error) *conflictTestDriver {
	for _, worker := range d.workers {
		worker.execFunc = fn
	}
	return d
}

func (d *conflictTestDriver) Run(ctx context.Context, n int) error {
	statusTicker := time.NewTicker(1 * time.Second)
	defer statusTicker.Stop()

	var counter int
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-statusTicker.C:
			log.Info("Generation progress", zap.Int("count", counter))
		default:
		}

		txn := &txnForTest{
			keys: d.generator.Next(),
			done: func() {
				d.pendingCount.Sub(1)
			},
		}

		d.pendingCount.Add(1)
		d.conflictDetector.Add(txn)
		counter++

		if counter > n {
			return nil
		}
	}
}

func (d *conflictTestDriver) Wait(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		default:
		}

		if d.pendingCount.Load() == 0 {
			return nil
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (d *conflictTestDriver) Close() {
	d.conflictDetector.Close()
	for _, worker := range d.workers {
		worker.Close()
	}
}
