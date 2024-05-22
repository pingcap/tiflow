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

package causality

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type mockTxnEvent struct{}

func (m mockTxnEvent) OnConflictResolved() {
}

func (m mockTxnEvent) GenSortedDedupKeysHash(numSlots uint64) []uint64 {
	return nil
}

func TestBoundedWorker(t *testing.T) {
	t.Parallel()

	size := 100
	// Create a worker with 1 worker and 1 cache size.
	worker := newTxnCache[txnEvent](TxnCacheOption{
		Count:         1,
		Size:          size,
		BlockStrategy: BlockStrategyWaitAvailable,
	})
	for i := 0; i < size; i++ {
		// Add 10 events to the worker.
		ok := worker.add(TxnWithNotifier[txnEvent]{
			TxnEvent:        mockTxnEvent{},
			PostTxnExecuted: func() {},
		})
		require.True(t, ok)
	}
	e := TxnWithNotifier[txnEvent]{
		TxnEvent:        mockTxnEvent{},
		PostTxnExecuted: func() {},
	}
	require.False(t, worker.add(e))
	e = <-worker.out()
	require.NotNil(t, e)
	require.True(t, worker.add(e))
}

func TestBoundedWorkerWithBlock(t *testing.T) {
	t.Parallel()

	size := 100
	// Create a worker with 1 worker and 1 cache size.
	worker := newTxnCache[txnEvent](TxnCacheOption{
		Count:         1,
		Size:          size,
		BlockStrategy: BlockStrategyWaitEmpty,
	})
	for i := 0; i < size; i++ {
		// Add 10 events to the worker.
		ok := worker.add(TxnWithNotifier[txnEvent]{
			TxnEvent:        mockTxnEvent{},
			PostTxnExecuted: func() {},
		})
		require.True(t, ok)
	}
	e := TxnWithNotifier[txnEvent]{
		TxnEvent:        mockTxnEvent{},
		PostTxnExecuted: func() {},
	}
	require.False(t, worker.add(e))
	e = <-worker.out()
	require.NotNil(t, e)
	require.False(t, worker.add(e))
}
