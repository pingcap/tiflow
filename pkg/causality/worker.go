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
	"sync/atomic"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type txnEvent interface {
	// OnConflictResolved is called when the event leaves ConflictDetector.
	OnConflictResolved()

	// Hashes are in range [0, math.MaxUint64) and must be deduped.
	//
	// NOTE: if the conflict detector is accessed by multiple threads concurrently,
	// GenSortedDedupKeysHash must also be sorted based on `key % numSlots`.
	GenSortedDedupKeysHash(numSlots uint64) []uint64
}

// TxnWithNotifier is a wrapper of txnEvent with a PostTxnExecuted.
type TxnWithNotifier[Txn txnEvent] struct {
	TxnEvent Txn
	// The PostTxnExecuted will remove the txn related Node in the conflict detector's
	// dependency graph and resolve related dependencies for these transacitons
	// which depend on this executed txn.
	//
	// NOTE: the PostTxnExecuted() must be called after the txn executed.
	PostTxnExecuted func()
}

// WorkerOption is the option for creating a worker.
type WorkerOption struct {
	WorkerCount int
	// CacheSize controls the max number of txns a worker can hold.
	CacheSize int
	// IsBlock indicates whether the worker should block when the cache is full.
	IsBlock bool
}

// In current implementation, the conflict detector will push txn to the workerCache.
type workerCache[Txn txnEvent] interface {
	// add adds a event to the workerCache.
	add(txn TxnWithNotifier[Txn]) bool
	// out returns a channel to receive events which are ready to be executed.
	out() <-chan TxnWithNotifier[Txn]
}

func newWorker[Txn txnEvent](opt WorkerOption) workerCache[Txn] {
	log.Info("create new worker cache in conflict detector",
		zap.Int("workerCount", opt.WorkerCount),
		zap.Int("cacheSize", opt.CacheSize), zap.Bool("isBlock", opt.IsBlock))
	if opt.CacheSize <= 0 {
		log.Panic("WorkerOption.CacheSize should be greater than 0, please report a bug")
	}

	if opt.IsBlock {
		return &boundedWorkerWithBlock[Txn]{ch: make(chan TxnWithNotifier[Txn], opt.CacheSize)}
	}
	return &boundedWorker[Txn]{ch: make(chan TxnWithNotifier[Txn], opt.CacheSize)}
}

// boundedWorker is a worker which has a limit on the number of txns it can hold.
type boundedWorker[Txn txnEvent] struct {
	ch chan TxnWithNotifier[Txn]
}

func (w *boundedWorker[Txn]) add(txn TxnWithNotifier[Txn]) bool {
	select {
	case w.ch <- txn:
		return true
	default:
		return false
	}
}

func (w *boundedWorker[Txn]) out() <-chan TxnWithNotifier[Txn] {
	return w.ch
}

// boundedWorkerWithBlock is a special boundedWorker. Once the worker
// is full, it will block until all cached txns are consumed.
type boundedWorkerWithBlock[Txn txnEvent] struct {
	ch        chan TxnWithNotifier[Txn]
	isBlocked atomic.Bool
}

func (w *boundedWorkerWithBlock[Txn]) add(txn TxnWithNotifier[Txn]) bool {
	if w.isBlocked.Load() && len(w.ch) <= 0 {
		w.isBlocked.Store(false)
	}

	if !w.isBlocked.Load() {
		select {
		case w.ch <- txn:
			return true
		default:
			w.isBlocked.CompareAndSwap(false, true)
		}
	}
	return false
}

func (w *boundedWorkerWithBlock[Txn]) out() <-chan TxnWithNotifier[Txn] {
	return w.ch
}

// TODO: maybe we can implement a strategy that can automatically adapt to different scenarios
