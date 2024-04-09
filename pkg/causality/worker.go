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
	"github.com/pingcap/tiflow/pkg/chann"
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

type TxnWithNotifier[Txn txnEvent] struct {
	TxnEvent Txn
	// The PostTxnExecuted will remove the txn related Node in the conflict detector's
	// dependency graph and resolve related dependencies for these transacitons
	// which depend on this executed txn.
	//
	// Note the PostTxnExecuted() must be called after the txn executed.
	PostTxnExecuted func()
}

// In current implementation, the conflict detector will push txn to the worker.
// TODO: maybe we can implement a pull based worker, which means the worker will
// pull txn from the conflict detector.
type worker[Txn txnEvent] interface {
	// Add adds a txnEvent to the worker.
	//
	Add(txn TxnWithNotifier[Txn]) bool
	// Out returns a channel to receive txnEvents which are ready to be executed.
	Out() <-chan TxnWithNotifier[Txn]
}

type WorkerOption struct {
	WorkerCount int
	Size        int
	IsBlock     bool
}

func newWorker[Txn txnEvent](opt WorkerOption) worker[Txn] {
	log.Info("create new worker in conflict detector",
		zap.Int("worker_count", opt.WorkerCount),
		zap.Int("size", opt.Size), zap.Bool("is_block", opt.IsBlock))
	if opt.Size == 0 {
		return &unboundedWorker[Txn]{ch: chann.NewDrainableChann[TxnWithNotifier[Txn]]()}
	}

	if opt.IsBlock {
		return &boundedWorkerWithBlock[Txn]{ch: make(chan TxnWithNotifier[Txn], opt.Size)}
	}

	return &boundedWorker[Txn]{ch: make(chan TxnWithNotifier[Txn], opt.Size)}
}

// unboundedWorker is a worker which has no limit on the number of txns it can hold.
type unboundedWorker[Txn txnEvent] struct {
	ch *chann.DrainableChann[TxnWithNotifier[Txn]]
}

func (w *unboundedWorker[Txn]) Add(txn TxnWithNotifier[Txn]) bool {
	w.ch.In() <- txn
	return true
}

func (w *unboundedWorker[Txn]) Out() <-chan TxnWithNotifier[Txn] {
	return w.ch.Out()
}

// boundedWorker is a worker which has a limit on the number of txns it can hold.
type boundedWorker[Txn txnEvent] struct {
	ch chan TxnWithNotifier[Txn]
}

func (w *boundedWorker[Txn]) Add(txn TxnWithNotifier[Txn]) bool {
	select {
	case w.ch <- txn:
		return true
	default:
		return false
	}
}

func (w *boundedWorker[Txn]) Out() <-chan TxnWithNotifier[Txn] {
	return w.ch
}

// boundedWorkerWithBlock is a special boundedWorker. Once the worker
// is full, it will block until all cacehed txns are consumed.
type boundedWorkerWithBlock[Txn txnEvent] struct {
	ch     chan TxnWithNotifier[Txn]
	isFull atomic.Bool
}

func (w *boundedWorkerWithBlock[Txn]) Add(txn TxnWithNotifier[Txn]) bool {
	if w.isFull.Load() && len(w.ch) <= 0 {
		w.isFull.Store(false)
	}

	if !w.isFull.Load() {
		select {
		case w.ch <- txn:
			return true
		default:
			w.isFull.CompareAndSwap(false, true)
		}
	}
	return false
}

func (w *boundedWorkerWithBlock[Txn]) Out() <-chan TxnWithNotifier[Txn] {
	return w.ch
}

// TODO: maybe we can implement a strategy that can automatically adapt to different scenarios
