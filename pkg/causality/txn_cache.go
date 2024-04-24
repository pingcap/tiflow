// Copyright 2024 PingCAP, Inc.
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

const (
	// BlockStrategyWaitAvailable means the cache will block until there is an available slot.
	BlockStrategyWaitAvailable BlockStrategy = "waitAvailable"
	// BlockStrategyWaitEmpty means the cache will block until all cached txns are consumed.
	BlockStrategyWaitEmpty = "waitEmpty"
	// TODO: maybe we can implement a strategy that can automatically adapt to different scenarios
)

// BlockStrategy is the strategy to handle the situation when the cache is full.
type BlockStrategy string

type txnEvent interface {
	// OnConflictResolved is called when the event leaves ConflictDetector.
	OnConflictResolved()
	// ConflictKeys returns the keys that the event conflicts with.
	ConflictKeys() []uint64
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

// TxnCacheOption is the option for creating a cache for resolved txns.
type TxnCacheOption struct {
	// Count controls the number of caches, txns in different caches could be executed concurrently.
	Count int
	// Size controls the max number of txns a cache can hold.
	Size int
	// BlockStrategy controls the strategy when the cache is full.
	BlockStrategy BlockStrategy
}

// In current implementation, the conflict detector will push txn to the txnCache.
type txnCache[Txn txnEvent] interface {
	// add adds a event to the Cache.
	add(txn TxnWithNotifier[Txn]) bool
	// out returns a channel to receive events which are ready to be executed.
	out() <-chan TxnWithNotifier[Txn]
}

func newTxnCache[Txn txnEvent](opt TxnCacheOption) txnCache[Txn] {
	log.Info("create new worker cache in conflict detector",
		zap.Int("cacheCount", opt.Count),
		zap.Int("cacheSize", opt.Size), zap.String("BlockStrategy", string(opt.BlockStrategy)))
	if opt.Size <= 0 {
		log.Panic("WorkerOption.CacheSize should be greater than 0, please report a bug")
	}

	switch opt.BlockStrategy {
	case BlockStrategyWaitAvailable:
		return &boundedTxnCache[Txn]{ch: make(chan TxnWithNotifier[Txn], opt.Size)}
	case BlockStrategyWaitEmpty:
		return &boundedTxnCacheWithBlock[Txn]{ch: make(chan TxnWithNotifier[Txn], opt.Size)}
	default:
		return nil
	}
}

// boundedTxnCache is a cache which has a limit on the number of txns it can hold.
//
//nolint:unused
type boundedTxnCache[Txn txnEvent] struct {
	ch chan TxnWithNotifier[Txn]
}

//nolint:unused
func (w *boundedTxnCache[Txn]) add(txn TxnWithNotifier[Txn]) bool {
	select {
	case w.ch <- txn:
		return true
	default:
		return false
	}
}

//nolint:unused
func (w *boundedTxnCache[Txn]) out() <-chan TxnWithNotifier[Txn] {
	return w.ch
}

// boundedTxnCacheWithBlock is a special boundedWorker. Once the cache
// is full, it will block until all cached txns are consumed.
type boundedTxnCacheWithBlock[Txn txnEvent] struct {
	ch chan TxnWithNotifier[Txn]
	//nolint:unused
	isBlocked atomic.Bool
}

//nolint:unused
func (w *boundedTxnCacheWithBlock[Txn]) add(txn TxnWithNotifier[Txn]) bool {
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

//nolint:unused
func (w *boundedTxnCacheWithBlock[Txn]) out() <-chan TxnWithNotifier[Txn] {
	return w.ch
}
