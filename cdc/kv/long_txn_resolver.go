// Copyright 2020 PingCAP, Inc.
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

package kv

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type txnStatusEntry struct {
	txnStatus  *cdcpb.TxnStatus
	updateTime time.Time
}

// TxnStatusCache caches statuses of transactions.
type TxnStatusCache struct {
	mu              *sync.RWMutex
	m               map[uint64]txnStatusEntry
	ttl             time.Duration
	cleanUpInterval time.Duration
	stopCh          chan interface{}
}

// NewTxnStatusCache creates a new TxnStatusCache.
func NewTxnStatusCache(ttl time.Duration) *TxnStatusCache {
	return &TxnStatusCache{
		mu:              &sync.RWMutex{},
		m:               make(map[uint64]txnStatusEntry),
		ttl:             ttl,
		cleanUpInterval: time.Minute * 5,
		stopCh:          make(chan interface{}),
	}
}

// StartBackgroundCleanup starts a background goroutine that cleans up expired items in the cache
// periodically.
func (m *TxnStatusCache) StartBackgroundCleanup() {
	go func() {
		for {
			select {
			case <-m.stopCh:
				break
			case <-time.After(m.cleanUpInterval):
			}

			m.CleanExpiredEntries()
		}
	}()
}

// StopBackgroundCleanup stops the background goroutine that cleans up expired items in the cache.
// This function should only be invoked when the background goroutine is already running.
func (m *TxnStatusCache) StopBackgroundCleanup() {
	m.stopCh <- nil
}

// CleanExpiredEntries cleans up expired entries from the cache.
func (m *TxnStatusCache) CleanExpiredEntries() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for startTs, entry := range m.m {
		if time.Since(entry.updateTime) > m.ttl {
			delete(m.m, startTs)
		}
	}
}

func isOlderThan(a, b *cdcpb.TxnStatus) bool {
	// b is committed or rolled back
	if a.MinCommitTs > 0 && b.MinCommitTs == 0 {
		return true
	}
	// b has a nerwe min commit ts
	return a.MinCommitTs < b.MinCommitTs
}

// Update updates statuses of some transactions.
func (m *TxnStatusCache) Update(txnStatuses []*cdcpb.TxnStatus) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, txn := range txnStatuses {
		entry, ok := m.m[txn.GetStartTs()]
		if !ok || isOlderThan(entry.txnStatus, txn) {
			m.m[txn.GetStartTs()] = txnStatusEntry{
				txnStatus:  txn,
				updateTime: time.Now(),
			}
		}
	}
}

// Get gets statuses of a set of transactions. Returns the cached txn status and the remaining
// `TxnInfo`s that doesn't have any cached txn status.
func (m *TxnStatusCache) Get(txnInfo []*cdcpb.TxnInfo) ([]*cdcpb.TxnStatus, []*cdcpb.TxnInfo) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*cdcpb.TxnStatus, 0, len(txnInfo))
	uncachedTxnInfo := make([]*cdcpb.TxnInfo, 0)

	for _, txn := range txnInfo {
		entry, ok := m.m[txn.GetStartTs()]
		if !ok || time.Since(entry.updateTime) > m.ttl {
			uncachedTxnInfo = append(uncachedTxnInfo, txn)
		} else {
			result = append(result, entry.txnStatus)
		}
	}
	return result, uncachedTxnInfo
}

// LongTxnResolver is used to resolve long-time running transactions
type LongTxnResolver struct {
	notifyTxnStatusCh          chan notifyTxnStatusTask
	txnStatusCache             *TxnStatusCache
	kvStorage                  tikv.Storage
	id                         string
	notifyTxnStatusChSizeGauge prometheus.Gauge
}

// NewLongTxnResolver creates a new LongTxnResolver
func NewLongTxnResolver(
	id string,
	kvStorage tikv.Storage,
	notifyTxnStatusCh chan notifyTxnStatusTask,
	cacheTTL time.Duration,
) *LongTxnResolver {
	return &LongTxnResolver{
		id:                         id,
		notifyTxnStatusCh:          notifyTxnStatusCh,
		txnStatusCache:             NewTxnStatusCache(cacheTTL),
		kvStorage:                  kvStorage,
		notifyTxnStatusChSizeGauge: clientChannelSize.WithLabelValues(id, "notify-txn-status"),
	}
}

// Start starts running background tasks of the LongTxnResolver
func (m *LongTxnResolver) Start() {
	m.txnStatusCache.StartBackgroundCleanup()
}

// Stop stops running background tasks of the LongTxnResolver
func (m *LongTxnResolver) Stop() {
	m.txnStatusCache.StopBackgroundCleanup()
}

func (m *LongTxnResolver) scheduleNotifyTxnStatus(
	ctx context.Context,
	regionID uint64,
	txnStatuses []*cdcpb.TxnStatus,
	blocking bool,
) {
	task := notifyTxnStatusTask{
		regionID:  regionID,
		txnStatus: txnStatuses,
	}
	if blocking {
		select {
		case m.notifyTxnStatusCh <- task:
			m.notifyTxnStatusChSizeGauge.Inc()
		case <-ctx.Done():
		}
	} else {
		select {
		case m.notifyTxnStatusCh <- task:
			m.notifyTxnStatusChSizeGauge.Inc()
			return
		default:
		}
		go func() {
			select {
			case m.notifyTxnStatusCh <- task:
				m.notifyTxnStatusChSizeGauge.Inc()
			case <-ctx.Done():
			}
		}()
	}
}

// Resolve resolves the given transactions. It first checks cached txn statuses and then access TiKV
// to get the transactions' status. Then it sends tasks to the notifyTxnStatusCh, whose receiver
// should be responsible to handle the task and send messages to TiKV.
func (m *LongTxnResolver) Resolve(ctx context.Context, regionID uint64, txns []*cdcpb.TxnInfo) error {
	txnStatuses, remainingTxns := m.txnStatusCache.Get(txns)

	log.Info("tikv reported long live transactions. Try to resolve.",
		zap.Uint64("regionID", regionID),
		zap.Int("txns", len(txns)),
		zap.Int("cached", len(txnStatuses)),
		zap.Int("uncached", len(remainingTxns)))

	if len(txnStatuses) > 0 {
		m.scheduleNotifyTxnStatus(ctx, regionID, txnStatuses, false)
	}
	if len(remainingTxns) == 0 {
		return nil
	}

	// Access TiKV to get the status of these transactions.
	currentVersion, err := m.kvStorage.CurrentVersion()
	if err != nil {
		return errors.Trace(err)
	}
	now := currentVersion.Ver

	txnStatuses = make([]*cdcpb.TxnStatus, 0, len(remainingTxns))
	var err1 error = nil
ForEachTxn:
	for _, txn := range remainingTxns {
		status, err := m.kvStorage.GetLockResolver().GetTxnStatus(txn.GetStartTs(), now, txn.GetPrimary())
		if err != nil {
			err1 = errors.Trace(err)
			break
		}

		convertedStatus := &cdcpb.TxnStatus{
			StartTs:      txn.GetStartTs(),
			IsRolledBack: status.TTL() == 0 && status.CommitTS() == 0,
			CommitTs:     status.CommitTS(),
		}

		switch status.Action() {
		case kvrpcpb.Action_MinCommitTSPushed:
			convertedStatus.MinCommitTs = now
		case kvrpcpb.Action_NoAction:
			if status.TTL() != 0 {
				log.Debug("a transaction is still running and cannot be pushed. Cannot resolve"+
					"the transaction",
					zap.Uint64("regionID", regionID),
					zap.Uint64("startTs", txn.GetStartTs()))
				continue ForEachTxn
			}
		default:
			log.Error("unsupported action returned by CheckTxnStatus. Do not resolve the transaction",
				zap.Uint64("regionID", regionID),
				zap.Uint64("startTs", txn.GetStartTs()))
			continue ForEachTxn
		case kvrpcpb.Action_LockNotExistRollback:
		case kvrpcpb.Action_TTLExpireRollback:
		}

		txnStatuses = append(txnStatuses, convertedStatus)

		// TODO: Otherwise, we can actually do resolvelocks here.
	}

	if len(txnStatuses) > 0 {
		m.txnStatusCache.Update(txnStatuses)
		m.scheduleNotifyTxnStatus(ctx, regionID, txnStatuses, false)
	}

	return err1
}
