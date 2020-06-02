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
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/cdcpb"
)

type txnStatusEntry struct {
	minCommitTs uint64
	updateTime  time.Time
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

// Update updates statuses of some transactions.
func (m *TxnStatusCache) Update(txnStatuses []*cdcpb.TxnStatus) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, txn := range txnStatuses {
		entry, ok := m.m[txn.GetStartTs()]
		if !ok || entry.minCommitTs < txn.GetMinCommitTs() {
			m.m[txn.GetStartTs()] = txnStatusEntry{
				minCommitTs: txn.GetMinCommitTs(),
				updateTime:  time.Now(),
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
			result = append(result, &cdcpb.TxnStatus{
				StartTs:     txn.GetStartTs(),
				MinCommitTs: entry.minCommitTs,
			})
		}
	}
	return result, uncachedTxnInfo
}
