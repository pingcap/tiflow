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

package memquota

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// MemConsumeRecord is used to trace memory usage.
type MemConsumeRecord struct {
	ResolvedTs model.ResolvedTs
	Size       uint64
}

// MemQuota is used to trace memory usage.
type MemQuota struct {
	changefeedID model.ChangeFeedID
	// totalBytes is the total memory quota for one changefeed.
	totalBytes uint64

	// usedBytes is the memory usage of one changefeed.
	usedBytes atomic.Uint64

	// isClosed is used to indicate whether the mem quota is closed.
	isClosed atomic.Bool

	// To close the background metrics goroutine.
	wg      sync.WaitGroup
	closeBg chan struct{}

	// blockAcquireCond is used to notify the blocked acquire.
	blockAcquireCond *sync.Cond

	metricTotal prometheus.Gauge
	metricUsed  prometheus.Gauge

	// mu protects the following fields.
	mu sync.Mutex
	// tableMemory is the memory usage of each table.
	tableMemory *spanz.HashMap[[]*MemConsumeRecord]
}

// NewMemQuota creates a MemQuota instance.
func NewMemQuota(changefeedID model.ChangeFeedID, totalBytes uint64, comp string) *MemQuota {
	m := &MemQuota{
		changefeedID:     changefeedID,
		totalBytes:       totalBytes,
		blockAcquireCond: sync.NewCond(&sync.Mutex{}),
		metricTotal: MemoryQuota.WithLabelValues(changefeedID.Namespace,
			changefeedID.ID, "total", comp),
		metricUsed: MemoryQuota.WithLabelValues(changefeedID.Namespace,
			changefeedID.ID, "used", comp),
		closeBg: make(chan struct{}, 1),

		tableMemory: spanz.NewHashMap[[]*MemConsumeRecord](),
	}
	m.metricTotal.Set(float64(totalBytes))
	m.metricUsed.Set(float64(0))

	log.Info("New memory quota",
		zap.String("namespace", changefeedID.Namespace),
		zap.String("changefeed", changefeedID.ID),
		zap.Uint64("total", totalBytes))

	m.wg.Add(1)
	go func() {
		timer := time.NewTicker(3 * time.Second)
		defer func() {
			timer.Stop()
			MemoryQuota.DeleteLabelValues(changefeedID.Namespace, changefeedID.ID, "total", comp)
			MemoryQuota.DeleteLabelValues(changefeedID.Namespace, changefeedID.ID, "used", comp)
		}()
		for {
			select {
			case <-timer.C:
				m.metricUsed.Set(float64(m.usedBytes.Load()))
			case <-m.closeBg:
				m.metricUsed.Set(0.0)
				m.wg.Done()
				return
			}
		}
	}()
	return m
}

// TryAcquire returns true if the memory quota is available, otherwise returns false.
func (m *MemQuota) TryAcquire(nBytes uint64) bool {
	for {
		usedBytes := m.usedBytes.Load()
		if usedBytes+nBytes > m.totalBytes {
			return false
		}
		if m.usedBytes.CompareAndSwap(usedBytes, usedBytes+nBytes) {
			return true
		}
	}
}

// ForceAcquire is used to force acquire the memory quota.
func (m *MemQuota) ForceAcquire(nBytes uint64) {
	m.usedBytes.Add(nBytes)
}

// BlockAcquire is used to block the request when the memory quota is not available.
func (m *MemQuota) BlockAcquire(nBytes uint64) error {
	for {
		if m.isClosed.Load() {
			return context.Canceled
		}
		usedBytes := m.usedBytes.Load()
		if usedBytes+nBytes > m.totalBytes {
			m.blockAcquireCond.L.Lock()
			m.blockAcquireCond.Wait()
			m.blockAcquireCond.L.Unlock()
			continue
		}
		if m.usedBytes.CompareAndSwap(usedBytes, usedBytes+nBytes) {
			return nil
		}
	}
}

// Refund directly release the memory quota.
func (m *MemQuota) Refund(nBytes uint64) {
	if nBytes == 0 {
		return
	}
	usedBytes := m.usedBytes.Load()
	if usedBytes < nBytes {
		log.Panic("MemQuota.refund fail",
			zap.Uint64("used", usedBytes), zap.Uint64("refund", nBytes))
	}
	if m.usedBytes.Add(^(nBytes - 1)) < m.totalBytes {
		m.blockAcquireCond.Broadcast()
	}
}

// AddTable adds a table into the quota.
func (m *MemQuota) AddTable(span tablepb.Span) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tableMemory.ReplaceOrInsert(span, make([]*MemConsumeRecord, 0, 2))
}

// Record records the memory usage of a table.
func (m *MemQuota) Record(span tablepb.Span, resolved model.ResolvedTs, nBytes uint64) {
	if nBytes == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tableMemory.Get(span); !ok {
		// Can't find the table record, the table must be removed.
		usedBytes := m.usedBytes.Load()
		if usedBytes < nBytes {
			log.Panic("MemQuota.record fail",
				zap.Uint64("used", usedBytes), zap.Uint64("record", nBytes))
		}
		// If we cannot find the table, then the previous acquired memory quota needed to be returned.
		// Note that "usedBytes.Add(^(nBytes - 1))" means "usedBytes.Sub(nBytes)". But atomic don't
		// have Sub method.
		if m.usedBytes.Add(^(nBytes - 1)) < m.totalBytes {
			m.blockAcquireCond.Broadcast()
		}
		return
	}
	m.tableMemory.ReplaceOrInsert(span, append(m.tableMemory.GetV(span), &MemConsumeRecord{
		ResolvedTs: resolved,
		Size:       nBytes,
	}))
}

// Release try to use resolvedTs to release the memory quota.
// Because we append records in order, we can use binary search to find the first record
// that is greater than resolvedTs, and release the memory quota of the records before it.
func (m *MemQuota) Release(span tablepb.Span, resolved model.ResolvedTs) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tableMemory.Get(span); !ok {
		// This can happen when
		// 1. the table has no data and never been recorded.
		// 2. the table is in async removing.
		return
	}
	records := m.tableMemory.GetV(span)
	i := sort.Search(len(records), func(i int) bool {
		return records[i].ResolvedTs.Greater(resolved)
	})
	var toRelease uint64 = 0
	for j := 0; j < i; j++ {
		toRelease += records[j].Size
	}
	m.tableMemory.ReplaceOrInsert(span, records[i:])
	if toRelease == 0 {
		return
	}

	usedBytes := m.usedBytes.Load()
	if usedBytes < toRelease {
		log.Panic("MemQuota.release fail",
			zap.Uint64("used", usedBytes), zap.Uint64("release", toRelease))
	}
	if m.usedBytes.Add(^(toRelease - 1)) < m.totalBytes {
		m.blockAcquireCond.Broadcast()
	}
}

// RemoveTable clears all records of the table and remove the table.
// Return the cleaned memory quota.
func (m *MemQuota) RemoveTable(span tablepb.Span) uint64 {
	m.mu.Lock()
	cleaned := m.clear(span)
	m.tableMemory.Delete(span)
	m.mu.Unlock()
	return cleaned
}

// ClearTable is like RemoveTable but only clear the memory usage records but doesn't
// remove the table.
func (m *MemQuota) ClearTable(span tablepb.Span) uint64 {
	m.mu.Lock()
	cleaned := m.clear(span)
	m.tableMemory.ReplaceOrInsert(span, make([]*MemConsumeRecord, 0, 2))
	m.mu.Unlock()
	return cleaned
}

func (m *MemQuota) clear(span tablepb.Span) uint64 {
	if _, ok := m.tableMemory.Get(span); !ok {
		// This can happen when the table has no data and never been recorded.
		log.Warn("Table consumed memory records not found",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Stringer("span", &span))
		return 0
	}

	cleaned := uint64(0)
	records := m.tableMemory.GetV(span)
	for _, record := range records {
		cleaned += record.Size
	}

	if m.usedBytes.Add(^(cleaned - 1)) < m.totalBytes {
		m.blockAcquireCond.Broadcast()
	}
	return cleaned
}

// Close the mem quota and notify the blocked acquire.
func (m *MemQuota) Close() {
	if m.isClosed.CompareAndSwap(false, true) {
		m.blockAcquireCond.Broadcast()
		close(m.closeBg)
		m.wg.Wait()
	}
}

// GetUsedBytes returns the used memory quota.
func (m *MemQuota) GetUsedBytes() uint64 {
	return m.usedBytes.Load()
}

// hasAvailable returns true if the memory quota is available, otherwise returns false.
func (m *MemQuota) hasAvailable(nBytes uint64) bool {
	return m.usedBytes.Load()+nBytes <= m.totalBytes
}
