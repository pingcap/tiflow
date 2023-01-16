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

package sinkmanager

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type memConsumeRecord struct {
	resolvedTs model.ResolvedTs
	size       uint64
}

type memQuota struct {
	changefeedID model.ChangeFeedID
	// totalBytes is the total memory quota for one changefeed.
	totalBytes uint64

	// blockAcquireCond is used to notify the blocked acquire.
	blockAcquireCond *sync.Cond

	// mu protects the following fields.
	mu sync.Mutex
	// usedBytes is the memory usage of one changefeed.
	usedBytes uint64
	// tableMemory is the memory usage of each table.
	tableMemory *spanz.HashMap[[]*memConsumeRecord]
	// isClosed is used to indicate whether the mem quota is closed.
	isClosed atomic.Bool

	metricTotal prometheus.Gauge
	metricUsed  prometheus.Gauge
}

func newMemQuota(changefeedID model.ChangeFeedID, totalBytes uint64, comp string) *memQuota {
	m := &memQuota{
		changefeedID: changefeedID,
		totalBytes:   totalBytes,
		usedBytes:    0,
		tableMemory:  spanz.NewHashMap[[]*memConsumeRecord](),
		metricTotal:  MemoryQuota.WithLabelValues(changefeedID.Namespace, changefeedID.ID, "total", comp),
		metricUsed:   MemoryQuota.WithLabelValues(changefeedID.Namespace, changefeedID.ID, "used", comp),
	}
	m.blockAcquireCond = sync.NewCond(&m.mu)
	m.metricTotal.Set(float64(totalBytes))
	m.metricUsed.Set(float64(0))

	log.Info("New memory quota",
		zap.String("namespace", changefeedID.Namespace),
		zap.String("changefeed", changefeedID.ID),
		zap.Uint64("total", totalBytes))
	return m
}

// tryAcquire returns true if the memory quota is available, otherwise returns false.
func (m *memQuota) tryAcquire(nBytes uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.usedBytes+nBytes > m.totalBytes {
		return false
	}
	m.usedBytes += nBytes
	m.metricUsed.Set(float64(m.usedBytes))
	return true
}

// forceAcquire is used to force acquire the memory quota.
func (m *memQuota) forceAcquire(nBytes uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.usedBytes += nBytes
	m.metricUsed.Set(float64(m.usedBytes))
}

// blockAcquire is used to block the request when the memory quota is not available.
func (m *memQuota) blockAcquire(nBytes uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for {
		if m.isClosed.Load() {
			return cerrors.ErrFlowControllerAborted.GenWithStackByArgs()
		}

		if m.usedBytes+nBytes <= m.totalBytes {
			m.usedBytes += nBytes
			m.metricUsed.Set(float64(m.usedBytes))
			return nil
		}
		m.blockAcquireCond.Wait()
	}
}

// refund directly release the memory quota.
func (m *memQuota) refund(nBytes uint64) {
	if nBytes == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.usedBytes < nBytes {
		log.Panic("memQuota.refund fail",
			zap.Uint64("used", m.usedBytes), zap.Uint64("refund", nBytes))
	}
	m.usedBytes -= nBytes
	m.metricUsed.Set(float64(m.usedBytes))
	if m.usedBytes < m.totalBytes {
		m.blockAcquireCond.Broadcast()
	}
}

func (m *memQuota) addTable(span tablepb.Span) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tableMemory.ReplaceOrInsert(span, make([]*memConsumeRecord, 0, 2))
}

// record records the memory usage of a table.
func (m *memQuota) record(span tablepb.Span, resolved model.ResolvedTs, nBytes uint64) {
	if nBytes == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tableMemory.Get(span); !ok {
		// Can't find the table record, the table must be removed.
		if m.usedBytes < nBytes {
			log.Panic("memQuota.refund fail",
				zap.Uint64("used", m.usedBytes), zap.Uint64("refund", nBytes))
		}
		m.usedBytes -= nBytes
		m.metricUsed.Set(float64(m.usedBytes))
		if m.usedBytes < m.totalBytes {
			m.blockAcquireCond.Broadcast()
		}
		return
	}
	m.tableMemory.ReplaceOrInsert(span, append(m.tableMemory.GetV(span), &memConsumeRecord{
		resolvedTs: resolved,
		size:       nBytes,
	}))
}

// release try to use resolvedTs to release the memory quota.
// Because we append records in order, we can use binary search to find the first record
// that is greater than resolvedTs, and release the memory quota of the records before it.
func (m *memQuota) release(span tablepb.Span, resolved model.ResolvedTs) {
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
		return records[i].resolvedTs.Greater(resolved)
	})
	var toRelease uint64 = 0
	for j := 0; j < i; j++ {
		toRelease += records[j].size
	}
	m.tableMemory.ReplaceOrInsert(span, records[i:])
	if toRelease == 0 {
		return
	}
	if m.usedBytes < toRelease {
		log.Panic("memQuota.release fail",
			zap.Uint64("used", m.usedBytes), zap.Uint64("release", toRelease))
	}
	m.usedBytes -= toRelease
	m.metricUsed.Set(float64(m.usedBytes))
	if m.usedBytes < m.totalBytes {
		m.blockAcquireCond.Broadcast()
	}
}

// clean all records of the table.
// Return the cleaned memory quota.
func (m *memQuota) clean(span tablepb.Span) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

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
		cleaned += record.size
	}
	m.usedBytes -= cleaned
	m.metricUsed.Set(float64(m.usedBytes))
	m.tableMemory.Delete(span)
	if m.usedBytes < m.totalBytes {
		m.blockAcquireCond.Broadcast()
	}
	return cleaned
}

// close the mem quota and notify the blocked acquire.
func (m *memQuota) close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	// NOTE: m.usedBytes is not reset, because refund can still be called after closed.
	m.tableMemory = spanz.NewHashMap[[]*memConsumeRecord]()
	m.metricUsed.Set(float64(0))
	m.isClosed.Store(true)
	m.blockAcquireCond.Broadcast()
}

// getUsedBytes returns the used memory quota.
func (m *memQuota) getUsedBytes() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.usedBytes
}

// hasAvailable returns true if the memory quota is available, otherwise returns false.
func (m *memQuota) hasAvailable(nBytes uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.usedBytes+nBytes <= m.totalBytes
}
