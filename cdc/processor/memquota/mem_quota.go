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
	"sort"
	"sync"
	"sync/atomic"
	"time"

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

type MemQuota struct {
	changefeedID model.ChangeFeedID
	// totalBytes is the total memory quota for one changefeed.
	totalBytes uint64

	// usedBytes is the memory usage of one changefeed. It's used as an atomic.
	usedBytes uint64

	// isClosed is used to indicate whether the mem quota is closed.
	isClosed atomic.Bool

	// To close the background metrics goroutine.
	wg      sync.WaitGroup
	closeBg chan struct{}

	// blockAcquireCond is used to notify the blocked acquire.
	blockAcquireCond *sync.Cond
	condMu           sync.Mutex

	metricTotal prometheus.Gauge
	metricUsed  prometheus.Gauge

	// mu protects the following fields.
	mu sync.Mutex
	// tableMemory is the memory usage of each table.
	tableMemory *spanz.HashMap[[]*memConsumeRecord]
}

func NewMemQuota(changefeedID model.ChangeFeedID, totalBytes uint64, comp string) *MemQuota {
	m := &MemQuota{
		changefeedID: changefeedID,
		totalBytes:   totalBytes,
		usedBytes:    0,
		metricTotal:  MemoryQuota.WithLabelValues(changefeedID.Namespace, changefeedID.ID, "total", comp),
		metricUsed:   MemoryQuota.WithLabelValues(changefeedID.Namespace, changefeedID.ID, "used", comp),
		closeBg:      make(chan struct{}, 1),

		tableMemory: spanz.NewHashMap[[]*memConsumeRecord](),
	}
	m.blockAcquireCond = sync.NewCond(&m.condMu)
	m.metricTotal.Set(float64(totalBytes))
	m.metricUsed.Set(float64(0))

	log.Info("New memory quota",
		zap.String("namespace", changefeedID.Namespace),
		zap.String("changefeed", changefeedID.ID),
		zap.Uint64("total", totalBytes))

	m.wg.Add(1)
	go func() {
		timer := time.NewTicker(3 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				m.metricUsed.Set(float64(atomic.LoadUint64(&m.usedBytes)))
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
		usedBytes := atomic.LoadUint64(&m.usedBytes)
		if usedBytes+nBytes > m.totalBytes {
			return false
		}
		if atomic.CompareAndSwapUint64(&m.usedBytes, usedBytes, usedBytes+nBytes) {
			return true
		}
	}
}

// ForceAcquire is used to force acquire the memory quota.
func (m *MemQuota) ForceAcquire(nBytes uint64) {
	atomic.AddUint64(&m.usedBytes, nBytes)
	return
}

// BlockAcquire is used to block the request when the memory quota is not available.
func (m *MemQuota) BlockAcquire(nBytes uint64) error {
	for {
		if m.isClosed.Load() {
			return cerrors.ErrFlowControllerAborted.GenWithStackByArgs()
		}
		usedBytes := atomic.LoadUint64(&m.usedBytes)
		if usedBytes+nBytes > m.totalBytes {
			m.condMu.Lock()
			m.blockAcquireCond.Wait()
			m.condMu.Unlock()
			continue
		}
		if atomic.CompareAndSwapUint64(&m.usedBytes, usedBytes, usedBytes+nBytes) {
			return nil
		}
	}
}

// Refund directly release the memory quota.
func (m *MemQuota) Refund(nBytes uint64) {
	if nBytes == 0 {
		return
	}
	usedBytes := atomic.LoadUint64(&m.usedBytes)
	if usedBytes < nBytes {
		log.Panic("MemQuota.refund fail",
			zap.Uint64("used", m.usedBytes), zap.Uint64("refund", nBytes))
	}
	if atomic.AddUint64(&m.usedBytes, ^(nBytes-1)) < m.totalBytes {
		m.blockAcquireCond.Broadcast()
	}
}

// AddTable adds a table into the quota.
func (m *MemQuota) AddTable(span tablepb.Span) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tableMemory.ReplaceOrInsert(span, make([]*memConsumeRecord, 0, 2))
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
		usedBytes := atomic.LoadUint64(&m.usedBytes)
		if usedBytes < nBytes {
			log.Panic("MemQuota.refund fail",
				zap.Uint64("used", m.usedBytes), zap.Uint64("refund", nBytes))
		}
		if atomic.AddUint64(&m.usedBytes, ^(nBytes-1)) < m.totalBytes {
			m.blockAcquireCond.Broadcast()
		}
		return
	}
	m.tableMemory.ReplaceOrInsert(span, append(m.tableMemory.GetV(span), &memConsumeRecord{
		resolvedTs: resolved,
		size:       nBytes,
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

	usedBytes := atomic.LoadUint64(&m.usedBytes)
	if usedBytes < toRelease {
		log.Panic("MemQuota.release fail",
			zap.Uint64("used", m.usedBytes), zap.Uint64("release", toRelease))
	}
	if atomic.AddUint64(&m.usedBytes, ^(toRelease-1)) < m.totalBytes {
		m.blockAcquireCond.Broadcast()
	}
}

// Clean all records of the table.
// Return the cleaned memory quota.
func (m *MemQuota) Clean(span tablepb.Span) uint64 {
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
	m.tableMemory.Delete(span)

	if atomic.AddUint64(&m.usedBytes, ^(cleaned-1)) < m.totalBytes {
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
	return atomic.LoadUint64(&m.usedBytes)
}

// hasAvailable returns true if the memory quota is available, otherwise returns false.
func (m *MemQuota) hasAvailable(nBytes uint64) bool {
	return atomic.LoadUint64(&m.usedBytes) < m.totalBytes
}

// MemoryQuota indicates memory usage of a changefeed.
var MemoryQuota = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sinkmanager",
		Name:      "memory_quota",
		Help:      "memory quota of the changefeed",
	},
	// type includes total, used, component includes sink and redo.
	[]string{"namespace", "changefeed", "type", "component"})

// InitMetrics registers all metrics in this file.
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(MemoryQuota)
}
