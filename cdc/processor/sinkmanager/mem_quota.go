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
	cerrors "github.com/pingcap/tiflow/pkg/errors"
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

	// mu protects the following fields.
	mu sync.Mutex
	// usedBytes is the memory usage of one changefeed.
	usedBytes uint64
	// tableMemory is the memory usage of each table.
	tableMemory map[model.TableID][]*memConsumeRecord
	// isClosed is used to indicate whether the mem quota is closed.
	isClosed atomic.Bool
	// blockAcquireCond is used to notify the blocked acquire.
	blockAcquireCond *sync.Cond

	metricTotal prometheus.Gauge
	metricUsed  prometheus.Gauge
}

func newMemQuota(changefeedID model.ChangeFeedID, totalBytes uint64) *memQuota {
	m := &memQuota{
		changefeedID: changefeedID,
		totalBytes:   totalBytes,
		usedBytes:    0,
		tableMemory:  make(map[model.TableID][]*memConsumeRecord),
		metricTotal:  MemoryQuota.WithLabelValues(changefeedID.Namespace, changefeedID.ID, "total"),
		metricUsed:   MemoryQuota.WithLabelValues(changefeedID.Namespace, changefeedID.ID, "used"),
	}
	m.blockAcquireCond = sync.NewCond(&m.mu)
	m.metricTotal.Set(float64(totalBytes))
	m.metricUsed.Set(float64(0))

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
	m.usedBytes -= nBytes
	m.metricUsed.Set(float64(m.usedBytes))
	m.blockAcquireCond.Signal()
}

// record records the memory usage of a table.
func (m *memQuota) record(tableID model.TableID, resolved model.ResolvedTs, size uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tableMemory[tableID]; !ok {
		m.tableMemory[tableID] = make([]*memConsumeRecord, 0, 2)
	}
	m.tableMemory[tableID] = append(m.tableMemory[tableID], &memConsumeRecord{
		resolvedTs: resolved,
		size:       size,
	})
}

// hasAvailable returns true if the memory quota is available, otherwise returns false.
func (m *memQuota) hasAvailable(nBytes uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.usedBytes+nBytes <= m.totalBytes
}

// release try to use resolvedTs to release the memory quota.
// Because we append records in order, we can use binary search to find the first record
// that is greater than resolvedTs, and release the memory quota of the records before it.
func (m *memQuota) release(tableID model.TableID, resolved model.ResolvedTs) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tableMemory[tableID]; !ok {
		// This can happen when
		// 1. the table has no data and never been recorded.
		// 2. the table is in async removing.
		return
	}
	records := m.tableMemory[tableID]
	i := sort.Search(len(records), func(i int) bool {
		return records[i].resolvedTs.Greater(resolved)
	})
	if i == 0 {
		return
	}
	for j := 0; j < i; j++ {
		m.usedBytes -= records[j].size
	}
	m.metricUsed.Set(float64(m.usedBytes))
	m.tableMemory[tableID] = append(make([]*memConsumeRecord, 0, len(records[i:])), records[i:]...)

	if m.usedBytes < m.totalBytes {
		m.blockAcquireCond.Signal()
	}
}

// clean all records of the table.
// Return the cleaned memory quota.
func (m *memQuota) clean(tableID model.TableID) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.tableMemory[tableID]; !ok {
		// This can happen when the table has no data and never been recorded.
		log.Warn("Table consumed memory records not found",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Int64("tableID", tableID))
		return 0
	}
	cleaned := uint64(0)
	records := m.tableMemory[tableID]
	for _, record := range records {
		cleaned += record.size
	}
	m.usedBytes -= cleaned
	m.metricUsed.Set(float64(m.usedBytes))
	delete(m.tableMemory, tableID)
	if m.usedBytes < m.totalBytes {
		m.blockAcquireCond.Broadcast()
	}
	return cleaned
}

// close the mem quota and notify the blocked acquire.
func (m *memQuota) close() {
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
