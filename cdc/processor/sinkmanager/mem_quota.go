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

	"github.com/pingcap/tiflow/cdc/model"
)

type memConsumeRecord struct {
	resolvedTs model.ResolvedTs
	size       uint64
}

type changefeedMemQuota struct {
	changefeedID model.ChangeFeedID
	// totalBytes is the total memory quota for one changefeed.
	totalBytes uint64

	// mu protects the following fields.
	mu sync.Mutex
	// usedBytes is the memory usage of one changefeed.
	usedBytes uint64
	// tableMemory is the memory usage of each table.
	tableMemory map[model.TableID][]*memConsumeRecord
}

func newMemQuota(changefeedID model.ChangeFeedID, totalBytes uint64) *changefeedMemQuota {
	return &changefeedMemQuota{
		changefeedID: changefeedID,
		totalBytes:   totalBytes,
		usedBytes:    0,
		tableMemory:  make(map[model.TableID][]*memConsumeRecord),
	}
}

func (m *changefeedMemQuota) tryAcquire(nBytes uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.usedBytes+nBytes > m.totalBytes {
		return false
	}
	m.usedBytes += nBytes
	return true
}

func (m *changefeedMemQuota) refund(nBytes uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.usedBytes -= nBytes
}

func (m *changefeedMemQuota) record(tableID model.TableID, resolved model.ResolvedTs, size uint64) {
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

func (m *changefeedMemQuota) hasAvailable(nBytes uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.usedBytes+nBytes <= m.totalBytes
}

func (m *changefeedMemQuota) release(tableID model.TableID, resolved model.ResolvedTs) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tableMemory[tableID]; !ok {
		// TODO log error
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
	m.tableMemory[tableID] = append(make([]*memConsumeRecord, 0, len(records[i:])), records[i:]...)
}
