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
	"unsafe"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type matchKey struct {
	startTs uint64
	key     string
}

func newMatchKey(row *cdcpb.Event_Row) matchKey {
	return matchKey{startTs: row.GetStartTs(), key: string(row.GetKey())}
}

func (m *matchKey) size() int {
	const sizeOfEmptyMatchKey = int(unsafe.Sizeof(matchKey{}))
	return sizeOfEmptyMatchKey + len(m.key)
}

func sizeOfEventRow(row *cdcpb.Event_Row) int {
	const sizeOfEmptyRow = int(unsafe.Sizeof(cdcpb.Event_Row{}))
	return sizeOfEmptyRow + len(row.Key) + len(row.Value) + len(row.OldValue)
}

type matcher struct {
	// TODO : clear the single prewrite
	unmatchedValue map[matchKey]*cdcpb.Event_Row
	cachedCommit   []*cdcpb.Event_Row

	unmatchedValueBytes int
}

func newMatcher() *matcher {
	return &matcher{
		unmatchedValue: make(map[matchKey]*cdcpb.Event_Row),
	}
}

func (m *matcher) putPrewriteRow(row *cdcpb.Event_Row) {
	key := newMatchKey(row)
	// tikv may send a fake prewrite event with empty value caused by txn heartbeat.
	// here we need to avoid the fake prewrite event overwrite the prewrite value.

	// when the old-value is disabled, the value of the fake prewrite event is empty.
	// when the old-value is enabled, the value of the fake prewrite event is also empty,
	// but the old value of the fake prewrite event is not empty.
	// We can distinguish fake prewrite events by whether the value is empty,
	// no matter the old-value is enabled or disabled
	if _, exist := m.unmatchedValue[key]; exist && len(row.GetValue()) == 0 {
		return
	}
	m.unmatchedValue[key] = row
	m.unmatchedValueBytes += (key.size() + sizeOfEventRow(row))
}

// matchRow matches the commit event with the cached prewrite event
// the Value and OldValue will be assigned if a matched prewrite event exists.
func (m *matcher) matchRow(row *cdcpb.Event_Row) bool {
	key := newMatchKey(row)
	if value, exist := m.unmatchedValue[key]; exist {
		row.Value = value.GetValue()
		row.OldValue = value.GetOldValue()
		delete(m.unmatchedValue, newMatchKey(row))
		m.unmatchedValueBytes -= (key.size() + sizeOfEventRow(value))
		if m.unmatchedValueBytes < 0 {
			m.unmatchedValueBytes = 0
		}
		return true
	}
	return false
}

func (m *matcher) cacheCommitRow(row *cdcpb.Event_Row) {
	m.cachedCommit = append(m.cachedCommit, row)
}

func (m *matcher) matchCachedRow() []*cdcpb.Event_Row {
	cachedCommit := m.cachedCommit
	m.cachedCommit = nil
	top := 0
	for i := 0; i < len(cachedCommit); i++ {
		cacheEntry := cachedCommit[i]
		ok := m.matchRow(cacheEntry)
		if !ok {
			// when cdc receives a commit log without a corresponding
			// prewrite log before initialized, a committed log  with
			// the same key and start-ts must have been received.
			log.Info("ignore commit event without prewrite",
				zap.Binary("key", cacheEntry.GetKey()),
				zap.Uint64("ts", cacheEntry.GetStartTs()))
			continue
		}
		cachedCommit[top] = cacheEntry
		top++
	}
	return cachedCommit[:top]
}

func (m *matcher) rollbackRow(row *cdcpb.Event_Row) {
	key := newMatchKey(row)
	row, ok := m.unmatchedValue[key]
	if ok {
		delete(m.unmatchedValue, key)
		m.unmatchedValueBytes -= (key.size() + sizeOfEventRow(row))
		if m.unmatchedValueBytes < 0 {
			m.unmatchedValueBytes = 0
		}
	}
}
