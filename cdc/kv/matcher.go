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
	"github.com/pingcap/kvproto/pkg/cdcpb"
)

type pendingValue struct {
	value    []byte
	oldValue []byte
}

type matcher struct {
	// TODO : clear the single prewrite
	unmatchedValue map[matchKey]*pendingValue
	cachedCommit   []*cdcpb.Event_Row
}

type matchKey struct {
	startTs uint64
	key     string
}

func newMatchKey(row *cdcpb.Event_Row) matchKey {
	return matchKey{startTs: row.GetStartTs(), key: string(row.GetKey())}
}

func newMatcher() *matcher {
	return &matcher{
		unmatchedValue: make(map[matchKey]*pendingValue),
	}
}

func (m *matcher) putPrewriteRow(row *cdcpb.Event_Row) {
	key := newMatchKey(row)
	value := row.GetValue()
	oldvalue := row.GetOldValue()
	// tikv may send a prewrite event with empty value (txn heartbeat)
	// here we need to avoid the invalid prewrite event overwrite the value
	if _, exist := m.unmatchedValue[key]; exist && len(value) == 0 {
		return
	}
	m.unmatchedValue[key] = &pendingValue{
		value:    value,
		oldValue: oldvalue,
	}
}

func (m *matcher) matchRow(row *cdcpb.Event_Row) (*pendingValue, bool) {
	if value, exist := m.unmatchedValue[newMatchKey(row)]; exist {
		delete(m.unmatchedValue, newMatchKey(row))
		return value, true
	}
	return nil, false
}

func (m *matcher) cacheCommitRow(row *cdcpb.Event_Row) {
	m.cachedCommit = append(m.cachedCommit, row)
}

func (m *matcher) clearCacheCommit() {
	m.cachedCommit = nil
}

func (m *matcher) rollbackRow(row *cdcpb.Event_Row) {
	delete(m.unmatchedValue, newMatchKey(row))
}
