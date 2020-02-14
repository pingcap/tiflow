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

package puller

import (
	"sort"

	"github.com/pingcap/ticdc/cdc/model"
)

// EntryGroup stores RawKVEntry and corresponding ts
type EntryGroup struct {
	entriesMap map[uint64][]*model.RawKVEntry
	sortedTs   []uint64
}

// NewEntryGroup creates a new EntryGroup
func NewEntryGroup() *EntryGroup {
	return &EntryGroup{
		entriesMap: make(map[uint64][]*model.RawKVEntry),
	}
}

// AddEntry adds an RawKVEntry to the EntryGroup, this method *IS NOT* thread safe.
func (eg *EntryGroup) AddEntry(ts uint64, entry *model.RawKVEntry) {
	i := sort.Search(len(eg.sortedTs), func(i int) bool { return eg.sortedTs[i] >= ts })
	if i < len(eg.sortedTs) && eg.sortedTs[i] == ts {
		eg.entriesMap[ts] = append(eg.entriesMap[ts], entry)
	} else {
		eg.sortedTs = append(eg.sortedTs, 0)
		copy(eg.sortedTs[i+1:], eg.sortedTs[i:])
		eg.sortedTs[i] = ts
		eg.entriesMap[ts] = []*model.RawKVEntry{entry}
	}
}

// Consume retrieves all RawKVEntry with ts no larger than resolvedTs,
// returns RawTxn list sorted by ts according these RawKVEntry and
// removes these RawKVEntry from EntryGroup. This method *IS NOT* thread safe.
func (eg *EntryGroup) Consume(resolvedTs uint64) (txns []model.RawTxn) {
	i := 0
	for ; i < len(eg.sortedTs); i++ {
		ts := eg.sortedTs[i]
		if ts > resolvedTs {
			break
		}
		txns = append(txns, model.RawTxn{Ts: ts, Entries: eg.entriesMap[ts]})
		delete(eg.entriesMap, ts)
	}
	eg.sortedTs = eg.sortedTs[i:]
	return
}
