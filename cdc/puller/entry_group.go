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
	sortedEntries []*model.RawRowGroup
	collectTxn    bool
}

// NewEntryGroup creates a new EntryGroup
func NewEntryGroup(collectTxn bool) *EntryGroup {
	return &EntryGroup{
		collectTxn: collectTxn,
	}
}

// AddEntry adds an RawKVEntry to the EntryGroup, this method *IS NOT* thread safe.
func (eg *EntryGroup) AddEntry(ts uint64, entry *model.RawKVEntry) {
	i := sort.Search(len(eg.sortedEntries), func(i int) bool { return eg.sortedEntries[i].Ts >= ts })
	if i >= len(eg.sortedEntries) || eg.sortedEntries[i].Ts != ts {
		eg.sortedEntries = append(eg.sortedEntries, nil)
		copy(eg.sortedEntries[i+1:], eg.sortedEntries[i:])
		eg.sortedEntries[i] = &model.RawRowGroup{Ts: ts, IsCompleteTxn: eg.collectTxn}
	}
	eg.sortedEntries[i].Entries = append(eg.sortedEntries[i].Entries, entry)
}

// Consume retrieves all RawKVEntry with ts no larger than resolvedTs,
// returns RawTxn list sorted by ts according these RawKVEntry and
// removes these RawKVEntry from EntryGroup. This method *IS NOT* thread safe.
func (eg *EntryGroup) Consume(resolvedTs uint64) (txns []model.RawRowGroup) {
	i := 0
	for ; i < len(eg.sortedEntries); i++ {
		if eg.sortedEntries[i].Ts > resolvedTs {
			break
		}
		txns = append(txns, *eg.sortedEntries[i])
	}
	if !eg.collectTxn {
		// find a more efficient way to sort entries
		result := model.RawRowGroup{IsCompleteTxn: false}
		for _, txn := range txns {
			result.Entries = append(result.Entries, txn.Entries...)
		}
		txns = []model.RawRowGroup{result}
	}
	eg.sortedEntries = eg.sortedEntries[i:]
	return
}
