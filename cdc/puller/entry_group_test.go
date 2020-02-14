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
	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
)

type mockEntryGroupSuite struct{}

var _ = check.Suite(&mockEntryGroupSuite{})

func (s *mockEntryGroupSuite) TestEntryGroup(c *check.C) {
	testCases := []struct {
		input       []*model.RawKVEntry
		resolvedTs  uint64
		expectTxns  []model.RawTxn
		remainEntry int
	}{
		{
			input:       []*model.RawKVEntry{{Ts: 1}, {Ts: 2}, {Ts: 4}, {Ts: 2}},
			resolvedTs:  0,
			expectTxns:  nil,
			remainEntry: 3,
		},
		{
			input:      []*model.RawKVEntry{{Ts: 3}, {Ts: 2}, {Ts: 5}},
			resolvedTs: 3,
			expectTxns: []model.RawTxn{
				{Ts: 1, Entries: []*model.RawKVEntry{{Ts: 1}}},
				{Ts: 2, Entries: []*model.RawKVEntry{{Ts: 2}, {Ts: 2}, {Ts: 2}}},
				{Ts: 3, Entries: []*model.RawKVEntry{{Ts: 3}}},
			},
			remainEntry: 2,
		},
		{
			input:      nil,
			resolvedTs: 6,
			expectTxns: []model.RawTxn{
				{Ts: 4, Entries: []*model.RawKVEntry{{Ts: 4}}},
				{Ts: 5, Entries: []*model.RawKVEntry{{Ts: 5}}},
			},
			remainEntry: 0,
		},
		{
			input:       []*model.RawKVEntry{{Ts: 7}},
			resolvedTs:  6,
			expectTxns:  nil,
			remainEntry: 1,
		},
	}
	eg := NewEntryGroup()
	for _, tc := range testCases {
		for _, entry := range tc.input {
			eg.AddEntry(entry.Ts, entry)
		}
		txns := eg.Consume(tc.resolvedTs)
		c.Check(txns, check.DeepEquals, tc.expectTxns)
		c.Check(eg.sortedTs, check.HasLen, tc.remainEntry)
	}
}
