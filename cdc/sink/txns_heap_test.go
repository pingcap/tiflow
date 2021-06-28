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

package sink

import (
	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

type TxnsHeapSuite struct{}

var _ = check.Suite(&TxnsHeapSuite{})

func (s TxnsHeapSuite) TestTxnsHeap(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		txnsMap  map[model.TableID][]*model.SingleTableTxn
		expected []*model.SingleTableTxn
	}{{
		txnsMap:  nil,
		expected: nil,
	}, {
		txnsMap: map[model.TableID][]*model.SingleTableTxn{
			1: {
				{CommitTs: 1}, {CommitTs: 3}, {CommitTs: 5}, {CommitTs: 7}, {CommitTs: 9},
			},
			2: {
				{CommitTs: 1}, {CommitTs: 10}, {CommitTs: 15}, {CommitTs: 15}, {CommitTs: 15},
			},
			3: {
				{CommitTs: 1}, {CommitTs: 1}, {CommitTs: 1}, {CommitTs: 2}, {CommitTs: 3},
			},
		},
		expected: []*model.SingleTableTxn{
			{CommitTs: 1},
			{CommitTs: 1},
			{CommitTs: 1},
			{CommitTs: 1},
			{CommitTs: 1},
			{CommitTs: 2},
			{CommitTs: 3},
			{CommitTs: 3},
			{CommitTs: 5},
			{CommitTs: 7},
			{CommitTs: 9},
			{CommitTs: 10},
			{CommitTs: 15},
			{CommitTs: 15},
			{CommitTs: 15},
		},
	}}

	for _, tc := range testCases {
		h := newTxnsHeap(tc.txnsMap)
		i := 0
		h.iter(func(txn *model.SingleTableTxn) {
			c.Assert(txn, check.DeepEquals, tc.expected[i])
			i++
		})
	}
}
