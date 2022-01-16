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

package common

import (
	"sort"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"github.com/stretchr/testify/require"
)

func TestSplitResolvedTxn(test *testing.T) {
	defer testleak.AfterTestT(test)()

	testCases := [][]struct {
		input         []*model.RowChangedEvent
		resolvedTsMap map[model.TableID]uint64
		expected      map[model.TableID][]*model.SingleTableTxn
	}{{{ // Testing basic transaction collocation, no txns with the same committs
		input: []*model.RowChangedEvent{
			{StartTs: 1, CommitTs: 5, Table: &model.TableName{TableID: 1}},
			{StartTs: 1, CommitTs: 5, Table: &model.TableName{TableID: 1}},
			{StartTs: 1, CommitTs: 6, Table: &model.TableName{TableID: 2}},
			{StartTs: 1, CommitTs: 7, Table: &model.TableName{TableID: 3}},
			{StartTs: 1, CommitTs: 8, Table: &model.TableName{TableID: 1}},
			{StartTs: 1, CommitTs: 11, Table: &model.TableName{TableID: 1}},
			{StartTs: 1, CommitTs: 12, Table: &model.TableName{TableID: 2}},
		},
		resolvedTsMap: map[model.TableID]uint64{
			1: uint64(6),
			2: uint64(6),
		},
		expected: map[model.TableID][]*model.SingleTableTxn{
			1: {{Table: &model.TableName{TableID: 1}, StartTs: 1, CommitTs: 5, Rows: []*model.RowChangedEvent{
				{StartTs: 1, CommitTs: 5, Table: &model.TableName{TableID: 1}},
				{StartTs: 1, CommitTs: 5, Table: &model.TableName{TableID: 1}},
			}}},
			2: {{Table: &model.TableName{TableID: 2}, StartTs: 1, CommitTs: 6, Rows: []*model.RowChangedEvent{
				{StartTs: 1, CommitTs: 6, Table: &model.TableName{TableID: 2}},
			}}},
		},
	}, {
		input: []*model.RowChangedEvent{
			{StartTs: 1, CommitTs: 8, Table: &model.TableName{TableID: 3}},
		},
		resolvedTsMap: map[model.TableID]uint64{
			1: uint64(13),
			2: uint64(13),
			3: uint64(13),
		},
		expected: map[model.TableID][]*model.SingleTableTxn{
			1: {{Table: &model.TableName{TableID: 1}, StartTs: 1, CommitTs: 8, Rows: []*model.RowChangedEvent{
				{StartTs: 1, CommitTs: 8, Table: &model.TableName{TableID: 1}},
			}}, {Table: &model.TableName{TableID: 1}, StartTs: 1, CommitTs: 11, Rows: []*model.RowChangedEvent{
				{StartTs: 1, CommitTs: 11, Table: &model.TableName{TableID: 1}},
			}}},
			2: {{Table: &model.TableName{TableID: 2}, StartTs: 1, CommitTs: 12, Rows: []*model.RowChangedEvent{
				{StartTs: 1, CommitTs: 12, Table: &model.TableName{TableID: 2}},
			}}},
			3: {{Table: &model.TableName{TableID: 3}, StartTs: 1, CommitTs: 7, Rows: []*model.RowChangedEvent{
				{StartTs: 1, CommitTs: 7, Table: &model.TableName{TableID: 3}},
			}}, {Table: &model.TableName{TableID: 3}, StartTs: 1, CommitTs: 8, Rows: []*model.RowChangedEvent{
				{StartTs: 1, CommitTs: 8, Table: &model.TableName{TableID: 3}},
			}}},
		},
	}}, {{ // Testing the short circuit path
		input: []*model.RowChangedEvent{},
		resolvedTsMap: map[model.TableID]uint64{
			1: uint64(13),
			2: uint64(13),
			3: uint64(13),
		},
		expected: nil,
	}, {
		input: []*model.RowChangedEvent{
			{StartTs: 1, CommitTs: 11, Table: &model.TableName{TableID: 1}},
			{StartTs: 1, CommitTs: 12, Table: &model.TableName{TableID: 1}},
			{StartTs: 1, CommitTs: 13, Table: &model.TableName{TableID: 2}},
		},
		resolvedTsMap: map[model.TableID]uint64{
			1: uint64(6),
			2: uint64(6),
		},
		expected: map[model.TableID][]*model.SingleTableTxn{},
	}}, {{ // Testing the txns with the same commitTs
		input: []*model.RowChangedEvent{
			{StartTs: 1, CommitTs: 5, Table: &model.TableName{TableID: 1}},
			{StartTs: 1, CommitTs: 8, Table: &model.TableName{TableID: 1}},
			{StartTs: 1, CommitTs: 6, Table: &model.TableName{TableID: 2}},
			{StartTs: 2, CommitTs: 6, Table: &model.TableName{TableID: 2}},
			{StartTs: 2, CommitTs: 8, Table: &model.TableName{TableID: 1}},
			{StartTs: 1, CommitTs: 8, Table: &model.TableName{TableID: 1}},
			{StartTs: 2, CommitTs: 8, Table: &model.TableName{TableID: 1}},
			{StartTs: 1, CommitTs: 6, Table: &model.TableName{TableID: 2}},
			{StartTs: 1, CommitTs: 7, Table: &model.TableName{TableID: 2}},
		},
		resolvedTsMap: map[model.TableID]uint64{
			1: uint64(6),
			2: uint64(6),
		},
		expected: map[model.TableID][]*model.SingleTableTxn{
			1: {{Table: &model.TableName{TableID: 1}, StartTs: 1, CommitTs: 5, Rows: []*model.RowChangedEvent{
				{StartTs: 1, CommitTs: 5, Table: &model.TableName{TableID: 1}},
			}}},
			2: {{Table: &model.TableName{TableID: 2}, StartTs: 1, CommitTs: 6, Rows: []*model.RowChangedEvent{
				{StartTs: 1, CommitTs: 6, Table: &model.TableName{TableID: 2}},
				{StartTs: 1, CommitTs: 6, Table: &model.TableName{TableID: 2}},
			}}, {Table: &model.TableName{TableID: 2}, StartTs: 2, CommitTs: 6, Rows: []*model.RowChangedEvent{
				{StartTs: 2, CommitTs: 6, Table: &model.TableName{TableID: 2}},
			}}},
		},
	}, {
		input: []*model.RowChangedEvent{
			{StartTs: 2, CommitTs: 7, Table: &model.TableName{TableID: 2}},
			{StartTs: 1, CommitTs: 7, Table: &model.TableName{TableID: 2}},
			{StartTs: 1, CommitTs: 8, Table: &model.TableName{TableID: 1}},
			{StartTs: 2, CommitTs: 8, Table: &model.TableName{TableID: 1}},
			{StartTs: 1, CommitTs: 9, Table: &model.TableName{TableID: 1}},
		},
		resolvedTsMap: map[model.TableID]uint64{
			1: uint64(13),
			2: uint64(13),
		},
		expected: map[model.TableID][]*model.SingleTableTxn{
			1: {{Table: &model.TableName{TableID: 1}, StartTs: 1, CommitTs: 8, Rows: []*model.RowChangedEvent{
				{StartTs: 1, CommitTs: 8, Table: &model.TableName{TableID: 1}},
				{StartTs: 1, CommitTs: 8, Table: &model.TableName{TableID: 1}},
				{StartTs: 1, CommitTs: 8, Table: &model.TableName{TableID: 1}},
			}}, {Table: &model.TableName{TableID: 1}, StartTs: 2, CommitTs: 8, Rows: []*model.RowChangedEvent{
				{StartTs: 2, CommitTs: 8, Table: &model.TableName{TableID: 1}},
				{StartTs: 2, CommitTs: 8, Table: &model.TableName{TableID: 1}},
				{StartTs: 2, CommitTs: 8, Table: &model.TableName{TableID: 1}},
			}}, {Table: &model.TableName{TableID: 1}, StartTs: 1, CommitTs: 9, Rows: []*model.RowChangedEvent{
				{StartTs: 1, CommitTs: 9, Table: &model.TableName{TableID: 1}},
			}}},
			2: {{Table: &model.TableName{TableID: 2}, StartTs: 1, CommitTs: 7, Rows: []*model.RowChangedEvent{
				{StartTs: 1, CommitTs: 7, Table: &model.TableName{TableID: 2}},
				{StartTs: 1, CommitTs: 7, Table: &model.TableName{TableID: 2}},
			}}, {Table: &model.TableName{TableID: 2}, StartTs: 2, CommitTs: 7, Rows: []*model.RowChangedEvent{
				{StartTs: 2, CommitTs: 7, Table: &model.TableName{TableID: 2}},
			}}},
		},
	}}}
	for _, tc := range testCases {
		cache := NewUnresolvedTxnCache()
		for _, t := range tc {
			cache.Append(nil, t.input...)
			resolvedTsMap := sync.Map{}
			for tableID, ts := range t.resolvedTsMap {
				resolvedTsMap.Store(tableID, ts)
			}
			_, resolved := cache.Resolved(&resolvedTsMap)
			for tableID, txns := range resolved {
				sort.Slice(txns, func(i, j int) bool {
					if txns[i].CommitTs != txns[j].CommitTs {
						return txns[i].CommitTs < txns[j].CommitTs
					}
					return txns[i].StartTs < txns[j].StartTs
				})
				resolved[tableID] = txns
			}
			require.Equal(test, t.expected, resolved, cmp.Diff(resolved, t.expected))
		}
	}
}
