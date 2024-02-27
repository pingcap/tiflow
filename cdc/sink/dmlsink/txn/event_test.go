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

package txn

import (
	"sort"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestGenKeyListCaseInSensitive(t *testing.T) {
	t.Parallel()

	columns := []*model.Column{
		{
			Value:     "XyZ",
			Type:      mysql.TypeVarchar,
			Collation: "utf8_unicode_ci",
		},
	}

	first := genKeyList(columns, 0, []int{0}, 1)

	columns = []*model.Column{
		{
			Value:     "xYZ",
			Type:      mysql.TypeVarchar,
			Collation: "utf8_unicode_ci",
		},
	}
	second := genKeyList(columns, 0, []int{0}, 1)

	require.Equal(t, first, second)
}

func TestGenKeys(t *testing.T) {
	t.Parallel()
	tableInfoWithOneCompositeUniqueKey := model.BuildTableInfo("common_1", "uk_without_pk", []*model.Column{
		{
			Name: "a1",
			Type: mysql.TypeLong,
			Flag: model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
		},
		{
			Name: "a3",
			Type: mysql.TypeLong,
			Flag: model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
		},
	}, [][]int{{0, 1}})

	tableInfoWithTwoUniqueKeys := model.BuildTableInfo("common_1", "uk_without_pk", []*model.Column{
		{
			Name: "a1",
			Type: mysql.TypeLong,
			Flag: model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
		},
		{
			Name: "a3",
			Type: mysql.TypeLong,
			Flag: model.BinaryFlag | model.MultipleKeyFlag | model.UniqueKeyFlag,
		},
	}, [][]int{{0}, {1}})

	testCases := []struct {
		txn      *model.SingleTableTxn
		expected []uint64
	}{{
		txn:      &model.SingleTableTxn{},
		expected: nil,
	}, {
		txn: &model.SingleTableTxn{
			Rows: []*model.RowChangedEvent{
				{
					StartTs:         418658114257813514,
					CommitTs:        418658114257813515,
					PhysicalTableID: 47,
					TableInfo:       tableInfoWithOneCompositeUniqueKey,
					PreColumns: model.Columns2ColumnDatas([]*model.Column{
						{
							Name:  "a1",
							Value: 12,
						},
						{
							Name:  "a3",
							Value: 1,
						},
					}, tableInfoWithOneCompositeUniqueKey),
				}, {
					StartTs:         418658114257813514,
					CommitTs:        418658114257813515,
					PhysicalTableID: 47,
					TableInfo:       tableInfoWithOneCompositeUniqueKey,
					PreColumns: model.Columns2ColumnDatas([]*model.Column{
						{
							Name:  "a1",
							Value: 1,
						},
						{
							Name:  "a3",
							Value: 21,
						},
					}, tableInfoWithOneCompositeUniqueKey),
				},
			},
		},
		expected: []uint64{2072713494, 3710968706},
	}, {
		txn: &model.SingleTableTxn{
			Rows: []*model.RowChangedEvent{
				{
					StartTs:         418658114257813514,
					CommitTs:        418658114257813515,
					PhysicalTableID: 47,
					TableInfo:       tableInfoWithTwoUniqueKeys,
					PreColumns: model.Columns2ColumnDatas([]*model.Column{
						{
							Name:  "a1",
							Value: 12,
						},
						{
							Name:  "a3",
							Value: 1,
						},
					}, tableInfoWithTwoUniqueKeys),
				}, {
					StartTs:         418658114257813514,
					CommitTs:        418658114257813515,
					TableInfo:       tableInfoWithTwoUniqueKeys,
					PhysicalTableID: 47,
					PreColumns: model.Columns2ColumnDatas([]*model.Column{
						{
							Name:  "a1",
							Value: 1,
						},
						{
							Name:  "a3",
							Value: 21,
						},
					}, tableInfoWithTwoUniqueKeys),
				},
			},
		},
		expected: []uint64{318190470, 2109733718, 2658640457, 2989258527},
	}, {
		txn: &model.SingleTableTxn{
			Rows: []*model.RowChangedEvent{
				{
					StartTs:         418658114257813514,
					CommitTs:        418658114257813515,
					PhysicalTableID: 47,
					TableInfo:       tableInfoWithTwoUniqueKeys,
					PreColumns: model.Columns2ColumnDatas([]*model.Column{
						{
							Name:  "a1",
							Value: nil,
						},
						{
							Name:  "a3",
							Value: nil,
						},
					}, tableInfoWithTwoUniqueKeys),
				}, {
					StartTs:         418658114257813514,
					CommitTs:        418658114257813515,
					TableInfo:       tableInfoWithTwoUniqueKeys,
					PhysicalTableID: 47,
					PreColumns: model.Columns2ColumnDatas([]*model.Column{
						{
							Name:  "a1",
							Value: 1,
						},
						{
							Name:  "a3",
							Value: 21,
						},
					}, tableInfoWithTwoUniqueKeys),
				},
			},
		},
		expected: []uint64{318190470, 2095136920, 2658640457},
	}}
	for _, tc := range testCases {
		keys := genTxnKeys(tc.txn)
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		require.Equal(t, tc.expected, keys)
	}
}

func TestSortAndDedupHash(t *testing.T) {
	// If a transaction contains multiple rows, these rows may generate the same hash
	// in some rare cases. We should dedup these hashes to avoid unnecessary self cyclic
	// dependency in the causality dependency graph.
	t.Parallel()
	testCases := []struct {
		hashes   []uint64
		expected []uint64
	}{{
		// No duplicate hashes
		hashes:   []uint64{1, 2, 3, 4, 5},
		expected: []uint64{1, 2, 3, 4, 5},
	}, {
		// Duplicate hashes
		hashes:   []uint64{1, 2, 3, 4, 5, 1, 2, 3, 4, 5},
		expected: []uint64{1, 2, 3, 4, 5},
	}, {
		// Has hash value larger than slots count, should sort by `hash % numSlots` first.
		hashes:   []uint64{4, 9, 9, 3},
		expected: []uint64{9, 3, 4},
	}}

	for _, tc := range testCases {
		require.Equal(t, tc.expected, sortAndDedupHashes(tc.hashes, 8))
	}
}
