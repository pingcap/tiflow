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
	"bytes"
	"sort"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

type testCausalitySuite struct{}

var _ = check.Suite(&testCausalitySuite{})

func (s *testCausalitySuite) TestCausality(c *check.C) {
	defer testleak.AfterTest(c)()
	rows := [][][]byte{
		{[]byte("a")},
		{[]byte("b")},
		{[]byte("c")},
	}
	ca := newCausality()
	for i, row := range rows {
		conflict, idx := ca.detectConflict(row)
		c.Assert(conflict, check.IsFalse)
		c.Assert(idx, check.Equals, -1)
		ca.add(row, i)
		// Test for single key index conflict.
		conflict, idx = ca.detectConflict(row)
		c.Assert(conflict, check.IsTrue)
		c.Assert(idx, check.Equals, i)
	}
	c.Assert(len(ca.relations), check.Equals, 3)
	cases := []struct {
		keys     [][]byte
		conflict bool
		idx      int
	}{
		// Test for single key index conflict.
		{[][]byte{[]byte("a"), []byte("ab")}, true, 0},
		{[][]byte{[]byte("b"), []byte("ba")}, true, 1},
		{[][]byte{[]byte("a"), []byte("a")}, true, 0},
		{[][]byte{[]byte("b"), []byte("b")}, true, 1},
		{[][]byte{[]byte("c"), []byte("c")}, true, 2},
		// Test for multi-key index conflict.
		{[][]byte{[]byte("a"), []byte("b")}, true, -1},
		{[][]byte{[]byte("b"), []byte("a")}, true, -1},
		{[][]byte{[]byte("b"), []byte("c")}, true, -1},
	}
	for _, cas := range cases {
		conflict, idx := ca.detectConflict(cas.keys)
		comment := check.Commentf("keys: %v", cas.keys)
		c.Assert(conflict, check.Equals, cas.conflict, comment)
		c.Assert(idx, check.Equals, cas.idx, comment)
	}
	ca.reset()
	c.Assert(len(ca.relations), check.Equals, 0)
}

func (s *testCausalitySuite) TestGenKeys(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		txn      *model.SingleTableTxn
		expected [][]byte
	}{{
		txn:      &model.SingleTableTxn{},
		expected: nil,
	}, {
		txn: &model.SingleTableTxn{
			Rows: []*model.RowChangedEvent{
				{
					StartTs:  418658114257813514,
					CommitTs: 418658114257813515,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk", TableID: 47},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 12,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 1,
					}},
					IndexColumns: [][]int{{1, 2}},
				}, {
					StartTs:  418658114257813514,
					CommitTs: 418658114257813515,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk", TableID: 47},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 1,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 21,
					}},
					IndexColumns: [][]int{{1, 2}},
				},
			},
		},
		expected: [][]byte{
			{'1', '2', 0x0, '1', 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 47},
			{'1', 0x0, '2', '1', 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 47},
		},
	}, {
		txn: &model.SingleTableTxn{
			Rows: []*model.RowChangedEvent{
				{
					StartTs:  418658114257813514,
					CommitTs: 418658114257813515,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk", TableID: 47},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.HandleKeyFlag,
						Value: 12,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.HandleKeyFlag,
						Value: 1,
					}},
					IndexColumns: [][]int{{1}, {2}},
				}, {
					StartTs:  418658114257813514,
					CommitTs: 418658114257813515,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk", TableID: 47},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.HandleKeyFlag,
						Value: 1,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.HandleKeyFlag,
						Value: 21,
					}},
					IndexColumns: [][]int{{1}, {2}},
				},
			},
		},
		expected: [][]byte{
			{'2', '1', 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 47},
			{'1', '2', 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 47},
			{'1', 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 47},
			{'1', 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 47},
		},
	}, {
		txn: &model.SingleTableTxn{
			Rows: []*model.RowChangedEvent{
				{
					StartTs:  418658114257813514,
					CommitTs: 418658114257813515,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk", TableID: 47},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.NullableFlag,
						Value: nil,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.NullableFlag,
						Value: nil,
					}},
					IndexColumns: [][]int{{1}, {2}},
				}, {
					StartTs:  418658114257813514,
					CommitTs: 418658114257813515,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk", TableID: 47},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.HandleKeyFlag,
						Value: 1,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.HandleKeyFlag,
						Value: 21,
					}},
					IndexColumns: [][]int{{1}, {2}},
				},
			},
		},
		expected: [][]uint8{
			{'2', '1', 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 47},
			{'1', 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 47},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 47},
		},
	}}
	for _, tc := range testCases {
		keys := genTxnKeys(tc.txn)
		sort.Slice(keys, func(i, j int) bool {
			return bytes.Compare(keys[i], keys[j]) > 0
		})
		c.Assert(keys, check.DeepEquals, tc.expected)
	}
}
