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

package cyclic

import (
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/cyclic/mark"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

type markSuite struct{}

var _ = check.Suite(&markSuite{})

func TestCyclic(t *testing.T) { check.TestingT(t) }

func (s *markSuite) TestFilterAndReduceTxns(c *check.C) {
	defer testleak.AfterTest(c)()
	rID := mark.CyclicReplicaIDCol
	testCases := []struct {
		input     map[model.TableID][]*model.SingleTableTxn
		output    map[model.TableID][]*model.SingleTableTxn
		filterID  []uint64
		replicaID uint64
	}{
		{
			input:     map[model.TableID][]*model.SingleTableTxn{},
			output:    map[model.TableID][]*model.SingleTableTxn{},
			filterID:  []uint64{},
			replicaID: 0,
		},
		{
			input:     map[model.TableID][]*model.SingleTableTxn{1: {{Table: &model.TableName{Table: "a"}, StartTs: 1}}},
			output:    map[model.TableID][]*model.SingleTableTxn{1: {{Table: &model.TableName{Table: "a"}, StartTs: 1, ReplicaID: 1}}},
			filterID:  []uint64{},
			replicaID: 1,
		},
		{
			input: map[model.TableID][]*model.SingleTableTxn{
				2: {
					{
						Table:   &model.TableName{Schema: "tidb_cdc"}, /* cyclic.SchemaName */
						StartTs: 1,
						Rows:    []*model.RowChangedEvent{{StartTs: 1, Columns: []*model.Column{{Name: rID, Value: uint64(10)}}}},
					},
				},
			},
			output:    map[model.TableID][]*model.SingleTableTxn{},
			filterID:  []uint64{},
			replicaID: 1,
		},
		{
			input: map[model.TableID][]*model.SingleTableTxn{
				1: {{Table: &model.TableName{Table: "a"}, StartTs: 1}},
				2: {
					{
						Table:   &model.TableName{Schema: "tidb_cdc"}, /* cyclic.SchemaName */
						StartTs: 1,
						Rows:    []*model.RowChangedEvent{{StartTs: 1, Columns: []*model.Column{{Name: rID, Value: uint64(10)}}}},
					},
				},
			},
			output:    map[model.TableID][]*model.SingleTableTxn{},
			filterID:  []uint64{10},
			replicaID: 1,
		},
		{
			input: map[model.TableID][]*model.SingleTableTxn{
				1: {{Table: &model.TableName{Table: "a"}, StartTs: 1}},
				2: {
					{
						Table:   &model.TableName{Schema: "tidb_cdc", Table: "1"},
						StartTs: 1,
						Rows:    []*model.RowChangedEvent{{StartTs: 1, Columns: []*model.Column{{Name: rID, Value: uint64(10)}}}},
					},
				},
				3: {
					{
						Table:   &model.TableName{Schema: "tidb_cdc", Table: "2"},
						StartTs: 2,
						Rows:    []*model.RowChangedEvent{{StartTs: 2, Columns: []*model.Column{{Name: rID, Value: uint64(10)}}}},
					},
				},
				4: {
					{
						Table:   &model.TableName{Schema: "tidb_cdc", Table: "3"},
						StartTs: 3,
						Rows:    []*model.RowChangedEvent{{StartTs: 3, Columns: []*model.Column{{Name: rID, Value: uint64(10)}}}},
					},
				},
			},
			output:    map[model.TableID][]*model.SingleTableTxn{},
			filterID:  []uint64{10},
			replicaID: 1,
		},
		{
			input: map[model.TableID][]*model.SingleTableTxn{
				1: {{Table: &model.TableName{Table: "a"}, StartTs: 1}},
				2: {{Table: &model.TableName{Table: "b2"}, StartTs: 2}},
				3: {{Table: &model.TableName{Table: "b2_1"}, StartTs: 2}},
				4: {
					{
						Table:   &model.TableName{Schema: "tidb_cdc", Table: "1"},
						StartTs: 1,
						Rows:    []*model.RowChangedEvent{{StartTs: 1, Columns: []*model.Column{{Name: rID, Value: uint64(10)}}}},
					},
				},
			},
			output: map[model.TableID][]*model.SingleTableTxn{
				2: {{Table: &model.TableName{Table: "b2"}, StartTs: 2, ReplicaID: 1}},
				3: {{Table: &model.TableName{Table: "b2_1"}, StartTs: 2, ReplicaID: 1}},
			},
			filterID:  []uint64{10},
			replicaID: 1,
		},
		{
			input: map[model.TableID][]*model.SingleTableTxn{
				1: {{Table: &model.TableName{Table: "a"}, StartTs: 1}},
				2: {{Table: &model.TableName{Table: "b2"}, StartTs: 2}},
				3: {{Table: &model.TableName{Table: "b2_1"}, StartTs: 2}},
				4: {{Table: &model.TableName{Table: "b3"}, StartTs: 3}},
				5: {{Table: &model.TableName{Table: "b3_1"}, StartTs: 3}},
				6: {
					{
						Table:   &model.TableName{Schema: "tidb_cdc", Table: "1"},
						StartTs: 2,
						Rows:    []*model.RowChangedEvent{{StartTs: 2, Columns: []*model.Column{{Name: rID, Value: uint64(10)}}}},
					},
					{
						Table:   &model.TableName{Schema: "tidb_cdc", Table: "1"},
						StartTs: 3,
						Rows:    []*model.RowChangedEvent{{StartTs: 3, Columns: []*model.Column{{Name: rID, Value: uint64(11)}}}},
					},
				},
			},
			output: map[model.TableID][]*model.SingleTableTxn{
				1: {{Table: &model.TableName{Table: "a"}, StartTs: 1, ReplicaID: 1}},
				4: {{Table: &model.TableName{Table: "b3"}, StartTs: 3, ReplicaID: 11}},
				5: {{Table: &model.TableName{Table: "b3_1"}, StartTs: 3, ReplicaID: 11}},
			},
			filterID:  []uint64{10}, // 10 -> 2, filter start ts 2
			replicaID: 1,
		},
		{
			input: map[model.TableID][]*model.SingleTableTxn{
				2: {{Table: &model.TableName{Table: "b2"}, StartTs: 2, CommitTs: 2}},
				3: {
					{Table: &model.TableName{Table: "b3"}, StartTs: 2, CommitTs: 2},
					{Table: &model.TableName{Table: "b3"}, StartTs: 3, CommitTs: 3},
					{Table: &model.TableName{Table: "b3"}, StartTs: 3, CommitTs: 3},
					{Table: &model.TableName{Table: "b3"}, StartTs: 4, CommitTs: 4},
				},
				6: {
					{
						Table:   &model.TableName{Schema: "tidb_cdc", Table: "1"},
						StartTs: 2,
						Rows:    []*model.RowChangedEvent{{StartTs: 2, Columns: []*model.Column{{Name: rID, Value: uint64(10)}}}},
					},
					{
						Table:   &model.TableName{Schema: "tidb_cdc", Table: "1"},
						StartTs: 3,
						Rows:    []*model.RowChangedEvent{{StartTs: 3, Columns: []*model.Column{{Name: rID, Value: uint64(11)}}}},
					},
				},
			},
			output: map[model.TableID][]*model.SingleTableTxn{
				3: {
					{Table: &model.TableName{Table: "b3"}, StartTs: 3, CommitTs: 3, ReplicaID: 11},
					{Table: &model.TableName{Table: "b3"}, StartTs: 3, CommitTs: 3, ReplicaID: 11},
					{Table: &model.TableName{Table: "b3"}, StartTs: 4, CommitTs: 4, ReplicaID: 1},
				},
			},
			filterID:  []uint64{10}, // 10 -> 2, filter start ts 2
			replicaID: 1,
		},
		{
			input: map[model.TableID][]*model.SingleTableTxn{
				2: {{Table: &model.TableName{Table: "b2"}, StartTs: 2}},
				6: {
					{
						Table:   &model.TableName{Schema: "tidb_cdc", Table: "1"},
						StartTs: 2,
						Rows:    []*model.RowChangedEvent{{StartTs: 2, Columns: []*model.Column{{Name: rID, Value: uint64(10)}}}},
					},
					{
						Table:   &model.TableName{Schema: "tidb_cdc", Table: "1"},
						StartTs: 2,
						Rows:    []*model.RowChangedEvent{{StartTs: 2, Columns: []*model.Column{{Name: rID, Value: uint64(10)}}}},
					},
				},
			},
			output:    map[model.TableID][]*model.SingleTableTxn{},
			filterID:  []uint64{10}, // 10 -> 2, filter start ts 2
			replicaID: 1,
		},
	}

	for i, tc := range testCases {
		FilterAndReduceTxns(tc.input, tc.filterID, tc.replicaID)
		c.Assert(tc.input, check.DeepEquals, tc.output, check.Commentf("case %d %s\n", i, spew.Sdump(tc)))
	}
}
