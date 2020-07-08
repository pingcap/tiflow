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
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/cyclic/mark"
)

type markSuit struct{}

var _ = check.Suite(&markSuit{})

func TestCyclic(t *testing.T) { check.TestingT(t) }

func (s *markSuit) TestFilterAndReduceTxns(c *check.C) {
	rID := mark.CyclicReplicaIDCol
	testCases := []struct {
		input     map[model.TableName][]*model.Txn
		output    map[model.TableName][]*model.Txn
		filterID  []uint64
		replicaID uint64
	}{
		{
			input:     map[model.TableName][]*model.Txn{},
			output:    map[model.TableName][]*model.Txn{},
			filterID:  []uint64{},
			replicaID: 0,
		}, {
			input:     map[model.TableName][]*model.Txn{{Table: "a"}: {{StartTs: 1}}},
			output:    map[model.TableName][]*model.Txn{{Table: "a"}: {{StartTs: 1, ReplicaID: 1}}},
			filterID:  []uint64{},
			replicaID: 1,
		}, {
			input: map[model.TableName][]*model.Txn{
				{Schema: "tidb_cdc"} /* cyclic.SchemaName */ : {
					{
						StartTs: 1,
						Rows:    []*model.RowChangedEvent{{StartTs: 1, Columns: map[string]*model.Column{rID: {Value: uint64(10)}}}},
					},
				},
			},
			output:    map[model.TableName][]*model.Txn{},
			filterID:  []uint64{},
			replicaID: 1,
		}, {
			input: map[model.TableName][]*model.Txn{
				{Table: "a"}: {{StartTs: 1}},
				{Schema: "tidb_cdc"}: {
					{
						StartTs: 1,
						Rows:    []*model.RowChangedEvent{{StartTs: 1, Columns: map[string]*model.Column{rID: {Value: uint64(10)}}}},
					},
				},
			},
			output:    map[model.TableName][]*model.Txn{},
			filterID:  []uint64{10},
			replicaID: 1,
		}, {
			input: map[model.TableName][]*model.Txn{
				{Table: "a"}: {{StartTs: 1}},
				{Schema: "tidb_cdc", Table: "1"}: {
					{
						StartTs: 1,
						Rows:    []*model.RowChangedEvent{{StartTs: 1, Columns: map[string]*model.Column{rID: {Value: uint64(10)}}}},
					},
				},
				{Schema: "tidb_cdc", Table: "2"}: {
					{
						StartTs: 2,
						Rows:    []*model.RowChangedEvent{{StartTs: 2, Columns: map[string]*model.Column{rID: {Value: uint64(10)}}}},
					},
				},
				{Schema: "tidb_cdc", Table: "3"}: {
					{
						StartTs: 3,
						Rows:    []*model.RowChangedEvent{{StartTs: 3, Columns: map[string]*model.Column{rID: {Value: uint64(10)}}}},
					},
				},
			},
			output:    map[model.TableName][]*model.Txn{},
			filterID:  []uint64{10},
			replicaID: 1,
		}, {
			input: map[model.TableName][]*model.Txn{
				{Table: "a"}:    {{StartTs: 1}},
				{Table: "b2"}:   {{StartTs: 2}},
				{Table: "b2_1"}: {{StartTs: 2}},
				{Schema: "tidb_cdc", Table: "1"}: {
					{
						StartTs: 1,
						Rows:    []*model.RowChangedEvent{{StartTs: 1, Columns: map[string]*model.Column{rID: {Value: uint64(10)}}}},
					},
				},
			},
			output: map[model.TableName][]*model.Txn{
				{Table: "b2"}:   {{StartTs: 2, ReplicaID: 1}},
				{Table: "b2_1"}: {{StartTs: 2, ReplicaID: 1}},
			},
			filterID:  []uint64{10},
			replicaID: 1,
		}, {
			input: map[model.TableName][]*model.Txn{
				{Table: "a"}:    {{StartTs: 1}},
				{Table: "b2"}:   {{StartTs: 2}},
				{Table: "b2_1"}: {{StartTs: 2}},
				{Table: "b3"}:   {{StartTs: 3}},
				{Table: "b3_1"}: {{StartTs: 3}},
				{Schema: "tidb_cdc", Table: "1"}: {
					{
						StartTs: 2,
						Rows:    []*model.RowChangedEvent{{StartTs: 2, Columns: map[string]*model.Column{rID: {Value: uint64(10)}}}},
					},
					{
						StartTs: 3,
						Rows:    []*model.RowChangedEvent{{StartTs: 3, Columns: map[string]*model.Column{rID: {Value: uint64(11)}}}},
					},
				},
			},
			output: map[model.TableName][]*model.Txn{
				{Table: "a"}:    {{StartTs: 1, ReplicaID: 1}},
				{Table: "b3"}:   {{StartTs: 3, ReplicaID: 11}},
				{Table: "b3_1"}: {{StartTs: 3, ReplicaID: 11}},
			},
			filterID:  []uint64{10}, // 10 -> 2, filter start ts 2
			replicaID: 1,
		}, {
			input: map[model.TableName][]*model.Txn{
				{Table: "b2"}: {{StartTs: 2, CommitTs: 2}},
				{Table: "b3"}: {
					{StartTs: 2, CommitTs: 2},
					{StartTs: 3, CommitTs: 3},
					{StartTs: 3, CommitTs: 3},
					{StartTs: 4, CommitTs: 4},
				},
				{Schema: "tidb_cdc", Table: "1"}: {
					{
						StartTs: 2,
						Rows:    []*model.RowChangedEvent{{StartTs: 2, Columns: map[string]*model.Column{rID: {Value: uint64(10)}}}},
					},
					{
						StartTs: 3,
						Rows:    []*model.RowChangedEvent{{StartTs: 3, Columns: map[string]*model.Column{rID: {Value: uint64(11)}}}},
					},
				},
			},
			output: map[model.TableName][]*model.Txn{
				{Table: "b3"}: {
					{StartTs: 3, CommitTs: 3, ReplicaID: 11},
					{StartTs: 3, CommitTs: 3, ReplicaID: 11},
					{StartTs: 4, CommitTs: 4, ReplicaID: 1},
				},
			},
			filterID:  []uint64{10}, // 10 -> 2, filter start ts 2
			replicaID: 1,
		}, {
			input: map[model.TableName][]*model.Txn{
				{Table: "b2"}: {{StartTs: 2}},
				{Schema: "tidb_cdc", Table: "1"}: {
					// Duplicate mark table changes.
					{
						StartTs: 2,
						Rows:    []*model.RowChangedEvent{{StartTs: 2, Columns: map[string]*model.Column{rID: {Value: uint64(10)}}}},
					},
					{
						StartTs: 2,
						Rows:    []*model.RowChangedEvent{{StartTs: 2, Columns: map[string]*model.Column{rID: {Value: uint64(10)}}}},
					},
				},
			},
			output:    map[model.TableName][]*model.Txn{},
			filterID:  []uint64{10}, // 10 -> 2, filter start ts 2
			replicaID: 1,
		},
	}

	for i, tc := range testCases {
		FilterAndReduceTxns(tc.input, tc.filterID, tc.replicaID)
		c.Assert(tc.input, check.DeepEquals, tc.output, check.Commentf("case %d %s\n", i, spew.Sdump(tc)))
	}
}
