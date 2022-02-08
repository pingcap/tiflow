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

package pipeline

import (
	"context"
	"sort"
	"sync"

	"github.com/google/go-cmp/cmp"
	"github.com/pingcap/check"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	"github.com/pingcap/tiflow/pkg/cyclic/mark"
	"github.com/pingcap/tiflow/pkg/pipeline"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

type markSuite struct{}

var _ = check.Suite(&markSuite{})

func (s *markSuite) TestCyclicMarkNode(c *check.C) {
	defer testleak.AfterTest(c)()
	markTableID := model.TableID(161025)
	testCases := []struct {
		input     []*model.RowChangedEvent
		expected  []*model.RowChangedEvent
		filterID  []uint64
		replicaID uint64
	}{
		{
			input:     []*model.RowChangedEvent{},
			expected:  []*model.RowChangedEvent{},
			filterID:  []uint64{},
			replicaID: 1,
		},
		{
			input:     []*model.RowChangedEvent{{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 1, CommitTs: 2}},
			expected:  []*model.RowChangedEvent{{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 1, CommitTs: 2, ReplicaID: 1}},
			filterID:  []uint64{},
			replicaID: 1,
		},
		{
			input: []*model.RowChangedEvent{
				{StartTs: 1, CommitTs: 2, Table: &model.TableName{Schema: "tidb_cdc", TableID: markTableID}, Columns: []*model.Column{{Name: mark.CyclicReplicaIDCol, Value: uint64(10)}}},
			},
			expected:  []*model.RowChangedEvent{},
			filterID:  []uint64{},
			replicaID: 1,
		},
		{
			input: []*model.RowChangedEvent{
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 1, CommitTs: 2},
				{StartTs: 1, CommitTs: 2, Table: &model.TableName{Schema: "tidb_cdc", TableID: markTableID}, Columns: []*model.Column{{Name: mark.CyclicReplicaIDCol, Value: uint64(10)}}},
			},
			expected:  []*model.RowChangedEvent{},
			filterID:  []uint64{10},
			replicaID: 1,
		},
		{
			input: []*model.RowChangedEvent{
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 1, CommitTs: 2},
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 3, CommitTs: 2},
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 2, CommitTs: 2},
				{StartTs: 3, CommitTs: 2, Table: &model.TableName{Schema: "tidb_cdc", TableID: markTableID}, Columns: []*model.Column{{Name: mark.CyclicReplicaIDCol, Value: uint64(10)}}},
				{StartTs: 1, CommitTs: 2, Table: &model.TableName{Schema: "tidb_cdc", TableID: markTableID}, Columns: []*model.Column{{Name: mark.CyclicReplicaIDCol, Value: uint64(11)}}},
			},
			expected: []*model.RowChangedEvent{
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 1, CommitTs: 2, ReplicaID: 11},
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 2, CommitTs: 2, ReplicaID: 1},
			},
			filterID:  []uint64{10},
			replicaID: 1,
		},
		{
			input: []*model.RowChangedEvent{
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 1, CommitTs: 2},
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 3, CommitTs: 2},
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 2, CommitTs: 2},
				{StartTs: 3, CommitTs: 2, Table: &model.TableName{Schema: "tidb_cdc", TableID: markTableID}, Columns: []*model.Column{{Name: mark.CyclicReplicaIDCol, Value: uint64(10)}}},
				{StartTs: 1, CommitTs: 5, Table: &model.TableName{Schema: "tidb_cdc", TableID: markTableID}, Columns: []*model.Column{{Name: mark.CyclicReplicaIDCol, Value: uint64(11)}}},
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 1, CommitTs: 5},
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 3, CommitTs: 5},
			},
			expected: []*model.RowChangedEvent{
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 1, CommitTs: 2, ReplicaID: 1},
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 2, CommitTs: 2, ReplicaID: 1},
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 1, CommitTs: 5, ReplicaID: 11},
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 3, CommitTs: 5, ReplicaID: 1},
			},
			filterID:  []uint64{10},
			replicaID: 1,
		},
		{
			input: []*model.RowChangedEvent{
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 1, CommitTs: 2},
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 3, CommitTs: 2},
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 2, CommitTs: 2},
				{StartTs: 3, CommitTs: 2, Table: &model.TableName{Schema: "tidb_cdc", TableID: markTableID}, Columns: []*model.Column{{Name: mark.CyclicReplicaIDCol, Value: uint64(10)}}},
				{StartTs: 1, CommitTs: 5, Table: &model.TableName{Schema: "tidb_cdc", TableID: markTableID}, Columns: []*model.Column{{Name: mark.CyclicReplicaIDCol, Value: uint64(11)}}},
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 1, CommitTs: 5},
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 3, CommitTs: 5},
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 5, CommitTs: 8},
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 3, CommitTs: 8},
				{StartTs: 5, CommitTs: 8, Table: &model.TableName{Schema: "tidb_cdc", TableID: markTableID}, Columns: []*model.Column{{Name: mark.CyclicReplicaIDCol, Value: uint64(12)}}},
			},
			expected: []*model.RowChangedEvent{
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 1, CommitTs: 2, ReplicaID: 1},
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 2, CommitTs: 2, ReplicaID: 1},
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 3, CommitTs: 5, ReplicaID: 1},
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 3, CommitTs: 8, ReplicaID: 1},
				{Table: &model.TableName{Table: "a", TableID: 1}, StartTs: 5, CommitTs: 8, ReplicaID: 12},
			},
			filterID:  []uint64{10, 11},
			replicaID: 1,
		},
	}

	for _, tc := range testCases {
		ctx := cdcContext.NewContext(context.Background(), &cdcContext.GlobalVars{})
		ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
			Info: &model.ChangeFeedInfo{
				Config: &config.ReplicaConfig{
					Cyclic: &config.CyclicConfig{
						Enable:          true,
						ReplicaID:       tc.replicaID,
						FilterReplicaID: tc.filterID,
					},
				},
			},
		})
		n := newCyclicMarkNode(markTableID)
		err := n.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil))
		c.Assert(err, check.IsNil)
		outputCh := make(chan pipeline.Message)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			defer close(outputCh)
			var lastCommitTs model.Ts
			for _, row := range tc.input {
				event := model.NewPolymorphicEvent(&model.RawKVEntry{
					OpType:  model.OpTypePut,
					Key:     tablecodec.GenTableRecordPrefix(row.Table.TableID),
					StartTs: row.StartTs,
					CRTs:    row.CommitTs,
				})
				event.Row = row
				err := n.Receive(pipeline.MockNodeContext4Test(ctx, pipeline.PolymorphicEventMessage(event), outputCh))
				c.Assert(err, check.IsNil)
				lastCommitTs = row.CommitTs
			}
			err := n.Receive(pipeline.MockNodeContext4Test(ctx, pipeline.PolymorphicEventMessage(model.NewResolvedPolymorphicEvent(0, lastCommitTs+1)), outputCh))
			c.Assert(err, check.IsNil)
		}()
		output := []*model.RowChangedEvent{}
		go func() {
			defer wg.Done()
			for row := range outputCh {
				if row.PolymorphicEvent.RawKV.OpType == model.OpTypeResolved {
					continue
				}
				output = append(output, row.PolymorphicEvent.Row)
			}
		}()
		wg.Wait()
		// check the commitTs is increasing
		var lastCommitTs model.Ts
		for _, row := range output {
			c.Assert(row.CommitTs, check.GreaterEqual, lastCommitTs)
			// Ensure that the ReplicaID of the row is set correctly.
			c.Assert(row.ReplicaID, check.Not(check.Equals), 0)
			lastCommitTs = row.CommitTs
		}
		sort.Slice(output, func(i, j int) bool {
			if output[i].CommitTs == output[j].CommitTs {
				return output[i].StartTs < output[j].StartTs
			}
			return output[i].CommitTs < output[j].CommitTs
		})
		c.Assert(output, check.DeepEquals, tc.expected,
			check.Commentf("%s", cmp.Diff(output, tc.expected)))
	}
}
