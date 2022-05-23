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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	"github.com/pingcap/tiflow/pkg/cyclic/mark"
	"github.com/pingcap/tiflow/pkg/pipeline"
	pmessage "github.com/pingcap/tiflow/pkg/pipeline/message"
	"github.com/stretchr/testify/require"
)

func TestCyclicMarkNode(t *testing.T) {
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
		err := n.Init(pipeline.MockNodeContext4Test(ctx, pmessage.Message{}, nil))
		require.Nil(t, err)
		outputCh := make(chan pmessage.Message)
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
				msg := pmessage.PolymorphicEventMessage(event)
				err := n.Receive(pipeline.MockNodeContext4Test(ctx, msg, outputCh))
				require.Nil(t, err)
				lastCommitTs = row.CommitTs
			}
			msg := pmessage.PolymorphicEventMessage(
				model.NewResolvedPolymorphicEvent(0, lastCommitTs+1))
			err := n.Receive(pipeline.MockNodeContext4Test(ctx, msg, outputCh))
			require.Nil(t, err)
		}()
		output := []*model.RowChangedEvent{}
		go func() {
			defer wg.Done()
			for row := range outputCh {
				if row.PolymorphicEvent.IsResolved() {
					continue
				}
				output = append(output, row.PolymorphicEvent.Row)
			}
		}()
		wg.Wait()
		// check the commitTs is increasing
		var lastCommitTs model.Ts
		for _, row := range output {
			require.GreaterOrEqual(t, row.CommitTs, lastCommitTs)
			// Ensure that the ReplicaID of the row is set correctly.
			require.NotEqual(t, 0, row.ReplicaID)
			lastCommitTs = row.CommitTs
		}
		sort.Slice(output, func(i, j int) bool {
			if output[i].CommitTs == output[j].CommitTs {
				return output[i].StartTs < output[j].StartTs
			}
			return output[i].CommitTs < output[j].CommitTs
		})
		require.Equal(t, tc.expected, output, cmp.Diff(output, tc.expected))
	}

	// table actor
	for _, tc := range testCases {
		ctx := newCyclicNodeContext(newContext(context.TODO(), "a.test", nil, 1, &cdcContext.ChangefeedVars{
			Info: &model.ChangeFeedInfo{
				Config: &config.ReplicaConfig{
					Cyclic: &config.CyclicConfig{
						Enable:          true,
						ReplicaID:       tc.replicaID,
						FilterReplicaID: tc.filterID,
					},
				},
			},
		}, nil, throwDoNothing))
		n := newCyclicMarkNode(markTableID)
		err := n.Init(ctx)
		require.Nil(t, err)
		output := []*model.RowChangedEvent{}
		putToOutput := func(row *pmessage.Message) {
			if row == nil || row.PolymorphicEvent.IsResolved() {
				return
			}
			output = append(output, row.PolymorphicEvent.Row)
		}

		var lastCommitTs model.Ts
		for _, row := range tc.input {
			event := model.NewPolymorphicEvent(&model.RawKVEntry{
				OpType:  model.OpTypePut,
				Key:     tablecodec.GenTableRecordPrefix(row.Table.TableID),
				StartTs: row.StartTs,
				CRTs:    row.CommitTs,
			})
			event.Row = row
			ok, err := n.TryHandleDataMessage(ctx, pmessage.PolymorphicEventMessage(event))
			require.Nil(t, err)
			require.True(t, ok)
			putToOutput(ctx.tryGetProcessedMessage())
			lastCommitTs = row.CommitTs
		}
		ok, err := n.TryHandleDataMessage(ctx,
			pmessage.PolymorphicEventMessage(model.NewResolvedPolymorphicEvent(0, lastCommitTs+1)))
		require.True(t, ok)
		putToOutput(ctx.tryGetProcessedMessage())
		require.Nil(t, err)
		for {
			msg := ctx.tryGetProcessedMessage()
			if msg == nil {
				break
			}
			putToOutput(msg)
		}

		// check the commitTs is increasing
		lastCommitTs = 0
		for _, row := range output {
			require.GreaterOrEqual(t, row.CommitTs, lastCommitTs)
			// Ensure that the ReplicaID of the row is set correctly.
			require.NotEqual(t, 0, row.ReplicaID)
			lastCommitTs = row.CommitTs
		}
		sort.Slice(output, func(i, j int) bool {
			if output[i].CommitTs == output[j].CommitTs {
				return output[i].StartTs < output[j].StartTs
			}
			return output[i].CommitTs < output[j].CommitTs
		})
		require.Equal(t, tc.expected, output, cmp.Diff(output, tc.expected))
	}
}
