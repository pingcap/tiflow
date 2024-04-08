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

package memory

import (
	"context"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/sorter"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
)

func TestEventSorter(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		input     []*model.RawKVEntry
		watermark uint64
		expect    []*model.RawKVEntry
	}{
		{
			input: []*model.RawKVEntry{
				{CRTs: 1, OpType: model.OpTypePut},
				{CRTs: 2, OpType: model.OpTypePut},
				{CRTs: 4, OpType: model.OpTypeDelete},
				{CRTs: 2, OpType: model.OpTypeDelete},
			},
			watermark: 0,
			expect:    []*model.RawKVEntry{},
		},
		{
			input: []*model.RawKVEntry{
				{CRTs: 3, OpType: model.OpTypePut},
				{CRTs: 2, OpType: model.OpTypePut},
				{CRTs: 5, OpType: model.OpTypePut},
			},
			watermark: 3,
			expect: []*model.RawKVEntry{
				{CRTs: 1, OpType: model.OpTypePut},
				{CRTs: 2, OpType: model.OpTypeDelete},
				{CRTs: 2, OpType: model.OpTypePut},
				{CRTs: 2, OpType: model.OpTypePut},
				{CRTs: 3, OpType: model.OpTypePut},
			},
		},
		{
			input:     []*model.RawKVEntry{},
			watermark: 3,
			expect:    []*model.RawKVEntry{},
		},
		{
			input: []*model.RawKVEntry{
				{CRTs: 7, OpType: model.OpTypePut},
			},
			watermark: 6,
			expect: []*model.RawKVEntry{
				{CRTs: 4, OpType: model.OpTypeDelete},
				{CRTs: 5, OpType: model.OpTypePut},
			},
		},
		{
			input:     []*model.RawKVEntry{{CRTs: 7, OpType: model.OpTypeDelete}},
			watermark: 6,
			expect:    []*model.RawKVEntry{},
		},
		{
			input:     []*model.RawKVEntry{{CRTs: 7, OpType: model.OpTypeDelete}},
			watermark: 8,
			expect: []*model.RawKVEntry{
				{CRTs: 7, OpType: model.OpTypeDelete},
				{CRTs: 7, OpType: model.OpTypeDelete},
				{CRTs: 7, OpType: model.OpTypePut},
			},
		},
		{
			input:     []*model.RawKVEntry{},
			watermark: 15,
			expect:    []*model.RawKVEntry{},
		},
	}

	span := spanz.TableIDToComparableSpan(1)
	es := New(context.Background())
	es.AddTable(span, 0)
	var nextToFetch sorter.Position
	for _, tc := range testCases {
		for _, entry := range tc.input {
			es.Add(span, model.NewPolymorphicEvent(entry))
		}
		es.Add(span, model.NewWatermarkPolymorphicEvent(0, tc.watermark))
		iter := es.FetchByTable(span, nextToFetch, sorter.Position{CommitTs: tc.watermark, StartTs: tc.watermark})
		for _, expect := range tc.expect {
			event, pos, _ := iter.Next()
			require.NotNil(t, event)
			require.Equal(t, expect, event.RawKV)
			if pos.Valid() {
				nextToFetch = pos.Next()
			}
		}
		event, _, _ := iter.Next()
		require.Nil(t, event)
	}
}

func TestEventLess(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		order    int
		i        *model.PolymorphicEvent
		j        *model.PolymorphicEvent
		expected bool
	}{
		{
			0,
			&model.PolymorphicEvent{
				CRTs: 1,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypePut,
				},
			},
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypePut,
				},
			},
			true,
		},
		{
			1,
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeDelete,
				},
			},
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeDelete,
				},
			},
			false,
		},
		{
			2,
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeWatermark,
				},
			},
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeWatermark,
				},
			},
			false,
		},
		{
			3,
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeWatermark,
				},
			},
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeDelete,
				},
			},
			false,
		},
		{
			4,
			&model.PolymorphicEvent{
				CRTs: 3,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeDelete,
				},
			},
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeWatermark,
				},
			},
			false,
		},
	}

	for i, tc := range testCases {
		require.Equal(t, tc.expected, eventLess(tc.i, tc.j), "case %d", i)
	}
}
