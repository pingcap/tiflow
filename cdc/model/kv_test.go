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

package model

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/stretchr/testify/require"
)

func TestRegionFeedEvent(t *testing.T) {
	t.Parallel()

	raw := &RawKVEntry{
		CRTs:   1,
		OpType: OpTypePut,
	}
	resolved := &ResolvedSpans{
		Spans: []RegionComparableSpan{{
			Span: tablepb.Span{StartKey: []byte("a"), EndKey: []byte("b")},
		}}, ResolvedTs: 111,
	}

	ev := &RegionFeedEvent{}
	require.Nil(t, ev.GetValue())

	ev = &RegionFeedEvent{Val: raw}
	require.Equal(t, raw, ev.GetValue())

	ev = &RegionFeedEvent{Resolved: resolved}
	require.Equal(t, resolved, ev.GetValue().(*ResolvedSpans))

	require.Equal(t, "span: [{{0 61 62} 0}], resolved-ts: 111", resolved.String())
}

func TestRawKVEntry(t *testing.T) {
	t.Parallel()

	raw := &RawKVEntry{
		StartTs: 100,
		CRTs:    101,
		OpType:  OpTypePut,
		Key:     []byte("123"),
		Value:   []byte("345"),
	}

	require.Equal(t,
		"OpType: 1, Key: 123, Value: 345, OldValue: , StartTs: 100, CRTs: 101, RegionID: 0",
		raw.String())
	require.Equal(t, int64(6), raw.ApproximateDataSize())
}
