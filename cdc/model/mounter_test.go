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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPolymorphicEvent(t *testing.T) {
	t.Parallel()
	raw := &RawKVEntry{
		StartTs:  99,
		CRTs:     100,
		OpType:   OpTypePut,
		RegionID: 2,
	}
	resolved := &RawKVEntry{
		OpType: OpTypeWatermark,
		CRTs:   101,
	}

	polyEvent := NewPolymorphicEvent(raw)
	require.Equal(t, raw, polyEvent.RawKV)
	require.Equal(t, raw.CRTs, polyEvent.CRTs)
	require.Equal(t, raw.StartTs, polyEvent.StartTs)
	require.Equal(t, raw.RegionID, polyEvent.RegionID())

	rawResolved := &RawKVEntry{CRTs: resolved.CRTs, OpType: OpTypeWatermark}
	polyEvent = NewPolymorphicEvent(resolved)
	require.Equal(t, rawResolved, polyEvent.RawKV)
	require.Equal(t, resolved.CRTs, polyEvent.CRTs)
	require.Equal(t, uint64(0), polyEvent.StartTs)
}

func TestWatermark(t *testing.T) {
	t.Parallel()

	invalidWatermark := Watermark{Mode: -1, Ts: 1}
	require.Equal(t, uint64(0), invalidWatermark.ResolvedMark())

	ts := rand.Uint64()%10 + 1
	batchID := rand.Uint64()%10 + 1
	normalWatermark := NewWatermark(ts)
	batchWatermark1 := Watermark{Mode: BatchResolvedMode, Ts: ts, BatchID: batchID}
	require.True(t, normalWatermark.EqualOrGreater(batchWatermark1))
	require.False(t, batchWatermark1.EqualOrGreater(normalWatermark))
	require.False(t, normalWatermark.Less(batchWatermark1))
	require.True(t, batchWatermark1.Less(normalWatermark))

	batchWatermark2 := Watermark{Mode: BatchResolvedMode, Ts: ts, BatchID: batchID + 1}
	require.True(t, normalWatermark.EqualOrGreater(batchWatermark2))
	require.True(t, batchWatermark2.EqualOrGreater(batchWatermark1))
	require.True(t, batchWatermark2.Less(normalWatermark))
	require.True(t, batchWatermark1.Less(batchWatermark2))

	largerTs := ts + rand.Uint64()%10 + 1
	largerWatermark := NewWatermark(largerTs)
	require.True(t, largerWatermark.EqualOrGreater(normalWatermark))
	largerBatchWatermark := Watermark{
		Mode:    BatchResolvedMode,
		Ts:      largerTs,
		BatchID: batchID,
	}
	require.True(t, largerBatchWatermark.EqualOrGreater(normalWatermark),
		"largerBatchWatermark:%+v\nnormalWatermark:%+v", largerBatchWatermark, normalWatermark)

	smallerWatermark := NewWatermark(0)
	require.True(t, normalWatermark.EqualOrGreater(smallerWatermark))
	smallerBatchWatermark := Watermark{Mode: BatchResolvedMode, Ts: 0, BatchID: batchID}
	require.True(t, batchWatermark1.EqualOrGreater(smallerBatchWatermark))
}

func TestWatermarkEqual(t *testing.T) {
	t1 := Watermark{Mode: BatchResolvedMode, Ts: 1, BatchID: 1}
	t2 := Watermark{Mode: BatchResolvedMode, Ts: 1, BatchID: 1}
	require.True(t, t1.Equal(t2))

	t3 := NewWatermark(1)
	require.False(t, t1.Equal(t3))

	t4 := Watermark{Mode: BatchResolvedMode, Ts: 1, BatchID: 2}
	require.False(t, t1.Equal(t4))

	t5 := Watermark{Mode: BatchResolvedMode, Ts: 2, BatchID: 1}
	require.False(t, t1.Equal(t5))
}

func TestComparePolymorphicEvents(t *testing.T) {
	cases := []struct {
		a *PolymorphicEvent
		b *PolymorphicEvent
	}{
		{
			a: NewPolymorphicEvent(&RawKVEntry{
				OpType: OpTypeDelete,
			}),
			b: NewPolymorphicEvent(&RawKVEntry{
				OpType: OpTypePut,
			}),
		},
		{
			a: NewPolymorphicEvent(&RawKVEntry{
				OpType:   OpTypePut,
				OldValue: []byte{0},
				Value:    []byte{0},
			}),
			b: NewPolymorphicEvent(&RawKVEntry{
				OpType: OpTypePut,
				Value:  []byte{0},
			}),
		},
	}
	for _, item := range cases {
		require.True(t, ComparePolymorphicEvents(item.a, item.b))
	}
}
