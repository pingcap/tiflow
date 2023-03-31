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
		OpType: OpTypeResolved,
		CRTs:   101,
	}

	polyEvent := NewPolymorphicEvent(raw)
	require.Equal(t, raw, polyEvent.RawKV)
	require.Equal(t, raw.CRTs, polyEvent.CRTs)
	require.Equal(t, raw.StartTs, polyEvent.StartTs)
	require.Equal(t, raw.RegionID, polyEvent.RegionID())

	rawResolved := &RawKVEntry{CRTs: resolved.CRTs, OpType: OpTypeResolved}
	polyEvent = NewPolymorphicEvent(resolved)
	require.Equal(t, rawResolved, polyEvent.RawKV)
	require.Equal(t, resolved.CRTs, polyEvent.CRTs)
	require.Equal(t, uint64(0), polyEvent.StartTs)
}

func TestResolvedTs(t *testing.T) {
	t.Parallel()

	invalidResolvedTs := ResolvedTs{Mode: -1, Ts: 1}
	require.Equal(t, uint64(0), invalidResolvedTs.ResolvedMark())

	ts := rand.Uint64()%10 + 1
	batchID := rand.Uint64()%10 + 1
	normalResolvedTs := NewResolvedTs(ts)
	batchResolvedTs1 := ResolvedTs{Mode: BatchResolvedMode, Ts: ts, BatchID: batchID}
	require.True(t, normalResolvedTs.EqualOrGreater(batchResolvedTs1))
	require.False(t, batchResolvedTs1.EqualOrGreater(normalResolvedTs))
	require.False(t, normalResolvedTs.Less(batchResolvedTs1))
	require.True(t, batchResolvedTs1.Less(normalResolvedTs))

	batchResolvedTs2 := ResolvedTs{Mode: BatchResolvedMode, Ts: ts, BatchID: batchID + 1}
	require.True(t, normalResolvedTs.EqualOrGreater(batchResolvedTs2))
	require.True(t, batchResolvedTs2.EqualOrGreater(batchResolvedTs1))
	require.True(t, batchResolvedTs2.Less(normalResolvedTs))
	require.True(t, batchResolvedTs1.Less(batchResolvedTs2))

	largerTs := ts + rand.Uint64()%10 + 1
	largerResolvedTs := NewResolvedTs(largerTs)
	require.True(t, largerResolvedTs.EqualOrGreater(normalResolvedTs))
	largerBatchResolvedTs := ResolvedTs{
		Mode:    BatchResolvedMode,
		Ts:      largerTs,
		BatchID: batchID,
	}
	require.True(t, largerBatchResolvedTs.EqualOrGreater(normalResolvedTs),
		"largerBatchResolvedTs:%+v\nnormalResolvedTs:%+v", largerBatchResolvedTs, normalResolvedTs)

	smallerResolvedTs := NewResolvedTs(0)
	require.True(t, normalResolvedTs.EqualOrGreater(smallerResolvedTs))
	smallerBatchResolvedTs := ResolvedTs{Mode: BatchResolvedMode, Ts: 0, BatchID: batchID}
	require.True(t, batchResolvedTs1.EqualOrGreater(smallerBatchResolvedTs))
}

func TestResolvedTsEqual(t *testing.T) {
	t1 := ResolvedTs{Mode: BatchResolvedMode, Ts: 1, BatchID: 1}
	t2 := ResolvedTs{Mode: BatchResolvedMode, Ts: 1, BatchID: 1}
	require.True(t, t1.Equal(t2))

	t3 := NewResolvedTs(1)
	require.False(t, t1.Equal(t3))

	t4 := ResolvedTs{Mode: BatchResolvedMode, Ts: 1, BatchID: 2}
	require.False(t, t1.Equal(t4))

	t5 := ResolvedTs{Mode: BatchResolvedMode, Ts: 2, BatchID: 1}
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
