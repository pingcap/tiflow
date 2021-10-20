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
	"context"
	"sync"
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

func TestPolymorphicEventPrepare(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	polyEvent := NewPolymorphicEvent(&RawKVEntry{OpType: OpTypeResolved})
	require.Nil(t, polyEvent.WaitPrepare(ctx))

	polyEvent = NewPolymorphicEvent(&RawKVEntry{OpType: OpTypePut})
	polyEvent.SetUpFinishedChan()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := polyEvent.WaitPrepare(ctx)
		require.Nil(t, err)
	}()
	polyEvent.PrepareFinished()
	wg.Wait()

	cctx, cancel := context.WithCancel(ctx)
	polyEvent = NewPolymorphicEvent(&RawKVEntry{OpType: OpTypePut})
	polyEvent.SetUpFinishedChan()
	cancel()
	err := polyEvent.WaitPrepare(cctx)
	require.Equal(t, context.Canceled, err)
}
