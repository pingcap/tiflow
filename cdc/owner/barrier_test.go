// Copyright 2021 PingCAP, Inc.
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

package owner

import (
	"math"
	"math/rand"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestBarrier(t *testing.T) {
	b := newBarriers()
	b.Update(ddlJobBarrier, 2)
	b.Update(syncPointBarrier, 3)
	b.Update(finishBarrier, 1)
	tp, ts := b.Min()
	require.Equal(t, tp, finishBarrier)
	require.Equal(t, ts, uint64(1))

	b.Update(finishBarrier, 4)
	tp, ts = b.Min()
	require.Equal(t, tp, ddlJobBarrier)
	require.Equal(t, ts, uint64(2))

	b.Remove(ddlJobBarrier)
	tp, ts = b.Min()
	require.Equal(t, tp, syncPointBarrier)
	require.Equal(t, ts, uint64(3))

	b.Update(finishBarrier, 1)
	tp, ts = b.Min()
	require.Equal(t, tp, finishBarrier)
	require.Equal(t, ts, uint64(1))

	b.Update(ddlJobBarrier, 5)
	tp, ts = b.Min()
	require.Equal(t, tp, finishBarrier)
	require.Equal(t, ts, uint64(1))
}

func TestBarrierRandom(t *testing.T) {
	maxBarrierType := 50
	maxBarrierTs := 1000000
	b := newBarriers()
	expectedBarriers := make(map[barrierType]model.Ts)

	// set a barrier which can not be removed to avoid the barrier map is empty
	b.Update(barrierType(maxBarrierType), model.Ts(maxBarrierTs))
	expectedBarriers[barrierType(maxBarrierType)] = model.Ts(maxBarrierTs)

	for i := 0; i < 100000; i++ {
		switch rand.Intn(2) {
		case 0:
			tp := barrierType(rand.Intn(maxBarrierType))
			ts := model.Ts(rand.Intn(maxBarrierTs))
			b.Update(tp, ts)
			expectedBarriers[tp] = ts
		case 1:
			tp := barrierType(rand.Intn(maxBarrierType))
			b.Remove(tp)
			delete(expectedBarriers, tp)
		}
		expectedMinTs := uint64(math.MaxUint64)
		for _, ts := range expectedBarriers {
			if ts < expectedMinTs {
				expectedMinTs = ts
			}
		}
		tp, ts := b.Min()
		require.Equal(t, ts, expectedMinTs)
		require.Equal(t, expectedBarriers[tp], expectedMinTs)
	}
}
