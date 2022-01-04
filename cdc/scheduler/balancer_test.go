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

package scheduler

import (
	"testing"

	"github.com/facebookgo/subset"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestBalancerFindVictimsDeterministic(t *testing.T) {
	balancer := newDeterministicTableNumberRebalancer(zap.L())
	tables := util.NewTableSet()

	tables.AddTableRecord(&util.TableRecord{
		TableID:   1,
		CaptureID: "capture-1",
	})
	tables.AddTableRecord(&util.TableRecord{
		TableID:   2,
		CaptureID: "capture-1",
	})
	tables.AddTableRecord(&util.TableRecord{
		TableID:   3,
		CaptureID: "capture-1",
	})
	tables.AddTableRecord(&util.TableRecord{
		TableID:   4,
		CaptureID: "capture-1",
	})
	tables.AddTableRecord(&util.TableRecord{
		TableID:   5,
		CaptureID: "capture-2",
	})
	tables.AddTableRecord(&util.TableRecord{
		TableID:   6,
		CaptureID: "capture-2",
	})

	mockCaptureInfos := map[model.CaptureID]*model.CaptureInfo{
		"capture-1": {
			ID: "capture-1",
		},
		"capture-2": {
			ID: "capture-2",
		},
		"capture-3": {
			ID: "capture-3",
		},
	}

	victims := balancer.FindVictims(tables, mockCaptureInfos)
	require.Len(t, victims, 2)
	require.Contains(t, victims, &util.TableRecord{
		TableID:   1,
		CaptureID: "capture-1",
	})
	require.Contains(t, victims, &util.TableRecord{
		TableID:   2,
		CaptureID: "capture-1",
	})
}

func TestBalancerFindVictimsRandomized(t *testing.T) {
	balancer := newTableNumberRebalancer(zap.L())
	tables := util.NewTableSet()

	tables.AddTableRecord(&util.TableRecord{
		TableID:   1,
		CaptureID: "capture-1",
	})
	tables.AddTableRecord(&util.TableRecord{
		TableID:   2,
		CaptureID: "capture-1",
	})
	tables.AddTableRecord(&util.TableRecord{
		TableID:   3,
		CaptureID: "capture-1",
	})
	tables.AddTableRecord(&util.TableRecord{
		TableID:   4,
		CaptureID: "capture-1",
	})
	tables.AddTableRecord(&util.TableRecord{
		TableID:   5,
		CaptureID: "capture-2",
	})
	tables.AddTableRecord(&util.TableRecord{
		TableID:   6,
		CaptureID: "capture-2",
	})

	mockCaptureInfos := map[model.CaptureID]*model.CaptureInfo{
		"capture-1": {
			ID: "capture-1",
		},
		"capture-2": {
			ID: "capture-2",
		},
		"capture-3": {
			ID: "capture-3",
		},
	}

	victims := balancer.FindVictims(tables, mockCaptureInfos)
	// We expect exactly 2 victims from "capture-1".
	require.Len(t, victims, 2)
	for _, record := range victims {
		require.Equal(t, "capture-1", record.CaptureID)
	}

	// Retry for at most 10 times to see if FindVictims can return
	// a different set of victims.
	for i := 0; i < 10; i++ {
		newVictims := balancer.FindVictims(tables, mockCaptureInfos)
		if !subset.Check(newVictims, victims) && !subset.Check(victims, newVictims) {
			return
		}
	}
	require.Fail(t, "randomness test failed")
}

func TestBalancerFindTarget(t *testing.T) {
	balancer := newTableNumberRebalancer(zap.L())
	tables := util.NewTableSet()

	tables.AddTableRecord(&util.TableRecord{
		TableID:   1,
		CaptureID: "capture-1",
	})
	tables.AddTableRecord(&util.TableRecord{
		TableID:   2,
		CaptureID: "capture-1",
	})
	tables.AddTableRecord(&util.TableRecord{
		TableID:   3,
		CaptureID: "capture-1",
	})
	tables.AddTableRecord(&util.TableRecord{
		TableID:   4,
		CaptureID: "capture-2",
	})
	tables.AddTableRecord(&util.TableRecord{
		TableID:   5,
		CaptureID: "capture-2",
	})
	tables.AddTableRecord(&util.TableRecord{
		TableID:   6,
		CaptureID: "capture-3",
	})

	mockCaptureInfos := map[model.CaptureID]*model.CaptureInfo{
		"capture-1": {
			ID: "capture-1",
		},
		"capture-2": {
			ID: "capture-2",
		},
		"capture-3": {
			ID: "capture-3",
		},
	}

	target, ok := balancer.FindTarget(tables, mockCaptureInfos)
	require.True(t, ok)
	require.Equal(t, "capture-3", target)
}

func TestBalancerFindTargetTied(t *testing.T) {
	balancer := newTableNumberRebalancer(zap.L())
	tables := util.NewTableSet()

	tables.AddTableRecord(&util.TableRecord{
		TableID:   1,
		CaptureID: "capture-1",
	})
	tables.AddTableRecord(&util.TableRecord{
		TableID:   2,
		CaptureID: "capture-1",
	})
	tables.AddTableRecord(&util.TableRecord{
		TableID:   3,
		CaptureID: "capture-2",
	})
	tables.AddTableRecord(&util.TableRecord{
		TableID:   4,
		CaptureID: "capture-2",
	})
	tables.AddTableRecord(&util.TableRecord{
		TableID:   5,
		CaptureID: "capture-3",
	})
	tables.AddTableRecord(&util.TableRecord{
		TableID:   6,
		CaptureID: "capture-3",
	})

	mockCaptureInfos := map[model.CaptureID]*model.CaptureInfo{
		"capture-1": {
			ID: "capture-1",
		},
		"capture-2": {
			ID: "capture-2",
		},
		"capture-3": {
			ID: "capture-3",
		},
	}

	target1, ok := balancer.FindTarget(tables, mockCaptureInfos)
	require.True(t, ok)

	target2, ok := balancer.FindTarget(tables, mockCaptureInfos)
	require.True(t, ok)

	target3, ok := balancer.FindTarget(tables, mockCaptureInfos)
	require.True(t, ok)

	require.False(t, target1 == target2 && target2 == target3)
}

func TestBalancerNoCaptureAvailable(t *testing.T) {
	balancer := newTableNumberRebalancer(zap.L())
	tables := util.NewTableSet()

	_, ok := balancer.FindTarget(tables, map[model.CaptureID]*model.CaptureInfo{})
	require.False(t, ok)
}

func TestBalancerRandomizeWorkload(t *testing.T) {
	balancer := newTableNumberRebalancer(zap.L())
	workload1 := balancer.(*tableNumberBalancer).randomizeWorkload(0)
	workload2 := balancer.(*tableNumberBalancer).randomizeWorkload(0)
	require.NotEqual(t, workload1, workload2)

	workload3 := balancer.(*tableNumberBalancer).randomizeWorkload(1)
	workload4 := balancer.(*tableNumberBalancer).randomizeWorkload(2)
	require.Greater(t, workload4, workload3)
}
