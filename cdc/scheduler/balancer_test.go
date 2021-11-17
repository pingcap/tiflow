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

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/scheduler/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestBalancerFindVictims(t *testing.T) {
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

func TestBalancerNoCaptureAvailable(t *testing.T) {
	balancer := newTableNumberRebalancer(zap.L())
	tables := util.NewTableSet()

	_, ok := balancer.FindTarget(tables, map[model.CaptureID]*model.CaptureInfo{})
	require.False(t, ok)
}
