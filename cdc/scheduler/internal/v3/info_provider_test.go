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

package v3

import (
	"math"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/scheduler/internal"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/keyspan"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/member"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestInfoProvider(t *testing.T) {
	t.Parallel()

	cfg := &config.SchedulerConfig{
		HeartbeatTick:      math.MaxInt,
		MaxTaskConcurrency: 1,
		ChangefeedSettings: config.GetDefaultReplicaConfig().Scheduler,
	}
	coord := newCoordinatorForTest("a", model.ChangeFeedID{}, 1, cfg, redo.NewDisabledMetaManager())
	cfg.ChangefeedSettings = config.GetDefaultReplicaConfig().Scheduler
	coord.reconciler = keyspan.NewReconcilerForTests(
		keyspan.NewMockRegionCache(), cfg.ChangefeedSettings)
	coord.captureM.Captures = map[model.CaptureID]*member.CaptureStatus{
		"a": {Tables: []tablepb.TableStatus{{
			Span:       tablepb.Span{TableID: 1},
			Checkpoint: tablepb.Checkpoint{CheckpointTs: 1},
		}, {
			Span:       tablepb.Span{TableID: 2},
			Checkpoint: tablepb.Checkpoint{CheckpointTs: 1},
		}}},
		"b": {},
	}

	// Smoke testing
	var ip internal.InfoProvider = coord
	tasks, err := ip.GetTaskStatuses()
	require.Nil(t, err)
	require.EqualValues(t, map[model.CaptureID]*model.TaskStatus{
		"a": {Tables: map[model.TableID]*model.TableReplicaInfo{
			1: {StartTs: 1},
			2: {StartTs: 1},
		}},
		"b": {Tables: map[model.TableID]*model.TableReplicaInfo{}},
	}, tasks)
}

func TestInfoProviderIsInitialized(t *testing.T) {
	t.Parallel()

	coord := newCoordinatorForTest("a", model.ChangeFeedID{}, 1, &config.SchedulerConfig{
		HeartbeatTick:      math.MaxInt,
		MaxTaskConcurrency: 1,
		ChangefeedSettings: config.GetDefaultReplicaConfig().Scheduler,
	},
		redo.NewDisabledMetaManager())
	var ip internal.InfoProvider = coord

	// Has not initialized yet.
	coord.captureM.Captures = map[model.CaptureID]*member.CaptureStatus{
		"a": {State: member.CaptureStateUninitialized},
		"b": {State: member.CaptureStateInitialized},
	}
	require.False(t, ip.IsInitialized())
	// Has not initialized yet.
	coord.captureM.Captures = map[model.CaptureID]*member.CaptureStatus{
		"a": {State: member.CaptureStateInitialized},
		"b": {State: member.CaptureStateInitialized},
	}
	require.False(t, ip.IsInitialized())

	// SetInitializedForTests
	coord.captureM.SetInitializedForTests(true)
	require.True(t, ip.IsInitialized())
}
