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
	"github.com/pingcap/tiflow/cdc/scheduler/internal"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/schedulepb"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestInfoProvider(t *testing.T) {
	t.Parallel()

	coord := newCoordinator("a", model.ChangeFeedID{}, 1, &config.SchedulerConfig{
		HeartbeatTick:      math.MaxInt,
		MaxTaskConcurrency: 1,
	})
	coord.captureM.Captures = map[model.CaptureID]*CaptureStatus{
		"a": {Tables: []schedulepb.TableStatus{{
			TableID:    1,
			Checkpoint: schedulepb.Checkpoint{CheckpointTs: 1},
		}, {
			TableID:    2,
			Checkpoint: schedulepb.Checkpoint{CheckpointTs: 1},
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
	pos, err := ip.GetTaskPositions()
	require.Nil(t, err)
	require.Len(t, pos, 2)
	require.Len(t, ip.GetTotalTableCounts(), 2)
	require.Empty(t, ip.GetPendingTableCounts())
}
