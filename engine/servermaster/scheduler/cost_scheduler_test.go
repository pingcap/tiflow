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

package scheduler

import (
	"math"
	"testing"

	"github.com/pingcap/tiflow/engine/model"
	schedModel "github.com/pingcap/tiflow/engine/servermaster/scheduler/model"
	"github.com/stretchr/testify/require"
)

func getMockCapacityData() *mockExecutorInfoProvider {
	return &mockExecutorInfoProvider{
		infos: map[model.ExecutorID]schedModel.ExecutorInfo{
			"executor-1": {
				ID: "executor-1",
				ResourceStatus: schedModel.ExecutorResourceStatus{
					Capacity: 100,
					Reserved: 50,
					Used:     50,
				},
			},
			"executor-2": {
				ID: "executor-2",
				ResourceStatus: schedModel.ExecutorResourceStatus{
					Capacity: 100,
					Reserved: 30,
					Used:     30,
				},
			},
			"executor-3": {
				ID: "executor-3",
				ResourceStatus: schedModel.ExecutorResourceStatus{
					Capacity: 100,
					Reserved: 10,
					Used:     10,
				},
			},
		},
	}
}

const randomSeedForTest = 0x1234

func TestScheduleByCostBasics(t *testing.T) {
	costSched := NewDeterministicCostScheduler(getMockCapacityData(), randomSeedForTest)

	target, ok := costSched.ScheduleByCost(85,
		[]model.ExecutorID{"executor-1", "executor-2", "executor-3"})
	require.True(t, ok)
	require.Equal(t, model.ExecutorID("executor-3"), target)

	_, ok = costSched.ScheduleByCost(95,
		[]model.ExecutorID{"executor-1", "executor-2", "executor-3"})
	require.False(t, ok)
}

func TestScheduleByCostBalance(t *testing.T) {
	costSched := NewDeterministicCostScheduler(getMockCapacityData(), randomSeedForTest)
	counters := make(map[model.ExecutorID]int)

	for i := 0; i < 999; i++ {
		target, ok := costSched.ScheduleByCost(5,
			[]model.ExecutorID{"executor-1", "executor-2", "executor-3"})
		require.True(t, ok)
		counters[target]++
	}

	stddev := math.Sqrt(math.Pow(float64(counters["executor-1"]-333), 2) +
		math.Pow(float64(counters["executor-2"]-333), 2) +
		math.Pow(float64(counters["executor-3"]-333), 2)/3.0)
	require.Less(t, stddev, 100.0)
}
