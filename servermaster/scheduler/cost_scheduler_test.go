package scheduler

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/model"
	schedModel "github.com/hanfei1991/microcosm/servermaster/scheduler/model"
)

func getMockCapacityData() CapacityProvider {
	return &MockCapacityProvider{
		Capacities: map[model.ExecutorID]*schedModel.ExecutorResourceStatus{
			"executor-1": {
				Capacity: 100,
				Reserved: 50,
				Used:     50,
			},
			"executor-2": {
				Capacity: 100,
				Reserved: 30,
				Used:     30,
			},
			"executor-3": {
				Capacity: 100,
				Reserved: 10,
				Used:     10,
			},
		},
	}
}

const randomSeedForTest = 0x1234

func TestScheduleByCostBasics(t *testing.T) {
	costSched := NewDeterministicCostScheduler(getMockCapacityData(), randomSeedForTest)

	target, ok := costSched.ScheduleByCost(85)
	require.True(t, ok)
	require.Equal(t, model.ExecutorID("executor-3"), target)

	_, ok = costSched.ScheduleByCost(95)
	require.False(t, ok)
}

func TestScheduleByCostBalance(t *testing.T) {
	costSched := NewDeterministicCostScheduler(getMockCapacityData(), randomSeedForTest)
	counters := make(map[model.ExecutorID]int)

	for i := 0; i < 999; i++ {
		target, ok := costSched.ScheduleByCost(5)
		require.True(t, ok)
		counters[target]++
	}

	stddev := math.Sqrt(math.Pow(float64(counters["executor-1"]-333), 2) +
		math.Pow(float64(counters["executor-2"]-333), 2) +
		math.Pow(float64(counters["executor-3"]-333), 2)/3.0)
	require.Less(t, stddev, 100.0)
}
