package scheduler

import (
	"math/rand"
	"time"

	"github.com/hanfei1991/microcosm/model"
	schedModel "github.com/hanfei1991/microcosm/servermaster/scheduler/model"
)

// CostScheduler is a random scheduler
type CostScheduler struct {
	capacityProvider CapacityProvider
	random           *rand.Rand
}

// NewRandomizedCostScheduler creates a CostScheduler instance
func NewRandomizedCostScheduler(capacityProvider CapacityProvider) *CostScheduler {
	return &CostScheduler{
		capacityProvider: capacityProvider,
		random:           rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// NewDeterministicCostScheduler is used in unit-testing.
func NewDeterministicCostScheduler(capacityProvider CapacityProvider, seed int64) *CostScheduler {
	return &CostScheduler{
		capacityProvider: capacityProvider,
		random:           rand.New(rand.NewSource(seed)),
	}
}

// ScheduleByCost is a native random based scheduling strategy
func (s *CostScheduler) ScheduleByCost(cost schedModel.ResourceUnit) (model.ExecutorID, bool) {
	executorCaps := s.capacityProvider.CapacitiesForAllExecutors()
	executorList := make([]model.ExecutorID, 0, len(executorCaps))
	for executorID := range executorCaps {
		executorList = append(executorList, executorID)
	}
	// TODO optimize for performance if the need arises.
	s.random.Shuffle(len(executorCaps), func(i, j int) {
		executorList[i], executorList[j] = executorList[j], executorList[i]
	})

	for _, executorID := range executorList {
		if executorCaps[executorID].Remaining() > cost {
			return executorID, true
		}
	}

	return "", false
}
