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
	"math/rand"
	"time"

	"github.com/pingcap/tiflow/engine/model"
	schedModel "github.com/pingcap/tiflow/engine/servermaster/scheduler/model"
)

type CostScheduler struct {
	capacityProvider CapacityProvider
	random           *rand.Rand
}

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
