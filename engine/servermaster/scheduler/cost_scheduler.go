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

// costScheduler is a random scheduler
type costScheduler struct {
	infoProvider executorInfoProvider
	random       *rand.Rand
}

// NewRandomizedCostScheduler creates a costScheduler instance
func NewRandomizedCostScheduler(infoProvider executorInfoProvider) *costScheduler {
	return &costScheduler{
		infoProvider: infoProvider,
		random:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// NewDeterministicCostScheduler is used in unit-testing.
func NewDeterministicCostScheduler(infoProvider executorInfoProvider, seed int64) *costScheduler {
	return &costScheduler{
		infoProvider: infoProvider,
		random:       rand.New(rand.NewSource(seed)),
	}
}

// ScheduleByCost is a native random based scheduling strategy
func (s *costScheduler) ScheduleByCost(cost schedModel.ResourceUnit, candidates []model.ExecutorID) (model.ExecutorID, bool) {
	infos := s.infoProvider.GetExecutorInfos()
	// TODO optimize for performance if the need arises.
	s.random.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	for _, executorID := range candidates {
		if infos[executorID].ResourceStatus.Remaining() > cost {
			return executorID, true
		}
	}

	return "", false
}
