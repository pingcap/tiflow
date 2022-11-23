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
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/model"
	schedModel "github.com/pingcap/tiflow/engine/servermaster/scheduler/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

type selectorFilter struct {
	infoProvider executorInfoProvider
}

func newSelectorFilter(infoProvider executorInfoProvider) *selectorFilter {
	return &selectorFilter{infoProvider: infoProvider}
}

func (f *selectorFilter) GetEligibleExecutors(
	ctx context.Context, request *schedModel.SchedulerRequest, candidates []model.ExecutorID,
) ([]model.ExecutorID, error) {
	// Query the information for all executors.
	allExecutors := f.infoProvider.GetExecutorInfos()
	executors := make(map[model.ExecutorID]schedModel.ExecutorInfo)
	for _, id := range candidates {
		if _, ok := allExecutors[id]; !ok {
			log.Info("skipping non-existent executor",
				zap.String("id", string(id)))
			continue
		}
		executors[id] = allExecutors[id]
	}

	// We iterate through all the selectors in the outer loop and
	// remove executors that do not match the current selector.
	// It is so designed that logging a problematic selector or label
	// becomes easier to troubleshoot.
	for _, selector := range request.Selectors {
		for id, info := range executors {
			if !selector.Matches(info.Labels) {
				delete(executors, id)
			}
		}
		if len(executors) == 0 {
			log.Info("no executor matches selector",
				zap.Any("selector", selector),
				zap.Any("request", request))
			return nil, errors.ErrSelectorUnsatisfied.GenWithStackByArgs(selector)
		}
	}

	ret := make([]model.ExecutorID, 0, len(executors))
	for id := range executors {
		ret = append(ret, id)
	}
	slices.Sort(ret) // for determinism
	return ret, nil
}
