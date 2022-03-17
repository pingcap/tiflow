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

	"go.uber.org/zap"

	"github.com/pingcap/tiflow/cdc/scheduler/util"
	"github.com/pingcap/tiflow/dm/pkg/log"
)

type balancer interface {
	// FindVictims returns a set of possible victim tables.
	// Removing these tables will make the workload more balanced.
	FindVictims(
		// if we want to support workload later, we can
		totalWeight int,
		workers map[string]*Worker,
		hasLoadTaskByWorkerAndSource func(string, string) bool,
	) (sourcesToBalance []string)
}

type tableNumberBalancer struct {
	logger log.Logger
}

func (r *tableNumberBalancer) FindVictims(
	sourceNumber int,
	workers map[string]*Worker,
	hasLoadTaskByWorkerAndSource func(string, string) bool,
) (sourcesToBalance []string) {
	workerNum := len(workers)

	if workerNum == 0 {
		return nil
	}
	upperLimitPerCapture := int(math.Ceil(float64(sourceNumber) / float64(workerNum)))

	r.logger.Info("Start rebalancing",
		zap.Int("sourceNumber", sourceNumber),
		zap.Int("workerNum", workerNum),
		zap.Int("targetLimit", upperLimitPerCapture))

	var victims []*util.TableRecord
	for _, w := range workers {
		var sourceList []sourceScore
		for source := range w.Bounds() {
			if hasLoadTaskByWorkerAndSource(w.BaseInfo().Name, source) {
				continue
			}
			sourceList = append(sourceList, sourceScore{score: sourceNumber, source: source})
		}

		if r.random != nil {
			// Complexity note: Shuffle has O(n), where `n` is the number of tables.
			// Also, during a single call of FindVictims, Shuffle can be called at most
			// `c` times, where `c` is the number of captures (TiCDC nodes).
			// Since FindVictims is called only when a rebalance is triggered, which happens
			// only rarely, we do not expect a performance degradation as a result of adding
			// the randomness.
			r.random.Shuffle(len(sourceList), func(i, j int) {
				sourceList[i], sourceList[j] = sourceList[j], sourceList[i]
			})
		} else {
			// We sort the tableIDs here so that the result is deterministic,
			// which would aid testing and debugging.
			util.SortTableIDs(sourceList)
		}

		tableNum2Remove := len(tables) - upperLimitPerCapture
		if tableNum2Remove <= 0 {
			continue
		}

		// here we pick `tableNum2Remove` tables to delete,
		for _, tableID := range sourceList {
			if tableNum2Remove <= 0 {
				break
			}

			record := tables[tableID]
			if record == nil {
				panic("unreachable")
			}

			r.logger.Info("Rebalance: find victim table",
				zap.Any("tableRecord", record))
			victims = append(victims, record)
			tableNum2Remove--
		}
	}
	return victims
}

type sourceScore struct {
	source string
	score  int
}
