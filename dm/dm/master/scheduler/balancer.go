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
	"math/rand"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/log"
)

const (
	rebalanceInterval = 2 * time.Second
	hasLoadTaskWeight = 1e6
)

type balancer interface {
	// FindVictims returns a set of possible victim tables.
	// Removing these tables will make the workload more balanced.
	FindVictims(
		// if we want to support workload later, we can
		sourceCfgs map[string]*config.SourceConfig,
		workers map[string]*Worker,
		relayWorkers map[string]map[string]struct{},
		hasLoadTaskByWorkerAndSource func(string, string) bool,
	) (sourcesToBalance []string)
	// CanBalance returns true if the worker is balanced.
	CanBalance(totalWeight int, workers map[string]*Worker, workerWeight int) bool
	// GetWorkerBoundsByWeight returns the weight of the worker.
	GetWorkerBoundsByWeight(w *Worker, relayWorkers map[string]map[string]struct{}, sourceCfgs map[string]*config.SourceConfig, hasLoadTaskByWorkerAndSource func(string, string) bool) sourceHelper
}

func newTableNumberBalancer(pLogger *log.Logger) *tableNumberBalancer {
	return &tableNumberBalancer{
		logger: pLogger.WithFields(zap.String("component", "balancer")),
	}
}

type tableNumberBalancer struct {
	logger log.Logger
}

func (r *tableNumberBalancer) FindVictims(
	sourceCfgs map[string]*config.SourceConfig,
	workers map[string]*Worker,
	relayWorkers map[string]map[string]struct{},
	hasLoadTaskByWorkerAndSource func(string, string) bool,
) []string {
	sourceNumber := len(sourceCfgs)
	workerNum := 0
	for _, w := range workers {
		if w.Stage() != WorkerOffline {
			workerNum++
		}
	}

	if workerNum == 0 {
		return nil
	}
	upperLimitPerCapture := int(math.Ceil(float64(sourceNumber) / float64(workerNum)))
	r.logger.Info("start rebalancing",
		zap.Int("sourceNumber", sourceNumber),
		zap.Int("workerNum", workerNum),
		zap.Int("targetLimit", upperLimitPerCapture))

	victims := make(sourceHelper, 0, len(workers))
	for _, w := range workers {
		bounds := w.Bounds()
		sourceNum2Remove := len(bounds) - upperLimitPerCapture
		if sourceNum2Remove <= 0 || w.Stage() == WorkerOffline {
			continue
		}

		sourceList := r.GetWorkerBoundsByWeight(w, relayWorkers, sourceCfgs, hasLoadTaskByWorkerAndSource)

		// here we pick `sourceNum2Remove` tables to delete,
		for _, record := range sourceList {
			if sourceNum2Remove <= 0 || record.score >= hasLoadTaskWeight {
				break
			}

			r.logger.Info("find victim source", zap.String("source", record.source), zap.Float32("score", record.score))
			victims = append(victims, record)
			sourceNum2Remove--
		}
	}

	sort.Sort(victims)
	victimSources := make([]string, 0, len(victims))
	for _, record := range victims {
		victimSources = append(victimSources, record.source)
	}
	return victimSources
}

func (r *tableNumberBalancer) GetWorkerBoundsByWeight(w *Worker, relayWorkers map[string]map[string]struct{},
	sourceCfgs map[string]*config.SourceConfig, hasLoadTaskByWorkerAndSource func(string, string) bool) sourceHelper {
	relaySources := w.RelaySources()
	bounds := w.Bounds()

	sourceList := make(sourceHelper, 0, len(bounds))
	for source := range bounds {
		var score float32
		_, hasRelay := relaySources[source]
		if !hasRelay {
			if sourceCfg, ok := sourceCfgs[source]; ok {
				hasRelay = sourceCfg.EnableRelay
			}
		}
		switch {
		// don't rebalance the source that has load task
		case hasLoadTaskByWorkerAndSource(w.BaseInfo().Name, source):
			score = hasLoadTaskWeight
		case hasRelay:
			score = 100 - float32(len(relayWorkers[source])) + rand.Float32()
		default:
			score = rand.Float32() - float32(len(relayWorkers[source])) // let workers with most relay workers to rebound other workers
		}
		sourceList = append(sourceList, sourceScore{score: score, source: source})
	}
	sort.Sort(sourceList)
	return sourceList
}

func (r *tableNumberBalancer) CanBalance(sourceNumber int, workers map[string]*Worker, workerWeight int) bool {
	workerNum := 0
	for _, w := range workers {
		if w.Stage() != WorkerOffline {
			workerNum++
		}
	}
	upperLimitPerCapture := int(math.Ceil(float64(sourceNumber) / float64(workerNum)))
	return workerWeight <= upperLimitPerCapture
}

type sourceScore struct {
	source string
	score  float32
}

type sourceHelper []sourceScore

func (s sourceHelper) Len() int {
	return len(s)
}

func (s sourceHelper) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sourceHelper) Less(i, j int) bool {
	return s[i].score < s[j].score
}
