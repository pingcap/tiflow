// Copyright 2021 PingCAP, Inc.
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

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/scheduler/util"
	"go.uber.org/zap"
)

// balancer is used to support the rebalance process, in which
// some victims are chosen and de-scheduled. Later, the victims
// will be automatically rescheduled, during which the target captures
// will be chosen so that the workload is the most balanced.
type balancer interface {
	// FindVictims returns a set of possible victim tables.
	// Removing these tables will make the workload more balanced.
	FindVictims(
		tables *util.TableSet,
		captures map[model.CaptureID]*model.CaptureInfo,
	) (tablesToRemove []*util.TableRecord)

	// FindTarget returns a target capture to add a table to.
	FindTarget(
		tables *util.TableSet,
		captures map[model.CaptureID]*model.CaptureInfo,
	) (minLoadCapture model.CaptureID, ok bool)
}

// tableNumberBalancer implements a balance strategy based on the
// current number of tables replicated by each capture.
// TODO: Implement finer-grained balance strategy based on the actual
// workload of each table.
type tableNumberBalancer struct {
	logger *zap.Logger
}

func newTableNumberRebalancer(logger *zap.Logger) balancer {
	return &tableNumberBalancer{
		logger: logger,
	}
}

// FindTarget returns the capture with the smallest workload (in table count).
func (r *tableNumberBalancer) FindTarget(
	tables *util.TableSet,
	captures map[model.CaptureID]*model.CaptureInfo,
) (minLoadCapture model.CaptureID, ok bool) {
	if len(captures) == 0 {
		return "", false
	}

	captureWorkload := make(map[model.CaptureID]int)
	for captureID := range captures {
		captureWorkload[captureID] = 0
	}

	for captureID, tables := range tables.GetAllTablesGroupedByCaptures() {
		// We use the number of tables as workload
		captureWorkload[captureID] = len(tables)
	}

	candidate := ""
	minWorkload := math.MaxInt64

	for captureID, workload := range captureWorkload {
		if workload < minWorkload {
			minWorkload = workload
			candidate = captureID
		}
	}

	if minWorkload == math.MaxInt64 {
		r.logger.Panic("unexpected minWorkerload == math.MaxInt64")
	}

	return candidate, true
}

// FindVictims returns some victims to remove.
// Read the comment in the function body on the details of the victim selection.
func (r *tableNumberBalancer) FindVictims(
	tables *util.TableSet,
	captures map[model.CaptureID]*model.CaptureInfo,
) []*util.TableRecord {
	// Algorithm overview: We try to remove some tables as the victims so that
	// no captures are assigned more tables than the average workload measured in table number,
	// modulo the necessary margin due to the fraction part of the average.
	//
	// In formula, we try to maintain the invariant:
	//
	// num(tables assigned to any capture) < num(tables) / num(captures) + 1

	totalTableNum := len(tables.GetAllTables())
	captureNum := len(captures)
	upperLimitPerCapture := int(math.Ceil(float64(totalTableNum) / float64(captureNum)))

	r.logger.Info("Start rebalancing",
		zap.Int("table-num", totalTableNum),
		zap.Int("capture-num", captureNum),
		zap.Int("target-limit", upperLimitPerCapture))

	var victims []*util.TableRecord
	for _, tables := range tables.GetAllTablesGroupedByCaptures() {
		var tableList []model.TableID
		for tableID := range tables {
			tableList = append(tableList, tableID)
		}
		// We sort the tableIDs here so that the result is deterministic,
		// which would aid testing and debugging.
		util.SortTableIDs(tableList)

		tableNum2Remove := len(tables) - upperLimitPerCapture
		if tableNum2Remove <= 0 {
			continue
		}

		// here we pick `tableNum2Remove` tables to delete,
		for _, tableID := range tableList {
			if tableNum2Remove <= 0 {
				break
			}

			record := tables[tableID]
			if record == nil {
				panic("unreachable")
			}

			r.logger.Info("Rebalance: find victim table",
				zap.Any("table-record", record))
			victims = append(victims, record)
			tableNum2Remove--
		}
	}
	return victims
}
