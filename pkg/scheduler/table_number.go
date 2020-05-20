// Copyright 2020 PingCAP, Inc.
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

import "github.com/pingcap/ticdc/cdc/model"

type TableNumberScheduler struct {
	workloads workloads
}

func NewTableNumberScheduler() *TableNumberScheduler {
	return &TableNumberScheduler{
		workloads: make(workloads),
	}
}

func (t *TableNumberScheduler) ResetWorkloads(captureID model.CaptureID, workloads map[int64]uint64) {
	t.workloads.SetCapture(captureID, workloads)
}

func (t *TableNumberScheduler) AlignCapture(captureIDs map[model.CaptureID]struct{}) {
	t.workloads.AlignCapture(captureIDs)
}

func (t *TableNumberScheduler) Skewness() float64 {
	return t.workloads.Skewness()
}

func (t *TableNumberScheduler) CalRebalanceOperates(targetSkewness float64, boundaryTs model.Ts) (float64, map[model.CaptureID]map[model.TableID]*model.TableOperation) {
	var totalTableNumber uint64
	for _, captureWorkloads := range t.workloads {
		totalTableNumber += uint64(len(captureWorkloads))
	}
	limitTableNumber := (totalTableNumber / uint64(len(t.workloads))) + 1
	appendTables := make(map[model.TableID]model.Ts)
	result := make(map[model.CaptureID]map[model.TableID]*model.TableOperation, len(t.workloads))

	for captureID, captureWorkloads := range t.workloads {
		for uint64(len(captureWorkloads)) > limitTableNumber {
			for tableID := range captureWorkloads {
				// find a table in this capture
				appendTables[tableID] = boundaryTs
				operations := result[captureID]
				if operations == nil {
					operations = make(map[model.TableID]*model.TableOperation)
					result[captureID] = operations
				}
				operations[tableID] = &model.TableOperation{
					Delete:     true,
					BoundaryTs: boundaryTs,
				}
				t.workloads.RemoveTable(captureID, tableID)
				break
			}
		}
	}
	truncateTables := t.DistributeTables(appendTables)
	for captureID, tableOperations := range truncateTables {
		operations := result[captureID]
		if operations == nil {
			operations = make(map[model.TableID]*model.TableOperation)
			result[captureID] = operations
		}
		AppendTaskOperations(operations, tableOperations)
	}
	return t.Skewness(), truncateTables
}

func (t *TableNumberScheduler) DistributeTables(tableIDs map[model.TableID]model.Ts) map[model.CaptureID]map[model.TableID]*model.TableOperation {
	result := make(map[model.CaptureID]map[model.TableID]*model.TableOperation, len(t.workloads))
	for tableID, boundaryTs := range tableIDs {
		captureID := t.workloads.SelectIdleCapture()
		operations := result[captureID]
		if operations == nil {
			operations = make(map[model.TableID]*model.TableOperation)
			result[captureID] = operations
		}
		operations[tableID] = &model.TableOperation{
			BoundaryTs: boundaryTs,
		}
		t.workloads.SetTable(captureID, tableID, 1)
	}
	return result
}

func AppendTaskOperations(targetOperations map[model.TableID]*model.TableOperation, toAppendOperations map[model.TableID]*model.TableOperation) {
	for tableID, operation := range toAppendOperations {
		AppendTaskOperation(targetOperations, tableID, operation)
	}
}

func AppendTaskOperation(targetOperations map[model.TableID]*model.TableOperation, tableID model.TableID, operation *model.TableOperation) {
	if originalOperation, exist := targetOperations[tableID]; exist {
		if originalOperation.Delete != operation.Delete {
			delete(targetOperations, tableID)
		}
		return
	}
	targetOperations[tableID] = operation
}
