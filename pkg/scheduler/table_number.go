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

// TableNumberScheduler provides a feature that scheduling by the table number
type TableNumberScheduler struct {
	workloads workloads
}

// NewTableNumberScheduler creates a new table number scheduler
func NewTableNumberScheduler() *TableNumberScheduler {
	return &TableNumberScheduler{
		workloads: make(workloads),
	}
}

// ResetWorkloads implements the Scheduler interface
func (t *TableNumberScheduler) ResetWorkloads(captureID model.CaptureID, workloads model.TaskWorkload) {
	t.workloads.SetCapture(captureID, workloads)
}

// AlignCapture implements the Scheduler interface
func (t *TableNumberScheduler) AlignCapture(captureIDs map[model.CaptureID]struct{}) {
	t.workloads.AlignCapture(captureIDs)
}

// Skewness implements the Scheduler interface
func (t *TableNumberScheduler) Skewness() float64 {
	return t.workloads.Skewness()
}

// CalRebalanceOperates implements the Scheduler interface
func (t *TableNumberScheduler) CalRebalanceOperates(targetSkewness float64, boundaryTs model.Ts) (
	skewness float64,
	deleteOperations map[model.CaptureID]map[model.TableID]*model.TableOperation,
	addOperations map[model.CaptureID]map[model.TableID]*model.TableOperation) {
	var totalTableNumber uint64
	for _, captureWorkloads := range t.workloads {
		totalTableNumber += uint64(len(captureWorkloads))
	}
	limitTableNumber := (totalTableNumber / uint64(len(t.workloads))) + 1
	appendTables := make(map[model.TableID]model.Ts)
	deleteOperations = make(map[model.CaptureID]map[model.TableID]*model.TableOperation, len(t.workloads))

	for captureID, captureWorkloads := range t.workloads {
		for uint64(len(captureWorkloads)) > limitTableNumber {
			for tableID := range captureWorkloads {
				// find a table in this capture
				appendTables[tableID] = boundaryTs
				operations := deleteOperations[captureID]
				if operations == nil {
					operations = make(map[model.TableID]*model.TableOperation)
					deleteOperations[captureID] = operations
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
	addOperations = t.DistributeTables(appendTables)
	for captureID, deleteOps := range deleteOperations {
		for tableID := range deleteOps {
			addOps := addOperations[captureID]
			if addOps == nil {
				continue
			}
			addOp := addOps[tableID]
			if addOp == nil {
				continue
			}
			delete(deleteOps, tableID)
			delete(addOps, tableID)
		}
	}
	skewness = t.Skewness()
	return
}

// DistributeTables implements the Scheduler interface
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
		t.workloads.SetTable(captureID, tableID, model.WorkloadInfo{Workload: 1})
	}
	return result
}
