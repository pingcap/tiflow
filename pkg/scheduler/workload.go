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

import (
	"math"

	"github.com/pingcap/tiflow/cdc/model"
)

type workloads map[model.CaptureID]model.TaskWorkload

func (w workloads) SetCapture(captureID model.CaptureID, workloads model.TaskWorkload) {
	w[captureID] = workloads
}

func (w workloads) AlignCapture(captureIDs map[model.CaptureID]struct{}) {
	for captureID := range captureIDs {
		if _, exist := w[captureID]; !exist {
			w[captureID] = make(model.TaskWorkload)
		}
	}
	for captureID := range w {
		if _, exist := captureIDs[captureID]; !exist {
			delete(w, captureID)
		}
	}
}

func (w workloads) SetTable(captureID model.CaptureID, tableID model.TableID, workload model.WorkloadInfo) {
	captureWorkloads, exist := w[captureID]
	if !exist {
		captureWorkloads = make(model.TaskWorkload)
		w[captureID] = captureWorkloads
	}
	captureWorkloads[tableID] = workload
}

func (w workloads) RemoveTable(captureID model.CaptureID, tableID model.TableID) {
	captureWorkloads, exist := w[captureID]
	if !exist {
		return
	}
	delete(captureWorkloads, tableID)
}

func (w workloads) AvgEachTable() uint64 {
	var totalWorkload uint64
	var totalTable uint64
	for _, captureWorkloads := range w {
		for _, workload := range captureWorkloads {
			totalWorkload += workload.Workload
		}
		totalTable += uint64(len(captureWorkloads))
	}
	return totalWorkload / totalTable
}

func (w workloads) Skewness() float64 {
	totalWorkloads := make([]uint64, 0, len(w))
	var workloadSum uint64
	for _, captureWorkloads := range w {
		var total uint64
		for _, workload := range captureWorkloads {
			total += workload.Workload
		}
		totalWorkloads = append(totalWorkloads, total)
		workloadSum += total
	}
	avgWorkload := float64(workloadSum) / float64(len(w))

	var totalVariance float64
	for _, totalWorkload := range totalWorkloads {
		totalVariance += math.Pow((float64(totalWorkload)/avgWorkload)-1, 2)
	}
	return math.Sqrt(totalVariance / float64(len(w)))
}

func (w workloads) SelectIdleCapture() model.CaptureID {
	minWorkload := uint64(math.MaxUint64)
	var minCapture model.CaptureID
	for captureID, captureWorkloads := range w {
		var totalWorkloadInCapture uint64
		for _, workload := range captureWorkloads {
			totalWorkloadInCapture += workload.Workload
		}
		if minWorkload > totalWorkloadInCapture {
			minWorkload = totalWorkloadInCapture
			minCapture = captureID
		}
	}
	return minCapture
}

func (w workloads) Clone() workloads {
	cloneWorkloads := make(map[model.CaptureID]model.TaskWorkload, len(w))
	for captureID, captureWorkloads := range w {
		cloneCaptureWorkloads := make(model.TaskWorkload, len(captureWorkloads))
		for tableID, workload := range captureWorkloads {
			cloneCaptureWorkloads[tableID] = workload
		}
		cloneWorkloads[captureID] = cloneCaptureWorkloads
	}
	return cloneWorkloads
}
