package scheduler

import (
	"math"

	"github.com/pingcap/ticdc/cdc/model"
)

type workloads map[model.CaptureID]map[model.TableID]uint64

func (w workloads) SetCapture(captureID model.CaptureID, workloads map[model.TableID]uint64) {
	w[captureID] = workloads
}

func (w workloads) AlignCapture(captureIDs map[model.CaptureID]struct{}) {
	for captureID := range captureIDs {
		if _, exist := w[captureID]; !exist {
			w[captureID] = make(map[model.TableID]uint64)
		}
	}
	for captureID := range w {
		if _, exist := captureIDs[captureID]; !exist {
			delete(w, captureID)
		}
	}
}

func (w workloads) SetTable(captureID model.CaptureID, tableID model.TableID, workload uint64) {
	captureWorkloads, exist := w[captureID]
	if !exist {
		captureWorkloads = make(map[model.TableID]uint64)
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
			totalWorkload += workload
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
			total += workload
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
			totalWorkloadInCapture += workload
		}
		if minWorkload > totalWorkloadInCapture {
			minWorkload = totalWorkloadInCapture
			minCapture = captureID
		}
	}
	return minCapture
}

func (w workloads) Clone() workloads {
	cloneWorkloads := make(map[model.CaptureID]map[int64]uint64, len(w))
	for captureID, captureWorkloads := range w {
		cloneCaptureWorkloads := make(map[int64]uint64, len(captureWorkloads))
		for tableID, workload := range captureWorkloads {
			cloneCaptureWorkloads[tableID] = workload
		}
		cloneWorkloads[captureID] = cloneCaptureWorkloads
	}
	return cloneWorkloads
}
