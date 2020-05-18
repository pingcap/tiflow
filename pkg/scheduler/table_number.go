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

func (t *TableNumberScheduler) Skewness() float64 {
	return t.workloads.Skewness()
}

func (t *TableNumberScheduler) CalRebalanceOperates(targetSkewness float64) (float64, []*TransportTable, error) {
	var totalTableNumber uint64
	for _, captureWorkloads := range t.workloads {
		totalTableNumber += uint64(len(captureWorkloads))
	}
	limitTableNumber := (totalTableNumber / uint64(len(t.workloads))) + 1
	var appendTables []int64
	tableOfCapture := make(map[int64]model.CaptureID)

	for captureID, captureWorkloads := range t.workloads {
		for uint64(len(captureWorkloads)) > limitTableNumber {
			for tableID := range captureWorkloads {
				// find a table in this capture
				appendTables = append(appendTables, tableID)
				tableOfCapture[tableID] = captureID
				t.workloads.RemoveTable(captureID, tableID)
				break
			}
		}
	}
	truncateTables := t.DistributeTables(appendTables)
	for i := 0; i < len(truncateTables); i++ {
		truncateTables[i].FromCaptureID = tableOfCapture[truncateTables[i].TableID]
		if truncateTables[i].FromCaptureID == truncateTables[i].ToCaptureID {
			truncateTables = append(truncateTables[:i], truncateTables[i+1:]...)
			i--
		}
	}
	return t.Skewness(), truncateTables, nil
}

func (t *TableNumberScheduler) DistributeTables(tableIDs []int64) []*TransportTable {
	var result []*TransportTable
	for _, tableID := range tableIDs {
		captureID := t.workloads.SelectIdleCapture()
		result = append(result, &TransportTable{
			TableID:     tableID,
			ToCaptureID: captureID,
		})
		t.workloads.SetTable(captureID, tableID, 1)
	}
	return result
}
