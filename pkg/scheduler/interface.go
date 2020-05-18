package scheduler

import "github.com/pingcap/ticdc/cdc/model"

type TransportTable struct {
	FromCaptureID string
	ToCaptureID   string
	TableID       int64
}

type Scheduler interface {
	ResetWorkloads(captureID model.CaptureID, workloads map[int64]uint64)
	Skewness() float64
	CalRebalanceOperates(targetSkewness float64) (float64, []*TransportTable, error)
	DistributeTables(tableIDs []int64) []*TransportTable
}
