package scheduler

import "github.com/pingcap/ticdc/cdc/model"

type Scheduler interface {
	ResetWorkloads(captureID model.CaptureID, workloads map[model.TableID]uint64)
	AlignCapture(captureIDs map[model.CaptureID]struct{})
	Skewness() float64
	CalRebalanceOperates(targetSkewness float64, boundaryTs model.Ts) (float64, map[model.CaptureID]map[model.TableID]*model.TableOperation)
	DistributeTables(tableIDs map[model.TableID]model.Ts) map[model.CaptureID]map[model.TableID]*model.TableOperation
}
