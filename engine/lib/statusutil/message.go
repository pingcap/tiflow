package statusutil

import (
	"fmt"

	libModel "github.com/hanfei1991/microcosm/lib/model"
)

// WorkerStatusMessage contains necessary fileds of a worker status message
type WorkerStatusMessage struct {
	Worker      libModel.WorkerID      `json:"worker"`
	MasterEpoch libModel.Epoch         `json:"master-epoch"`
	Status      *libModel.WorkerStatus `json:"status"`
}

// WorkerStatusTopic returns the p2p topic for worker status subscription of a
// given master.
func WorkerStatusTopic(masterID libModel.MasterID) string {
	return fmt.Sprintf("worker-status-%s", masterID)
}
