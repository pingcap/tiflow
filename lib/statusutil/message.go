package statusutil

import (
	"fmt"

	libModel "github.com/hanfei1991/microcosm/lib/model"
)

type WorkerStatusMessage struct {
	Worker      libModel.WorkerID      `json:"worker"`
	MasterEpoch libModel.Epoch         `json:"master-epoch"`
	Status      *libModel.WorkerStatus `json:"status"`
}

func WorkerStatusTopic(masterID libModel.MasterID) string {
	return fmt.Sprintf("worker-status-%s", masterID)
}
