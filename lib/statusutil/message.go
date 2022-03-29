package statusutil

import (
	"fmt"

	libModel "github.com/hanfei1991/microcosm/lib/model"
)

type WorkerStatusMessage struct {
	Worker      WorkerID               `json:"worker"`
	MasterEpoch Epoch                  `json:"master-epoch"`
	Status      *libModel.WorkerStatus `json:"status"`
}

func WorkerStatusTopic(masterID MasterID) string {
	return fmt.Sprintf("worker-status-%s", masterID)
}
