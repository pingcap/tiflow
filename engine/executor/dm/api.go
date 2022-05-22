package dm

import (
	"context"
	"encoding/json"

	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
)

// QueryStatus implements the api of query status request
func (t *BaseTask) QueryStatus(ctx context.Context, req *dmpkg.QueryStatusRequest) *dmpkg.QueryStatusResponse {
	// get status from unit
	status := t.unitHolder.Status(ctx)
	// copy status via json
	statusBytes, err := json.Marshal(status)
	if err != nil {
		return &dmpkg.QueryStatusResponse{ErrorMsg: err.Error()}
	}
	taskStatus := runtime.NewTaskStatus(t.workerType, t.taskID, t.stage)
	err = json.Unmarshal(statusBytes, taskStatus)
	return &dmpkg.QueryStatusResponse{ErrorMsg: err.Error(), TaskStatus: taskStatus}
}
