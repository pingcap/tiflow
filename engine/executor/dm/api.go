package dm

import (
	"context"
	"encoding/json"

	"github.com/pingcap/errors"

	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
)

// QueryStatus implements the api of query status request.
func (t *BaseTask) QueryStatus(ctx context.Context, req dmpkg.QueryStatusRequest) (dmpkg.QueryStatusResponse, error) {
	// get status from unit
	status := t.unitHolder.Status(ctx)
	// copy status via json
	statusBytes, err := json.Marshal(status)
	if err != nil {
		return dmpkg.QueryStatusResponse{ErrorMsg: err.Error()}, nil
	}
	taskStatus := runtime.NewTaskStatus(t.workerType, t.taskID, t.stage)
	err = json.Unmarshal(statusBytes, taskStatus)
	return dmpkg.QueryStatusResponse{ErrorMsg: err.Error(), TaskStatus: taskStatus}, nil
}

// StopWorker implements the api of stop worker message which kill itself.
func (t *BaseTask) StopWorker(ctx context.Context, msg dmpkg.StopWorkerMessage) error {
	if t.taskID != msg.Task {
		return errors.Errorf("task id mismatche, get %s, actually %s", msg.Task, t.taskID)
	}
	return t.Close(ctx)
}
