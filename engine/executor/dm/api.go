// Copyright 2022 PingCAP, Inc.
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

package dm

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"

	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
)

// QueryStatus implements the api of query status request.
// QueryStatus is called by refection of commandHandler.
func (w *dmWorker) QueryStatus(ctx context.Context, req *dmpkg.QueryStatusRequest) *dmpkg.QueryStatusResponse {
	if w.taskID != req.Task {
		return &dmpkg.QueryStatusResponse{ErrorMsg: fmt.Sprintf("task id mismatch, get %s, actually %s", req.Task, w.taskID)}
	}
	// get status from unit
	status := w.unitHolder.Status(ctx)
	// copy status via json
	statusBytes, err := json.Marshal(status)
	if err != nil {
		return &dmpkg.QueryStatusResponse{ErrorMsg: err.Error()}
	}
	return &dmpkg.QueryStatusResponse{
		Unit:   w.workerType,
		Stage:  w.getStage(),
		Status: statusBytes,
	}
}

// StopWorker implements the api of stop worker message which kill itself.
// StopWorker is called by refection of commandHandler.
func (w *dmWorker) StopWorker(ctx context.Context, msg *dmpkg.StopWorkerMessage) error {
	if w.taskID != msg.Task {
		return errors.Errorf("task id mismatch, get %s, actually %s", msg.Task, w.taskID)
	}
	return w.closeAndExit(ctx, w.workerStatus())
}
