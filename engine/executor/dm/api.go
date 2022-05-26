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

	"github.com/pingcap/errors"

	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
)

// QueryStatus implements the api of query status request.
func (t *baseTask) QueryStatus(ctx context.Context, req *dmpkg.QueryStatusRequest) *dmpkg.QueryStatusResponse {
	// get status from unit
	status := t.unitHolder.Status(ctx)
	// copy status via json
	statusBytes, err := json.Marshal(status)
	if err != nil {
		return &dmpkg.QueryStatusResponse{ErrorMsg: err.Error()}
	}
	return &dmpkg.QueryStatusResponse{
		Unit:   t.workerType,
		Stage:  t.getStage(),
		Status: statusBytes,
	}
}

// StopWorker implements the api of stop worker message which kill itself.
func (t *baseTask) StopWorker(ctx context.Context, msg *dmpkg.StopWorkerMessage) error {
	if t.taskID != msg.Task {
		return errors.Errorf("task id mismatch, get %s, actually %s", msg.Task, t.taskID)
	}
	// Disscuss: is it correct?
	return t.exit(ctx, t.workerStatus(), nil)
}
