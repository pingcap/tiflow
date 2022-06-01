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

package resourcejob

import (
	"context"
	"encoding/json"

	"github.com/pingcap/errors"

	"github.com/pingcap/tiflow/engine/lib"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
)

const (
	resourceTestWorkerType libModel.WorkerType = 10001
)

type workerConfig struct {
	ResourceID resModel.ResourceID `json:"resource_id"`
}

// Worker is the worker for Resource-Job.
type Worker struct {
	lib.BaseWorker

	status *workerStatus
}

func (w *Worker) InitImpl(ctx context.Context) error {
	return w.persistStatus(ctx)
}

func (w *Worker) Tick(ctx context.Context) error {
	// TODO implement me
	panic("implement me")
}

func (w *Worker) OnMasterMessage(topic p2p.Topic, message p2p.MessageValue) error {
	// TODO implement me
	panic("implement me")
}

func (w *Worker) CloseImpl(ctx context.Context) error {
	// TODO implement me
	panic("implement me")
}

func (w *Worker) persistStatus(ctx context.Context) error {
	byteData, err := json.Marshal(w.status)
	if err != nil {
		return errors.Trace(err)
	}

	err = w.UpdateStatus(ctx, libModel.WorkerStatus{
		Code:     libModel.WorkerStatusNormal,
		ExtBytes: byteData,
	})
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
