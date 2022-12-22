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
	"bytes"
	"context"
	"fmt"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/tiflow/engine/framework"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
	"github.com/pingcap/tiflow/pkg/errors"
)

// QueryStatus implements the api of query status request.
// QueryStatus is called by refection of commandHandler.
func (w *dmWorker) QueryStatus(ctx context.Context, req *dmpkg.QueryStatusRequest) *dmpkg.QueryStatusResponse {
	if w.taskID != req.Task {
		return &dmpkg.QueryStatusResponse{ErrorMsg: fmt.Sprintf("task id mismatch, get %s, actually %s", req.Task, w.taskID)}
	}
	// get status from unit
	status := w.unitHolder.Status(ctx)
	stage, result := w.unitHolder.Stage()
	// copy status via json
	mar := jsonpb.Marshaler{EmitDefaults: true}
	var buf bytes.Buffer
	err := mar.Marshal(&buf, status.(proto.Message))
	if err != nil {
		return &dmpkg.QueryStatusResponse{ErrorMsg: err.Error()}
	}
	return &dmpkg.QueryStatusResponse{
		Unit:             w.workerType,
		Stage:            stage,
		Result:           dmpkg.NewProcessResultFromPB(result),
		Status:           buf.Bytes(),
		IoTotalBytes:     w.cfg.IOTotalBytes.Load(),
		DumpIoTotalBytes: w.cfg.DumpIOTotalBytes.Load(),
	}
}

// StopWorker implements the api of stop worker message which kill itself.
// StopWorker is called by refection of commandHandler.
func (w *dmWorker) StopWorker(ctx context.Context, msg *dmpkg.StopWorkerMessage) error {
	if w.taskID != msg.Task {
		return errors.Errorf("task id mismatch, get %s, actually %s", msg.Task, w.taskID)
	}

	workerStatus := w.workerStatus(ctx)
	return w.Exit(ctx, framework.ExitReasonCanceled, nil, workerStatus.ExtBytes)
}

// OperateTask implements the api of operate task message.
// OperateTask is called by refection of commandHandler.
func (w *dmWorker) OperateTask(ctx context.Context, msg *dmpkg.OperateTaskMessage) error {
	if w.taskID != msg.Task {
		return errors.Errorf("task id mismatch, get %s, actually %s", msg.Task, w.taskID)
	}
	switch msg.Op {
	case dmpkg.Pause:
		return w.unitHolder.Pause(ctx)
	case dmpkg.Resume:
		return w.unitHolder.Resume(ctx)
	default:
		return errors.Errorf("unsupported op type %d for task %s", msg.Op, w.taskID)
	}
}

// BinlogTask implements the api of binlog task request.
// BinlogTask is called by refection of commandHandler.
func (w *dmWorker) BinlogTask(ctx context.Context, req *dmpkg.BinlogTaskRequest) *dmpkg.CommonTaskResponse {
	msg, err := w.unitHolder.Binlog(ctx, req)
	if err != nil {
		return &dmpkg.CommonTaskResponse{ErrorMsg: err.Error()}
	}
	return &dmpkg.CommonTaskResponse{Msg: msg}
}

// BinlogSchemaTask implements the api of binlog schema request.
// BinlogSchemaTask is called by refection of commandHandler.
func (w *dmWorker) BinlogSchemaTask(ctx context.Context, req *dmpkg.BinlogSchemaTaskRequest) *dmpkg.CommonTaskResponse {
	if w.taskID != req.Source {
		return &dmpkg.CommonTaskResponse{ErrorMsg: fmt.Sprintf("task id mismatch, get %s, actually %s", req.Source, w.taskID)}
	}
	msg, err := w.unitHolder.BinlogSchema(ctx, req)
	if err != nil {
		return &dmpkg.CommonTaskResponse{ErrorMsg: err.Error()}
	}
	return &dmpkg.CommonTaskResponse{Msg: msg}
}
