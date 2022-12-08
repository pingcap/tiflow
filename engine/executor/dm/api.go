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
	dmproto "github.com/pingcap/tiflow/engine/pkg/dm/proto"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// QueryStatus implements the api of query status request.
// QueryStatus is called by refection of commandHandler.
func (w *dmWorker) QueryStatus(ctx context.Context, req *dmproto.QueryStatusRequest) *dmproto.QueryStatusResponse {
	if w.taskID != req.Task {
		return &dmproto.QueryStatusResponse{ErrorMsg: fmt.Sprintf("task id mismatch, get %s, actually %s", req.Task, w.taskID)}
	}
	// get status from unit
	status := w.unitHolder.Status(ctx)
	stage, result := w.unitHolder.Stage()
	// copy status via json
	mar := jsonpb.Marshaler{EmitDefaults: true}
	var buf bytes.Buffer
	err := mar.Marshal(&buf, status.(proto.Message))
	if err != nil {
		return &dmproto.QueryStatusResponse{ErrorMsg: err.Error()}
	}
	return &dmproto.QueryStatusResponse{
		Unit:         w.workerType,
		Stage:        stage,
		Result:       dmproto.NewProcessResultFromPB(result),
		Status:       buf.Bytes(),
		IoTotalBytes: w.cfg.IOTotalBytes.Load(),
	}
}

// StopWorker implements the api of stop worker message which kill itself.
// StopWorker is called by refection of commandHandler.
func (w *dmWorker) StopWorker(ctx context.Context, msg *dmproto.StopWorkerMessage) error {
	if w.taskID != msg.Task {
		return errors.Errorf("task id mismatch, get %s, actually %s", msg.Task, w.taskID)
	}

	workerStatus := w.workerStatus(ctx)
	return w.Exit(ctx, framework.ExitReasonCanceled, nil, workerStatus.ExtBytes)
}

// OperateTask implements the api of operate task message.
// OperateTask is called by refection of commandHandler.
func (w *dmWorker) OperateTask(ctx context.Context, msg *dmproto.OperateTaskMessage) error {
	if w.taskID != msg.Task {
		return errors.Errorf("task id mismatch, get %s, actually %s", msg.Task, w.taskID)
	}
	switch msg.Op {
	case dmproto.Pause:
		return w.unitHolder.Pause(ctx)
	case dmproto.Resume:
		return w.unitHolder.Resume(ctx)
	default:
		return errors.Errorf("unsupported op type %d for task %s", msg.Op, w.taskID)
	}
}

// BinlogTask implements the api of binlog task request.
// BinlogTask is called by refection of commandHandler.
func (w *dmWorker) BinlogTask(ctx context.Context, req *dmproto.BinlogTaskRequest) *dmproto.CommonTaskResponse {
	msg, err := w.unitHolder.Binlog(ctx, req)
	if err != nil {
		return &dmproto.CommonTaskResponse{ErrorMsg: err.Error()}
	}
	return &dmproto.CommonTaskResponse{Msg: msg}
}

// BinlogSchemaTask implements the api of binlog schema request.
// BinlogSchemaTask is called by refection of commandHandler.
func (w *dmWorker) BinlogSchemaTask(ctx context.Context, req *dmproto.BinlogSchemaTaskRequest) *dmproto.CommonTaskResponse {
	if w.taskID != req.Source {
		return &dmproto.CommonTaskResponse{ErrorMsg: fmt.Sprintf("task id mismatch, get %s, actually %s", req.Source, w.taskID)}
	}
	msg, err := w.unitHolder.BinlogSchema(ctx, req)
	if err != nil {
		return &dmproto.CommonTaskResponse{ErrorMsg: err.Error()}
	}
	return &dmproto.CommonTaskResponse{Msg: msg}
}

// CoordinateDDL is the function declaration for message agent to receive coordinate ddl response.
func (w *dmWorker) CoordinateDDL(ctx context.Context) *dmproto.CoordinateDDLResponse {
	return nil
}

// RedirectDDL implements the api of redirect ddl request.
// RedirectDDL is called by refection of commandHandler.
func (w *dmWorker) RedirectDDL(ctx context.Context, req *dmproto.RedirectDDLRequest) *dmproto.CommonTaskResponse {
	w.Logger().Info("receive a redirect DDL request", zap.Any("req", req))
	err := w.unitHolder.RedirectDDL(ctx, req)
	if err != nil {
		return &dmproto.CommonTaskResponse{ErrorMsg: err.Error()}
	}
	return &dmproto.CommonTaskResponse{}
}
