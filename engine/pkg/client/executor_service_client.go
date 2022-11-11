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

package client

import (
	"context"

	"github.com/google/uuid"
	"github.com/pingcap/log"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/pkg/client/internal"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

type (
	// StartWorkerCallback alias to the function that is called after the pre
	// dispatch task is successful and before confirm dispatch task.
	StartWorkerCallback = func()
)

// ExecutorServiceClient wraps a pb.ExecutorServiceClient and
// provides a DispatchTask method, which will call PreDispatchTask
// and ConfirmDispatchTask.
// TODO The service called "Executor" should be renamed "Dispatch", so that
// we have separate services for two sets of functionalities, i.e., dispatching
// tasks and managing resources (broker service).
type ExecutorServiceClient interface {
	DispatchTask(
		ctx context.Context,
		args *DispatchTaskArgs,
		startWorkerTimer StartWorkerCallback,
	) error
}

// DispatchTaskArgs contains the required parameters for creating a worker.
type DispatchTaskArgs struct {
	ProjectInfo  tenant.ProjectInfo
	WorkerID     string
	MasterID     string
	WorkerType   int64
	WorkerConfig []byte
	WorkerEpoch  int64
}

type executorServiceClient struct {
	cli pb.ExecutorServiceClient
}

// NewExecutorServiceClient creates a new ExecutorServiceClient.
func NewExecutorServiceClient(cli pb.ExecutorServiceClient) ExecutorServiceClient {
	return &executorServiceClient{
		cli: cli,
	}
}

func (c *executorServiceClient) DispatchTask(
	ctx context.Context,
	args *DispatchTaskArgs,
	startWorkerTimer StartWorkerCallback,
) error {
	// requestID is regenerated each time for tracing purpose.
	requestID := uuid.New().String()

	predispatchReq := &pb.PreDispatchTaskRequest{
		ProjectInfo: &pb.ProjectInfo{
			TenantId:  args.ProjectInfo.TenantID(),
			ProjectId: args.ProjectInfo.ProjectID(),
		},
		TaskTypeId:  args.WorkerType,
		TaskConfig:  args.WorkerConfig,
		MasterId:    args.MasterID,
		WorkerId:    args.WorkerID,
		WorkerEpoch: args.WorkerEpoch,
		RequestId:   requestID,
	}

	_, err := internal.NewCall(c.cli.PreDispatchTask, predispatchReq).Do(ctx)
	if err != nil {
		return err
	}

	// The timer should be started before invoking ConfirmDispatchTask
	// because we are expecting heartbeats once the worker is started,
	// and we need to call startWorkerTimer before the first heartbeat.
	startWorkerTimer()

	confirmDispatchReq := &pb.ConfirmDispatchTaskRequest{
		WorkerId:  args.WorkerID,
		RequestId: requestID,
	}

	_, err = internal.NewCall(
		c.cli.ConfirmDispatchTask,
		confirmDispatchReq,
		internal.WithForceNoRetry(),
	).Do(ctx)

	if err == nil {
		// Succeeded without any error
		return nil
	}

	if errors.Is(err, errors.ErrRuntimeIncomingQueueFull) || errors.Is(err, errors.ErrDispatchTaskRequestIDNotFound) {
		// Guaranteed to have failed.
		return err
	}

	log.L().Warn("DispatchTask: received ignorable error", zap.Error(err))
	// Error is ignorable.
	return nil
}
