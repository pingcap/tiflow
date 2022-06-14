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

package clients

import (
	"context"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/pingcap/tiflow/dm/pkg/log"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/pkg/clients/internal"
	"github.com/pingcap/tiflow/engine/pkg/rpcerror"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
)

type (
	// StartWorkerCallback alias to the function that is called after the pre
	// dispatch task is successful and before confirm dispatch task.
	StartWorkerCallback = func()
	// AbortWorkerCallback alias to the function that is called only if the
	// failure is guaranteed when creating worker.
	AbortWorkerCallback = func(error)
)

type ExecutorServiceClient interface {
	DispatchTask(
		ctx context.Context,
		args *DispatchTaskArgs,
		startWorkerTimer StartWorkerCallback,
		abortWorker AbortWorkerCallback,
	) error
}

// DispatchTaskArgs contains the required parameters for creating a worker.
type DispatchTaskArgs struct {
	ProjectInfo  tenant.ProjectInfo
	WorkerID     string
	MasterID     string
	WorkerType   int64
	WorkerConfig []byte
}

type executorServiceClient struct {
	cli pb.ExecutorClient
}

func NewExecutorServiceClient(conn *grpc.ClientConn) ExecutorServiceClient {
	return &executorServiceClient{
		cli: pb.NewExecutorClient(conn),
	}
}

func (c *executorServiceClient) DispatchTask(
	ctx context.Context,
	args *DispatchTaskArgs,
	startWorkerTimer StartWorkerCallback,
	abortWorker AbortWorkerCallback,
) error {
	// requestID is regenerated each time for tracing purpose.
	requestID := uuid.New().String()

	predispatchReq := &pb.PreDispatchTaskRequest{
		ProjectInfo: &pb.ProjectInfo{
			TenantId:  args.ProjectInfo.TenantID(),
			ProjectId: args.ProjectInfo.ProjectID(),
		},
		TaskTypeId: args.WorkerType,
		TaskConfig: args.WorkerConfig,
		MasterId:   args.MasterID,
		WorkerId:   args.WorkerID,
		RequestId:  requestID,
	}

	_, err := internal.NewCall(c.cli.PreDispatchTask, predispatchReq).Do(ctx)
	if err != nil {
		abortWorker(err)
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

	code, ok := rpcerror.GRPCStatusCode(err)
	if !ok {
		// Not an grpc error
		return err
	}

	if code == codes.NotFound || code == codes.Aborted {
		// Guaranteed to have failed.
		return err
	}

	log.L().Warn("DispatchTask: received ignorable error", zap.Error(err))
	// Error is ignorable.
	return nil
}
