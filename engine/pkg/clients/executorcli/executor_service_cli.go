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

package executorcli

import (
	"context"

	"google.golang.org/grpc"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/pkg/clients/internal"
)

type ExecutorServiceClient struct {
	cli pb.ExecutorClient
}

func (c *ExecutorServiceClient) PreDispatchTask(
	ctx context.Context,
	in *pb.PreDispatchTaskRequest,
	opts ...grpc.CallOption,
) (*pb.PreDispatchTaskResponse, error) {
	return internal.Call(ctx, c.cli.PreDispatchTask, in)
}

func (c *ExecutorServiceClient) ConfirmDispatchTask(
	ctx context.Context,
	in *pb.ConfirmDispatchTaskRequest,
	opts ...grpc.CallOption,
) (*pb.ConfirmDispatchTaskResponse, error) {
	// TODO implement me
	panic("implement me")
}
