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

package integration

import (
	"context"

	"google.golang.org/grpc"

	"github.com/pingcap/tiflow/engine/client"
	"github.com/pingcap/tiflow/engine/pb"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/broker"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/manager"
)

type resourceClientStub struct {
	service *manager.Service
}

func (c *resourceClientStub) CreateResource(
	ctx context.Context,
	req *pb.CreateResourceRequest,
	_ ...grpc.CallOption,
) (*pb.CreateResourceResponse, error) {
	return c.service.CreateResource(ctx, req)
}

func (c *resourceClientStub) QueryResource(
	ctx context.Context,
	req *pb.QueryResourceRequest,
	_ ...grpc.CallOption,
) (*pb.QueryResourceResponse, error) {
	return c.service.QueryResource(ctx, req)
}

func (c *resourceClientStub) RemoveResource(
	ctx context.Context,
	req *pb.RemoveResourceRequest,
	_ ...grpc.CallOption,
) (*pb.RemoveResourceResponse, error) {
	return c.service.RemoveResource(ctx, req)
}

type executorClientStub struct {
	// embedded for providing a panicking implementation for DispatchTask
	// TODO make a generic ExecutorClientManager[T],
	// where T can be a client to any service provided by the executor.
	*client.TaskDispatcher

	brk broker.Broker
}

func (c *executorClientStub) RemoveLocalResource(ctx context.Context, req *pb.RemoveLocalResourceRequest) error {
	_, err := c.brk.RemoveResource(ctx, req)
	return err
}
