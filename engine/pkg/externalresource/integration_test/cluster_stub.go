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

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/pkg/client"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/broker"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/manager"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/engine/pkg/rpcerror"
)

type resourceClientStub struct {
	service *manager.Service
}

func (c *resourceClientStub) CreateResource(
	ctx context.Context,
	req *pb.CreateResourceRequest,
) error {
	_, err := c.service.CreateResource(ctx, req)
	return rpcerror.ToGRPCError(err)
}

func (c *resourceClientStub) QueryResource(
	ctx context.Context,
	req *pb.QueryResourceRequest,
) (*pb.QueryResourceResponse, error) {
	resp, err := c.service.QueryResource(ctx, req)
	if err != nil {
		return nil, rpcerror.ToGRPCError(err)
	}
	return resp, nil
}

func (c *resourceClientStub) RemoveResource(
	ctx context.Context,
	req *pb.RemoveResourceRequest,
) error {
	_, err := c.service.RemoveResource(ctx, req)
	return rpcerror.ToGRPCError(err)
}

type executorClientStub struct {
	client.ExecutorClient
	brk broker.Broker
}

func (c *executorClientStub) RemoveResource(
	ctx context.Context,
	creatorID model.WorkerID,
	resourceID resModel.ResourceID,
) error {
	_, err := c.brk.RemoveResource(ctx, &pb.RemoveLocalResourceRequest{
		ResourceId: resourceID,
		CreatorId:  creatorID,
	})
	return rpcerror.ToGRPCError(err)
}
