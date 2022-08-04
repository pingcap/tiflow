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

	"github.com/pingcap/tiflow/engine/pb"
	"github.com/pingcap/tiflow/engine/pkg/client/internal"
)

// ResourceManagerClient is a client to the service ResourceManager, which
// currently is part of the server master.
type ResourceManagerClient interface {
	CreateResource(ctx context.Context, request *pb.CreateResourceRequest) error
	QueryResource(ctx context.Context, request *pb.QueryResourceRequest) (*pb.QueryResourceResponse, error)
	RemoveResource(ctx context.Context, request *pb.RemoveResourceRequest) error
}

type resourceManagerClient struct {
	cli pb.ResourceManagerClient
}

// NewResourceManagerClient returns a ResourceManagerClient.
func NewResourceManagerClient(cli pb.ResourceManagerClient) ResourceManagerClient {
	return &resourceManagerClient{cli: cli}
}

func (c *resourceManagerClient) CreateResource(ctx context.Context, request *pb.CreateResourceRequest) error {
	call := internal.NewCall(c.cli.CreateResource, request)
	_, err := call.Do(ctx)
	// TODO specialized retry strategy.
	return err
}

func (c *resourceManagerClient) QueryResource(ctx context.Context, request *pb.QueryResourceRequest) (*pb.QueryResourceResponse, error) {
	call := internal.NewCall(c.cli.QueryResource, request)
	return call.Do(ctx)
}

func (c *resourceManagerClient) RemoveResource(ctx context.Context, request *pb.RemoveResourceRequest) error {
	call := internal.NewCall(c.cli.RemoveResource, request)
	_, err := call.Do(ctx)
	// TODO specialized retry strategy.
	return err
}
