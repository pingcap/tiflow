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

	"github.com/pingcap/tiflow/engine/enginepb"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/pkg/client/internal"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
)

// BrokerServiceClient wraps a pb.BrokerServiceClient
type BrokerServiceClient interface {
	RemoveResource(
		ctx context.Context,
		WorkerID frameModel.WorkerID,
		resourceID resModel.ResourceID,
	) error
}

type brokerServiceClient struct {
	cli enginepb.BrokerServiceClient
}

// NewBrokerServiceClient returns a new BrokerServiceClient.
func NewBrokerServiceClient(cli enginepb.BrokerServiceClient) BrokerServiceClient {
	return &brokerServiceClient{cli: cli}
}

// RemoveResource removes a local file resource from an executor.
func (c *brokerServiceClient) RemoveResource(
	ctx context.Context,
	WorkerID frameModel.WorkerID,
	resourceID resModel.ResourceID,
) error {
	call := internal.NewCall(
		c.cli.RemoveResource,
		&enginepb.RemoveLocalResourceRequest{
			ResourceId: resourceID,
			WorkerId:   WorkerID,
		})
	_, err := call.Do(ctx)
	return err
}
