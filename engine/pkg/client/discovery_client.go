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

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/client/internal"
	"github.com/pingcap/tiflow/pkg/errors"
)

// DiscoveryClient is a client to the Discovery service on the server master.
type DiscoveryClient interface {
	// RegisterExecutor registers an executor. The server
	// will allocate and records a UUID.
	RegisterExecutor(
		ctx context.Context,
		request *pb.RegisterExecutorRequest,
	) (model.ExecutorID, error)

	// ListExecutors lists all executors.
	ListExecutors(ctx context.Context) ([]*pb.Executor, error)

	// ListMasters lists all masters.
	ListMasters(ctx context.Context) ([]*pb.Master, error)

	// Heartbeat sends a heartbeat message to the server.
	Heartbeat(
		ctx context.Context,
		request *pb.HeartbeatRequest,
	) (*pb.HeartbeatResponse, error)

	// QueryMetaStore queries the details of a metastore.
	QueryMetaStore(
		ctx context.Context,
		request *pb.QueryMetaStoreRequest,
	) (*pb.QueryMetaStoreResponse, error)

	// QueryStorageConfig queries the storage config.
	QueryStorageConfig(
		ctx context.Context,
		in *pb.QueryStorageConfigRequest,
	) (*pb.QueryStorageConfigResponse, error)
}

var _ DiscoveryClient = &discoveryClient{}

type discoveryClient struct {
	cli pb.DiscoveryClient
}

// NewDiscoveryClient returns a DiscoveryClient.
func NewDiscoveryClient(cli pb.DiscoveryClient) DiscoveryClient {
	return &discoveryClient{cli: cli}
}

func (c *discoveryClient) RegisterExecutor(
	ctx context.Context,
	request *pb.RegisterExecutorRequest,
) (model.ExecutorID, error) {
	var ret model.ExecutorID
	call := internal.NewCall(
		c.cli.RegisterExecutor,
		request,
		// RegisterExecutor is not idempotent in general
		// TODO review idempotency
		// internal.WithForceNoRetry()
	)
	executor, err := call.Do(ctx)
	if err != nil {
		return "", errors.Trace(err)
	}
	ret = model.ExecutorID(executor.Id)
	return ret, nil
}

func (c *discoveryClient) ListExecutors(ctx context.Context) ([]*pb.Executor, error) {
	call := internal.NewCall(c.cli.ListExecutors, &pb.ListExecutorsRequest{})
	resp, err := call.Do(ctx)
	if err != nil {
		return nil, err
	}
	return resp.Executors, nil
}

func (c *discoveryClient) ListMasters(ctx context.Context) ([]*pb.Master, error) {
	call := internal.NewCall(c.cli.ListMasters, &pb.ListMastersRequest{})
	resp, err := call.Do(ctx)
	if err != nil {
		return nil, err
	}
	return resp.Masters, nil
}

// Heartbeat sends a heartbeat to the DiscoveryService.
// Note: HeartbeatResponse contains Leader & Addr, which gives the call
// "Heartbeat" double responsibilities, i.e., keep-alive and get-members.
// TODO refactor this.
func (c *discoveryClient) Heartbeat(
	ctx context.Context,
	request *pb.HeartbeatRequest,
) (*pb.HeartbeatResponse, error) {
	call := internal.NewCall(
		c.cli.Heartbeat,
		request,
		// No need to retry heartbeats
		internal.WithForceNoRetry())
	return call.Do(ctx)
}

func (c *discoveryClient) QueryMetaStore(
	ctx context.Context,
	request *pb.QueryMetaStoreRequest,
) (*pb.QueryMetaStoreResponse, error) {
	call := internal.NewCall(
		c.cli.QueryMetaStore,
		request)
	return call.Do(ctx)
}

func (c *discoveryClient) QueryStorageConfig(
	ctx context.Context,
	request *pb.QueryStorageConfigRequest,
) (*pb.QueryStorageConfigResponse, error) {
	call := internal.NewCall(
		c.cli.QueryStorageConfig,
		request)
	return call.Do(ctx)
}
