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
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/pkg/client/internal"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type (
	// MasterServerList is an alias for map[string]bool.
	// It is a mapping from servers' address to whether they are the leader.
	MasterServerList = internal.MasterServerList
)

// ServerMasterClient is a client for connecting to the server master.
type ServerMasterClient interface {
	TaskSchedulerClient
	DiscoveryClient
	ResourceManagerClient

	// Close closes the gRPC connection used to create the client.
	Close()
}

// ServerMasterClientWithFailOver implements ServerMasterClient.
// It maintains an updatable list of servers and records the leader's address.
type ServerMasterClientWithFailOver struct {
	TaskSchedulerClient
	DiscoveryClient
	ResourceManagerClient

	conn     *grpc.ClientConn
	resolver *internal.LeaderResolver
}

// NewServerMasterClientWithEndpointList creates a new ServerMasterClientWithFailOver
// with an endpoint list.
func NewServerMasterClientWithEndpointList(
	endpoints []string,
	credentials *security.Credential,
) (*ServerMasterClientWithFailOver, error) {
	serverList := make(MasterServerList, len(endpoints))
	for _, addr := range endpoints {
		serverList[addr] = false
	}
	return NewServerMasterClientWithFailOver(serverList, credentials)
}

// NewServerMasterClientWithFailOver creates a new ServerMasterClientWithFailOver.
// It is recommended that we use a singleton pattern here: Create one ServerMasterClientWithFailOver
// in each executor process.
func NewServerMasterClientWithFailOver(
	serverList MasterServerList,
	credentials *security.Credential,
) (*ServerMasterClientWithFailOver, error) {
	if credentials == nil {
		credentials = &security.Credential{}
	}

	dialOpt, err := credentials.ToGRPCDialOption()
	if err != nil {
		return nil, errors.Trace(err)
	}

	leaderResolver := internal.NewLeaderResolver(serverList)
	conn, err := grpc.Dial(
		"tiflow://dummy", // Dummy
		grpc.WithResolvers(leaderResolver),
		dialOpt)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &ServerMasterClientWithFailOver{
		TaskSchedulerClient:   NewTaskSchedulerClient(enginepb.NewTaskSchedulerClient(conn)),
		DiscoveryClient:       NewDiscoveryClient(enginepb.NewDiscoveryClient(conn)),
		ResourceManagerClient: NewResourceManagerClient(enginepb.NewResourceManagerClient(conn)),
		conn:                  conn,
		resolver:              leaderResolver,
	}, nil
}

// Close closes the NewServerMasterClientWithFailOver.
func (c *ServerMasterClientWithFailOver) Close() {
	if err := c.conn.Close(); err != nil {
		log.L().Warn("failed to close client", zap.Error(err))
	}
}

// UpdateServerList updates the server list maintained by the client.
// It is thread-safe.
func (c *ServerMasterClientWithFailOver) UpdateServerList(serverList MasterServerList) {
	c.resolver.UpdateServerList(serverList)
}
