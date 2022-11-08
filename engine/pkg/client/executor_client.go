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
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// ExecutorClient is the public interface for an executor client.
// It is only for one executor. For a client to a group of executors,
// refer to ExecutorGroup.
type ExecutorClient interface {
	// An executor's gRPC server exposes two services for now:
	// ExecutorService and BrokerService. This design is for better decoupling
	// and for mocking these two services separately.

	ExecutorServiceClient
	BrokerServiceClient

	// Close closes the gRPC connection used to create the client.
	Close()
}

type executorClientImpl struct {
	ExecutorServiceClient
	BrokerServiceClient

	conn *grpc.ClientConn
}

// NewExecutorClient creates a new executor client.
// Note that conn will be closed if the returned client is closed.
func NewExecutorClient(conn *grpc.ClientConn) ExecutorClient {
	return &executorClientImpl{
		ExecutorServiceClient: NewExecutorServiceClient(enginepb.NewExecutorServiceClient(conn)),
		BrokerServiceClient:   NewBrokerServiceClient(enginepb.NewBrokerServiceClient(conn)),
		conn:                  conn,
	}
}

// Close closes the gRPC connection maintained by the client.
func (c *executorClientImpl) Close() {
	if err := c.conn.Close(); err != nil {
		log.L().Warn("failed to close client", zap.Error(err))
	}
}

type executorClientFactory interface {
	NewExecutorClient(addr string) (ExecutorClient, error)
}

type executorClientFactoryImpl struct {
	credentials *security.Credential
	logger      *zap.Logger
}

func newExecutorClientFactory(
	credentials *security.Credential,
	logger *zap.Logger,
) *executorClientFactoryImpl {
	if credentials == nil {
		credentials = &security.Credential{}
	}
	if logger == nil {
		logger = zap.L()
	}

	return &executorClientFactoryImpl{
		credentials: credentials,
		logger:      logger,
	}
}

func (f *executorClientFactoryImpl) NewExecutorClient(addr string) (ExecutorClient, error) {
	tlsOptions, err := f.credentials.ToGRPCDialOption()
	if err != nil {
		return nil, err
	}

	// Note that we should not use a blocking dial here, which
	// would increase the risk of deadlocking if the calling is holding
	// a lock. Also note that since the dial is non-blocking, no context.Context
	// is needed here.
	conn, err := grpc.Dial(addr, tlsOptions)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewExecutorClient(conn), nil
}
