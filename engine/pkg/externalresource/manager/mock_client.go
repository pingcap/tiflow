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

package manager

import (
	"context"

	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"

	"github.com/pingcap/tiflow/engine/pkg/rpcutil"

	"github.com/pingcap/tiflow/engine/pb"
)

var _ pb.ResourceManagerClient = &MockClient{}

// MockClient is a mock implementation of ResourceManagerClient interface
type MockClient struct {
	mock.Mock
}

// NewWrappedMockClient creates a FailoverRPCClients with MockClient underlying
func NewWrappedMockClient() *rpcutil.FailoverRPCClients[pb.ResourceManagerClient] {
	return rpcutil.NewFailoverRPCClientsForTest[pb.ResourceManagerClient](&MockClient{})
}

// CreateResource implements ResourceManagerClient.CreateResource
func (m *MockClient) CreateResource(ctx context.Context, in *pb.CreateResourceRequest, opts ...grpc.CallOption) (*pb.CreateResourceResponse, error) {
	args := m.Called(ctx, in, opts)
	return args.Get(0).(*pb.CreateResourceResponse), args.Error(1)
}

// QueryResource implements ResourceManagerClient.QueryResource
func (m *MockClient) QueryResource(ctx context.Context, in *pb.QueryResourceRequest, opts ...grpc.CallOption) (*pb.QueryResourceResponse, error) {
	args := m.Called(ctx, in, opts)
	return args.Get(0).(*pb.QueryResourceResponse), args.Error(1)
}

// RemoveResource implements ResourceManagerClient.RemoveResource
func (m *MockClient) RemoveResource(ctx context.Context, in *pb.RemoveResourceRequest, opts ...grpc.CallOption) (*pb.RemoveResourceResponse, error) {
	args := m.Called(ctx, in, opts)
	return args.Get(0).(*pb.RemoveResourceResponse), args.Error(1)
}
