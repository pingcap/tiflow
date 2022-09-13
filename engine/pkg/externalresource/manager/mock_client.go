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

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/pkg/client"
	"github.com/stretchr/testify/mock"
)

var _ client.ResourceManagerClient = &MockClient{}

// MockClient is a mock implementation of ResourceManagerClient interface
type MockClient struct {
	mock.Mock
}

// NewMockClient creates a MockClient
func NewMockClient() *MockClient {
	return &MockClient{}
}

// CreateResource implements ResourceManagerClient.CreateResource
func (m *MockClient) CreateResource(ctx context.Context, req *pb.CreateResourceRequest) error {
	args := m.Called(ctx, req)
	return args.Error(0)
}

// QueryResource implements ResourceManagerClient.QueryResource
func (m *MockClient) QueryResource(ctx context.Context, req *pb.QueryResourceRequest) (*pb.QueryResourceResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(*pb.QueryResourceResponse), args.Error(1)
}

// RemoveResource implements ResourceManagerClient.RemoveResource
func (m *MockClient) RemoveResource(ctx context.Context, req *pb.RemoveResourceRequest) error {
	args := m.Called(ctx, req)
	return args.Error(0)
}
