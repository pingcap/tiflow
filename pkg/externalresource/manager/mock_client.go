package manager

import (
	"context"

	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"

	"github.com/hanfei1991/microcosm/pkg/rpcutil"

	"github.com/hanfei1991/microcosm/pb"
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
