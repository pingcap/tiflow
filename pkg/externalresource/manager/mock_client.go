package manager

import (
	"context"

	"github.com/hanfei1991/microcosm/pkg/rpcutil"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"

	"github.com/hanfei1991/microcosm/pb"
)

var _ pb.ResourceManagerClient = &MockClient{}

type MockClient struct {
	mock.Mock
}

func NewWrappedMockClient() *rpcutil.FailoverRPCClients[pb.ResourceManagerClient] {
	return rpcutil.NewFailoverRPCClientsForTest[pb.ResourceManagerClient](&MockClient{})
}

func (m *MockClient) CreateResource(ctx context.Context, in *pb.CreateResourceRequest, opts ...grpc.CallOption) (*pb.CreateResourceResponse, error) {
	args := m.Called(ctx, in, opts)
	return args.Get(0).(*pb.CreateResourceResponse), args.Error(1)
}

func (m *MockClient) QueryResource(ctx context.Context, in *pb.QueryResourceRequest, opts ...grpc.CallOption) (*pb.QueryResourceResponse, error) {
	args := m.Called(ctx, in, opts)
	return args.Get(0).(*pb.QueryResourceResponse), args.Error(1)
}
