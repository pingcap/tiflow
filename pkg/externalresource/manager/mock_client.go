package manager

import (
	"context"

	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"

	"github.com/hanfei1991/microcosm/pb"
)

var _ pb.ResourceManagerClient = &MockClient{}

type MockClient struct {
	mock.Mock
}

func NewMockClient() *MockClient {
	return &MockClient{}
}

func (m *MockClient) CreateResource(ctx context.Context, in *pb.CreateResourceRequest, opts ...grpc.CallOption) (*pb.CreateResourceResponse, error) {
	args := m.Called(ctx, in, opts)
	return args.Get(0).(*pb.CreateResourceResponse), args.Error(1)
}
