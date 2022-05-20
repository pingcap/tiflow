package client

import (
	"context"
	"sync"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/hanfei1991/microcosm/pb"
)

// MockExecutorClient is a mock implement of ExecutorClient interface
type MockExecutorClient struct {
	mu sync.Mutex
	mock.Mock
}

var _ ExecutorClient = (*MockExecutorClient)(nil)

// Send implements ExecutorClient.Send
func (c *MockExecutorClient) Send(ctx context.Context, request *ExecutorRequest) (*ExecutorResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	args := c.Mock.Called(ctx, request)
	return args.Get(0).(*ExecutorResponse), args.Error(1)
}

// DispatchTask implements ExecutorClient.DispatchTask
func (c *MockExecutorClient) DispatchTask(
	ctx context.Context,
	args *DispatchTaskArgs,
	startWorkerTimer StartWorkerCallback,
	abortWorker AbortWorkerCallback,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	retArgs := c.Mock.Called(ctx, args, startWorkerTimer, abortWorker)
	return retArgs.Error(0)
}

// MockServerMasterClient mocks server master gRPC client
type MockServerMasterClient struct {
	mu sync.Mutex
	mock.Mock
}

// UpdateClients implements MasterClient.UpdateClients
func (c *MockServerMasterClient) UpdateClients(ctx context.Context, urls []string, _ string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.Mock.Called(ctx, urls)
}

// Endpoints implements MasterClient.Endpoints
func (c *MockServerMasterClient) Endpoints() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	args := c.Mock.Called()
	return args.Get(0).([]string)
}

// Heartbeat implements MasterClient.Heartbeat
func (c *MockServerMasterClient) Heartbeat(
	ctx context.Context,
	req *pb.HeartbeatRequest,
	timeout time.Duration,
) (resp *pb.HeartbeatResponse, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	args := c.Mock.Called(ctx, req, timeout)
	return args.Get(0).(*pb.HeartbeatResponse), args.Error(1)
}

// RegisterExecutor implements MasterClient.RegisterExecutor
func (c *MockServerMasterClient) RegisterExecutor(
	ctx context.Context,
	req *pb.RegisterExecutorRequest,
	timeout time.Duration,
) (resp *pb.RegisterExecutorResponse, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	args := c.Mock.Called(ctx, req, timeout)
	return args.Get(0).(*pb.RegisterExecutorResponse), args.Error(1)
}

// SubmitJob implements MasterClient.SubmitJob
func (c *MockServerMasterClient) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (resp *pb.SubmitJobResponse, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	args := c.Mock.Called(ctx, req)
	return args.Get(0).(*pb.SubmitJobResponse), args.Error(1)
}

// QueryJob implements MasterClient.QueryJob
func (c *MockServerMasterClient) QueryJob(ctx context.Context, req *pb.QueryJobRequest) (resp *pb.QueryJobResponse, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	args := c.Mock.Called(ctx, req)
	return args.Get(0).(*pb.QueryJobResponse), args.Error(1)
}

// PauseJob implements MasterClient.PauseJob
func (c *MockServerMasterClient) PauseJob(ctx context.Context, req *pb.PauseJobRequest) (resp *pb.PauseJobResponse, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	args := c.Mock.Called(ctx, req)
	return args.Get(0).(*pb.PauseJobResponse), args.Error(1)
}

// CancelJob implements MasterClient.CancelJob
func (c *MockServerMasterClient) CancelJob(ctx context.Context, req *pb.CancelJobRequest) (resp *pb.CancelJobResponse, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	args := c.Mock.Called(ctx, req)
	return args.Get(0).(*pb.CancelJobResponse), args.Error(1)
}

// QueryMetaStore implements MasterClient.QueryMetaStore
func (c *MockServerMasterClient) QueryMetaStore(
	ctx context.Context,
	req *pb.QueryMetaStoreRequest,
	timeout time.Duration,
) (resp *pb.QueryMetaStoreResponse, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	args := c.Mock.Called(ctx, req, timeout)
	return args.Get(0).(*pb.QueryMetaStoreResponse), args.Error(1)
}

// Close implements MasterClient.Close
func (c *MockServerMasterClient) Close() (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	args := c.Mock.Called()
	return args.Error(0)
}

// ReportExecutorWorkload implements MasterClient.ReportExecutorWorkload
func (c *MockServerMasterClient) ReportExecutorWorkload(
	ctx context.Context,
	req *pb.ExecWorkloadRequest,
) (resp *pb.ExecWorkloadResponse, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	args := c.Mock.Called()
	return args.Get(0).(*pb.ExecWorkloadResponse), args.Error(1)
}

// GetLeaderClient implements MasterClient.GetLeaderClient
func (c *MockServerMasterClient) GetLeaderClient() pb.MasterClient {
	panic("implement me")
}

// ScheduleTask implements MasterClient.ScheduleTask
func (c *MockServerMasterClient) ScheduleTask(
	ctx context.Context,
	req *pb.ScheduleTaskRequest,
	timeout time.Duration,
) (resp *pb.ScheduleTaskResponse, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	args := c.Called(ctx, req, timeout)
	return args.Get(0).(*pb.ScheduleTaskResponse), args.Error(1)
}

// PersistResource implements MasterClient.PersistResource
func (c *MockServerMasterClient) PersistResource(
	ctx context.Context,
	in *pb.PersistResourceRequest,
) (*pb.PersistResourceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	args := c.Mock.Called()
	return args.Get(0).(*pb.PersistResourceResponse), args.Error(1)
}
