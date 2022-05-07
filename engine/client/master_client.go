package client

import (
	"context"
	"time"

	"google.golang.org/grpc"

	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/rpcutil"
	"github.com/hanfei1991/microcosm/test"
	"github.com/hanfei1991/microcosm/test/mock"
)

const DialTimeout = 5 * time.Second

type MasterClient interface {
	UpdateClients(ctx context.Context, urls []string, leaderURL string)
	Endpoints() []string
	Heartbeat(ctx context.Context, req *pb.HeartbeatRequest, timeout time.Duration) (resp *pb.HeartbeatResponse, err error)
	RegisterExecutor(ctx context.Context, req *pb.RegisterExecutorRequest, timeout time.Duration) (resp *pb.RegisterExecutorResponse, err error)
	ReportExecutorWorkload(
		ctx context.Context,
		req *pb.ExecWorkloadRequest,
	) (resp *pb.ExecWorkloadResponse, err error)
	SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (resp *pb.SubmitJobResponse, err error)
	QueryJob(ctx context.Context, req *pb.QueryJobRequest) (resp *pb.QueryJobResponse, err error)
	PauseJob(ctx context.Context, req *pb.PauseJobRequest) (resp *pb.PauseJobResponse, err error)
	CancelJob(ctx context.Context, req *pb.CancelJobRequest) (resp *pb.CancelJobResponse, err error)
	QueryMetaStore(
		ctx context.Context, req *pb.QueryMetaStoreRequest, timeout time.Duration,
	) (resp *pb.QueryMetaStoreResponse, err error)
	ScheduleTask(
		ctx context.Context,
		req *pb.ScheduleTaskRequest,
		timeout time.Duration,
	) (resp *pb.ScheduleTaskResponse, err error)
	PersistResource(
		ctx context.Context,
		request *pb.PersistResourceRequest,
	) (*pb.PersistResourceResponse, error)
	Close() (err error)
	GetLeaderClient() pb.MasterClient
}

type MasterClientImpl struct {
	*rpcutil.FailoverRPCClients[pb.MasterClient]
}

var dialImpl = func(ctx context.Context, addr string) (pb.MasterClient, rpcutil.CloseableConnIface, error) {
	ctx, cancel := context.WithTimeout(ctx, DialTimeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, nil, errors.Wrap(errors.ErrGrpcBuildConn, err)
	}
	return pb.NewMasterClient(conn), conn, nil
}

var mockDialImpl = func(ctx context.Context, addr string) (pb.MasterClient, rpcutil.CloseableConnIface, error) {
	conn, err := mock.Dial(addr)
	if err != nil {
		return nil, nil, errors.Wrap(errors.ErrGrpcBuildConn, err)
	}
	return mock.NewMasterClient(conn), conn, nil
}

func NewMasterClient(ctx context.Context, join []string) (*MasterClientImpl, error) {
	dialer := dialImpl
	if test.GetGlobalTestFlag() {
		dialer = mockDialImpl
	}
	clients, err := rpcutil.NewFailoverRPCClients(ctx, join, dialer)
	if err != nil {
		return nil, err
	}
	return &MasterClientImpl{FailoverRPCClients: clients}, nil
}

// Heartbeat wraps Heartbeat rpc to master-server.
func (c *MasterClientImpl) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest, timeout time.Duration) (resp *pb.HeartbeatResponse, err error) {
	return rpcutil.DoFailoverRPC(ctx, c.FailoverRPCClients, req, pb.MasterClient.Heartbeat)
}

// RegisterExecutor to master-server.
func (c *MasterClientImpl) RegisterExecutor(ctx context.Context, req *pb.RegisterExecutorRequest, timeout time.Duration) (resp *pb.RegisterExecutorResponse, err error) {
	return rpcutil.DoFailoverRPC(ctx, c.FailoverRPCClients, req, pb.MasterClient.RegisterExecutor)
}

func (c *MasterClientImpl) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (resp *pb.SubmitJobResponse, err error) {
	return rpcutil.DoFailoverRPC(ctx, c.FailoverRPCClients, req, pb.MasterClient.SubmitJob)
}

func (c *MasterClientImpl) QueryJob(ctx context.Context, req *pb.QueryJobRequest) (resp *pb.QueryJobResponse, err error) {
	return rpcutil.DoFailoverRPC(ctx, c.FailoverRPCClients, req, pb.MasterClient.QueryJob)
}

func (c *MasterClientImpl) PauseJob(ctx context.Context, req *pb.PauseJobRequest) (resp *pb.PauseJobResponse, err error) {
	return rpcutil.DoFailoverRPC(ctx, c.FailoverRPCClients, req, pb.MasterClient.PauseJob)
}

func (c *MasterClientImpl) CancelJob(ctx context.Context, req *pb.CancelJobRequest) (resp *pb.CancelJobResponse, err error) {
	return rpcutil.DoFailoverRPC(ctx, c.FailoverRPCClients, req, pb.MasterClient.CancelJob)
}

func (c *MasterClientImpl) QueryMetaStore(
	ctx context.Context, req *pb.QueryMetaStoreRequest, timeout time.Duration,
) (resp *pb.QueryMetaStoreResponse, err error) {
	ctx1, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return rpcutil.DoFailoverRPC(ctx1, c.FailoverRPCClients, req, pb.MasterClient.QueryMetaStore)
}

// ScheduleTask sends TaskSchedulerRequest to server master and master
// will ask resource manager for resource and allocates executors to given tasks
func (c *MasterClientImpl) ScheduleTask(
	ctx context.Context,
	req *pb.ScheduleTaskRequest,
	timeout time.Duration,
) (resp *pb.ScheduleTaskResponse, err error) {
	ctx1, cancel := context.WithCancel(ctx)
	defer cancel()
	return rpcutil.DoFailoverRPC(ctx1, c.FailoverRPCClients, req, pb.MasterClient.ScheduleTask)
}

func (c *MasterClientImpl) ReportExecutorWorkload(
	ctx context.Context,
	req *pb.ExecWorkloadRequest,
) (resp *pb.ExecWorkloadResponse, err error) {
	return rpcutil.DoFailoverRPC(ctx, c.FailoverRPCClients, req, pb.MasterClient.ReportExecutorWorkload)
}

func (c *MasterClientImpl) PersistResource(
	ctx context.Context,
	req *pb.PersistResourceRequest,
) (resp *pb.PersistResourceResponse, err error) {
	return rpcutil.DoFailoverRPC(ctx, c.FailoverRPCClients, req, pb.MasterClient.PersistResource)
}
