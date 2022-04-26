package client

import (
	"context"

	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"

	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/test"
	"github.com/hanfei1991/microcosm/test/mock"
)

type ExecutorClient interface {
	Send(context.Context, *ExecutorRequest) (*ExecutorResponse, error)
}

type closeableConnIface interface {
	Close() error
}

type executorClient struct {
	conn   closeableConnIface
	client pb.ExecutorClient
}

func (c *executorClient) Send(ctx context.Context, req *ExecutorRequest) (*ExecutorResponse, error) {
	resp := &ExecutorResponse{}
	var err error
	switch req.Cmd {
	case CmdSubmitBatchTasks:
		resp.Resp, err = c.client.SubmitBatchTasks(ctx, req.SubmitBatchTasks())
	case CmdCancelBatchTasks:
		resp.Resp, err = c.client.CancelBatchTasks(ctx, req.CancelBatchTasks())
	case CmdPauseBatchTasks:
		resp.Resp, err = c.client.PauseBatchTasks(ctx, req.PauseBatchTasks())
	case CmdDispatchTask:
		resp.Resp, err = c.client.DispatchTask(ctx, req.DispatchTask())
	case CmdPreDispatchTask:
		resp.Resp, err = c.client.PreDispatchTask(ctx, req.PreDispatchTask())
	case CmdConfirmDispatchTask:
		resp.Resp, err = c.client.ConfirmDispatchTask(ctx, req.ConfirmDispatchTask())
	}
	if err != nil {
		log.L().Logger.Error("send req meet error", zap.Error(err))
	}
	return resp, err
}

func newExecutorClientForTest(addr string) (*executorClient, error) {
	conn, err := mock.Dial(addr)
	if err != nil {
		return nil, errors.ErrGrpcBuildConn.GenWithStackByArgs(addr)
	}
	return &executorClient{
		conn:   conn,
		client: mock.NewExecutorClient(conn),
	}, nil
}

func newExecutorClient(addr string) (*executorClient, error) {
	if test.GetGlobalTestFlag() {
		return newExecutorClientForTest(addr)
	}
	conn, err := grpc.Dial(
		addr,
		grpc.WithInsecure(),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.DefaultConfig}),
		grpc.WithBlock(),
		// We log gRPC requests here to aid debugging
		// TODO add a switch to turn off the gRPC request log.
		grpc.WithUnaryInterceptor(grpc_zap.UnaryClientInterceptor(log.L().Logger)))
	if err != nil {
		return nil, errors.ErrGrpcBuildConn.GenWithStackByArgs(addr)
	}

	return &executorClient{
		conn:   conn,
		client: pb.NewExecutorClient(conn),
	}, nil
}

type CmdType uint16

const (
	CmdSubmitBatchTasks CmdType = 1 + iota
	CmdCancelBatchTasks
	CmdPauseBatchTasks
	CmdDispatchTask
	CmdPreDispatchTask
	CmdConfirmDispatchTask
)

type ExecutorRequest struct {
	Cmd CmdType
	Req interface{}
}

func (e *ExecutorRequest) SubmitBatchTasks() *pb.SubmitBatchTasksRequest {
	return e.Req.(*pb.SubmitBatchTasksRequest)
}

func (e *ExecutorRequest) CancelBatchTasks() *pb.CancelBatchTasksRequest {
	return e.Req.(*pb.CancelBatchTasksRequest)
}

func (e *ExecutorRequest) PauseBatchTasks() *pb.PauseBatchTasksRequest {
	return e.Req.(*pb.PauseBatchTasksRequest)
}

func (e *ExecutorRequest) DispatchTask() *pb.DispatchTaskRequest {
	return e.Req.(*pb.DispatchTaskRequest)
}

func (e *ExecutorRequest) PreDispatchTask() *pb.PreDispatchTaskRequest {
	return e.Req.(*pb.PreDispatchTaskRequest)
}

func (e *ExecutorRequest) ConfirmDispatchTask() *pb.ConfirmDispatchTaskRequest {
	return e.Req.(*pb.ConfirmDispatchTaskRequest)
}

type ExecutorResponse struct {
	Resp interface{}
}
