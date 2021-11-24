package cluster

import (
	"context"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/test"
	"github.com/hanfei1991/microcosm/test/mock"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type ExecutorClient interface {
	Send(context.Context, model.ExecutorID, *ExecutorRequest) (*ExecutorResponse, error)
}

type closeable interface {
	Close() error
}

type executorClient struct {
	conn   closeable
	client pb.ExecutorClient
}

func (c *executorClient) close() error {
	return c.conn.Close()
}

func (c *executorClient) send(ctx context.Context, req *ExecutorRequest) (*ExecutorResponse, error) {
	resp := &ExecutorResponse{}
	var err error
	switch req.Cmd {
	case CmdSubmitBatchTasks:
		resp.Resp, err = c.client.SubmitBatchTasks(ctx, req.SubmitBatchTasks())
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
	if test.GlobalTestFlag {
		return newExecutorClientForTest(addr)
	}
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock())
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
)

type ExecutorRequest struct {
	Cmd CmdType
	Req interface{}
}

func (e *ExecutorRequest) SubmitBatchTasks() *pb.SubmitBatchTasksRequest {
	return e.Req.(*pb.SubmitBatchTasksRequest)
}

type ExecutorResponse struct {
	Resp interface{}
}
