package cluster

import (
	"context"

	"github.com/hanfei1991/microcosom/model"
	"github.com/hanfei1991/microcosom/pb"
	"github.com/hanfei1991/microcosom/pkg/terror"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type ExecutorClient interface {
	Send(context.Context, model.ExecutorID, *ExecutorRequest) (*ExecutorResponse, error)
}

type executorClient struct {
	conn   *grpc.ClientConn
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

func newExecutorClient(addr string) (*executorClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, terror.ErrDBBadConn.Generatef("cannot build conn with %s", addr)
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
