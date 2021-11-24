package executor

import (
	"context"
	"time"

	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/test"
	"github.com/hanfei1991/microcosm/test/mock"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type MasterClient struct {
	urls   []string
	leader string
	conn   closeable
	client pb.MasterClient
}

type closeable interface {
	Close() error
}

func (c *MasterClient) init(ctx context.Context) error {
	log.L().Logger.Info("dialing master", zap.String("leader", c.leader))
	conn, err := grpc.DialContext(ctx, c.leader, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return errors.ErrGrpcBuildConn.GenWithStackByArgs(c.leader)
	}
	c.client = pb.NewMasterClient(conn)
	c.conn = conn
	return nil
}

func (c *MasterClient) initForTest(ctx context.Context) error {
	log.L().Logger.Info("dialing master", zap.String("leader", c.leader))
	conn, err := mock.Dial(c.leader)
	if err != nil {
		return errors.ErrGrpcBuildConn.GenWithStackByArgs(c.leader)
	}
	c.client = mock.NewMasterClient(conn)
	c.conn = conn
	return nil
}

func NewMasterClient(ctx context.Context, join []string) (*MasterClient, error) {
	client := &MasterClient{
		urls: join,
	}
	client.leader = client.urls[0]
	var err error
	if test.GlobalTestFlag {
		err = client.initForTest(ctx)
	} else {
		err = client.init(ctx)
	}
	return client, err
}

// SendHeartbeat to master-server.
func (c *MasterClient) SendHeartbeat(ctx context.Context, req *pb.HeartbeatRequest, timeout time.Duration) (*pb.HeartbeatResponse, error) {
	ctx1, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return c.client.Heartbeat(ctx1, req)
}

// RegisterExecutor to master-server.
func (c *MasterClient) RegisterExecutor(ctx context.Context, req *pb.RegisterExecutorRequest, timeout time.Duration) (resp *pb.RegisterExecutorResponse, err error) {
	ctx1, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return c.client.RegisterExecutor(ctx1, req)
}
