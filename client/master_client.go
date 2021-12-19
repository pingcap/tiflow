package client

import (
	"context"
	"time"

	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/test"
	"github.com/hanfei1991/microcosm/test/mock"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type MasterClient struct {
	urls   []string
	leader string
	conn   closeable
	client pb.MasterClient
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

func (c *MasterClient) initForTest(_ context.Context) error {
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
	if err != nil {
		return nil, err
	}
	return client, nil
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

func (c *MasterClient) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (resp *pb.SubmitJobResponse, err error) {
	return c.client.SubmitJob(ctx, req)
}

func (c *MasterClient) CancelJob(ctx context.Context, req *pb.CancelJobRequest) (resp *pb.CancelJobResponse, err error) {
	return c.client.CancelJob(ctx, req)
}

func (c *MasterClient) QueryMetaStore(
	ctx context.Context, req *pb.QueryMetaStoreRequest, timeout time.Duration,
) (*pb.QueryMetaStoreResponse, error) {
	ctx1, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return c.client.QueryMetaStore(ctx1, req)
}

// RequestForSchedule sends TaskSchedulerRequest to server master and master
// will ask resource manager for resource and allocates executors to given tasks
func (c *MasterClient) RequestForSchedule(
	ctx context.Context,
	req *pb.TaskSchedulerRequest,
	timeout time.Duration,
) (*pb.TaskSchedulerResponse, error) {
	ctx1, cancel := context.WithCancel(ctx)
	defer cancel()
	return c.client.ScheduleTask(ctx1, req)
}
