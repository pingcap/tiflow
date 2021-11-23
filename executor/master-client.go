package executor

import (
	"context"
	"errors"
	"time"

	"github.com/hanfei1991/microcosm/pb"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type MasterClient struct {
	urls   []string
	leader string
	conn   *grpc.ClientConn
	client pb.MasterClient
}

func NewMasterClient(ctx context.Context, join []string) (*MasterClient, error) {
	client := &MasterClient{
		urls: join,
	}
	client.leader = client.urls[0]
	log.L().Logger.Info("dialing master", zap.String("leader", client.leader))
	var err error
	client.conn, err = grpc.DialContext(ctx, client.leader, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, errors.New("cannot build conn")
	}
	client.client = pb.NewMasterClient(client.conn)
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
