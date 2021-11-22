package executor

import (
	"context"
	"errors"
	"strings"

	"github.com/hanfei1991/microcosom/pb"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type MasterClient struct {
	cfg *Config

	urls   []string
	leader string
	conn   *grpc.ClientConn
	client pb.MasterClient
}

func getJoinURLs(addrs string) []string {
	return strings.Split(addrs, ",")
}

func NewMasterClient(ctx context.Context, cfg *Config) (*MasterClient, error) {
	client := &MasterClient{
		cfg: cfg,
	}
	client.urls = getJoinURLs(cfg.Join)
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

func (c *MasterClient) SendHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	return c.client.Heartbeat(ctx, req)
}

func (c *MasterClient) RegisterExecutor(ctx context.Context, req *pb.RegisterExecutorRequest) (resp *pb.RegisterExecutorResponse, err error) {
	return c.client.RegisterExecutor(ctx, req)
}
