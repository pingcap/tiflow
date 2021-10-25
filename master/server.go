package master

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/hanfei1991/microcosom/pkg/etcdutil"
	"github.com/hanfei1991/microcosom/pkg/log"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/hanfei1991/microcosom/pb"
)

// Server handles PRC requests for df master.

// TODO: Do we need pd client?

type Server struct {

	etcd *embed.Etcd

	etcdClient *clientv3.Client
	//election *election.Election

	// sched scheduler
	scheduler *Scheduler
	// jobMng jobManager
	// 

	cfg *Config
}

func NewServer(cfg *Config) *Server {
	server := &Server {
		cfg: cfg,
		scheduler: &Scheduler{},
	}
	return server
}

func (s *Server) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	return &pb.SubmitJobResponse{}, nil
}

// 
func (s *Server) RegisterExecutor(ctx context.Context, req *pb.RegisterExecutorRequest) (*pb.RegisterExecutorResponse, error) {
	// register executor to scheduler
	s.scheduler.AddExecutor(req.Name, req.Address)
	return &pb.RegisterExecutorResponse{}, nil
}

func (s *Server) Start(ctx context.Context) (err error) {
	etcdCfg := genEmbedEtcdConfigWithLogger(s.cfg.LogLevel)
	// prepare to join an existing etcd cluster.
	//err = prepareJoinEtcd(s.cfg)
	//if err != nil {
	//	return
	//}
	log.L().Info("config after join prepared", zap.Stringer("config", s.cfg))

	// generates embed etcd config before any concurrent gRPC calls.
	// potential concurrent gRPC calls:
	//   - workerrpc.NewGRPCClient
	//   - getHTTPAPIHandler
	// no `String` method exists for embed.Config, and can not marshal it to join too.
	// but when starting embed etcd server, the etcd pkg will log the config.
	// https://github.com/etcd-io/etcd/blob/3cf2f69b5738fb702ba1a935590f36b52b18979b/embed/etcd.go#L299
	etcdCfg, err = s.cfg.genEmbedEtcdConfig(etcdCfg)
	if err != nil {
		return
	}

	gRPCSvr := func(gs *grpc.Server) { 
		pb.RegisterMasterServer(gs, s) 
		// TODO: register msg server
	}

	// TODO: implement http api/
	//apiHandler, err := getHTTPAPIHandler(ctx, s.cfg.AdvertiseAddr, tls2.ToGRPCDialOption())
	//if err != nil {
	//	return
	//}


	// generate grpcServer
	s.etcd, err = startEtcd(etcdCfg, gRPCSvr, nil, time.Minute)

	// start grpc server

	s.etcdClient, err = etcdutil.CreateClient([]string{withHost(s.cfg.MasterAddr)}, nil)

	// start leader election
	// TODO: Consider election. And Notify workers when leader changes.
	//s.election, err = election.NewElection(ctx, )
	
	// start keep alive
	return nil
}

func withHost(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		// do nothing
		return addr
	}
	if len(host) == 0 {
		return fmt.Sprintf("127.0.0.1:%s", port)
	}

	return addr
}