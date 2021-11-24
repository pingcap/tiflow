package master

import (
	"context"
	"fmt"
	"net"

	"github.com/hanfei1991/microcosm/master/cluster"
	"github.com/hanfei1991/microcosm/master/jobmaster"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/test"
	"github.com/hanfei1991/microcosm/test/mock"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/ticdc/dm/pkg/etcdutil"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/hanfei1991/microcosm/pb"
)

// Server handles PRC requests for df master.
type Server struct {
	etcd *embed.Etcd

	etcdClient *clientv3.Client
	// election *election.Election

	// sched scheduler
	executorManager *cluster.ExecutorManager
	jobManager      *jobmaster.JobManager
	//
	cfg *Config

	// mocked server for test
	mockGrpcServer mock.GrpcServer
}

// NewServer creates a new master-server.
func NewServer(cfg *Config, ctx *test.Context) (*Server, error) {
	executorNotifier := make(chan model.ExecutorID, 100)
	executorManager := cluster.NewExecutorManager(executorNotifier, cfg.KeepAliveTTL, cfg.KeepAliveInterval, ctx)
	jobManager := jobmaster.NewJobManager(executorManager, executorManager, executorNotifier)
	server := &Server{
		cfg:             cfg,
		executorManager: executorManager,
		jobManager:      jobManager,
	}
	return server, nil
}

// Heartbeat implements pb interface.
func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	return s.executorManager.HandleHeartbeat(req)
}

// SubmitJob passes request onto "JobManager".
func (s *Server) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	return s.jobManager.SubmitJob(ctx, req), nil
}

// RegisterExecutor implements grpc interface, and passes request onto executor manager.
func (s *Server) RegisterExecutor(ctx context.Context, req *pb.RegisterExecutorRequest) (*pb.RegisterExecutorResponse, error) {
	// register executor to scheduler
	// TODO: check leader, if not leader, return notLeader error.
	execInfo, err := s.executorManager.AddExecutor(req)
	if err != nil {
		log.L().Logger.Error("add executor failed", zap.Error(err))
		return &pb.RegisterExecutorResponse{
			Err: errors.ToPBError(err),
		}, nil
	}
	return &pb.RegisterExecutorResponse{
		ExecutorId: int32(execInfo.ID),
	}, nil
}

// DeleteExecutor deletes an executor, but have yet implemented.
func (s *Server) DeleteExecutor() {
	// To implement
}

func (s *Server) startForTest(ctx context.Context) (err error) {
	// TODO: implement mock-etcd and leader election

	s.mockGrpcServer, err = mock.NewMasterServer(s.cfg.MasterAddr, s)
	if err != nil {
		return err
	}

	s.executorManager.Start(ctx)
	s.jobManager.Start(ctx)

	return nil
}

// Stop and clean resources.
// TODO: implement stop gracefully.
func (s *Server) Stop() {
	if s.mockGrpcServer != nil {
		s.mockGrpcServer.Stop()
	}
}

// Start the master-server.
func (s *Server) Start(ctx context.Context) (err error) {
	if test.GlobalTestFlag {
		return s.startForTest(ctx)
	}
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
	s.etcd, err = startEtcd(etcdCfg, gRPCSvr, nil, etcdStartTimeout)
	if err != nil {
		return
	}

	log.L().Logger.Info("start etcd successfully")

	// start grpc server

	s.etcdClient, err = etcdutil.CreateClient([]string{withHost(s.cfg.MasterAddr)}, nil)
	if err != nil {
		return
	}

	// start leader election
	// TODO: Consider election. And Notify workers when leader changes.
	// s.election, err = election.NewElection(ctx, )

	// start keep alive
	s.executorManager.Start(ctx)
	s.jobManager.Start(ctx)
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
