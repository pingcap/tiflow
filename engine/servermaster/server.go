// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package servermaster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/engine/client"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/framework"
	"github.com/pingcap/tiflow/engine/framework/metadata"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/model"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/deps"
	externRescManager "github.com/pingcap/tiflow/engine/pkg/externalresource/manager"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/resourcetypes"
	"github.com/pingcap/tiflow/engine/pkg/meta"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/engine/pkg/rpcutil"
	"github.com/pingcap/tiflow/engine/pkg/serverutil"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/pingcap/tiflow/engine/servermaster/scheduler"
	schedModel "github.com/pingcap/tiflow/engine/servermaster/scheduler/model"
	"github.com/pingcap/tiflow/engine/test"
	"github.com/pingcap/tiflow/engine/test/mock"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/tcpserver"
	p2pProtocol "github.com/pingcap/tiflow/proto/p2p"
)

// use a slice instead of map because in small data size, slice search is faster
// than map search.
var masterRPCLimiterAllowList = []string{
	"SubmitJob",
	"CancelJob",
	"ScheduleTask",
}

var resourceRPCLimiterAllowList = []string{
	"CreateResource",
	"RemoveResource",
}

// Server handles PRC requests for df master.
type Server struct {
	id string // Server id, randomly generated when server is created.

	cfg     *Config
	info    *model.NodeInfo
	metrics *serverMasterMetric

	etcdClient *clientv3.Client

	leader          atomic.Value
	masterCli       *rpcutil.LeaderClientWithLock[pb.MasterClient]
	resourceCli     *rpcutil.LeaderClientWithLock[pb.ResourceManagerClient]
	leaderServiceFn func(context.Context) error
	masterRPCHook   *rpcutil.PreRPCHook[pb.MasterClient]

	// sched scheduler
	executorManager        ExecutorManager
	jobManager             JobManager
	resourceManagerService *externRescManager.Service
	scheduler              *scheduler.Scheduler

	// file resource GC
	gcRunner      externRescManager.GCRunner
	gcCoordinator externRescManager.GCCoordinator

	msgService      *p2p.MessageRPCService
	p2pMsgRouter    p2p.MessageRouter
	rpcLogRL        *rate.Limiter
	discoveryKeeper *serverutil.DiscoveryKeepaliver

	metaStoreManager MetaStoreManager

	leaderInitialized atomic.Bool

	// mocked server for test
	mockGrpcServer mock.GrpcServer

	testCtx *test.Context

	// framework metastore client
	frameMetaClient    pkgOrm.Client
	businessClientConn metaModel.ClientConn
}

// PersistResource implements pb.MasterServer.PersistResource
func (s *Server) PersistResource(ctx context.Context, request *pb.PersistResourceRequest) (*pb.PersistResourceResponse, error) {
	// TODO implement me
	panic("implement me")
}

type serverMasterMetric struct {
	metricJobNum      map[pb.QueryJobResponse_JobStatus]prometheus.Gauge
	metricExecutorNum map[model.ExecutorStatus]prometheus.Gauge
}

func newServerMasterMetric() *serverMasterMetric {
	// Following are leader only metrics
	metricJobNum := make(map[pb.QueryJobResponse_JobStatus]prometheus.Gauge)
	for status, name := range pb.QueryJobResponse_JobStatus_name {
		metric := serverJobNumGauge.WithLabelValues(name)
		metricJobNum[pb.QueryJobResponse_JobStatus(status)] = metric
	}

	metricExecutorNum := make(map[model.ExecutorStatus]prometheus.Gauge)
	for status, name := range model.ExecutorStatusNameMapping {
		metric := serverExecutorNumGauge.WithLabelValues(name)
		metricExecutorNum[status] = metric
	}

	return &serverMasterMetric{
		metricJobNum:      metricJobNum,
		metricExecutorNum: metricExecutorNum,
	}
}

// NewServer creates a new master-server.
func NewServer(cfg *Config, ctx *test.Context) (_ *Server, finalErr error) {
	log.Info("creating server master", zap.Stringer("config", cfg))

	executorManager := NewExecutorManagerImpl(cfg.KeepAliveTTL, cfg.KeepAliveInterval, ctx)

	id := "server-master-" + uuid.New().String()
	info := &model.NodeInfo{
		Type: model.NodeTypeServerMaster,
		ID:   model.DeployNodeID(id),
		Addr: cfg.AdvertiseAddr,
	}
	msgService := p2p.NewMessageRPCServiceWithRPCServer(id, nil, nil)
	p2pMsgRouter := p2p.NewMessageRouter(p2p.NodeID(info.ID), info.Addr)

	etcdClient, err := etcdutil.CreateClient(cfg.ETCDEndpoints, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if finalErr != nil {
			if err := etcdClient.Close(); err != nil {
				log.Warn("close etcd client failed", zap.Error(err))
			}
		}
	}()

	discoveryKeeper := serverutil.NewDiscoveryKeepaliver(
		info, etcdClient, int(defaultSessionTTL/time.Second),
		defaultDiscoverTicker, p2pMsgRouter,
	)

	server := &Server{
		id:                id,
		cfg:               cfg,
		info:              info,
		executorManager:   executorManager,
		leaderInitialized: *atomic.NewBool(false),
		testCtx:           ctx,
		leader:            atomic.Value{},
		masterCli:         &rpcutil.LeaderClientWithLock[pb.MasterClient]{},
		resourceCli:       &rpcutil.LeaderClientWithLock[pb.ResourceManagerClient]{},
		msgService:        msgService,
		p2pMsgRouter:      p2pMsgRouter,
		rpcLogRL:          rate.NewLimiter(rate.Every(time.Second*5), 3 /*burst*/),
		discoveryKeeper:   discoveryKeeper,
		metrics:           newServerMasterMetric(),
		metaStoreManager:  NewMetaStoreManager(),
		etcdClient:        etcdClient,
	}
	server.leaderServiceFn = server.runLeaderService
	masterRPCHook := rpcutil.NewPreRPCHook(
		id,
		&server.leader,
		server.masterCli,
		&server.leaderInitialized,
		server.rpcLogRL,
		masterRPCLimiterAllowList,
	)
	server.masterRPCHook = masterRPCHook
	return server, nil
}

// Heartbeat implements pb interface.
func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	resp2 := &pb.HeartbeatResponse{}
	shouldRet, err := s.masterRPCHook.PreRPC(ctx, req, &resp2)
	if shouldRet {
		return resp2, err
	}

	resp, err := s.executorManager.HandleHeartbeat(req)
	if err == nil && resp.Err == nil {
		for _, nodeInfo := range s.discoveryKeeper.Snapshot() {
			if nodeInfo.Type == model.NodeTypeServerMaster {
				resp.Addrs = append(resp.Addrs, nodeInfo.Addr)
			}
		}
		leader, exists := s.masterRPCHook.CheckLeader()
		if exists {
			resp.Leader = leader.AdvertiseAddr
		}
	}
	return resp, err
}

// SubmitJob passes request onto "JobManager".
func (s *Server) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	resp2 := &pb.SubmitJobResponse{}
	shouldRet, err := s.masterRPCHook.PreRPC(ctx, req, &resp2)
	if shouldRet {
		return resp2, err
	}
	return s.jobManager.SubmitJob(ctx, req), nil
}

// QueryJob implements pb.MasterServer.QueryJob
func (s *Server) QueryJob(ctx context.Context, req *pb.QueryJobRequest) (*pb.QueryJobResponse, error) {
	resp2 := &pb.QueryJobResponse{}
	shouldRet, err := s.masterRPCHook.PreRPC(ctx, req, &resp2)
	if shouldRet {
		return resp2, err
	}
	return s.jobManager.QueryJob(ctx, req), nil
}

// CancelJob implements pb.MasterServer.CancelJob
func (s *Server) CancelJob(ctx context.Context, req *pb.CancelJobRequest) (*pb.CancelJobResponse, error) {
	resp2 := &pb.CancelJobResponse{}
	shouldRet, err := s.masterRPCHook.PreRPC(ctx, req, &resp2)
	if shouldRet {
		return resp2, err
	}
	return s.jobManager.CancelJob(ctx, req), nil
}

// PauseJob implements pb.MasterServer.PauseJob
func (s *Server) PauseJob(ctx context.Context, req *pb.PauseJobRequest) (*pb.PauseJobResponse, error) {
	resp2 := &pb.PauseJobResponse{}
	shouldRet, err := s.masterRPCHook.PreRPC(ctx, req, &resp2)
	if shouldRet {
		return resp2, err
	}
	return s.jobManager.PauseJob(ctx, req), nil
}

// RegisterExecutor implements grpc interface, and passes request onto executor manager.
func (s *Server) RegisterExecutor(ctx context.Context, req *pb.RegisterExecutorRequest) (*pb.RegisterExecutorResponse, error) {
	resp2 := &pb.RegisterExecutorResponse{}
	shouldRet, err := s.masterRPCHook.PreRPC(ctx, req, &resp2)
	if shouldRet {
		return resp2, err
	}
	// register executor to scheduler
	// TODO: check leader, if not leader, return notLeader error.
	execInfo, err := s.executorManager.AllocateNewExec(req)
	if err != nil {
		log.Error("add executor failed", zap.Error(err))
		return &pb.RegisterExecutorResponse{
			Err: cerrors.ToPBError(err),
		}, nil
	}
	return &pb.RegisterExecutorResponse{
		ExecutorId: string(execInfo.ID),
	}, nil
}

// ScheduleTask implements grpc interface. It works as follows
// - receives request from job master
// - queries resource manager to allocate resource and maps tasks to executors
// - returns scheduler response to job master
func (s *Server) ScheduleTask(ctx context.Context, req *pb.ScheduleTaskRequest) (*pb.ScheduleTaskResponse, error) {
	resp2 := &pb.ScheduleTaskResponse{}
	shouldRet, err := s.masterRPCHook.PreRPC(ctx, req, &resp2)
	if shouldRet {
		return resp2, err
	}

	schedulerReq := &schedModel.SchedulerRequest{
		Cost:              schedModel.ResourceUnit(req.GetCost()),
		ExternalResources: resModel.ToResourceKeys(req.GetResourceRequirements()),
	}
	schedulerResp, err := s.scheduler.ScheduleTask(ctx, schedulerReq)
	if err != nil {
		return nil, schedModel.SchedulerErrorToGRPCError(err)
	}

	addr, ok := s.executorManager.GetAddr(schedulerResp.ExecutorID)
	if !ok {
		log.Warn("Executor is gone, RPC call needs retry",
			zap.Any("request", req),
			zap.String("executor-id", string(schedulerResp.ExecutorID)))
		errOut := cerrors.ErrUnknownExecutorID.GenWithStackByArgs(string(schedulerResp.ExecutorID))
		return nil, status.Error(codes.Internal, errOut.Error())
	}

	return &pb.ScheduleTaskResponse{
		ExecutorId:   string(schedulerResp.ExecutorID),
		ExecutorAddr: addr,
	}, nil
}

// RegisterMetaStore registers backend metastore to server master,
// but have not implemented yet.
func (s *Server) RegisterMetaStore(
	ctx context.Context, req *pb.RegisterMetaStoreRequest,
) (*pb.RegisterMetaStoreResponse, error) {
	return nil, nil
}

// QueryMetaStore implements gRPC interface
func (s *Server) QueryMetaStore(
	ctx context.Context, req *pb.QueryMetaStoreRequest,
) (*pb.QueryMetaStoreResponse, error) {
	getStore := func(storeID string) *pb.QueryMetaStoreResponse {
		store := s.metaStoreManager.GetMetaStore(storeID)
		if store == nil {
			return &pb.QueryMetaStoreResponse{
				Err: &pb.Error{
					Code:    pb.ErrorCode_MetaStoreNotExists,
					Message: fmt.Sprintf("store ID: %s", storeID),
				},
			}
		}
		b, err := json.Marshal(store)
		if err != nil {
			return &pb.QueryMetaStoreResponse{
				Err: &pb.Error{
					Code:    pb.ErrorCode_MetaStoreSerializeFail,
					Message: fmt.Sprintf("raw store config params: %v", store),
				},
			}
		}

		return &pb.QueryMetaStoreResponse{
			Address: string(b),
		}
	}

	switch req.Tp {
	case pb.StoreType_ServiceDiscovery:
		if len(s.cfg.ETCDEndpoints) > 0 {
			return &pb.QueryMetaStoreResponse{
				Address: s.cfg.ETCDEndpoints[0],
			}, nil
		}
		return &pb.QueryMetaStoreResponse{
			Err: &pb.Error{
				Code:    pb.ErrorCode_MetaStoreNotExists,
				Message: fmt.Sprintf("store type: %s", req.Tp),
			},
		}, nil
	case pb.StoreType_SystemMetaStore:
		return getStore(FrameMetaID), nil
	case pb.StoreType_AppMetaStore:
		return getStore(DefaultBusinessMetaID), nil
	default:
		return &pb.QueryMetaStoreResponse{
			Err: &pb.Error{
				Code:    pb.ErrorCode_InvalidMetaStoreType,
				Message: fmt.Sprintf("store type: %s", req.Tp),
			},
		}, nil
	}
}

// ReportExecutorWorkload implements pb.MasterServer.ReportExecutorWorkload
func (s *Server) ReportExecutorWorkload(
	ctx context.Context, req *pb.ExecWorkloadRequest,
) (*pb.ExecWorkloadResponse, error) {
	// TODO: pass executor workload to capacity manager
	log.Debug("receive workload report", zap.String("executor", req.ExecutorId))
	for _, res := range req.GetWorkloads() {
		log.Debug("workload", zap.Int32("type", res.GetTp()), zap.Int32("usage", res.GetUsage()))
	}
	return &pb.ExecWorkloadResponse{}, nil
}

func (s *Server) startForTest(ctx context.Context) (err error) {
	// TODO: implement mock-etcd and leader election

	s.mockGrpcServer, err = mock.NewMasterServer(s.cfg.Addr, s)
	if err != nil {
		return err
	}

	s.executorManager.Start(ctx)
	// TODO: start job manager
	s.leader.Store(&rpcutil.Member{Name: s.name(), IsLeader: true})
	s.leaderInitialized.Store(true)
	return
}

// Stop and clean resources.
// TODO: implement stop gracefully.
func (s *Server) Stop() {
	if s.mockGrpcServer != nil {
		s.mockGrpcServer.Stop()
	}
	if s.etcdClient != nil {
		s.etcdClient.Close()
	}
	// in some tests this fields is not initialized
	if s.masterCli != nil {
		s.masterCli.Close()
	}
	if s.resourceCli != nil {
		s.resourceCli.Close()
	}
	if s.frameMetaClient != nil {
		s.frameMetaClient.Close()
	}
	if s.businessClientConn != nil {
		s.businessClientConn.Close()
	}
	if s.executorManager != nil {
		s.executorManager.Stop()
	}
}

// Run the server master.
func (s *Server) Run(ctx context.Context) error {
	if test.GetGlobalTestFlag() {
		return s.startForTest(ctx)
	}

	// TODO: need context here to initialize the metastore connection
	err := s.registerMetaStore()
	if err != nil {
		return err
	}

	// ResourceManagerService should be initialized after registerMetaStore.
	// FIXME: We should do these work inside NewServer.
	s.initResourceManagerService()
	s.scheduler = makeScheduler(s.executorManager, s.resourceManagerService)

	wg, ctx := errgroup.WithContext(ctx)

	wg.Go(func() error {
		return s.serve(ctx)
	})

	wg.Go(func() error {
		return s.msgService.GetMessageServer().Run(ctx)
	})

	wg.Go(func() error {
		return s.leaderLoop(ctx)
	})

	wg.Go(func() error {
		return s.discoveryKeeper.Keepalive(ctx)
	})

	return wg.Wait()
}

func (s *Server) registerMetaStore() error {
	// register metastore for framework
	cfg := s.cfg
	if err := s.metaStoreManager.Register(cfg.FrameMetaConf.StoreID, cfg.FrameMetaConf); err != nil {
		return err
	}
	var err error
	// TODO: replace default db config
	if s.frameMetaClient, err = pkgOrm.NewClient(*cfg.FrameMetaConf, *(cfg.FrameMetaConf.DBConf)); err != nil {
		log.Error("connect to framework metastore fail", zap.Any("config", cfg.FrameMetaConf), zap.Error(err))
		return err
	}

	log.Info("register framework metastore successfully", zap.Any("metastore", cfg.FrameMetaConf))

	// register metastore for business
	err = s.metaStoreManager.Register(cfg.BusinessMetaConf.StoreID, cfg.BusinessMetaConf)
	if err != nil {
		return err
	}
	s.businessClientConn, err = meta.NewClientConn(cfg.BusinessMetaConf)
	if err != nil {
		log.Error("connect to business metastore fail", zap.Any("config", cfg.BusinessMetaConf), zap.Error(err))
		return err
	}
	log.Info("register business metastore successfully", zap.Any("metastore", cfg.BusinessMetaConf))

	return nil
}

func (s *Server) initResourceManagerService() {
	resourceRPCHook := rpcutil.NewPreRPCHook(
		s.id,
		&s.leader,
		s.resourceCli,
		&s.leaderInitialized,
		s.rpcLogRL,
		resourceRPCLimiterAllowList,
	)
	s.resourceManagerService = externRescManager.NewService(
		s.frameMetaClient,
		s.executorManager,
		resourceRPCHook,
	)
}

func (s *Server) serve(ctx context.Context) error {
	errGroup, ctx := errgroup.WithContext(ctx)

	// TODO: Support TLS.
	tcpServer, err := tcpserver.NewTCPServer(s.cfg.Addr, &security.Credential{})
	if err != nil {
		return err
	}
	defer tcpServer.Close()
	errGroup.Go(func() error {
		return tcpServer.Run(ctx)
	})

	httpServer := s.createHTTPServer()
	defer httpServer.Close()
	errGroup.Go(func() error {
		return httpServer.Serve(tcpServer.HTTP1Listener())
	})

	grpcServer := s.createGRPCServer()
	defer grpcServer.Stop()
	errGroup.Go(func() error {
		return grpcServer.Serve(tcpServer.GrpcListener())
	})

	return errGroup.Wait()
}

func (s *Server) createGRPCServer() *grpc.Server {
	grpcServer := grpc.NewServer(
		grpc.StreamInterceptor(grpcprometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpcprometheus.UnaryServerInterceptor),
	)
	pb.RegisterMasterServer(grpcServer, s)
	pb.RegisterResourceManagerServer(grpcServer, s.resourceManagerService)
	p2pProtocol.RegisterCDCPeerToPeerServer(grpcServer, s.msgService.GetMessageServer())
	return grpcServer
}

func (s *Server) createHTTPServer() *http.Server {
	router := gin.New()
	openapi := NewOpenAPI(s)
	RegisterRoutes(router, openapi)

	return &http.Server{
		Handler: router,
	}
}

// member returns member information of the server
func (s *Server) member() string {
	m := &rpcutil.Member{
		Name:          s.name(),
		AdvertiseAddr: s.cfg.AdvertiseAddr,
	}
	val, err := m.String()
	if err != nil {
		return s.name()
	}
	return val
}

// name is a shortcut to etcd name
func (s *Server) name() string {
	return s.id
}

func (s *Server) initializedBackendMeta(ctx context.Context) error {
	bctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := s.frameMetaClient.Initialize(bctx); err != nil {
		log.Error("framework metastore initialized all backend tables fail", zap.Error(err))
		return err
	}

	return nil
}

func (s *Server) runLeaderService(ctx context.Context) (err error) {
	start := time.Now()

	// leader master need Initialize all backend tables first
	err = s.initializedBackendMeta(ctx)
	if err != nil {
		return
	}

	// rebuild states from existing meta if needed
	err = s.resetExecutor(ctx)
	if err != nil {
		return
	}

	// start background managers
	s.resourceManagerService.StartBackgroundWorker()
	defer func() {
		s.resourceManagerService.Stop()
		log.Info("resource manager exited")
	}()

	clients := client.NewClientManager()
	err = clients.AddMasterClient(ctx, []string{s.cfg.Addr})
	if err != nil {
		return
	}
	dctx := dcontext.NewContext(ctx)
	dctx.Environ.Addr = s.cfg.AdvertiseAddr
	dctx.Environ.NodeID = s.name()
	dctx.ProjectInfo = tenant.FrameProjectInfo

	masterMeta := &frameModel.MasterMetaKVData{
		ProjectID: tenant.FrameProjectInfo.UniqueID(),
		ID:        metadata.JobManagerUUID,
		Tp:        framework.JobManager,
		// TODO: add other infos
	}
	masterMetaBytes, err := masterMeta.Marshal()
	if err != nil {
		return
	}
	dctx.Environ.MasterMetaBytes = masterMetaBytes

	dp := deps.NewDeps()
	if err := dp.Provide(func() pkgOrm.Client {
		return s.frameMetaClient
	}); err != nil {
		return err
	}

	if err := dp.Provide(func() metaModel.ClientConn {
		return s.businessClientConn
	}); err != nil {
		return err
	}

	if err := dp.Provide(func() client.ClientsManager {
		return clients
	}); err != nil {
		return err
	}

	if err := dp.Provide(func() client.MasterClient {
		return clients.MasterClient()
	}); err != nil {
		return err
	}

	if err := dp.Provide(func() p2p.MessageSender {
		return p2p.NewMessageSender(s.p2pMsgRouter)
	}); err != nil {
		return err
	}

	if err := dp.Provide(func() p2p.MessageHandlerManager {
		return s.msgService.MakeHandlerManager()
	}); err != nil {
		return err
	}

	s.leader.Store(&rpcutil.Member{
		Name:          s.name(),
		AdvertiseAddr: s.cfg.AdvertiseAddr,
		IsLeader:      true,
	})
	defer func() {
		s.leaderInitialized.Store(false)
		s.leader.Store(&rpcutil.Member{})
	}()

	dctx = dctx.WithDeps(dp)
	s.jobManager, err = NewJobManagerImplV2(dctx, metadata.JobManagerUUID)
	if err != nil {
		return
	}
	defer func() {
		err := s.jobManager.Close(ctx)
		if err != nil {
			log.Warn("job manager close with error", zap.Error(err))
		}
		log.Info("job manager exited")
	}()

	s.gcRunner = externRescManager.NewGCRunner(s.frameMetaClient, map[resModel.ResourceType]externRescManager.GCHandlerFunc{
		"local": resourcetypes.NewLocalFileResourceType(clients).GCHandler(),
	})
	s.gcCoordinator = externRescManager.NewGCCoordinator(s.executorManager, s.jobManager, s.frameMetaClient, s.gcRunner)

	// TODO refactor this method to make it more readable and maintainable.
	errg, errgCtx := errgroup.WithContext(ctx)

	errg.Go(func() error {
		return s.gcRunner.Run(errgCtx)
	})
	errg.Go(func() error {
		return s.gcCoordinator.Run(errgCtx)
	})

	errg.Go(func() error {
		defer func() {
			s.executorManager.Stop()
			log.Info("executor manager exited")
		}()
		s.executorManager.Start(errgCtx)
		return nil
	})

	errg.Go(func() error {
		metricTicker := time.NewTicker(defaultMetricInterval)
		defer metricTicker.Stop()
		leaderTicker := time.NewTicker(time.Millisecond * 200)
		defer leaderTicker.Stop()
		for {
			select {
			case <-errgCtx.Done():
				// errgCtx is a leaderCtx actually
				return errors.Trace(errgCtx.Err())
			case <-leaderTicker.C:
				if err := s.jobManager.Poll(errgCtx); err != nil {
					log.Warn("Polling JobManager failed", zap.Error(err))
					return err
				}
			case <-leaderTicker.C:
				s.collectLeaderMetric()
			}
		}
	})

	s.leaderInitialized.Store(true)
	log.Info("leader is initialized", zap.Duration("took", time.Since(start)))

	return errg.Wait()
}

func (s *Server) collectLeaderMetric() {
	for status := range pb.QueryJobResponse_JobStatus_name {
		pbStatus := pb.QueryJobResponse_JobStatus(status)
		s.metrics.metricJobNum[pbStatus].Set(float64(s.jobManager.JobCount(pbStatus)))
	}
	for status := range model.ExecutorStatusNameMapping {
		s.metrics.metricExecutorNum[status].Set(float64(s.executorManager.ExecutorCount(status)))
	}
}

// makeScheduler is a helper function for Server to create a scheduler.Scheduler.
// This function makes it clear how a Scheduler is supposed to be constructed
// using concrete type, from the perspective of Server.
func makeScheduler(
	executorManager ExecutorManager,
	externalResourceManager *externRescManager.Service,
) *scheduler.Scheduler {
	return scheduler.NewScheduler(
		executorManager.CapacityProvider(),
		externalResourceManager,
	)
}

// IsLeader implements ServerInfoProvider.IsLeader.
func (s *Server) IsLeader() bool {
	leader, ok := s.leader.Load().(*rpcutil.Member)
	if !ok || leader == nil {
		return false
	}
	return leader.Name == s.id
}

// LeaderAddr implements ServerInfoProvider.LeaderAddr.
func (s *Server) LeaderAddr() (string, bool) {
	leader, ok := s.leader.Load().(*rpcutil.Member)
	if !ok || leader == nil {
		return "", false
	}
	return leader.AdvertiseAddr, true
}

// JobManager implements ServerInfoProvider.JobManager.
func (s *Server) JobManager() (JobManager, bool) {
	if s.leaderInitialized.Load() && s.jobManager != nil {
		return s.jobManager, true
	}
	return nil, false
}

// ExecutorManager implements ServerInfoProvider.ExecutorManager.
func (s *Server) ExecutorManager() (ExecutorManager, bool) {
	if s.leaderInitialized.Load() && s.executorManager != nil {
		return s.executorManager, true
	}
	return nil, false
}
