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
	"database/sql"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/pingcap/log"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/framework/metadata"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/model"
	pkgClient "github.com/pingcap/tiflow/engine/pkg/client"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/deps"
	"github.com/pingcap/tiflow/engine/pkg/election"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/broker"
	externRescManager "github.com/pingcap/tiflow/engine/pkg/externalresource/manager"
	"github.com/pingcap/tiflow/engine/pkg/meta"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/engine/pkg/openapi"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/engine/pkg/rpcutil"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/pingcap/tiflow/engine/servermaster/scheduler"
	schedModel "github.com/pingcap/tiflow/engine/servermaster/scheduler/model"
	"github.com/pingcap/tiflow/engine/servermaster/serverutil"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/label"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/tcpserver"
	p2pProtocol "github.com/pingcap/tiflow/proto/p2p"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
)

// TODO: make it configurable in the future.
const (
	leaderElectionTable  = "leader_election"
	resignLeaderDuration = 10 * time.Second
	grpcConnBufSize      = 32 * 1024
)

// use a slice instead of map because in small data size, slice search is faster
// than map search.
var masterRPCLimiterAllowList = []string{
	"CreateJob",
	"CancelJob",
	"ScheduleTask",
	"CreateResource",
	"RemoveResource",
}

// Server handles PRC requests for df master.
type Server struct {
	id string // Server id, randomly generated when server is created.

	cfg     *Config
	metrics *serverMasterMetric

	elector election.Elector

	leaderServiceFn func(context.Context) error

	// sched scheduler
	executorManager        ExecutorManager
	jobManager             JobManager
	resourceManagerService *externRescManager.Service
	scheduler              *scheduler.Scheduler

	// file resource GC
	gcRunner      externRescManager.GCRunner
	gcCoordinator externRescManager.GCCoordinator

	msgService   *p2p.MessageRPCService
	p2pMsgRouter p2p.MessageRouter
	rpcLogRL     *rate.Limiter

	metaStoreManager MetaStoreManager

	leaderDegrader *featureDegrader
	forwardChecker *forwardChecker

	// framework metastore client
	frameMetaClient     pkgOrm.Client
	frameworkClientConn metaModel.ClientConn
	businessClientConn  metaModel.ClientConn
}

type serverMasterMetric struct {
	metricJobNum      map[pb.Job_State]prometheus.Gauge
	metricExecutorNum map[model.ExecutorStatus]prometheus.Gauge
}

func newServerMasterMetric() *serverMasterMetric {
	// Following are leader only metrics
	metricJobNum := make(map[pb.Job_State]prometheus.Gauge)
	for state, statusName := range pb.Job_State_name {
		metric := serverJobNumGauge.WithLabelValues(statusName)
		metricJobNum[pb.Job_State(state)] = metric
	}

	metricExecutorNum := make(map[model.ExecutorStatus]prometheus.Gauge)
	for state, name := range model.ExecutorStatusNameMapping {
		metric := serverExecutorNumGauge.WithLabelValues(name)
		metricExecutorNum[state] = metric
	}

	return &serverMasterMetric{
		metricJobNum:      metricJobNum,
		metricExecutorNum: metricExecutorNum,
	}
}

// NewServer creates a new master-server.
func NewServer(cfg *Config) (*Server, error) {
	log.Info("creating server master", zap.Stringer("config", cfg))

	id := generateNodeID(cfg.Name)
	msgService := p2p.NewMessageRPCServiceWithRPCServer(id, nil, nil)
	p2pMsgRouter := p2p.NewMessageRouter(id, cfg.AdvertiseAddr)

	server := &Server{
		id:               id,
		cfg:              cfg,
		leaderDegrader:   newFeatureDegrader(),
		msgService:       msgService,
		p2pMsgRouter:     p2pMsgRouter,
		rpcLogRL:         rate.NewLimiter(rate.Every(time.Second*5), 3 /*burst*/),
		metrics:          newServerMasterMetric(),
		metaStoreManager: NewMetaStoreManager(),
	}
	server.leaderServiceFn = server.runLeaderService
	return server, nil
}

// Heartbeat implements pb interface.
func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	return s.executorManager.HandleHeartbeat(req)
}

// CreateJob delegates request to leader's JobManager.CreateJob.
func (s *Server) CreateJob(ctx context.Context, req *pb.CreateJobRequest) (*pb.Job, error) {
	return s.jobManager.CreateJob(ctx, req)
}

// GetJob delegates request to leader's JobManager.GetJob.
func (s *Server) GetJob(ctx context.Context, req *pb.GetJobRequest) (*pb.Job, error) {
	return s.jobManager.GetJob(ctx, req)
}

// ListJobs delegates request to leader's JobManager.ListJobs.
func (s *Server) ListJobs(ctx context.Context, req *pb.ListJobsRequest) (*pb.ListJobsResponse, error) {
	return s.jobManager.ListJobs(ctx, req)
}

// CancelJob delegates request to leader's JobManager.CancelJob.
func (s *Server) CancelJob(ctx context.Context, req *pb.CancelJobRequest) (*pb.Job, error) {
	return s.jobManager.CancelJob(ctx, req)
}

// DeleteJob delegates request to leader's JobManager.DeleteJob.
func (s *Server) DeleteJob(ctx context.Context, req *pb.DeleteJobRequest) (*emptypb.Empty, error) {
	return s.jobManager.DeleteJob(ctx, req)
}

// RegisterExecutor implements grpc interface, and passes request onto executor manager.
func (s *Server) RegisterExecutor(ctx context.Context, req *pb.RegisterExecutorRequest) (*pb.Executor, error) {
	executorMeta, err := s.executorManager.AllocateNewExec(ctx, req)
	if err != nil {
		return nil, err
	}

	return &pb.Executor{
		Id:      string(executorMeta.ID),
		Name:    executorMeta.Name,
		Address: executorMeta.Address,
		Labels:  label.Set(executorMeta.Labels).ToMap(),
	}, nil
}

// ListExecutors implements DiscoveryServer.ListExecutors.
func (s *Server) ListExecutors(ctx context.Context, req *pb.ListExecutorsRequest) (*pb.ListExecutorsResponse, error) {
	resp := &pb.ListExecutorsResponse{}
	executors := s.executorManager.ListExecutors()
	for _, executor := range executors {
		resp.Executors = append(resp.Executors, &pb.Executor{
			Id:      string(executor.ID),
			Name:    executor.Name,
			Address: executor.Address,
			Labels:  executor.Labels.ToMap(),
		})
	}
	sort.Slice(resp.Executors, func(i, j int) bool {
		return resp.Executors[i].Id < resp.Executors[j].Id
	})
	return resp, nil
}

// ListMasters implements DiscoveryServer.ListMasters.
func (s *Server) ListMasters(ctx context.Context, req *pb.ListMastersRequest) (*pb.ListMastersResponse, error) {
	resp := &pb.ListMastersResponse{}
	leaderAddr, ok := s.leaderAddr()
	for _, m := range s.elector.GetMembers() {
		isLeader := ok && m.Address == leaderAddr
		resp.Masters = append(resp.Masters, &pb.Master{
			Id:       m.ID,
			Name:     m.Name,
			Address:  m.Address,
			IsLeader: isLeader,
		})
	}
	sort.Slice(resp.Masters, func(i, j int) bool {
		return resp.Masters[i].Id < resp.Masters[j].Id
	})
	return resp, nil
}

// ScheduleTask implements grpc interface. It works as follows
// - receives request from job master
// - queries resource manager to allocate resource and maps tasks to executors
// - returns scheduler response to job master
func (s *Server) ScheduleTask(ctx context.Context, req *pb.ScheduleTaskRequest) (*pb.ScheduleTaskResponse, error) {
	schedulerReq, err := schedModel.NewSchedulerRequestFromPB(req)
	if err != nil {
		return nil, err
	}

	schedulerResp, err := s.scheduler.ScheduleTask(ctx, schedulerReq)
	if err != nil {
		return nil, err
	}

	addr, ok := s.executorManager.GetAddr(schedulerResp.ExecutorID)
	if !ok {
		log.Warn("Executor is gone, RPC call needs retry",
			zap.Any("request", req),
			zap.String("executor-id", string(schedulerResp.ExecutorID)))
		return nil, errors.ErrUnknownExecutor.GenWithStackByArgs(string(schedulerResp.ExecutorID))
	}

	return &pb.ScheduleTaskResponse{
		ExecutorId:   string(schedulerResp.ExecutorID),
		ExecutorAddr: addr,
	}, nil
}

// QueryMetaStore implements gRPC interface
func (s *Server) QueryMetaStore(
	ctx context.Context, req *pb.QueryMetaStoreRequest,
) (*pb.QueryMetaStoreResponse, error) {
	getStore := func(storeID string) (*pb.QueryMetaStoreResponse, error) {
		store := s.metaStoreManager.GetMetaStore(storeID)
		if store == nil {
			return nil, errors.ErrMetaStoreNotExists.GenWithStackByArgs(storeID)
		}
		config, err := json.Marshal(store)
		if err != nil {
			return nil, errors.Trace(err)
		}

		return &pb.QueryMetaStoreResponse{
			Config: config,
		}, nil
	}

	switch req.Tp {
	case pb.StoreType_SystemMetaStore:
		return getStore(FrameMetaID)
	case pb.StoreType_AppMetaStore:
		return getStore(DefaultBusinessMetaID)
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown store type %v", req.Tp)
	}
}

// QueryStorageConfig implements gRPC interface
func (s *Server) QueryStorageConfig(
	ctx context.Context, req *pb.QueryStorageConfigRequest,
) (*pb.QueryStorageConfigResponse, error) {
	config, err := json.Marshal(s.cfg.Storage)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &pb.QueryStorageConfigResponse{
		Config: config,
	}, nil
}

// GetLeader implements DiscoveryServer.GetLeader.
func (s *Server) GetLeader(_ context.Context, _ *pb.GetLeaderRequest) (*pb.GetLeaderResponse, error) {
	leaderAddr, ok := s.leaderAddr()
	if !ok {
		return nil, status.Error(codes.NotFound, "no leader")
	}
	return &pb.GetLeaderResponse{
		AdvertiseAddr: leaderAddr,
	}, nil
}

// ResignLeader implements DiscoveryServer.ResignLeader.
func (s *Server) ResignLeader(ctx context.Context, _ *pb.ResignLeaderRequest) (*emptypb.Empty, error) {
	if err := s.elector.ResignLeader(ctx, resignLeaderDuration); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// Run the server master.
func (s *Server) Run(ctx context.Context) error {
	err := s.registerMetaStore(ctx)
	if err != nil {
		return err
	}

	// Elector relies on meta store, so it should be initialized after meta store.
	if err := s.initElector(); err != nil {
		return errors.Trace(err)
	}

	// resourceManagerService relies on meta store
	s.initResourceManagerService()

	if err := broker.PreCheckConfig(s.cfg.Storage); err != nil {
		return err
	}

	defer func() {
		if s.forwardChecker != nil {
			if err := s.forwardChecker.Close(); err != nil {
				log.Warn("failed to close forward checker", zap.Error(err))
			}
		}
	}()

	wg, ctx := errgroup.WithContext(ctx)

	wg.Go(func() error {
		return s.serve(ctx)
	})

	wg.Go(func() error {
		return s.msgService.GetMessageServer().Run(ctx)
	})

	wg.Go(func() error {
		return s.elector.Run(ctx)
	})

	return wg.Wait()
}

func (s *Server) registerMetaStore(ctx context.Context) error {
	// register metastore for framework
	cfg := s.cfg
	if err := s.metaStoreManager.Register(cfg.FrameworkMeta.StoreID, cfg.FrameworkMeta); err != nil {
		return err
	}
	if cfg.FrameworkMeta.StoreType == metaModel.StoreTypeMySQL {
		// Normally, a schema will be created in advance, and we may have no privilege
		// to create schema for framework meta. Just for easy test here. Ignore any error.
		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		if err := meta.CreateSchemaIfNotExists(ctx, *(s.cfg.FrameworkMeta)); err != nil {
			log.Error("create schema for framework metastore fail",
				zap.String("schema", s.cfg.FrameworkMeta.Schema),
				zap.Error(err))
			return err
		}
	}
	var err error
	s.frameworkClientConn, err = meta.NewClientConn(cfg.FrameworkMeta)
	if err != nil {
		log.Error("connect to framework metastore fail", zap.Any("config", cfg.FrameworkMeta), zap.Error(err))
		return err
	}
	// some components depend on this framework meta client
	s.frameMetaClient, err = pkgOrm.NewClient(s.frameworkClientConn)
	if err != nil {
		log.Error("create framework meta client fail", zap.Error(err))
		return err
	}
	log.Info("register framework metastore successfully", zap.Any("config", cfg.FrameworkMeta))

	// register metastore for business
	err = s.metaStoreManager.Register(cfg.BusinessMeta.StoreID, cfg.BusinessMeta)
	if err != nil {
		return err
	}
	if cfg.BusinessMeta.StoreType == metaModel.StoreTypeMySQL {
		// Normally, a schema will be created in advance, and we may have no privilege
		// to create schema for business meta. Just for easy test here. Ignore any error.
		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		if err := meta.CreateSchemaIfNotExists(ctx, *(s.cfg.BusinessMeta)); err != nil {
			log.Error("create schema for business metastore fail",
				zap.String("schema", s.cfg.BusinessMeta.Schema),
				zap.Error(err))
			return err
		}
	}
	s.businessClientConn, err = meta.NewClientConn(cfg.BusinessMeta)
	if err != nil {
		log.Error("connect to business metastore fail", zap.Any("config", cfg.BusinessMeta), zap.Error(err))
		return err
	}
	log.Info("register business metastore successfully", zap.Any("config", cfg.BusinessMeta))

	return nil
}

func (s *Server) initResourceManagerService() {
	s.resourceManagerService = externRescManager.NewService(s.frameMetaClient)
}

func (s *Server) initElector() error {
	conn, err := s.frameworkClientConn.GetConn()
	if err != nil {
		return errors.Trace(err)
	}
	db, ok := conn.(*sql.DB)
	if !ok {
		return errors.Errorf("failed to convert conn to *sql.DB, got %T", conn)
	}

	sqlStorage, err := election.NewSQLStorage(db, leaderElectionTable)
	if err != nil {
		return err
	}

	elector, err := election.NewElector(election.Config{
		ID:             s.id,
		Name:           s.cfg.Name,
		Address:        s.cfg.AdvertiseAddr,
		Storage:        sqlStorage,
		LeaderCallback: s.leaderServiceFn,
	})
	if err != nil {
		return err
	}

	s.elector = elector
	s.forwardChecker = newForwardChecker(elector)
	return err
}

func (s *Server) serve(ctx context.Context) error {
	var (
		cleaupFuncs []func()
		cleanupOnce sync.Once
	)
	cleanup := func() {
		cleanupOnce.Do(func() {
			for _, f := range cleaupFuncs {
				f()
			}
		})
	}
	defer cleanup()

	errGroup, ctx := errgroup.WithContext(ctx)

	// TODO: Support TLS.
	tcpServer, err := tcpserver.NewTCPServer(s.cfg.Addr, &security.Credential{})
	if err != nil {
		return err
	}
	cleaupFuncs = append(cleaupFuncs, func() {
		if err := tcpServer.Close(); err != nil {
			log.Warn("failed to close tcp server", zap.Error(err))
		}
	})
	errGroup.Go(func() error {
		return tcpServer.Run(ctx)
	})

	grpcServer := s.createGRPCServer()
	cleaupFuncs = append(cleaupFuncs, grpcServer.Stop)
	errGroup.Go(func() error {
		return grpcServer.Serve(tcpServer.GrpcListener())
	})

	// gRPC-Gateway doesn't call gRPC interceptors when it directly forwards requests to the service handler.
	// See https://github.com/grpc-ecosystem/grpc-gateway/issues/1043.
	//
	// To make the gRPC interceptors work, we have to create a client connection to the gRPC server and register it to
	// the gRPC-Gateway ServerMux. bufconn is used to create an in-process gRPC server, so the client connection can
	// bypass the network stack. See https://github.com/grpc/grpc-go/issues/906 for more details.
	ln := bufconn.Listen(grpcConnBufSize)
	cleaupFuncs = append(cleaupFuncs, func() {
		if err := ln.Close(); err != nil {
			log.Warn("failed to close bufconn", zap.Error(err))
		}
	})
	inProcessGRPCServer := s.createGRPCServer()
	cleaupFuncs = append(cleaupFuncs, inProcessGRPCServer.Stop)
	errGroup.Go(func() error {
		return inProcessGRPCServer.Serve(ln)
	})

	dial := func(ctx context.Context, target string) (net.Conn, error) {
		return ln.DialContext(ctx)
	}
	conn, err := grpc.DialContext(
		ctx,
		"bufnet",
		grpc.WithContextDialer(dial),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return errors.Trace(err)
	}
	cleaupFuncs = append(cleaupFuncs, func() {
		if err := conn.Close(); err != nil {
			log.Warn("failed to close grpc connection", zap.Error(err))
		}
	})

	httpServer, err := s.createHTTPServer(conn)
	if err != nil {
		return err
	}
	cleaupFuncs = append(cleaupFuncs, func() {
		if err := httpServer.Close(); err != nil {
			log.Warn("failed to close http server", zap.Error(err))
		}
	})
	errGroup.Go(func() error {
		return httpServer.Serve(tcpServer.HTTP1Listener())
	})

	// Some background goroutines may still be running after context is canceled.
	// We need to explicitly stop or close them and wait for them to exit.
	errGroup.Go(func() error {
		<-ctx.Done()
		cleanup()
		return errors.Trace(ctx.Err())
	})

	return errGroup.Wait()
}

func (s *Server) createGRPCServer() *grpc.Server {
	logLimiter := rate.NewLimiter(rate.Every(time.Second*5), 3 /*burst*/)
	grpcServer := grpc.NewServer(
		grpc.StreamInterceptor(grpcprometheus.StreamServerInterceptor),
		grpc.ChainUnaryInterceptor(
			grpcprometheus.UnaryServerInterceptor,
			rpcutil.ForwardToLeader[multiClient](s.forwardChecker),
			rpcutil.CheckAvailable(s.leaderDegrader),
			rpcutil.Logger(masterRPCLimiterAllowList, logLimiter),
			rpcutil.NormalizeError(),
		),
	)
	pb.RegisterDiscoveryServer(grpcServer, s)
	pb.RegisterTaskSchedulerServer(grpcServer, s)
	pb.RegisterJobManagerServer(grpcServer, s)
	pb.RegisterResourceManagerServer(grpcServer, s.resourceManagerService)
	p2pProtocol.RegisterCDCPeerToPeerServer(grpcServer, s.msgService.GetMessageServer())
	return grpcServer
}

func (s *Server) createHTTPServer(conn *grpc.ClientConn) (*http.Server, error) {
	grpcMux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
			MarshalOptions:   protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true},
			UnmarshalOptions: protojson.UnmarshalOptions{},
		}),
		runtime.WithErrorHandler(func(ctx context.Context, mux *runtime.ServeMux,
			_ runtime.Marshaler, writer http.ResponseWriter, _ *http.Request, err error,
		) {
			// Since request may be forwarded to other servers through grpc, we should try
			// to extract the error from gRPC error first, and then convert it to HTTP error.
			openapi.WriteHTTPError(writer, rpcutil.FromGRPCError(err))
		}),
	)
	if err := pb.RegisterJobManagerHandler(context.Background(), grpcMux, conn); err != nil {
		return nil, errors.Trace(err)
	}
	if err := pb.RegisterDiscoveryHandler(context.Background(), grpcMux, conn); err != nil {
		return nil, errors.Trace(err)
	}

	router := http.NewServeMux()
	registerRoutes(router, grpcMux, s.forwardJobAPI)

	return &http.Server{
		Handler:           router,
		ReadHeaderTimeout: time.Minute,
	}, nil
}

func (s *Server) forwardJobAPI(w http.ResponseWriter, r *http.Request) {
	if err := s.handleForwardJobAPI(w, r); err != nil {
		openapi.WriteHTTPError(w, err)
	}
}

func (s *Server) handleForwardJobAPI(w http.ResponseWriter, r *http.Request) error {
	apiPath := strings.TrimPrefix(r.URL.Path, openapi.JobAPIPrefix)
	fields := strings.SplitN(apiPath, "/", 2)
	if len(fields) != 2 {
		return errors.New("invalid job api path")
	}
	jobID := fields[0]

	var targetAddr string
	if s.elector.IsLeader() {
		forwardAddr, err := s.jobManager.GetJobMasterForwardAddress(r.Context(), jobID)
		if err != nil {
			return err
		}
		targetAddr = forwardAddr
	} else if leaderAddr, ok := s.leaderAddr(); ok {
		targetAddr = leaderAddr
	} else {
		return errors.New("no leader found")
	}

	// TODO: Support TLS.
	u, err := url.Parse("http://" + targetAddr)
	if err != nil {
		return errors.Errorf("invalid target address %s", targetAddr)
	}
	proxy := httputil.NewSingleHostReverseProxy(u)
	proxy.ServeHTTP(w, r)
	return nil
}

// name is a shortcut to etcd name
func (s *Server) name() string {
	return s.id
}

func (s *Server) initializedBackendMeta(ctx context.Context) error {
	bctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := pkgOrm.InitAllFrameworkModels(bctx, s.frameworkClientConn); err != nil {
		log.Error("framework metastore initializes all backend tables fail", zap.Error(err))
		return err
	}

	// Since we have the sql-type business metastore,
	// we need to initialize the logic_epoches table for all jobs
	if s.cfg.BusinessMeta.StoreType == metaModel.StoreTypeMySQL {
		if err := pkgOrm.InitEpochModel(ctx, s.businessClientConn); err != nil {
			log.Error("business metastore initializes the logic epoch table fail", zap.Error(err))
			return err
		}
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

	// TODO support TLS.
	executorClients := pkgClient.NewExecutorGroup(&security.Credential{}, log.L())
	masterClient, err := pkgClient.NewServerMasterClientWithFailOver(
		pkgClient.MasterServerList{
			s.cfg.Addr: true,
		},
		nil, // TODO support TLS
	)
	if err != nil {
		return
	}

	dctx := dcontext.NewContext(ctx)
	dctx.Environ.Addr = s.cfg.AdvertiseAddr
	dctx.Environ.NodeID = s.name()
	dctx.ProjectInfo = tenant.FrameProjectInfo

	masterMeta := &frameModel.MasterMeta{
		ProjectID: tenant.FrameProjectInfo.UniqueID(),
		ID:        metadata.JobManagerUUID,
		Type:      frameModel.JobManager,
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

	if err := dp.Provide(func() pkgClient.ExecutorGroup {
		return executorClients
	}); err != nil {
		return err
	}

	if err := dp.Provide(func() pkgClient.ServerMasterClient {
		return masterClient
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

	defer func() {
		s.leaderDegrader.reset()
	}()

	// The following member variables are used in leader only and released after
	// the leader is resigned, so initialize these variables in this function,
	// instead of initializing them in the NewServer or Server.Run

	// executorMetaClient needs to be initialized after frameMetaClient is initialized.
	s.executorManager = NewExecutorManagerImpl(s.frameMetaClient, s.cfg.KeepAliveTTL, s.cfg.KeepAliveInterval)

	// ResourceManagerService should be initialized after registerMetaStore.
	s.scheduler = scheduler.NewScheduler(
		s.executorManager,
		s.resourceManagerService)

	// TODO refactor this method to make it more readable and maintainable.
	errg, errgCtx := errgroup.WithContext(ctx)

	errg.Go(func() error {
		return s.executorManager.Run(errgCtx)
	})

	errg.Go(func() error {
		updater := executorInfoUpdater{
			msgRouter:     s.p2pMsgRouter,
			executorGroup: executorClients,
		}
		return serverutil.WatchExecutors(errgCtx, s.executorManager, updater)
	})

	s.leaderDegrader.updateExecutorManager(true)

	dctx = dctx.WithDeps(dp)
	s.jobManager, err = NewJobManagerImpl(dctx, metadata.JobManagerUUID, s.cfg.JobBackoff)
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

	s.gcRunner = externRescManager.NewGCRunner(s.frameMetaClient, executorClients, &s.cfg.Storage)
	s.gcCoordinator = externRescManager.NewGCCoordinator(s.executorManager, s.jobManager, s.frameMetaClient, s.gcRunner)

	errg.Go(func() error {
		return s.gcRunner.Run(errgCtx)
	})
	errg.Go(func() error {
		return s.gcCoordinator.Run(errgCtx)
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

	s.leaderDegrader.updateMasterWorkerManager(true)
	log.Info("leader is initialized", zap.Duration("took", time.Since(start)))

	return errg.Wait()
}

type executorInfoUpdater struct {
	msgRouter     p2p.MessageRouter
	executorGroup *pkgClient.DefaultExecutorGroup
}

func (e executorInfoUpdater) UpdateExecutorList(executors map[model.ExecutorID]string) error {
	for id, addr := range executors {
		e.msgRouter.AddPeer(string(id), addr)
	}
	return e.executorGroup.UpdateExecutorList(executors)
}

func (e executorInfoUpdater) AddExecutor(executorID model.ExecutorID, addr string) error {
	e.msgRouter.AddPeer(string(executorID), addr)
	return e.executorGroup.AddExecutor(executorID, addr)
}

func (e executorInfoUpdater) RemoveExecutor(executorID model.ExecutorID) error {
	e.msgRouter.RemovePeer(string(executorID))
	return e.executorGroup.RemoveExecutor(executorID)
}

func (s *Server) collectLeaderMetric() {
	for statusName := range pb.Job_State_name {
		pbStatus := pb.Job_State(statusName)
		s.metrics.metricJobNum[pbStatus].Set(float64(s.jobManager.JobCount(pbStatus)))
	}
	for statusName := range model.ExecutorStatusNameMapping {
		s.metrics.metricExecutorNum[statusName].Set(float64(s.executorManager.ExecutorCount(statusName)))
	}
}

func (s *Server) leaderAddr() (string, bool) {
	leader, ok := s.elector.GetLeader()
	if ok {
		return leader.Address, true
	}
	return "", false
}
