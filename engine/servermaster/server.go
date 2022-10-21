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
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strings"
	"time"

	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/pingcap/errors"
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
	"github.com/pingcap/tiflow/engine/pkg/rpcerror"
	"github.com/pingcap/tiflow/engine/pkg/rpcutil"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/pingcap/tiflow/engine/servermaster/executormeta"
	"github.com/pingcap/tiflow/engine/servermaster/scheduler"
	schedModel "github.com/pingcap/tiflow/engine/servermaster/scheduler/model"
	"github.com/pingcap/tiflow/engine/servermaster/serverutil"
	"github.com/pingcap/tiflow/engine/test/mock"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/label"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/tcpserver"
	p2pProtocol "github.com/pingcap/tiflow/proto/p2p"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
)

// TODO: make it configurable in the future.
const (
	leaderElectionTable  = "leader_election"
	resignLeaderDuration = 10 * time.Second
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

	elector   *election.Elector
	leader    atomic.Value
	masterCli *rpcutil.LeaderClientWithLock[multiClient]

	leaderServiceFn func(context.Context) error
	masterRPCHook   rpcutil.PreRPCHook

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

	leaderInitialized atomic.Bool

	// mocked server for test
	mockGrpcServer mock.GrpcServer

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
	for status, statusName := range pb.Job_State_name {
		metric := serverJobNumGauge.WithLabelValues(statusName)
		metricJobNum[pb.Job_State(status)] = metric
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
func NewServer(cfg *Config) (*Server, error) {
	log.Info("creating server master", zap.Stringer("config", cfg))

	id := generateNodeID(cfg.Name)
	msgService := p2p.NewMessageRPCServiceWithRPCServer(id, nil, nil)
	p2pMsgRouter := p2p.NewMessageRouter(id, cfg.AdvertiseAddr)

	server := &Server{
		id:                id,
		cfg:               cfg,
		leaderInitialized: *atomic.NewBool(false),
		leader:            atomic.Value{},
		masterCli:         &rpcutil.LeaderClientWithLock[multiClient]{},
		msgService:        msgService,
		p2pMsgRouter:      p2pMsgRouter,
		rpcLogRL:          rate.NewLimiter(rate.Every(time.Second*5), 3 /*burst*/),
		metrics:           newServerMasterMetric(),
		metaStoreManager:  NewMetaStoreManager(),
	}
	server.leaderServiceFn = server.runLeaderService
	masterRPCHook := rpcutil.NewPreRPCHook[multiClient](
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
		for _, m := range s.elector.GetMembers() {
			resp.Addrs = append(resp.Addrs, m.Address)
		}
		leader, exists := s.masterRPCHook.CheckLeader()
		if exists {
			resp.Leader = leader.AdvertiseAddr
		}
	}
	return resp, err
}

// CreateJob delegates request to leader's JobManager.CreateJob.
func (s *Server) CreateJob(ctx context.Context, req *pb.CreateJobRequest) (*pb.Job, error) {
	job := &pb.Job{}
	shouldRet, err := s.masterRPCHook.PreRPC(ctx, req, &job)
	if shouldRet {
		return job, err
	}
	return s.jobManager.CreateJob(ctx, req)
}

// GetJob delegates request to leader's JobManager.GetJob.
func (s *Server) GetJob(ctx context.Context, req *pb.GetJobRequest) (*pb.Job, error) {
	job := &pb.Job{}
	shouldRet, err := s.masterRPCHook.PreRPC(ctx, req, &job)
	if shouldRet {
		return job, err
	}
	return s.jobManager.GetJob(ctx, req)
}

// ListJobs delegates request to leader's JobManager.ListJobs.
func (s *Server) ListJobs(ctx context.Context, req *pb.ListJobsRequest) (*pb.ListJobsResponse, error) {
	resp := &pb.ListJobsResponse{}
	shouldRet, err := s.masterRPCHook.PreRPC(ctx, req, &resp)
	if shouldRet {
		return resp, err
	}
	return s.jobManager.ListJobs(ctx, req)
}

// CancelJob delegates request to leader's JobManager.CancelJob.
func (s *Server) CancelJob(ctx context.Context, req *pb.CancelJobRequest) (*pb.Job, error) {
	job := &pb.Job{}
	shouldRet, err := s.masterRPCHook.PreRPC(ctx, req, &job)
	if shouldRet {
		return job, err
	}
	return s.jobManager.CancelJob(ctx, req)
}

// DeleteJob delegates request to leader's JobManager.DeleteJob.
func (s *Server) DeleteJob(ctx context.Context, req *pb.DeleteJobRequest) (*emptypb.Empty, error) {
	empty := &emptypb.Empty{}
	shouldRet, err := s.masterRPCHook.PreRPC(ctx, req, &empty)
	if shouldRet {
		return empty, err
	}
	return s.jobManager.DeleteJob(ctx, req)
}

// RegisterExecutor implements grpc interface, and passes request onto executor manager.
func (s *Server) RegisterExecutor(ctx context.Context, req *pb.RegisterExecutorRequest) (*pb.Executor, error) {
	pbExecutor := &pb.Executor{}
	shouldRet, err := s.masterRPCHook.PreRPC(ctx, req, &pbExecutor)
	if shouldRet {
		return pbExecutor, err
	}

	executorMeta, err := s.executorManager.AllocateNewExec(ctx, req)
	if err != nil {
		return nil, err
	}

	return &pb.Executor{
		Id:         string(executorMeta.ID),
		Name:       executorMeta.Name,
		Address:    executorMeta.Address,
		Capability: int64(executorMeta.Capability),
		Labels:     label.Set(executorMeta.Labels).ToMap(),
	}, nil
}

// ListExecutors implements DiscoveryServer.ListExecutors.
func (s *Server) ListExecutors(ctx context.Context, req *pb.ListExecutorsRequest) (*pb.ListExecutorsResponse, error) {
	resp := &pb.ListExecutorsResponse{}
	shouldRet, err := s.masterRPCHook.PreRPC(ctx, req, &resp)
	if shouldRet {
		return resp, err
	}

	executors := s.executorManager.ListExecutors()
	for _, executor := range executors {
		resp.Executors = append(resp.Executors, &pb.Executor{
			Id:         string(executor.ID),
			Name:       executor.Name,
			Address:    executor.Address,
			Capability: int64(executor.Capability),
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
	leaderAddr, ok := s.LeaderAddr()
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
	resp2 := &pb.ScheduleTaskResponse{}
	shouldRet, err := s.masterRPCHook.PreRPC(ctx, req, &resp2)
	if shouldRet {
		return resp2, err
	}

	schedulerReq, err := schedModel.NewSchedulerRequestFromPB(req)
	if err != nil {
		return nil, err
	}

	schedulerResp, err := s.scheduler.ScheduleTask(ctx, schedulerReq)
	if err != nil {
		return nil, rpcerror.ToGRPCError(err)
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

// QueryStorageConfig implements gRPC interface
func (s *Server) QueryStorageConfig(
	ctx context.Context, req *pb.QueryStorageConfigRequest,
) (*pb.QueryStorageConfigResponse, error) {
	b, err := json.Marshal(s.cfg.Storage)
	if err != nil {
		return &pb.QueryStorageConfigResponse{
			Err: &pb.Error{
				Code:    pb.ErrorCode_MetaStoreSerializeFail,
				Message: fmt.Sprintf("raw storage config params: %v", s.cfg.Storage),
			},
		}, nil
	}
	return &pb.QueryStorageConfigResponse{
		Config: string(b),
	}, nil
}

// GetLeader implements DiscoveryServer.GetLeader.
func (s *Server) GetLeader(_ context.Context, _ *pb.GetLeaderRequest) (*pb.GetLeaderResponse, error) {
	leaderAddr, ok := s.LeaderAddr()
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

// Stop and clean resources.
// TODO: implement stop gracefully.
func (s *Server) Stop() {
	if s.mockGrpcServer != nil {
		s.mockGrpcServer.Stop()
	}
	// in some tests this fields is not initialized
	if s.masterCli != nil {
		s.masterCli.Close()
	}
	if s.frameMetaClient != nil {
		s.frameMetaClient.Close()
	}
	if s.frameworkClientConn != nil {
		s.frameworkClientConn.Close()
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
	err := s.registerMetaStore(ctx)
	if err != nil {
		return err
	}

	// Elector relies on meta store, so it should be initialized after meta store.
	if err := s.initElector(); err != nil {
		return errors.Trace(err)
	}

	if err := broker.PreCheckConfig(s.cfg.Storage); err != nil {
		return err
	}

	// executorMetaClient needs to be initialized after frameworkClientConn is initialized.
	executorMetaClient, err := executormeta.NewClient(s.frameworkClientConn)
	if err != nil {
		return errors.Trace(err)
	}
	s.executorManager = NewExecutorManagerImpl(executorMetaClient, s.cfg.KeepAliveTTL, s.cfg.KeepAliveInterval)

	// ResourceManagerService should be initialized after registerMetaStore.
	// FIXME: We should do these work inside NewServer.
	s.initResourceManagerService()
	s.scheduler = scheduler.NewScheduler(
		s.executorManager,
		s.resourceManagerService)

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

	wg.Go(func() error {
		return s.watchLeader(ctx)
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
	s.resourceManagerService = externRescManager.NewService(s.frameMetaClient, s.masterRPCHook)
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

	s.elector, err = election.NewElector(election.Config{
		ID:             s.id,
		Name:           s.cfg.Name,
		Address:        s.cfg.AdvertiseAddr,
		Storage:        sqlStorage,
		LeaderCallback: s.leaderServiceFn,
	})
	return err
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

	httpServer, err := s.createHTTPServer()
	if err != nil {
		return err
	}
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
		grpc.ChainUnaryInterceptor(grpcprometheus.UnaryServerInterceptor, rpcerror.UnaryServerInterceptor),
	)
	pb.RegisterDiscoveryServer(grpcServer, s)
	pb.RegisterTaskSchedulerServer(grpcServer, s)
	pb.RegisterJobManagerServer(grpcServer, s)
	pb.RegisterResourceManagerServer(grpcServer, s.resourceManagerService)
	p2pProtocol.RegisterCDCPeerToPeerServer(grpcServer, s.msgService.GetMessageServer())
	return grpcServer
}

func (s *Server) createHTTPServer() (*http.Server, error) {
	grpcMux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
			MarshalOptions:   protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true},
			UnmarshalOptions: protojson.UnmarshalOptions{},
		}),
		runtime.WithErrorHandler(func(ctx context.Context, mux *runtime.ServeMux,
			marshaler runtime.Marshaler, writer http.ResponseWriter, request *http.Request, err error,
		) {
			errOut := rpcerror.ToGRPCError(err)
			st, ok := status.FromError(errOut)
			if !ok {
				st = status.FromContextError(err)
			}
			runtime.DefaultHTTPErrorHandler(ctx, mux, marshaler, writer, request, st.Err())
		}),
	)
	if err := pb.RegisterJobManagerHandlerServer(context.Background(), grpcMux, s); err != nil {
		return nil, errors.Trace(err)
	}
	if err := pb.RegisterDiscoveryHandlerServer(context.Background(), grpcMux, s); err != nil {
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
		// using normalized error to construct grpc.status if possible
		// NOTE: normalized error should have 'message' to keep the same as normal error
		st, ok := status.FromError(rpcerror.ToGRPCError(err))
		if !ok {
			st = status.FromContextError(err)
		}
		payload, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(st.Proto())
		if err != nil {
			log.Warn("failed to marshal grpc status", zap.Error(err))
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(runtime.HTTPStatusFromCode(st.Code()))
		if _, err := w.Write(payload); err != nil {
			log.Warn("failed to write response", zap.Error(err))
		}
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
	if s.IsLeader() {
		forwardAddr, err := s.jobManager.GetJobMasterForwardAddress(r.Context(), jobID)
		if err != nil {
			return err
		}
		targetAddr = forwardAddr
	} else if leaderAddr, ok := s.LeaderAddr(); ok {
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
		return s.executorManager.Start(errgCtx)
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

	errg.Go(func() error {
		updater := executorInfoUpdater{
			msgRouter:     s.p2pMsgRouter,
			executorGroup: executorClients,
		}
		return serverutil.WatchExecutors(errgCtx, s.executorManager, updater)
	})

	s.leaderInitialized.Store(true)
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
	if !ok || leader == nil || leader.AdvertiseAddr == "" {
		return "", false
	}
	return leader.AdvertiseAddr, true
}
