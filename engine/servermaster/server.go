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
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/log"
	p2pProtocol "github.com/pingcap/tiflow/proto/p2p"
	"github.com/prometheus/client_golang/prometheus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/tiflow/engine/client"
	"github.com/pingcap/tiflow/engine/lib"
	"github.com/pingcap/tiflow/engine/lib/metadata"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pb"
	"github.com/pingcap/tiflow/engine/pkg/adapter"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/deps"
	derrors "github.com/pingcap/tiflow/engine/pkg/errors"
	"github.com/pingcap/tiflow/engine/pkg/etcdutils"
	externRescManager "github.com/pingcap/tiflow/engine/pkg/externalresource/manager"
	extkv "github.com/pingcap/tiflow/engine/pkg/meta/extension"
	"github.com/pingcap/tiflow/engine/pkg/meta/kvclient"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/pingcap/tiflow/engine/pkg/rpcutil"
	"github.com/pingcap/tiflow/engine/pkg/serverutils"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/pingcap/tiflow/engine/servermaster/cluster"
	"github.com/pingcap/tiflow/engine/servermaster/scheduler"
	schedModel "github.com/pingcap/tiflow/engine/servermaster/scheduler/model"
	"github.com/pingcap/tiflow/engine/test"
	"github.com/pingcap/tiflow/engine/test/mock"
)

// Server handles PRC requests for df master.
type Server struct {
	etcd *embed.Etcd

	etcdClient *clientv3.Client
	session    *concurrency.Session
	election   cluster.Election
	leaderCtx  context.Context
	resignFn   context.CancelFunc
	leader     atomic.Value
	members    struct {
		sync.RWMutex
		m []*Member
	}
	masterCli       *rpcutil.LeaderClientWithLock[pb.MasterClient]
	resourceCli     *rpcutil.LeaderClientWithLock[pb.ResourceManagerClient]
	membership      Membership
	leaderServiceFn func(context.Context) error
	masterRPCHook   *rpcutil.PreRPCHook[pb.MasterClient]

	// sched scheduler
	executorManager        ExecutorManager
	jobManager             JobManager
	resourceManagerService *externRescManager.Service
	scheduler              *scheduler.Scheduler

	//
	cfg     *Config
	info    *model.NodeInfo
	metrics *serverMasterMetric
	// id contains etcd name plus an uuid, each server master has a unique id
	// and the id changes after it restarts.
	id string

	msgService      *p2p.MessageRPCService
	p2pMsgRouter    p2p.MessageRouter
	rpcLogRL        *rate.Limiter
	discoveryKeeper *serverutils.DiscoveryKeepaliver

	metaStoreManager MetaStoreManager

	leaderInitialized atomic.Bool

	// mocked server for test
	mockGrpcServer mock.GrpcServer

	testCtx *test.Context

	// framework metastore client
	frameMetaClient pkgOrm.Client
	// user metastore kvclient
	userMetaKVClient extkv.KVClientEx
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

func genServerMasterUUID(etcdName string) string {
	return etcdName + "-" + uuid.New().String()
}

// NewServer creates a new master-server.
func NewServer(cfg *Config, ctx *test.Context) (*Server, error) {
	executorManager := NewExecutorManagerImpl(cfg.KeepAliveTTL, cfg.KeepAliveInterval, ctx)

	urls, err := parseURLs(cfg.MasterAddr)
	if err != nil {
		return nil, err
	}
	masterAddrs := make([]string, 0, len(urls))
	for _, u := range urls {
		masterAddrs = append(masterAddrs, u.Host)
	}

	id := genServerMasterUUID(cfg.Etcd.Name)
	info := &model.NodeInfo{
		Type: model.NodeTypeServerMaster,
		ID:   model.DeployNodeID(id),
		Addr: cfg.AdvertiseAddr,
	}
	p2pMsgRouter := p2p.NewMessageRouter(p2p.NodeID(info.ID), info.Addr)

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
		p2pMsgRouter:      p2pMsgRouter,
		rpcLogRL:          rate.NewLimiter(rate.Every(time.Second*5), 3 /*burst*/),
		metrics:           newServerMasterMetric(),
		metaStoreManager:  NewMetaStoreManager(),
	}
	server.leaderServiceFn = server.runLeaderService
	masterRPCHook := rpcutil.NewPreRPCHook[pb.MasterClient](
		id,
		&server.leader,
		server.masterCli,
		&server.leaderInitialized,
		server.rpcLogRL,
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
		s.members.RLock()
		defer s.members.RUnlock()
		addrs := make([]string, 0, len(s.members.m))
		for _, member := range s.members.m {
			addrs = append(addrs, member.AdvertiseAddr)
		}
		resp.Addrs = addrs
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
		log.L().Logger.Error("add executor failed", zap.Error(err))
		return &pb.RegisterExecutorResponse{
			Err: derrors.ToPBError(err),
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
		ExternalResources: req.GetResourceRequirements(),
	}
	schedulerResp, err := s.scheduler.ScheduleTask(ctx, schedulerReq)
	if err != nil {
		return nil, schedModel.SchedulerErrorToGRPCError(err)
	}

	addr, ok := s.executorManager.GetAddr(schedulerResp.ExecutorID)
	if !ok {
		log.L().Warn("Executor is gone, RPC call needs retry",
			zap.Any("request", req),
			zap.String("executor-id", string(schedulerResp.ExecutorID)))
		errOut := derrors.ErrUnknownExecutorID.GenWithStackByArgs(string(schedulerResp.ExecutorID))
		return nil, status.Error(codes.Internal, errOut.Error())
	}

	return &pb.ScheduleTaskResponse{
		ExecutorId:   string(schedulerResp.ExecutorID),
		ExecutorAddr: addr,
	}, nil
}

// DeleteExecutor deletes an executor, but have yet implemented.
func (s *Server) DeleteExecutor() {
	// To implement
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
		return &pb.QueryMetaStoreResponse{
			Address: s.cfg.AdvertiseAddr,
		}, nil
	case pb.StoreType_SystemMetaStore:
		return getStore(metaclient.FrameMetaID), nil
	case pb.StoreType_AppMetaStore:
		return &pb.QueryMetaStoreResponse{
			Address: s.cfg.UserMetaConf.Endpoints[0],
		}, nil
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
	log.L().Debug("receive workload report", zap.String("executor", req.ExecutorId))
	for _, res := range req.GetWorkloads() {
		log.L().Debug("workload", zap.Int32("type", int32(res.GetTp())), zap.Int32("usage", res.GetUsage()))
	}
	return &pb.ExecWorkloadResponse{}, nil
}

func (s *Server) startForTest(ctx context.Context) (err error) {
	// TODO: implement mock-etcd and leader election

	s.mockGrpcServer, err = mock.NewMasterServer(s.cfg.MasterAddr, s)
	if err != nil {
		return err
	}

	s.executorManager.Start(ctx)
	// TODO: start job manager
	s.leader.Store(&Member{Name: s.name(), IsServLeader: true, IsEtcdLeader: true})
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
	if s.etcd != nil {
		s.etcd.Close()
	}
	if s.frameMetaClient != nil {
		s.frameMetaClient.Close()
	}
	if s.userMetaKVClient != nil {
		s.userMetaKVClient.Close()
	}
}

// Run the server master.
func (s *Server) Run(ctx context.Context) (err error) {
	if test.GetGlobalTestFlag() {
		return s.startForTest(ctx)
	}

	registerMetrics()

	err = s.registerMetaStore()
	if err != nil {
		return err
	}

	// startResourceManager should be put after registerMetaStore
	err = s.startResourceManager()
	if err != nil {
		return err
	}

	err = s.startGrpcSrv(ctx)
	if err != nil {
		return
	}
	err = s.reset(ctx)
	if err != nil {
		return
	}

	wg, ctx := errgroup.WithContext(ctx)

	wg.Go(func() error {
		return s.msgService.GetMessageServer().Run(ctx)
	})

	wg.Go(func() error {
		return s.leaderLoop(ctx)
	})

	wg.Go(func() error {
		return s.memberLoop(ctx)
	})

	s.discoveryKeeper = serverutils.NewDiscoveryKeepaliver(
		s.info, s.etcdClient, int(defaultSessionTTL/time.Second),
		defaultDiscoverTicker, s.p2pMsgRouter,
	)
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
	if s.frameMetaClient, err = pkgOrm.NewClient(*cfg.FrameMetaConf, pkgOrm.NewDefaultDBConfig()); err != nil {
		log.L().Error("connect to framework metastore fail", zap.Any("config", cfg.FrameMetaConf), zap.Error(err))
		return err
	}

	log.L().Info("register framework metastore successfully", zap.Any("metastore", cfg.FrameMetaConf))

	// register metastore for user
	err = s.metaStoreManager.Register(cfg.UserMetaConf.StoreID, cfg.UserMetaConf)
	if err != nil {
		return err
	}
	if s.userMetaKVClient, err = kvclient.NewKVClient(cfg.UserMetaConf); err != nil {
		log.L().Error("connect to user metastore fail", zap.Any("config", cfg.UserMetaConf), zap.Error(err))
		return err
	}
	log.L().Info("register user metastore successfully", zap.Any("metastore", cfg.UserMetaConf))

	return nil
}

func (s *Server) startResourceManager() error {
	resourceRPCHook := rpcutil.NewPreRPCHook[pb.ResourceManagerClient](
		s.id,
		&s.leader,
		s.resourceCli,
		&s.leaderInitialized,
		s.rpcLogRL,
	)
	s.resourceManagerService = externRescManager.NewService(
		s.frameMetaClient,
		s.executorManager,
		resourceRPCHook,
	)
	s.scheduler = makeScheduler(s.executorManager, s.resourceManagerService)
	return nil
}

func (s *Server) startGrpcSrv(ctx context.Context) (err error) {
	etcdCfg := etcdutils.GenEmbedEtcdConfigWithLogger(s.cfg.LogLevel)
	// prepare to join an existing etcd cluster.
	err = etcdutils.PrepareJoinEtcd(s.cfg.Etcd, s.cfg.MasterAddr)
	if err != nil {
		return
	}
	log.L().Info("config after join prepared", zap.Stringer("config", s.cfg))

	// generates embed etcd config before any concurrent gRPC calls.
	// potential concurrent gRPC calls:
	//   - workerrpc.NewGRPCClient
	//   - getHTTPAPIHandler
	// no `String` method exists for embed.Config, and can not marshal it to join too.
	// but when starting embed etcd server, the etcd pkg will log the config.
	// https://github.com/etcd-io/etcd/blob/3cf2f69b5738fb702ba1a935590f36b52b18979b/embed/etcd.go#L299
	etcdCfg, err = etcdutils.GenEmbedEtcdConfig(etcdCfg, s.cfg.MasterAddr, s.cfg.AdvertiseAddr, s.cfg.Etcd)
	if err != nil {
		return
	}

	gRPCSvr := func(gs *grpc.Server) {
		pb.RegisterMasterServer(gs, s)
		pb.RegisterResourceManagerServer(gs, s.resourceManagerService)
		s.msgService = p2p.NewMessageRPCServiceWithRPCServer(s.name(), nil, gs)
		p2pProtocol.RegisterCDCPeerToPeerServer(gs, s.msgService.GetMessageServer())
	}

	httpHandlers := map[string]http.Handler{
		"/debug/":  getDebugHandler(),
		"/metrics": promutil.HTTPHandlerForMetric(),
	}

	// generate grpcServer
	s.etcd, err = startEtcd(ctx, etcdCfg, gRPCSvr, httpHandlers, etcdStartTimeout)
	if err != nil {
		return
	}
	log.L().Logger.Info("start etcd successfully")

	// start grpc server
	s.etcdClient, err = etcdutil.CreateClient([]string{withHost(s.cfg.MasterAddr)}, nil)
	return
}

// member returns member information of the server
func (s *Server) member() string {
	m := &Member{
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

func (s *Server) reset(ctx context.Context) error {
	sess, err := concurrency.NewSession(
		s.etcdClient, concurrency.WithTTL(int(defaultSessionTTL.Seconds())))
	if err != nil {
		return derrors.Wrap(derrors.ErrMasterNewServer, err)
	}

	// register NodeInfo key used in service discovery
	value, err := s.info.ToJSON()
	if err != nil {
		return derrors.Wrap(derrors.ErrMasterNewServer, err)
	}
	_, err = s.etcdClient.Put(ctx, s.info.EtcdKey(), value, clientv3.WithLease(sess.Lease()))
	if err != nil {
		return derrors.Wrap(derrors.ErrEtcdAPIError, err)
	}

	s.session = sess
	s.election, err = cluster.NewEtcdElection(ctx, s.etcdClient, sess, cluster.EtcdElectionConfig{
		CreateSessionTimeout: s.cfg.RPCTimeout, // FIXME (zixiong): use a separate timeout here
		TTL:                  s.cfg.KeepAliveTTL,
		Prefix:               adapter.MasterCampaignKey.Path(),
	})
	if err != nil {
		return err
	}
	s.membership = &EtcdMembership{etcdCli: s.etcdClient}
	err = s.updateServerMasterMembers(ctx)
	if err != nil {
		log.L().Warn("failed to update server master members", zap.Error(err))
	}
	return nil
}

func (s *Server) initializedBackendMeta(ctx context.Context) error {
	bctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := s.frameMetaClient.Initialize(bctx); err != nil {
		log.L().Error("framework metastore initialized all backend tables fail", zap.Error(err))
		return err
	}

	return nil
}

func (s *Server) runLeaderService(ctx context.Context) (err error) {
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
	s.executorManager.Start(ctx)
	s.resourceManagerService.StartBackgroundWorker()
	defer func() {
		s.resourceManagerService.Stop()
	}()
	clients := client.NewClientManager()
	err = clients.AddMasterClient(ctx, []string{s.cfg.MasterAddr})
	if err != nil {
		return
	}
	dctx := dcontext.NewContext(ctx, log.L())
	dctx.Environ.Addr = s.cfg.AdvertiseAddr
	dctx.Environ.NodeID = s.name()

	masterMeta := &libModel.MasterMetaKVData{
		ProjectID: tenant.FrameTenantID,
		ID:        metadata.JobManagerUUID,
		Tp:        lib.JobManager,
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

	if err := dp.Provide(func() extkv.KVClientEx {
		return s.userMetaKVClient
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

	s.leader.Store(&Member{
		Name:          s.name(),
		IsServLeader:  true,
		IsEtcdLeader:  true,
		AdvertiseAddr: s.cfg.AdvertiseAddr,
	})
	defer func() {
		s.leaderInitialized.Store(false)
		s.leader.Store(&Member{})
		s.resign()
	}()

	dctx = dctx.WithDeps(dp)
	s.jobManager, err = NewJobManagerImplV2(dctx, metadata.JobManagerUUID)
	if err != nil {
		return
	}
	defer func() {
		err := s.jobManager.Close(ctx)
		if err != nil {
			log.L().Warn("job manager close with error", zap.Error(err))
		}
	}()
	s.leaderInitialized.Store(true)

	metricTicker := time.NewTicker(defaultMetricInterval)
	defer metricTicker.Stop()
	leaderTicker := time.NewTicker(time.Millisecond * 200)
	defer leaderTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			// ctx is a leaderCtx actually
			return ctx.Err()
		case <-leaderTicker.C:
			if !s.isEtcdLeader() {
				log.L().Info("etcd leader changed, resigns server master leader",
					zap.String("old-leader-name", s.name()))
				return derrors.ErrEtcdLeaderChanged.GenWithStackByArgs()
			}
			err := s.jobManager.Poll(ctx)
			if err != nil {
				log.L().Warn("Polling JobManager failed", zap.Error(err))
				return err
			}
		case <-leaderTicker.C:
			s.collectLeaderMetric()
		}
	}
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

func (s *Server) memberLoop(ctx context.Context) error {
	ticker := time.NewTicker(defaultMemberLoopInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.updateServerMasterMembers(ctx); err != nil {
				log.L().Warn("update server master members failed", zap.Error(err))
			}
		}
	}
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
