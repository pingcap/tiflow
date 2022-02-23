package servermaster

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/adapter"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/etcdutils"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/hanfei1991/microcosm/pkg/serverutils"
	"github.com/hanfei1991/microcosm/servermaster/cluster"
	"github.com/hanfei1991/microcosm/test"
	"github.com/hanfei1991/microcosm/test/mock"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/log"
	p2pProtocol "github.com/pingcap/tiflow/proto/p2p"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/embed"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
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
	leaderClient struct {
		sync.RWMutex
		cli *client.MasterClientImpl
	}
	membership      Membership
	leaderServiceFn func(context.Context) error

	// sched scheduler
	executorManager ExecutorManager
	jobManager      JobManager
	//
	cfg     *Config
	info    *model.NodeInfo
	metrics *serverMasterMetric

	msgService      *p2p.MessageRPCService
	p2pMsgRouter    p2p.MessageRouter
	rpcLogRL        *rate.Limiter
	discoveryKeeper *serverutils.DiscoveryKeepaliver

	initialized atomic.Bool

	// mocked server for test
	mockGrpcServer mock.GrpcServer

	testCtx *test.Context
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

	info := &model.NodeInfo{
		Type: model.NodeTypeServerMaster,
		ID:   model.DeployNodeID(cfg.Etcd.Name),
		Addr: cfg.AdvertiseAddr,
	}
	p2pMsgRouter := p2p.NewMessageRouter(p2p.NodeID(info.ID), info.Addr)

	server := &Server{
		cfg:             cfg,
		info:            info,
		executorManager: executorManager,
		initialized:     *atomic.NewBool(false),
		testCtx:         ctx,
		leader:          atomic.Value{},
		p2pMsgRouter:    p2pMsgRouter,
		rpcLogRL:        rate.NewLimiter(rate.Every(time.Second*5), 3 /*burst*/),
		metrics:         newServerMasterMetric(),
	}
	server.leaderServiceFn = server.runLeaderService
	return server, nil
}

// Heartbeat implements pb interface.
func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	var (
		resp2 *pb.HeartbeatResponse
		err2  error
	)
	shouldRet := s.rpcForwardIfNeeded(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	checkErr := s.apiPreCheck()
	if checkErr != nil {
		return &pb.HeartbeatResponse{Err: checkErr}, nil
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
		leader, exists := s.checkLeader()
		if exists {
			resp.Leader = leader.AdvertiseAddr
		}
	}
	return resp, err
}

// SubmitJob passes request onto "JobManager".
func (s *Server) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	var (
		resp2 *pb.SubmitJobResponse
		err2  error
	)
	shouldRet := s.rpcForwardIfNeeded(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	err := s.apiPreCheck()
	if err != nil {
		return &pb.SubmitJobResponse{Err: err}, nil
	}
	return s.jobManager.SubmitJob(ctx, req), nil
}

func (s *Server) QueryJob(ctx context.Context, req *pb.QueryJobRequest) (*pb.QueryJobResponse, error) {
	var (
		resp2 *pb.QueryJobResponse
		err2  error
	)
	shouldRet := s.rpcForwardIfNeeded(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	err := s.apiPreCheck()
	if err != nil {
		return &pb.QueryJobResponse{Err: err}, nil
	}
	return s.jobManager.QueryJob(ctx, req), nil
}

func (s *Server) CancelJob(ctx context.Context, req *pb.CancelJobRequest) (*pb.CancelJobResponse, error) {
	var (
		resp2 *pb.CancelJobResponse
		err2  error
	)
	shouldRet := s.rpcForwardIfNeeded(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	err := s.apiPreCheck()
	if err != nil {
		return &pb.CancelJobResponse{Err: err}, nil
	}
	return s.jobManager.CancelJob(ctx, req), nil
}

func (s *Server) PauseJob(ctx context.Context, req *pb.PauseJobRequest) (*pb.PauseJobResponse, error) {
	var (
		resp2 *pb.PauseJobResponse
		err2  error
	)
	shouldRet := s.rpcForwardIfNeeded(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	err := s.apiPreCheck()
	if err != nil {
		return &pb.PauseJobResponse{Err: err}, nil
	}
	return s.jobManager.PauseJob(ctx, req), nil
}

// RegisterExecutor implements grpc interface, and passes request onto executor manager.
func (s *Server) RegisterExecutor(ctx context.Context, req *pb.RegisterExecutorRequest) (*pb.RegisterExecutorResponse, error) {
	var (
		resp2 *pb.RegisterExecutorResponse
		err2  error
	)
	shouldRet := s.rpcForwardIfNeeded(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	ckErr := s.apiPreCheck()
	if ckErr != nil {
		return &pb.RegisterExecutorResponse{Err: ckErr}, nil
	}
	// register executor to scheduler
	// TODO: check leader, if not leader, return notLeader error.
	execInfo, err := s.executorManager.AllocateNewExec(req)
	if err != nil {
		log.L().Logger.Error("add executor failed", zap.Error(err))
		return &pb.RegisterExecutorResponse{
			Err: errors.ToPBError(err),
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
func (s *Server) ScheduleTask(ctx context.Context, req *pb.TaskSchedulerRequest) (*pb.TaskSchedulerResponse, error) {
	var (
		resp2 *pb.TaskSchedulerResponse
		err2  error
	)
	shouldRet := s.rpcForwardIfNeeded(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	checkErr := s.apiPreCheck()
	if checkErr != nil {
		return &pb.TaskSchedulerResponse{Err: checkErr}, nil
	}

	tasks := req.GetTasks()
	success, resp := s.executorManager.Allocate(tasks)
	if !success {
		return nil, errors.ErrClusterResourceNotEnough.GenWithStackByArgs()
	}
	return resp, nil
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
	switch req.Tp {
	case pb.StoreType_ServiceDiscovery:
		return &pb.QueryMetaStoreResponse{
			Address: s.cfg.AdvertiseAddr,
		}, nil
	case pb.StoreType_SystemMetaStore:
		// TODO: independent system metastore
		return &pb.QueryMetaStoreResponse{
			Address: s.cfg.AdvertiseAddr,
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

func (s *Server) ReportExecutorWorkload(
	ctx context.Context, req *pb.ExecWorkloadRequest,
) (*pb.ExecWorkloadResponse, error) {
	// TODO: pass executor workload to resource manager
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
	s.initialized.Store(true)
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
	s.leaderClient.Lock()
	if s.leaderClient.cli != nil {
		s.leaderClient.cli.Close()
	}
	s.leaderClient.Unlock()
	if s.etcd != nil {
		s.etcd.Close()
	}
}

// Run the master-server.
func (s *Server) Run(ctx context.Context) (err error) {
	if test.GetGlobalTestFlag() {
		return s.startForTest(ctx)
	}

	registerMetrics()

	err = s.startGrpcSrv(ctx)
	if err != nil {
		return
	}
	err = s.reset(ctx)
	if err != nil {
		return
	}
	s.initialized.Store(true)

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
		s.msgService = p2p.NewMessageRPCServiceWithRPCServer(s.name(), nil, gs)
		p2pProtocol.RegisterCDCPeerToPeerServer(gs, s.msgService.GetMessageServer())
	}

	httpHandlers := map[string]http.Handler{
		"/debug/":  getDebugHandler(),
		"/metrics": promhttp.Handler(),
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
	return s.cfg.Etcd.Name
}

func (s *Server) reset(ctx context.Context) error {
	sess, err := concurrency.NewSession(
		s.etcdClient, concurrency.WithTTL(int(defaultSessionTTL.Seconds())))
	if err != nil {
		return errors.Wrap(errors.ErrMasterNewServer, err)
	}

	// register NodeInfo key used in service discovery
	value, err := s.info.ToJSON()
	if err != nil {
		return errors.Wrap(errors.ErrMasterNewServer, err)
	}
	_, err = s.etcdClient.Put(ctx, s.info.EtcdKey(), value, clientv3.WithLease(sess.Lease()))
	if err != nil {
		return errors.Wrap(errors.ErrEtcdAPIError, err)
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

func (s *Server) runLeaderService(ctx context.Context) (err error) {
	// rebuild states from existing meta if needed
	err = s.resetExecutor(ctx)
	if err != nil {
		return
	}

	// start background managers
	s.executorManager.Start(ctx)

	clients := client.NewClientManager()
	err = clients.AddMasterClient(ctx, []string{s.cfg.MasterAddr})
	if err != nil {
		return
	}
	dctx := dcontext.NewContext(ctx, log.L())
	dctx.Environ.Addr = s.cfg.AdvertiseAddr
	dctx.Environ.NodeID = s.name()
	masterMetaExt := &lib.MasterMetaExt{
		ID: lib.JobManagerUUID,
		Tp: lib.JobManager,
	}
	masterMetaExtBytes, err := masterMetaExt.Marshal()
	if err != nil {
		return
	}
	dctx.Environ.MasterMetaExt = masterMetaExtBytes
	s.jobManager, err = NewJobManagerImplV2(dctx, "", lib.JobManagerUUID,
		s.msgService.MakeHandlerManager(), p2p.NewMessageSender(s.p2pMsgRouter),
		clients, metadata.NewMetaEtcd(s.etcdClient))
	if err != nil {
		return
	}

	s.leader.Store(&Member{
		Name:          s.name(),
		IsServLeader:  true,
		IsEtcdLeader:  true,
		AdvertiseAddr: s.cfg.AdvertiseAddr,
	})
	defer func() {
		s.leader.Store(&Member{})
		s.resign()
	}()

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
				return errors.ErrEtcdLeaderChanged.GenWithStackByArgs()
			}
			err := s.jobManager.Poll(ctx)
			if err != nil {
				return err
			}
		case <-leaderTicker.C:
			s.collectLeaderMetric()
		}
	}
}

func (s *Server) checkLeader() (leader *Member, exist bool) {
	lp := s.leader.Load()
	if lp == nil {
		return
	}
	leader = lp.(*Member)
	exist = leader.Name != ""
	return
}

func (s *Server) isLeaderAndNeedForward(ctx context.Context) (isLeader, needForward bool) {
	leader, exist := s.checkLeader()
	// leader is nil, retry for 3 seconds
	if !exist {
		retry := 10
		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()

		for !exist {
			if retry == 0 {
				log.L().Error("leader is not found, please retry later")
				return false, false
			}
			select {
			case <-ctx.Done():
				return false, false
			case <-ticker.C:
				retry--
			}
			leader, exist = s.checkLeader()
		}
	}
	isLeader = leader.Name == s.name()
	s.leaderClient.RLock()
	needForward = s.leaderClient.cli != nil
	s.leaderClient.RUnlock()
	return
}

// rpcForwardIfNeeded forwards gRPC if needed.
// arguments with `Pointer` suffix should be pointer to that variable its name indicated
// return `true` means caller should return with variable that `xxPointer` modified.
func (s *Server) rpcForwardIfNeeded(ctx context.Context, req interface{}, respPointer interface{}, errPointer *error) bool {
	pc, _, _, _ := runtime.Caller(1)
	fullMethodName := runtime.FuncForPC(pc).Name()
	methodName := fullMethodName[strings.LastIndexByte(fullMethodName, '.')+1:]

	// TODO: rate limiter based on different sender
	if s.rpcLogRL.Allow() {
		log.L().Info("", zap.Any("payload", req), zap.String("request", methodName))
	}

	isLeader, needForward := s.isLeaderAndNeedForward(ctx)
	if isLeader {
		return false
	}
	if needForward {
		leader, exist := s.checkLeader()
		if !exist {
			respType := reflect.ValueOf(respPointer).Elem().Type()
			reflect.ValueOf(respPointer).Elem().Set(reflect.Zero(respType))
			*errPointer = errors.ErrMasterRPCNotForward.GenWithStackByArgs()
			return true
		}
		log.L().Info("will forward rpc request", zap.String("from", s.name()),
			zap.String("to", leader.Name), zap.String("request", methodName))

		params := []reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(req)}
		s.leaderClient.RLock()
		defer s.leaderClient.RUnlock()
		results := reflect.ValueOf(s.leaderClient.cli.GetLeaderClient()).
			MethodByName(methodName).
			Call(params)
		// result's inner types should be (*pb.XXResponse, error), which is same as s.leaderClient.XXRPCMethod
		reflect.ValueOf(respPointer).Elem().Set(results[0])
		errInterface := results[1].Interface()
		// nil can't pass type conversion, so we handle it separately
		if errInterface == nil {
			*errPointer = nil
		} else {
			*errPointer = errInterface.(error)
		}
		return true
	}
	respType := reflect.ValueOf(respPointer).Elem().Type()
	reflect.ValueOf(respPointer).Elem().Set(reflect.Zero(respType))
	*errPointer = errors.ErrMasterRPCNotForward.GenWithStackByArgs()
	return true
}

// apiPreCheck checks whether server master(leader) is ready to serve
func (s *Server) apiPreCheck() *pb.Error {
	if !s.initialized.Load() {
		return &pb.Error{
			Code: pb.ErrorCode_MasterNotReady,
		}
	}
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
