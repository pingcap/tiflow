// Copyright 2019 PingCAP, Inc.
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

package master

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	toolutils "github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/pingcap/tidb/util/dbutil"
	"github.com/pingcap/tiflow/dm/checker"
	dmcommon "github.com/pingcap/tiflow/dm/common"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/config/dbconfig"
	"github.com/pingcap/tiflow/dm/config/security"
	ctlcommon "github.com/pingcap/tiflow/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/loader"
	"github.com/pingcap/tiflow/dm/master/metrics"
	"github.com/pingcap/tiflow/dm/master/scheduler"
	"github.com/pingcap/tiflow/dm/master/shardddl"
	"github.com/pingcap/tiflow/dm/master/workerrpc"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/cputil"
	"github.com/pingcap/tiflow/dm/pkg/election"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/ui"
	"github.com/pingcap/tiflow/dm/unit"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	// the DM-master leader election key prefix
	// DM-master cluster : etcd cluster = 1 : 1 now.
	electionKey = "/dm-master/leader"

	// getLeaderBlockTime is the max block time for get leader information from election.
	getLeaderBlockTime = 10 * time.Minute
)

var (
	// the retry times for dm-master to confirm the dm-workers status is expected.
	maxRetryNum = 10
	// the retry interval for dm-master to confirm the dm-workers status is expected.
	retryInterval = time.Second

	useTLS atomic.Bool

	// the session's TTL in seconds for leader election.
	// NOTE: select this value carefully when adding a mechanism relying on leader election.
	electionTTL = 60

	// typically there's only one server running in one process, but testMaster.TestOfflineMember starts 3 servers,
	// so we need sync.Once to prevent data race.
	registerOnce      sync.Once
	runBackgroundOnce sync.Once

	// CheckAndAdjustSourceConfigFunc is exposed to dataflow engine.
	// the difference of below functions is checkAndAdjustSourceConfigForDMCtlFunc will not AdjustCaseSensitive. It's a
	// compatibility compromise.
	// When we need to change the implementation of dmctl to OpenAPI, we should notice the user about this change.
	CheckAndAdjustSourceConfigFunc         = checkAndAdjustSourceConfig
	checkAndAdjustSourceConfigForDMCtlFunc = checkAndAdjustSourceConfigForDMCtl
)

// Server handles RPC requests for dm-master.
type Server struct {
	sync.RWMutex

	cfg *Config

	// the embed etcd server, and the gRPC/HTTP API server also attached to it.
	etcd *embed.Etcd

	etcdClient *clientv3.Client
	election   *election.Election

	// below three leader related variables should be protected by a lock (currently Server's lock) to provide integrity
	// except for leader == oneselfStartingLeader which is a intermedia state, which means caller may retry sometime later
	leader         atomic.String
	leaderClient   pb.MasterClient
	leaderGrpcConn *grpc.ClientConn

	// dm-worker-ID(host:ip) -> dm-worker client management
	scheduler *scheduler.Scheduler

	// shard DDL pessimist
	pessimist *shardddl.Pessimist
	// shard DDL optimist
	optimist *shardddl.Optimist

	// agent pool
	ap *AgentPool

	// WaitGroup for background functions.
	bgFunWg sync.WaitGroup

	closed atomic.Bool

	openapiHandles *gin.Engine // injected in `InitOpenAPIHandles`

	clusterID atomic.Uint64
}

// NewServer creates a new Server.
func NewServer(cfg *Config) *Server {
	logger := log.L()
	server := Server{
		cfg:       cfg,
		scheduler: scheduler.NewScheduler(&logger, cfg.Security),
		ap:        NewAgentPool(&RateLimitConfig{rate: cfg.RPCRateLimit, burst: cfg.RPCRateBurst}),
	}
	server.pessimist = shardddl.NewPessimist(&logger, server.getTaskSourceNameList)
	server.optimist = shardddl.NewOptimist(&logger, server.scheduler.GetDownstreamMetaByTask)
	server.closed.Store(true)
	setUseTLS(&cfg.Security)

	return &server
}

// Start starts to serving.
func (s *Server) Start(ctx context.Context) (err error) {
	etcdCfg := genEmbedEtcdConfigWithLogger(s.cfg.LogLevel)
	// prepare config to join an existing cluster
	err = prepareJoinEtcd(s.cfg)
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
	etcdCfg, err = s.cfg.genEmbedEtcdConfig(etcdCfg)
	if err != nil {
		return
	}

	tls, err := toolutils.NewTLS(s.cfg.SSLCA, s.cfg.SSLCert, s.cfg.SSLKey, s.cfg.AdvertiseAddr, s.cfg.CertAllowedCN)
	if err != nil {
		return terror.ErrMasterTLSConfigNotValid.Delegate(err)
	}

	// tls2 is used for grpc client in grpc gateway
	tls2, err := toolutils.NewTLS(s.cfg.SSLCA, s.cfg.SSLCert, s.cfg.SSLKey, s.cfg.AdvertiseAddr, s.cfg.CertAllowedCN)
	if err != nil {
		return terror.ErrMasterTLSConfigNotValid.Delegate(err)
	}

	apiHandler, err := getHTTPAPIHandler(ctx, s.cfg.AdvertiseAddr, tls2.ToGRPCDialOption())
	if err != nil {
		return
	}

	registerOnce.Do(metrics.RegistryMetrics)

	// HTTP handlers on etcd's client IP:port. etcd will add a builtin `/metrics` route
	// NOTE: after received any HTTP request from chrome browser,
	// the server may be blocked when closing sometime.
	// And any request to etcd's builtin handler has the same problem.
	// And curl or safari browser does trigger this problem.
	// But I haven't figured it out.
	// (maybe more requests are sent from chrome or its extensions).

	userHandles := map[string]http.Handler{
		"/apis/":  apiHandler,
		"/status": getStatusHandle(),
		"/debug/": getDebugHandler(),
	}
	if s.cfg.OpenAPI {
		// tls3 is used to openapi reverse proxy
		tls3, err1 := toolutils.NewTLS(s.cfg.SSLCA, s.cfg.SSLCert, s.cfg.SSLKey, s.cfg.AdvertiseAddr, s.cfg.CertAllowedCN)
		if err1 != nil {
			return terror.ErrMasterTLSConfigNotValid.Delegate(err1)
		}
		if initOpenAPIErr := s.InitOpenAPIHandles(tls3.TLSConfig()); initOpenAPIErr != nil {
			return terror.ErrOpenAPICommonError.Delegate(initOpenAPIErr)
		}

		const dashboardPrefix = "/dashboard/"
		scheme := "http://"
		if tls3.TLSConfig() != nil {
			scheme = "https://"
		}
		log.L().Info("Web UI enabled", zap.String("dashboard", scheme+s.cfg.AdvertiseAddr+dashboardPrefix))

		// Register handlers for OpenAPI and dashboard.
		userHandles["/api/v1/"] = s.openapiHandles
		userHandles[dashboardPrefix] = ui.InitWebUIRouter()
	}

	// gRPC API server
	gRPCSvr := func(gs *grpc.Server) { pb.RegisterMasterServer(gs, s) }

	// start embed etcd server, gRPC API server and HTTP (API, status and debug) server.
	s.etcd, err = startEtcd(etcdCfg, gRPCSvr, userHandles, 10*time.Second)
	if err != nil {
		return
	}

	// create an etcd client used in the whole server instance.
	// NOTE: we only use the local member's address now, but we can use all endpoints of the cluster if needed.
	s.etcdClient, err = etcdutil.CreateClient([]string{withHost(s.cfg.AdvertiseAddr)}, tls.TLSConfig())
	if err != nil {
		return
	}

	// start leader election
	// TODO: s.cfg.Name -> address
	s.election, err = election.NewElection(ctx, s.etcdClient, electionTTL, electionKey, s.cfg.Name, s.cfg.AdvertiseAddr, getLeaderBlockTime)
	if err != nil {
		return
	}

	s.closed.Store(false) // the server started now.

	s.bgFunWg.Add(1)
	go func() {
		defer s.bgFunWg.Done()
		s.ap.Start(ctx)
	}()

	s.bgFunWg.Add(1)
	go func() {
		defer s.bgFunWg.Done()
		s.electionNotify(ctx)
	}()

	runBackgroundOnce.Do(func() {
		s.bgFunWg.Add(1)
		go func() {
			defer s.bgFunWg.Done()
			metrics.RunBackgroundJob(ctx)
		}()
	})

	failpoint.Inject("FailToElect", func(val failpoint.Value) {
		masterStrings := val.(string)
		if strings.Contains(masterStrings, s.cfg.Name) {
			log.L().Info("master election failed", zap.String("failpoint", "FailToElect"))
			s.election.Close()
		}
	})

	log.L().Info("listening gRPC API and status request", zap.String("address", s.cfg.MasterAddr))
	return nil
}

// Close close the RPC server, this function can be called multiple times.
func (s *Server) Close() {
	if s.closed.Load() {
		return
	}
	log.L().Info("closing server")
	defer func() {
		log.L().Info("server closed")
	}()

	// wait for background functions returned
	s.bgFunWg.Wait()

	s.Lock()
	defer s.Unlock()

	if s.election != nil {
		s.election.Close()
	}

	if s.etcdClient != nil {
		s.etcdClient.Close()
	}

	// close the etcd and other attached servers
	if s.etcd != nil {
		s.etcd.Close()
	}
	s.closed.Store(true)
}

func errorCommonWorkerResponse(msg string, source, worker string) *pb.CommonWorkerResponse {
	return &pb.CommonWorkerResponse{
		Result: false,
		Msg:    msg,
		Source: source,
		Worker: worker,
	}
}

// RegisterWorker registers the worker to the master, and all the worker will be store in the path:
// key:   /dm-worker/r/name
// value: workerInfo
func (s *Server) RegisterWorker(ctx context.Context, req *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	var (
		resp2 *pb.RegisterWorkerResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	err := s.scheduler.AddWorker(req.Name, req.Address)
	if err != nil {
		// nolint:nilerr
		return &pb.RegisterWorkerResponse{
			Result: false,
			Msg:    err.Error(),
		}, nil
	}
	log.L().Info("register worker successfully", zap.String("name", req.Name), zap.String("address", req.Address))
	return &pb.RegisterWorkerResponse{
		Result: true,
	}, nil
}

// OfflineMember removes info of the master/worker which has been Closed
// all the masters are store in etcd member list
// all the workers are store in the path:
// key:   /dm-worker/r
// value: WorkerInfo
func (s *Server) OfflineMember(ctx context.Context, req *pb.OfflineMemberRequest) (*pb.OfflineMemberResponse, error) {
	var (
		resp2 *pb.OfflineMemberResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	switch req.Type {
	case ctlcommon.Worker:
		err := s.scheduler.RemoveWorker(req.Name)
		if err != nil {
			// nolint:nilerr
			return &pb.OfflineMemberResponse{
				Result: false,
				Msg:    err.Error(),
			}, nil
		}
	case ctlcommon.Master:
		err := s.deleteMasterByName(ctx, req.Name)
		if err != nil {
			// nolint:nilerr
			return &pb.OfflineMemberResponse{
				Result: false,
				Msg:    err.Error(),
			}, nil
		}
	default:
		return &pb.OfflineMemberResponse{
			Result: false,
			Msg:    terror.ErrMasterInvalidOfflineType.Generate(req.Type).Error(),
		}, nil
	}
	log.L().Info("offline member successfully", zap.String("type", req.Type), zap.String("name", req.Name))
	return &pb.OfflineMemberResponse{
		Result: true,
	}, nil
}

func (s *Server) deleteMasterByName(ctx context.Context, name string) error {
	cli := s.etcdClient
	// Get etcd ID by name.
	var id uint64
	listResp, err := etcdutil.ListMembers(cli)
	if err != nil {
		return err
	}
	for _, m := range listResp.Members {
		if name == m.Name {
			id = m.ID
			break
		}
	}
	if id == 0 {
		return terror.ErrMasterMasterNameNotExist.Generate(name)
	}

	_, err = s.election.ClearSessionIfNeeded(ctx, name)
	if err != nil {
		return err
	}

	_, err = etcdutil.RemoveMember(cli, id)
	return err
}

func subtaskCfgPointersToInstances(stCfgPointers ...*config.SubTaskConfig) []config.SubTaskConfig {
	stCfgs := make([]config.SubTaskConfig, 0, len(stCfgPointers))
	for _, stCfg := range stCfgPointers {
		stCfgs = append(stCfgs, *stCfg)
	}
	return stCfgs
}

func (s *Server) initClusterID(ctx context.Context) error {
	log.L().Info("init cluster id begin")
	ctx1, cancel := context.WithTimeout(ctx, etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := s.etcdClient.Get(ctx1, dmcommon.ClusterIDKey)
	if err != nil {
		return err
	}

	// New cluster, generate a cluster id and backfill it to etcd
	if len(resp.Kvs) == 0 {
		ts := uint64(time.Now().Unix())
		clusterID := (ts << 32) + uint64(rand.Uint32())
		clusterIDBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(clusterIDBytes, clusterID)
		_, err = s.etcdClient.Put(ctx1, dmcommon.ClusterIDKey, string(clusterIDBytes))
		if err != nil {
			return err
		}
		s.clusterID.Store(clusterID)
		log.L().Info("generate and init cluster id success", zap.Uint64("cluster_id", s.clusterID.Load()))
		return nil
	}

	if len(resp.Kvs[0].Value) != 8 {
		return terror.ErrMasterInvalidClusterID.Generate(resp.Kvs[0].Value)
	}

	s.clusterID.Store(binary.BigEndian.Uint64(resp.Kvs[0].Value))
	log.L().Info("init cluster id success", zap.Uint64("cluster_id", s.clusterID.Load()))
	return nil
}

// ClusterID return correct cluster id when as leader.
func (s *Server) ClusterID() uint64 {
	return s.clusterID.Load()
}

// StartTask implements MasterServer.StartTask.
func (s *Server) StartTask(ctx context.Context, req *pb.StartTaskRequest) (*pb.StartTaskResponse, error) {
	var (
		resp2 *pb.StartTaskResponse
		err2  error
	)
	failpoint.Inject("LongRPCResponse", func() {
		var b strings.Builder
		size := 5 * 1024 * 1024
		b.Grow(size)
		for i := 0; i < size; i++ {
			b.WriteByte(0)
		}
		resp2 = &pb.StartTaskResponse{Msg: b.String()}
		failpoint.Return(resp2, nil)
	})

	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	resp := &pb.StartTaskResponse{}
	respWithErr := func(err error) (*pb.StartTaskResponse, error) {
		resp.Msg += err.Error()
		// nolint:nilerr
		return resp, nil
	}

	cliArgs := config.TaskCliArgs{
		StartTime: req.StartTime,
	}
	if err := cliArgs.Verify(); err != nil {
		return respWithErr(err)
	}

	cfg, stCfgs, err := s.generateSubTask(ctx, req.Task, &cliArgs)
	if err != nil {
		return respWithErr(err)
	}
	stCfgsForCheck, err := s.generateSubTasksForCheck(stCfgs)
	if err != nil {
		return respWithErr(err)
	}
	msg, err := checker.CheckSyncConfigFunc(ctx, stCfgsForCheck, ctlcommon.DefaultErrorCnt, ctlcommon.DefaultWarnCnt)
	if err != nil {
		resp.CheckResult = terror.WithClass(err, terror.ClassDMMaster).Error()
		return resp, nil
	}
	resp.CheckResult = msg

	log.L().Info("", zap.String("task name", cfg.Name), zap.String("task", cfg.JSON()), zap.String("request", "StartTask"))

	sourceRespCh := make(chan *pb.CommonWorkerResponse, len(stCfgs))
	if len(req.Sources) > 0 {
		// specify only start task on partial sources
		sourceCfg := make(map[string]*config.SubTaskConfig, len(stCfgs))
		for _, stCfg := range stCfgs {
			sourceCfg[stCfg.SourceID] = stCfg
		}
		stCfgs = make([]*config.SubTaskConfig, 0, len(req.Sources))
		for _, source := range req.Sources {
			if stCfg, ok := sourceCfg[source]; ok {
				stCfgs = append(stCfgs, stCfg)
			} else {
				sourceRespCh <- errorCommonWorkerResponse("source not found in task's config", source, "")
			}
		}
	}

	var sourceResps []*pb.CommonWorkerResponse
	// there are invalid sourceCfgs
	if len(sourceRespCh) > 0 {
		sourceResps = sortCommonWorkerResults(sourceRespCh)
	} else {
		sources := make([]string, 0, len(stCfgs))
		for _, stCfg := range stCfgs {
			sources = append(sources, stCfg.SourceID)
		}

		var (
			latched = false
			release scheduler.ReleaseFunc
			err3    error
		)

		if req.RemoveMeta {
			// TODO: Remove lightning checkpoint and meta.
			// use same latch for remove-meta and start-task
			release, err3 = s.scheduler.AcquireSubtaskLatch(cfg.Name)
			if err3 != nil {
				return respWithErr(terror.ErrSchedulerLatchInUse.Generate("RemoveMeta", cfg.Name))
			}
			defer release()
			latched = true

			if scm := s.scheduler.GetSubTaskCfgsByTask(cfg.Name); len(scm) > 0 {
				return respWithErr(terror.Annotate(terror.ErrSchedulerSubTaskExist.Generate(cfg.Name, sources),
					"while remove-meta is true"))
			}
			err = s.removeMetaData(ctx, cfg.Name, cfg.MetaSchema, cfg.TargetDB)
			if err != nil {
				return respWithErr(terror.Annotate(err, "while removing metadata"))
			}
		}

		if req.StartTime == "" {
			err = ha.DeleteAllTaskCliArgs(s.etcdClient, cfg.Name)
			if err != nil {
				return respWithErr(terror.Annotate(err, "while removing task command line arguments"))
			}
		} else {
			err = ha.PutTaskCliArgs(s.etcdClient, cfg.Name, sources, cliArgs)
			if err != nil {
				return respWithErr(terror.Annotate(err, "while putting task command line arguments"))
			}
		}
		err = s.scheduler.AddSubTasks(latched, pb.Stage_Running, subtaskCfgPointersToInstances(stCfgs...)...)
		if err != nil {
			return respWithErr(err)
		}

		if release != nil {
			release()
		}

		go s.scheduler.TryResolveLoadTask(sources)

		resp.Result = true
		if cfg.RemoveMeta {
			resp.Msg = "`remove-meta` in task config is deprecated, please use `start-task ... --remove-meta` instead"
		}
		sourceResps = s.getSourceRespsAfterOperation(ctx, cfg.Name, sources, []string{}, req)
	}

	resp.Sources = sourceResps
	return resp, nil
}

// OperateTask implements MasterServer.OperateTask.
func (s *Server) OperateTask(ctx context.Context, req *pb.OperateTaskRequest) (*pb.OperateTaskResponse, error) {
	var (
		resp2 *pb.OperateTaskResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	resp := &pb.OperateTaskResponse{
		Op:     req.Op,
		Result: false,
	}

	sources := req.Sources
	if len(req.Sources) == 0 {
		sources = s.getTaskSourceNameList(req.Name)
	}
	if len(sources) == 0 {
		resp.Msg = fmt.Sprintf("task %s has no source or not exist, please check the task name and status", req.Name)
		return resp, nil
	}
	var expect pb.Stage
	switch req.Op {
	case pb.TaskOp_Pause:
		expect = pb.Stage_Paused
	case pb.TaskOp_Resume:
		expect = pb.Stage_Running
	case pb.TaskOp_Delete:
		// op_delete means delete this running subtask, we not have the expected stage now
	default:
		resp.Msg = terror.ErrMasterInvalidOperateOp.Generate(req.Op.String(), "task").Error()
		return resp, nil
	}
	var err error
	if req.Op == pb.TaskOp_Delete {
		err = s.scheduler.RemoveSubTasks(req.Name, sources...)
	} else {
		err = s.scheduler.UpdateExpectSubTaskStage(expect, req.Name, sources...)
	}
	if err != nil {
		resp.Msg = err.Error()
		// nolint:nilerr
		return resp, nil
	}

	resp.Result = true
	resp.Sources = s.getSourceRespsAfterOperation(ctx, req.Name, sources, []string{}, req)

	if req.Op == pb.TaskOp_Delete {
		// delete meta data for optimist
		if len(req.Sources) == 0 {
			err2 = s.optimist.RemoveMetaDataWithTask(req.Name)
		} else {
			err2 = s.optimist.RemoveMetaDataWithTaskAndSources(req.Name, sources...)
		}
		if err2 != nil {
			log.L().Error("failed to delete metadata for task", zap.String("task name", req.Name), log.ShortError(err2))
		}
	}
	return resp, nil
}

// GetSubTaskCfg implements MasterServer.GetSubTaskCfg.
func (s *Server) GetSubTaskCfg(ctx context.Context, req *pb.GetSubTaskCfgRequest) (*pb.GetSubTaskCfgResponse, error) {
	var (
		resp2 *pb.GetSubTaskCfgResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	subCfgs := s.scheduler.GetSubTaskCfgsByTask(req.Name)
	if len(subCfgs) == 0 {
		return &pb.GetSubTaskCfgResponse{
			Result: false,
			Msg:    "task not found",
		}, nil
	}

	cfgs := make([]string, 0, len(subCfgs))

	for _, cfg := range subCfgs {
		cfgBytes, err := cfg.Toml()
		if err != nil {
			// nolint:nilerr
			return &pb.GetSubTaskCfgResponse{
				Result: false,
				Msg:    err.Error(),
			}, nil
		}
		cfgs = append(cfgs, cfgBytes)
	}

	return &pb.GetSubTaskCfgResponse{
		Result: true,
		Cfgs:   cfgs,
	}, nil
}

// UpdateTask implements MasterServer.UpdateTask
// TODO: support update task later.
func (s *Server) UpdateTask(ctx context.Context, req *pb.UpdateTaskRequest) (*pb.UpdateTaskResponse, error) {
	var (
		resp2 *pb.UpdateTaskResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	cfg, stCfgs, err := s.generateSubTask(ctx, req.Task, nil)
	resp := &pb.UpdateTaskResponse{}
	if err != nil {
		resp.Msg = err.Error()
		// nolint:nilerr
		return resp, nil
	}
	stCfgsForCheck, err := s.generateSubTasksForCheck(stCfgs)
	if err != nil {
		resp.Msg = err.Error()
		// nolint:nilerr
		return resp, nil
	}

	msg, err := checker.CheckSyncConfigFunc(ctx, stCfgsForCheck, ctlcommon.DefaultErrorCnt, ctlcommon.DefaultWarnCnt)
	if err != nil {
		resp.CheckResult = terror.WithClass(err, terror.ClassDMMaster).Error()
		return resp, nil
	}
	resp.CheckResult = msg
	log.L().Info("update task", zap.String("task name", cfg.Name), zap.Stringer("task", cfg))

	workerRespCh := make(chan *pb.CommonWorkerResponse, len(stCfgs)+len(req.Sources))
	if len(req.Sources) > 0 {
		// specify only update task on partial sources
		// filter sub-task-configs through user specified sources
		// if a source not exist, an error message will return
		subtaskCfgs := make(map[string]*config.SubTaskConfig)
		for _, stCfg := range stCfgs {
			subtaskCfgs[stCfg.SourceID] = stCfg
		}
		stCfgs = make([]*config.SubTaskConfig, 0, len(req.Sources))
		for _, source := range req.Sources {
			if sourceCfg, ok := subtaskCfgs[source]; ok {
				stCfgs = append(stCfgs, sourceCfg)
			} else {
				workerRespCh <- errorCommonWorkerResponse("source not found in task's config", source, "")
			}
		}
	}

	// TODO: update task config
	// s.scheduler.UpdateTaskCfg(*cfg)

	workerRespMap := make(map[string]*pb.CommonWorkerResponse, len(stCfgs))
	workers := make([]string, 0, len(stCfgs))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerRespMap[workerResp.Source] = workerResp
		workers = append(workers, workerResp.Source)
	}

	sort.Strings(workers)
	workerResps := make([]*pb.CommonWorkerResponse, 0, len(workers))
	for _, worker := range workers {
		workerResps = append(workerResps, workerRespMap[worker])
	}

	resp.Result = true
	resp.Sources = workerResps
	return resp, nil
}

type hasWokers interface {
	GetSources() []string
	GetName() string
}

func extractSources(s *Server, req hasWokers) (sources []string, specifiedSource bool, err error) {
	switch {
	case len(req.GetSources()) > 0:
		sources = req.GetSources()
		var invalidSource []string
		for _, source := range sources {
			if s.scheduler.GetSourceCfgByID(source) == nil {
				invalidSource = append(invalidSource, source)
			}
		}
		if len(invalidSource) > 0 {
			return nil, false, errors.Errorf("sources %s haven't been added", invalidSource)
		}
		specifiedSource = true
	case len(req.GetName()) > 0:
		// query specified task's sources
		sources = s.getTaskSourceNameList(req.GetName())
		if len(sources) == 0 {
			return nil, false, errors.Errorf("task %s has no source or not exist", req.GetName())
		}
	default:
		// query all sources
		log.L().Info("get sources")
		sources = s.scheduler.BoundSources()
	}
	return
}

// QueryStatus implements MasterServer.QueryStatus.
func (s *Server) QueryStatus(ctx context.Context, req *pb.QueryStatusListRequest) (*pb.QueryStatusListResponse, error) {
	var (
		resp2 *pb.QueryStatusListResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}
	sources, specifiedSource, err := extractSources(s, req)
	if err != nil {
		// nolint:nilerr
		return &pb.QueryStatusListResponse{
			Result: false,
			Msg:    err.Error(),
		}, nil
	}
	resps := s.getStatusFromWorkers(ctx, sources, req.Name, specifiedSource)
	workerRespMap := make(map[string][]*pb.QueryStatusResponse, len(sources)) // sourceName -> worker QueryStatusResponse
	inSlice := func(s []string, e string) bool {
		for _, v := range s {
			if v == e {
				return true
			}
		}
		return false
	}
	for _, workerResp := range resps {
		workerRespMap[workerResp.SourceStatus.Source] = append(workerRespMap[workerResp.SourceStatus.Source], workerResp)
		// append some offline worker responses
		if !inSlice(sources, workerResp.SourceStatus.Source) {
			sources = append(sources, workerResp.SourceStatus.Source)
		}
	}
	workerResps := make([]*pb.QueryStatusResponse, 0)
	sort.Strings(sources) // display status sorted by source name
	for _, sourceName := range sources {
		workerResps = append(workerResps, workerRespMap[sourceName]...)
	}
	return &pb.QueryStatusListResponse{Result: true, Sources: workerResps}, nil
}

// adjust unsynced field in sync status by looking at DDL locks.
// because if a DM-worker doesn't receive any shard DDL, it doesn't even know it's unsynced for itself.
func (s *Server) fillUnsyncedStatus(resps []*pb.QueryStatusResponse) {
	for _, resp := range resps {
		for _, subtaskStatus := range resp.SubTaskStatus {
			syncStatus := subtaskStatus.GetSync()
			if syncStatus == nil || len(syncStatus.UnresolvedGroups) != 0 {
				continue
			}
			// TODO: look at s.optimist when `query-status` support show `UnresolvedGroups` in optimistic mode.
			locks := s.pessimist.ShowLocks(subtaskStatus.Name, []string{resp.SourceStatus.Source})
			if len(locks) == 0 {
				continue
			}

			for _, l := range locks {
				db, table := utils.ExtractDBAndTableFromLockID(l.ID)
				syncStatus.UnresolvedGroups = append(syncStatus.UnresolvedGroups, &pb.ShardingGroup{
					Target:   dbutil.TableName(db, table),
					Unsynced: []string{"this DM-worker doesn't receive any shard DDL of this group"},
				})
			}
		}
	}
}

// ShowDDLLocks implements MasterServer.ShowDDLLocks.
func (s *Server) ShowDDLLocks(ctx context.Context, req *pb.ShowDDLLocksRequest) (*pb.ShowDDLLocksResponse, error) {
	var (
		resp2 *pb.ShowDDLLocksResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	resp := &pb.ShowDDLLocksResponse{
		Result: true,
	}

	// show pessimistic locks.
	resp.Locks = append(resp.Locks, s.pessimist.ShowLocks(req.Task, req.Sources)...)
	// show optimistic locks.
	locks, err := s.optimist.ShowLocks(req.Task, req.Sources)
	resp.Locks = append(resp.Locks, locks...)

	if len(resp.Locks) == 0 {
		resp.Msg = "no DDL lock exists"
	} else if err != nil {
		resp.Msg = fmt.Sprintf("may lost owner and ddls info for optimistic locks, err: %s", err)
	}
	return resp, nil
}

// UnlockDDLLock implements MasterServer.UnlockDDLLock
// TODO(csuzhangxc): implement this later.
func (s *Server) UnlockDDLLock(ctx context.Context, req *pb.UnlockDDLLockRequest) (*pb.UnlockDDLLockResponse, error) {
	var (
		resp2 *pb.UnlockDDLLockResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	resp := &pb.UnlockDDLLockResponse{}

	task := utils.ExtractTaskFromLockID(req.ID)
	if task == "" {
		resp.Msg = "can't find task name from lock-ID"
		return resp, nil
	}
	subtasks := s.scheduler.GetSubTaskCfgsByTask(task)
	unlockType := config.ShardPessimistic
	if len(subtasks) > 0 {
		// subtasks should have same ShardMode
		for _, subtask := range subtasks {
			if subtask.ShardMode == config.ShardOptimistic {
				unlockType = config.ShardOptimistic
			}
		}
	} else {
		// task is deleted so worker is not watching etcd, automatically set --force-remove
		req.ForceRemove = true
	}

	var err error
	switch unlockType {
	case config.ShardPessimistic:
		err = s.pessimist.UnlockLock(ctx, req.ID, req.ReplaceOwner, req.ForceRemove)
	case config.ShardOptimistic:
		if len(req.Sources) != 1 {
			resp.Msg = "optimistic locks should have only one source"
			return resp, nil
		}
		err = s.optimist.UnlockLock(ctx, req.ID, req.Sources[0], req.Database, req.Table, req.Op)
	}
	if err != nil {
		resp.Msg = err.Error()
	} else {
		resp.Result = true
	}

	return resp, nil
}

// PurgeWorkerRelay implements MasterServer.PurgeWorkerRelay.
func (s *Server) PurgeWorkerRelay(ctx context.Context, req *pb.PurgeWorkerRelayRequest) (*pb.PurgeWorkerRelayResponse, error) {
	var (
		resp2 *pb.PurgeWorkerRelayResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	workerReq := &workerrpc.Request{
		Type: workerrpc.CmdPurgeRelay,
		PurgeRelay: &pb.PurgeRelayRequest{
			Inactive: req.Inactive,
			Time:     req.Time,
			Filename: req.Filename,
			SubDir:   req.SubDir,
		},
	}

	var (
		workerResps  = make([]*pb.CommonWorkerResponse, 0, len(req.Sources))
		workerRespMu sync.Mutex
	)
	setWorkerResp := func(resp *pb.CommonWorkerResponse) {
		workerRespMu.Lock()
		workerResps = append(workerResps, resp)
		workerRespMu.Unlock()
	}

	var wg sync.WaitGroup
	for _, source := range req.Sources {
		var (
			workers       []*scheduler.Worker
			workerNameSet = make(map[string]struct{})
			err           error
		)

		workers, err = s.scheduler.GetRelayWorkers(source)
		if err != nil {
			return nil, err
		}
		// returned workers is not duplicated
		for _, w := range workers {
			workerNameSet[w.BaseInfo().Name] = struct{}{}
		}
		// subtask workers may have been found in relay workers
		taskWorker := s.scheduler.GetWorkerBySource(source)
		if taskWorker != nil {
			if _, ok := workerNameSet[taskWorker.BaseInfo().Name]; !ok {
				workers = append(workers, taskWorker)
			}
		}

		if len(workers) == 0 {
			setWorkerResp(errorCommonWorkerResponse(fmt.Sprintf("relay worker for source %s not found, please `start-relay` first", source), source, ""))
			continue
		}
		for _, worker := range workers {
			if worker == nil {
				setWorkerResp(errorCommonWorkerResponse(fmt.Sprintf("relay worker instance for source %s not found, please `start-relay` first", source), source, ""))
				continue
			}
			wg.Add(1)
			go func(worker *scheduler.Worker, source string) {
				defer wg.Done()
				var workerResp *pb.CommonWorkerResponse
				resp, err3 := worker.SendRequest(ctx, workerReq, s.cfg.RPCTimeout)
				if err3 != nil {
					workerResp = errorCommonWorkerResponse(err3.Error(), source, worker.BaseInfo().Name)
				} else {
					workerResp = resp.PurgeRelay
				}
				workerResp.Source = source
				setWorkerResp(workerResp)
			}(worker, source)
		}
	}
	wg.Wait()

	workerRespMap := make(map[string][]*pb.CommonWorkerResponse, len(req.Sources))
	for _, workerResp := range workerResps {
		workerRespMap[workerResp.Source] = append(workerRespMap[workerResp.Source], workerResp)
	}

	sort.Strings(req.Sources)
	returnResps := make([]*pb.CommonWorkerResponse, 0, len(req.Sources))
	for _, worker := range req.Sources {
		returnResps = append(returnResps, workerRespMap[worker]...)
	}

	return &pb.PurgeWorkerRelayResponse{
		Result:  true,
		Sources: returnResps,
	}, nil
}

// OperateWorkerRelayTask implements MasterServer.OperateWorkerRelayTask.
func (s *Server) OperateWorkerRelayTask(ctx context.Context, req *pb.OperateWorkerRelayRequest) (*pb.OperateWorkerRelayResponse, error) {
	var (
		resp2 *pb.OperateWorkerRelayResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	resp := &pb.OperateWorkerRelayResponse{
		Op:     req.Op,
		Result: false,
	}
	var expect pb.Stage
	switch req.Op {
	case pb.RelayOp_ResumeRelay:
		expect = pb.Stage_Running
	case pb.RelayOp_PauseRelay:
		expect = pb.Stage_Paused
	default:
		resp.Msg = terror.ErrMasterInvalidOperateOp.Generate(req.Op.String(), "relay").Error()
		return resp, nil
	}
	err := s.scheduler.UpdateExpectRelayStage(expect, req.Sources...)
	if err != nil {
		resp.Msg = err.Error()
		// nolint:nilerr
		return resp, nil
	}
	resp.Result = true
	resp.Sources = s.getSourceRespsAfterOperation(ctx, "", req.Sources, []string{}, req)
	return resp, nil
}

// getTaskSourceNameList gets workers relevant to specified task.
func (s *Server) getTaskSourceNameList(taskName string) []string {
	s.Lock()
	defer s.Unlock()
	cfgM := s.scheduler.GetSubTaskCfgsByTask(taskName)
	// do a copy
	ret := make([]string, 0, len(cfgM))
	for source := range cfgM {
		ret = append(ret, source)
	}
	return ret
}

// getStatusFromWorkers does RPC request to get status from dm-workers.
func (s *Server) getStatusFromWorkers(
	ctx context.Context, sources []string, taskName string, specifiedSource bool,
) []*pb.QueryStatusResponse {
	workerReq := &workerrpc.Request{
		Type:        workerrpc.CmdQueryStatus,
		QueryStatus: &pb.QueryStatusRequest{Name: taskName},
	}
	var (
		workerResps  = make([]*pb.QueryStatusResponse, 0, len(sources))
		workerRespMu sync.Mutex
	)
	setWorkerResp := func(resp *pb.QueryStatusResponse) {
		workerRespMu.Lock()
		workerResps = append(workerResps, resp)
		workerRespMu.Unlock()
	}

	handleErr := func(err error, source string, worker string) {
		log.L().Error("response error", zap.Error(err))
		resp := &pb.QueryStatusResponse{
			Result: false,
			Msg:    err.Error(),
			SourceStatus: &pb.SourceStatus{
				Source: source,
			},
		}
		if worker != "" {
			resp.SourceStatus.Worker = worker
		}
		setWorkerResp(resp)
	}

	var wg sync.WaitGroup
	for _, source := range sources {
		var (
			workers       []*scheduler.Worker
			workerNameSet = make(map[string]struct{})
			err2          error
		)
		// if user specified sources, query relay workers instead of task workers
		if specifiedSource {
			workers, err2 = s.scheduler.GetRelayWorkers(source)
			if err2 != nil {
				handleErr(err2, source, "")
				continue
			}
			// returned workers is not duplicated
			for _, w := range workers {
				workerNameSet[w.BaseInfo().Name] = struct{}{}
			}
		}

		// subtask workers may have been found in relay workers
		if taskWorker := s.scheduler.GetWorkerBySource(source); taskWorker != nil {
			if _, ok := workerNameSet[taskWorker.BaseInfo().Name]; !ok {
				workers = append(workers, taskWorker)
			}
		}

		if len(workers) == 0 {
			err := terror.ErrMasterWorkerArgsExtractor.Generatef("%s relevant worker-client not found", source)
			handleErr(err, source, "")
			continue
		}

		for _, worker := range workers {
			wg.Add(1)
			go s.ap.Emit(ctx, 0, func(args ...interface{}) {
				defer wg.Done()
				sourceID := args[0].(string)
				w, _ := args[1].(*scheduler.Worker)

				var workerStatus *pb.QueryStatusResponse
				resp, err := w.SendRequest(ctx, workerReq, s.cfg.RPCTimeout)
				if err != nil {
					workerStatus = &pb.QueryStatusResponse{
						Result:       false,
						Msg:          err.Error(),
						SourceStatus: &pb.SourceStatus{},
					}
				} else {
					workerStatus = resp.QueryStatus
				}
				workerStatus.SourceStatus.Source = sourceID
				setWorkerResp(workerStatus)
			}, func(args ...interface{}) {
				defer wg.Done()
				sourceID, _ := args[0].(string)
				w, _ := args[1].(*scheduler.Worker)
				workerName := ""
				if w != nil {
					workerName = w.BaseInfo().Name
				}
				handleErr(terror.ErrMasterNoEmitToken.Generate(sourceID), sourceID, workerName)
			}, source, worker)
		}
	}
	wg.Wait()
	s.fillUnsyncedStatus(workerResps)

	// when taskName is empty we need list all task even the worker that handle this task is not running.
	// see more here https://github.com/pingcap/tiflow/issues/3348
	if taskName == "" {
		// sourceName -> resp
		fakeWorkerRespM := make(map[string]*pb.QueryStatusResponse)
		existWorkerRespM := make(map[string]*pb.QueryStatusResponse)
		for _, resp := range workerResps {
			existWorkerRespM[resp.SourceStatus.Source] = resp
		}

		findNeedInResp := func(sourceName, taskName string) bool {
			if _, ok := existWorkerRespM[sourceName]; ok {
				// if we found the source resp, it must have the subtask of this task
				return true
			}
			if fake, ok := fakeWorkerRespM[sourceName]; ok {
				for _, status := range fake.SubTaskStatus {
					if status.Name == taskName {
						return true
					}
				}
			}
			return false
		}

		setOrAppendFakeResp := func(sourceName, taskName string) {
			if _, ok := fakeWorkerRespM[sourceName]; !ok {
				fakeWorkerRespM[sourceName] = &pb.QueryStatusResponse{
					Result:        false,
					SubTaskStatus: []*pb.SubTaskStatus{},
					SourceStatus:  &pb.SourceStatus{Source: sourceName, Worker: "source not bound"},
					Msg:           fmt.Sprintf("can't find task=%s from dm-worker for source=%s, please use dmctl list-member to check if worker is offline.", taskName, sourceName),
				}
			}
			fakeWorkerRespM[sourceName].SubTaskStatus = append(fakeWorkerRespM[sourceName].SubTaskStatus, &pb.SubTaskStatus{Name: taskName})
		}

		for taskName, sourceM := range s.scheduler.GetSubTaskCfgs() {
			// only add use specified source related to this task
			if specifiedSource {
				for _, needDisplayedSource := range sources {
					if _, ok := sourceM[needDisplayedSource]; ok && !findNeedInResp(needDisplayedSource, taskName) {
						setOrAppendFakeResp(needDisplayedSource, taskName)
					}
				}
			} else {
				// make fake response for every source related to this task
				for sourceName := range sourceM {
					if !findNeedInResp(sourceName, taskName) {
						setOrAppendFakeResp(sourceName, taskName)
					}
				}
			}
		}

		for _, fake := range fakeWorkerRespM {
			setWorkerResp(fake)
		}
	}
	return workerResps
}

// TODO: refine the call stack of this API, query worker configs that we needed only.
func (s *Server) getSourceConfigs(sources []string) map[string]*config.SourceConfig {
	cfgs := make(map[string]*config.SourceConfig)
	for _, source := range sources {
		if cfg := s.scheduler.GetSourceCfgByID(source); cfg != nil {
			// check the password
			cfg.DecryptPassword()
			cfgs[source] = cfg
		}
	}
	return cfgs
}

// CheckTask checks legality of task configuration.
func (s *Server) CheckTask(ctx context.Context, req *pb.CheckTaskRequest) (*pb.CheckTaskResponse, error) {
	var (
		resp2 *pb.CheckTaskResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	cliArgs := config.TaskCliArgs{
		StartTime: req.StartTime,
	}
	if err := cliArgs.Verify(); err != nil {
		// nolint:nilerr
		return &pb.CheckTaskResponse{
			Result: false,
			Msg:    err.Error(),
		}, nil
	}
	resp := &pb.CheckTaskResponse{}
	_, stCfgs, err := s.generateSubTask(ctx, req.Task, &cliArgs)
	if err != nil {
		resp.Msg = err.Error()
		// nolint:nilerr
		return resp, nil
	}
	stCfgsForCheck, err := s.generateSubTasksForCheck(stCfgs)
	if err != nil {
		resp.Msg = err.Error()
		// nolint:nilerr
		return resp, nil
	}

	msg, err := checker.CheckSyncConfigFunc(ctx, stCfgsForCheck, req.ErrCnt, req.WarnCnt)
	if err != nil {
		resp.Msg = terror.WithClass(err, terror.ClassDMMaster).Error()
		return resp, nil
	}
	resp.Msg = msg
	resp.Result = true

	return resp, nil
}

func parseAndAdjustSourceConfig(ctx context.Context, contents []string) ([]*config.SourceConfig, error) {
	cfgs := make([]*config.SourceConfig, len(contents))
	for i, content := range contents {
		cfg, err := config.ParseYaml(content)
		if err != nil {
			return cfgs, err
		}
		if err := checkAndAdjustSourceConfigForDMCtlFunc(ctx, cfg); err != nil {
			return cfgs, err
		}
		cfgs[i] = cfg
	}
	return cfgs, nil
}

func innerCheckAndAdjustSourceConfig(
	ctx context.Context,
	cfg *config.SourceConfig,
	hook func(sourceConfig *config.SourceConfig, ctx context.Context, db *conn.BaseDB) error,
) error {
	dbConfig := cfg.GenerateDBConfig()
	fromDB, err := conn.GetUpstreamDB(dbConfig)
	if err != nil {
		return err
	}
	defer fromDB.Close()
	if err = cfg.Adjust(ctx, fromDB); err != nil {
		return err
	}
	if hook != nil {
		if err = hook(cfg, ctx, fromDB); err != nil {
			return err
		}
	}
	if _, err = cfg.Yaml(); err != nil {
		return err
	}
	return cfg.Verify()
}

func checkAndAdjustSourceConfig(ctx context.Context, cfg *config.SourceConfig) error {
	return innerCheckAndAdjustSourceConfig(ctx, cfg, (*config.SourceConfig).AdjustCaseSensitive)
}

func checkAndAdjustSourceConfigForDMCtl(ctx context.Context, cfg *config.SourceConfig) error {
	return innerCheckAndAdjustSourceConfig(ctx, cfg, nil)
}

func parseSourceConfig(contents []string) ([]*config.SourceConfig, error) {
	cfgs := make([]*config.SourceConfig, len(contents))
	for i, content := range contents {
		cfg, err := config.ParseYaml(content)
		if err != nil {
			return cfgs, err
		}
		cfgs[i] = cfg
	}
	return cfgs, nil
}

// GetLatestMeta gets newest meta(binlog name, pos, gtid) from upstream.
func GetLatestMeta(ctx context.Context, flavor string, dbConfig *dbconfig.DBConfig) (*config.Meta, error) {
	cfg := *dbConfig
	if len(cfg.Password) > 0 {
		cfg.Password = utils.DecryptOrPlaintext(cfg.Password)
	}

	fromDB, err := conn.GetUpstreamDB(&cfg)
	if err != nil {
		return nil, err
	}
	defer fromDB.Close()

	pos, gtidSet, err := conn.GetPosAndGs(tcontext.NewContext(ctx, log.L()), fromDB, flavor)
	if err != nil {
		return nil, err
	}

	gSet := ""
	if gtidSet != nil {
		gSet = gtidSet.String()
	}
	return &config.Meta{BinLogName: pos.Name, BinLogPos: pos.Pos, BinLogGTID: gSet}, nil
}

func AdjustTargetDB(ctx context.Context, dbConfig *dbconfig.DBConfig) error {
	cfg := *dbConfig
	if len(cfg.Password) > 0 {
		cfg.Password = utils.DecryptOrPlaintext(cfg.Password)
	}

	failpoint.Inject("MockSkipAdjustTargetDB", func() {
		failpoint.Return(nil)
	})

	toDB, err := conn.GetDownstreamDB(&cfg)
	if err != nil {
		return err
	}
	defer toDB.Close()

	value, err := dbutil.ShowVersion(ctx, toDB.DB)
	if err != nil {
		return err
	}

	version, err := conn.ExtractTiDBVersion(value)
	// Do not adjust if not TiDB
	if err == nil {
		config.AdjustTargetDBSessionCfg(dbConfig, version)
	} else {
		log.L().Warn("get tidb version", log.ShortError(err))
	}
	return nil
}

// OperateSource will create or update an upstream source.
func (s *Server) OperateSource(ctx context.Context, req *pb.OperateSourceRequest) (*pb.OperateSourceResponse, error) {
	var (
		resp2 *pb.OperateSourceResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	var (
		cfgs []*config.SourceConfig
		err  error
		resp = &pb.OperateSourceResponse{
			Result: false,
		}
	)
	switch req.Op {
	case pb.SourceOp_StartSource, pb.SourceOp_UpdateSource:
		cfgs, err = parseAndAdjustSourceConfig(ctx, req.Config)
	default:
		// don't check the upstream connections, because upstream may be inaccessible
		cfgs, err = parseSourceConfig(req.Config)
	}
	if err != nil {
		resp.Msg = err.Error()
		// nolint:nilerr
		return resp, nil
	}

	// boundM: sourceID -> worker are used to query status from worker, to return a more real status
	boundM := map[string]*scheduler.Worker{}

	switch req.Op {
	case pb.SourceOp_StartSource:
		var (
			started  []string
			hasError = false
			err      error
		)
		for _, cfg := range cfgs {
			// add source with worker when specify a worker name
			if req.WorkerName != "" {
				err = s.scheduler.AddSourceCfgWithWorker(cfg, req.WorkerName)
			} else {
				err = s.scheduler.AddSourceCfg(cfg)
			}
			// return first error and try to revert, so user could copy-paste same start command after error
			if err != nil {
				resp.Msg = err.Error()
				hasError = true
				break
			}
			started = append(started, cfg.SourceID)
		}

		if hasError {
			log.L().Info("reverting start source", zap.String("error", err.Error()))
			for _, sid := range started {
				err := s.scheduler.RemoveSourceCfg(sid)
				if err != nil {
					log.L().Error("while reverting started source, another error happens",
						zap.String("source id", sid),
						zap.String("error", err.Error()))
				}
			}
			return resp, nil
		}
		// for start source, we should get worker after start source
		for _, sid := range started {
			boundM[sid] = s.scheduler.GetWorkerBySource(sid)
		}
	case pb.SourceOp_UpdateSource:
		// TODO: support SourceOp_UpdateSource later
		resp.Msg = "Update worker config is not supported by dm-ha now"
		return resp, nil
	case pb.SourceOp_StopSource:
		toRemove := make([]string, 0, len(cfgs)+len(req.SourceID))
		toRemove = append(toRemove, req.SourceID...)
		for _, cfg := range cfgs {
			toRemove = append(toRemove, cfg.SourceID)
		}

		for _, sid := range toRemove {
			boundM[sid] = s.scheduler.GetWorkerBySource(sid)
			err3 := s.scheduler.RemoveSourceCfg(sid)
			// TODO(lance6716):
			// user could not copy-paste same command if encounter error halfway:
			// `operate-source stop  correct-id-1     wrong-id-2`
			//                       remove success   some error
			// `operate-source stop  correct-id-1     correct-id-2`
			//                       not exist, error
			// find a way to distinguish this scenario and wrong source id
			// or give a command to show existing source id
			if err3 != nil {
				resp.Msg = err3.Error()
				// nolint:nilerr
				return resp, nil
			}
		}
	case pb.SourceOp_ShowSource:
		for _, id := range req.SourceID {
			boundM[id] = s.scheduler.GetWorkerBySource(id)
		}
		for _, cfg := range cfgs {
			id := cfg.SourceID
			boundM[id] = s.scheduler.GetWorkerBySource(id)
		}

		if len(boundM) == 0 {
			for _, id := range s.scheduler.GetSourceCfgIDs() {
				boundM[id] = s.scheduler.GetWorkerBySource(id)
			}
		}
	default:
		resp.Msg = terror.ErrMasterInvalidOperateOp.Generate(req.Op.String(), "source").Error()
		return resp, nil
	}

	resp.Result = true

	var noWorkerMsg string
	switch req.Op {
	case pb.SourceOp_StartSource, pb.SourceOp_ShowSource:
		noWorkerMsg = "source is added but there is no free worker to bound"
	case pb.SourceOp_StopSource:
		noWorkerMsg = "source is stopped and hasn't bound to worker before being stopped"
	}

	var (
		sourceToCheck []string
		workerToCheck []string
	)

	for id, w := range boundM {
		if w == nil {
			resp.Sources = append(resp.Sources, &pb.CommonWorkerResponse{
				Result: true,
				Msg:    noWorkerMsg,
				Source: id,
			})
		} else {
			sourceToCheck = append(sourceToCheck, id)
			workerToCheck = append(workerToCheck, w.BaseInfo().Name)
		}
	}
	if len(sourceToCheck) > 0 {
		resp.Sources = append(resp.Sources, s.getSourceRespsAfterOperation(ctx, "", sourceToCheck, workerToCheck, req)...)
	}
	return resp, nil
}

// OperateLeader implements MasterServer.OperateLeader
// Note: this request doesn't need to forward to leader.
func (s *Server) OperateLeader(ctx context.Context, req *pb.OperateLeaderRequest) (*pb.OperateLeaderResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "OperateLeader"))

	switch req.Op {
	case pb.LeaderOp_EvictLeaderOp:
		s.election.EvictLeader()
	case pb.LeaderOp_CancelEvictLeaderOp:
		s.election.CancelEvictLeader()
	default:
		return &pb.OperateLeaderResponse{
			Result: false,
			Msg:    fmt.Sprintf("operate %s is not supported", req.Op),
		}, nil
	}

	return &pb.OperateLeaderResponse{
		Result: true,
	}, nil
}

func (s *Server) generateSubTask(
	ctx context.Context,
	task string,
	cliArgs *config.TaskCliArgs,
) (*config.TaskConfig, []*config.SubTaskConfig, error) {
	var err error
	cfg := config.NewTaskConfig()
	// bypass the meta check by set any value. If start-time is specified, DM-worker will not use meta field.
	if cliArgs != nil && cliArgs.StartTime != "" {
		err = cfg.RawDecode(task)
		if err != nil {
			return nil, nil, terror.WithClass(err, terror.ClassDMMaster)
		}
		for _, inst := range cfg.MySQLInstances {
			inst.Meta = &config.Meta{BinLogName: binlog.FakeBinlogName}
		}
		err = cfg.Adjust()
	} else {
		err = cfg.Decode(task)
	}
	if err != nil {
		return nil, nil, terror.WithClass(err, terror.ClassDMMaster)
	}

	err = AdjustTargetDB(ctx, cfg.TargetDB)
	if err != nil {
		return nil, nil, terror.WithClass(err, terror.ClassDMMaster)
	}

	sourceIDs := make([]string, 0, len(cfg.MySQLInstances))
	for _, inst := range cfg.MySQLInstances {
		sourceIDs = append(sourceIDs, inst.SourceID)
	}
	sourceCfgs := s.getSourceConfigs(sourceIDs)
	dbConfigs := make(map[string]dbconfig.DBConfig, len(sourceCfgs))
	for _, sourceCfg := range sourceCfgs {
		dbConfigs[sourceCfg.SourceID] = sourceCfg.From
	}

	if cfg.TaskMode == config.ModeIncrement && (cliArgs == nil || cliArgs.StartTime == "") {
		for _, inst := range cfg.MySQLInstances {
			if inst.Meta == nil {
				sourceCfg := sourceCfgs[inst.SourceID]
				meta, err2 := GetLatestMeta(ctx, sourceCfg.Flavor, &sourceCfg.From)
				if err2 != nil {
					return nil, nil, err2
				}
				inst.Meta = meta
			}
		}
	}

	stCfgs, err := config.TaskConfigToSubTaskConfigs(cfg, dbConfigs)
	if err != nil {
		return nil, nil, terror.WithClass(err, terror.ClassDMMaster)
	}

	var firstMode config.LoadMode
	for _, stCfg := range stCfgs {
		if firstMode == "" {
			firstMode = stCfg.LoaderConfig.ImportMode
		} else if firstMode != stCfg.LoaderConfig.ImportMode {
			return nil, nil, terror.ErrConfigInvalidLoadMode.Generatef("found two import-mode %s and %s in task config, DM only supports one value", firstMode, stCfg.LoaderConfig.ImportMode)
		}
	}
	return cfg, stCfgs, nil
}

func (s *Server) generateSubTasksForCheck(stCfgs []*config.SubTaskConfig) ([]*config.SubTaskConfig, error) {
	sourceIDs := make([]string, 0, len(stCfgs))
	for _, stCfg := range stCfgs {
		sourceIDs = append(sourceIDs, stCfg.SourceID)
	}

	sourceCfgs := s.getSourceConfigs(sourceIDs)
	stCfgsForCheck := make([]*config.SubTaskConfig, 0, len(stCfgs))
	for i, stCfg := range stCfgs {
		stCfgForCheck, err := stCfg.Clone()
		if err != nil {
			return nil, err
		}
		stCfgsForCheck = append(stCfgsForCheck, stCfgForCheck)
		if sourceCfg, ok := sourceCfgs[stCfgForCheck.SourceID]; ok {
			stCfgsForCheck[i].Flavor = sourceCfg.Flavor
			stCfgsForCheck[i].ServerID = sourceCfg.ServerID
			stCfgsForCheck[i].EnableGTID = sourceCfg.EnableGTID

			if sourceCfg.EnableRelay {
				stCfgsForCheck[i].UseRelay = true
				continue // skip the following check
			}
		}
		workers, err := s.scheduler.GetRelayWorkers(stCfgForCheck.SourceID)
		if err != nil {
			return nil, err
		}
		stCfgsForCheck[i].UseRelay = len(workers) > 0
	}
	return stCfgsForCheck, nil
}

func setUseTLS(tlsCfg *security.Security) {
	if enableTLS(tlsCfg) {
		useTLS.Store(true)
	} else {
		useTLS.Store(false)
	}
}

func enableTLS(tlsCfg *security.Security) bool {
	if tlsCfg == nil {
		return false
	}

	if len(tlsCfg.SSLCA) == 0 {
		return false
	}

	return true
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

func (s *Server) removeMetaData(ctx context.Context, taskName, metaSchema string, toDBCfg *dbconfig.DBConfig) error {
	failpoint.Inject("MockSkipRemoveMetaData", func() {
		failpoint.Return(nil)
	})
	toDBCfg.Adjust()

	// clear shard meta data for pessimistic/optimist
	err := s.pessimist.RemoveMetaData(taskName)
	if err != nil {
		return err
	}
	err = s.optimist.RemoveMetaDataWithTask(taskName)
	if err != nil {
		return err
	}
	err = s.scheduler.RemoveLoadTaskAndLightningStatus(taskName)
	if err != nil {
		return err
	}

	// set up db and clear meta data in downstream db
	baseDB, err := conn.GetDownstreamDB(toDBCfg)
	if err != nil {
		return terror.WithScope(err, terror.ScopeDownstream)
	}
	defer baseDB.Close()
	dbConn, err := baseDB.GetBaseConn(ctx)
	if err != nil {
		return terror.WithScope(err, terror.ScopeDownstream)
	}
	defer func() {
		err2 := baseDB.ForceCloseConn(dbConn)
		if err2 != nil {
			log.L().Warn("fail to close connection", zap.Error(err2))
		}
	}()

	ctctx := tcontext.NewContext(ctx, log.With(zap.String("job", "remove metadata")))

	sqls := make([]string, 0, 4)
	// clear loader and syncer checkpoints
	sqls = append(sqls, fmt.Sprintf("DROP TABLE IF EXISTS %s",
		dbutil.TableName(metaSchema, cputil.LoaderCheckpoint(taskName))))
	sqls = append(sqls, fmt.Sprintf("DROP TABLE IF EXISTS %s",
		dbutil.TableName(metaSchema, cputil.LightningCheckpoint(taskName))))
	sqls = append(sqls, fmt.Sprintf("DROP TABLE IF EXISTS %s",
		dbutil.TableName(metaSchema, cputil.SyncerCheckpoint(taskName))))
	sqls = append(sqls, fmt.Sprintf("DROP TABLE IF EXISTS %s",
		dbutil.TableName(metaSchema, cputil.SyncerShardMeta(taskName))))
	sqls = append(sqls, fmt.Sprintf("DROP TABLE IF EXISTS %s",
		dbutil.TableName(metaSchema, cputil.SyncerOnlineDDL(taskName))))
	sqls = append(sqls, fmt.Sprintf("DROP TABLE IF EXISTS %s",
		dbutil.TableName(metaSchema, cputil.ValidatorCheckpoint(taskName))))
	sqls = append(sqls, fmt.Sprintf("DROP TABLE IF EXISTS %s",
		dbutil.TableName(metaSchema, cputil.ValidatorPendingChange(taskName))))
	sqls = append(sqls, fmt.Sprintf("DROP TABLE IF EXISTS %s",
		dbutil.TableName(metaSchema, cputil.ValidatorErrorChange(taskName))))
	sqls = append(sqls, fmt.Sprintf("DROP TABLE IF EXISTS %s",
		dbutil.TableName(metaSchema, cputil.ValidatorTableStatus(taskName))))
	// clear lightning error manager table
	sqls = append(sqls, fmt.Sprintf("DROP DATABASE IF EXISTS %s",
		dbutil.ColumnName(loader.GetTaskInfoSchemaName(metaSchema, taskName))))

	_, err = dbConn.ExecuteSQL(ctctx, nil, taskName, sqls)
	if err == nil {
		metrics.RemoveDDLPending(taskName)
	}
	return err
}

func extractWorkerError(result *pb.ProcessResult) error {
	if result != nil && len(result.Errors) > 0 {
		return terror.ErrMasterOperRespNotSuccess.Generate(unit.JoinProcessErrors(result.Errors))
	}
	return nil
}

// waitOperationOk calls QueryStatus internally to implement a declarative API. It will determine operation is OK by
//
// Source:
//
//	OperateSource:
//	  - StartSource, UpdateSource: sourceID = Source
//	  - StopSource: return resp.Result = false && resp.Msg = “worker has not started”.
//
// Task:
//
//	StartTask, UpdateTask: query status and related subTask stage is running

//	OperateTask:
//	  - pause: related task status is paused
//	  - resume: related task status is running
//	  - stop: related task can't be found in worker's result
//
// Relay:
//
//	OperateRelay:
//	  - start: related relay status is running
//	  - stop: related relay status can't be found in worker's result
//	OperateWorkerRelay:
//	  - pause: related relay status is paused
//	  - resume: related relay status is running
//
// returns OK, error message of QueryStatusResponse, raw QueryStatusResponse, error that not from QueryStatusResponse.
func (s *Server) waitOperationOk(
	ctx context.Context,
	cli *scheduler.Worker,
	taskName string,
	sourceID string,
	masterReq interface{},
) (bool, string, *pb.QueryStatusResponse, error) {
	var expect pb.Stage
	switch req := masterReq.(type) {
	case *pb.OperateSourceRequest:
		switch req.Op {
		case pb.SourceOp_StartSource, pb.SourceOp_UpdateSource, pb.SourceOp_ShowSource:
			expect = pb.Stage_Running
		case pb.SourceOp_StopSource:
			expect = pb.Stage_Stopped
		}
	case *pb.StartTaskRequest, *pb.UpdateTaskRequest:
		expect = pb.Stage_Running
	case *pb.OperateTaskRequest:
		switch req.Op {
		case pb.TaskOp_Resume:
			expect = pb.Stage_Running
		case pb.TaskOp_Pause:
			expect = pb.Stage_Paused
		case pb.TaskOp_Delete:
		}
	case *pb.OperateWorkerRelayRequest:
		switch req.Op {
		case pb.RelayOp_ResumeRelay:
			expect = pb.Stage_Running
		case pb.RelayOp_PauseRelay:
			expect = pb.Stage_Paused
		case pb.RelayOp_StopRelay:
			expect = pb.Stage_Stopped
		}
	case *pb.OperateRelayRequest:
		switch req.Op {
		case pb.RelayOpV2_StartRelayV2:
			expect = pb.Stage_Running
		case pb.RelayOpV2_StopRelayV2:
			expect = pb.Stage_Stopped
		}
	default:
		return false, "", nil, terror.ErrMasterIsNotAsyncRequest.Generate(masterReq)
	}
	req := &workerrpc.Request{
		Type: workerrpc.CmdQueryStatus,
		QueryStatus: &pb.QueryStatusRequest{
			Name: taskName,
		},
	}

	for num := 0; num < maxRetryNum; num++ {
		if num > 0 {
			select {
			case <-ctx.Done():
				return false, "", nil, ctx.Err()
			case <-time.After(retryInterval):
			}
		}

		// check whether source relative worker has been removed by scheduler
		if _, ok := masterReq.(*pb.OperateSourceRequest); ok {
			if expect == pb.Stage_Stopped {
				resp := &pb.QueryStatusResponse{
					Result:       true,
					SourceStatus: &pb.SourceStatus{Source: sourceID, Worker: cli.BaseInfo().Name},
				}
				if w := s.scheduler.GetWorkerByName(cli.BaseInfo().Name); w == nil {
					return true, "", resp, nil
				} else if cli.Stage() == scheduler.WorkerOffline {
					return true, "", resp, nil
				}
			}
		}

		// TODO: this is 30s, too long compared with retryInterval
		resp, err := cli.SendRequest(ctx, req, s.cfg.RPCTimeout)
		if err != nil {
			log.L().Error("fail to query operation",
				zap.Int("retryNum", num),
				zap.String("task", taskName),
				zap.String("source", sourceID),
				zap.Stringer("expect", expect),
				log.ShortError(err))
		} else {
			queryResp := resp.QueryStatus
			if queryResp == nil {
				// should not happen
				return false, "", nil, errors.Errorf("expect a query-status response, got type %v", resp.Type)
			}

			switch masterReq.(type) {
			case *pb.OperateSourceRequest:
				if queryResp.SourceStatus == nil {
					continue
				}
				switch expect {
				case pb.Stage_Running:
					if queryResp.SourceStatus.Source == sourceID {
						msg := ""
						if err2 := extractWorkerError(queryResp.SourceStatus.Result); err2 != nil {
							msg = err2.Error()
						}
						return true, msg, queryResp, nil
					}
				case pb.Stage_Stopped:
					// we don't use queryResp.SourceStatus.Source == "" because worker might be re-arranged after being stopped
					if queryResp.SourceStatus.Source != sourceID {
						msg := ""
						if err2 := extractWorkerError(queryResp.SourceStatus.Result); err2 != nil {
							msg = err2.Error()
						}
						return true, msg, queryResp, nil
					}
				}
			case *pb.StartTaskRequest, *pb.UpdateTaskRequest, *pb.OperateTaskRequest:
				if opTaskReq, ok := masterReq.(*pb.OperateTaskRequest); ok && opTaskReq.Op == pb.TaskOp_Delete && len(queryResp.SubTaskStatus) == 0 {
					return true, "", queryResp, nil
				}
				if len(queryResp.SubTaskStatus) == 1 {
					if subtaskStatus := queryResp.SubTaskStatus[0]; subtaskStatus != nil {
						msg := ""
						if err2 := extractWorkerError(subtaskStatus.Result); err2 != nil {
							msg = err2.Error()
						}
						ok := false
						// If expect stage is running, finished should also be okay
						var finished pb.Stage = -1
						if expect == pb.Stage_Running {
							finished = pb.Stage_Finished
						}
						if opTaskReq, ok2 := masterReq.(*pb.OperateTaskRequest); ok2 && opTaskReq.Op == pb.TaskOp_Delete {
							if st, ok3 := subtaskStatus.Status.(*pb.SubTaskStatus_Msg); ok3 && st.Msg == dmcommon.NoSubTaskMsg(taskName) {
								ok = true
							} else {
								// make sure there is no subtask
								continue
							}
						} else if subtaskStatus.Name == taskName && (subtaskStatus.Stage == expect || subtaskStatus.Stage == finished) {
							ok = true
						}
						if ok || msg != "" {
							return ok, msg, queryResp, nil
						}
					}
				}
			case *pb.OperateWorkerRelayRequest:
				if queryResp.SourceStatus == nil {
					continue
				}
				if relayStatus := queryResp.SourceStatus.RelayStatus; relayStatus != nil {
					msg := ""
					if err2 := extractWorkerError(relayStatus.Result); err2 != nil {
						msg = err2.Error()
					}
					ok := false
					if relayStatus.Stage == expect {
						ok = true
					}

					if ok || msg != "" {
						return ok, msg, queryResp, nil
					}
				} else {
					return false, "", queryResp, terror.ErrMasterOperRespNotSuccess.Generate("relay is disabled for this source")
				}
			case *pb.OperateRelayRequest:
				// including the situation that source status is nil and relay status is nil
				relayStatus := queryResp.GetSourceStatus().GetRelayStatus()
				if relayStatus == nil {
					if expect == pb.Stage_Stopped {
						return true, "", queryResp, nil
					}
				} else {
					msg := ""
					if err2 := extractWorkerError(relayStatus.Result); err2 != nil {
						msg = err2.Error()
					}
					ok := relayStatus.Stage == expect

					if ok || msg != "" {
						return ok, msg, queryResp, nil
					}
				}
			}
			log.L().Info("fail to get expect operation result", zap.Int("retryNum", num), zap.String("task", taskName),
				zap.String("source", sourceID), zap.Stringer("expect", expect), zap.Stringer("resp", queryResp))
		}
	}

	return false, "", nil, terror.ErrMasterFailToGetExpectResult
}

func (s *Server) handleOperationResult(ctx context.Context, cli *scheduler.Worker, taskName, sourceID string, req interface{}) *pb.CommonWorkerResponse {
	if cli == nil {
		return errorCommonWorkerResponse(sourceID+" relevant worker-client not found", sourceID, "")
	}
	var response *pb.CommonWorkerResponse
	ok, msg, queryResp, err := s.waitOperationOk(ctx, cli, taskName, sourceID, req)
	if err != nil {
		response = errorCommonWorkerResponse(err.Error(), sourceID, cli.BaseInfo().Name)
	} else {
		response = &pb.CommonWorkerResponse{
			Result: ok,
			Msg:    msg,
			Source: queryResp.SourceStatus.Source,
			Worker: queryResp.SourceStatus.Worker,
		}
	}
	return response
}

func sortCommonWorkerResults(sourceRespCh chan *pb.CommonWorkerResponse) []*pb.CommonWorkerResponse {
	sourceResps := make([]*pb.CommonWorkerResponse, 0, cap(sourceRespCh))
	for len(sourceRespCh) > 0 {
		r := <-sourceRespCh
		sourceResps = append(sourceResps, r)
	}
	sort.Slice(sourceResps, func(i, j int) bool {
		return sourceResps[i].Source < sourceResps[j].Source
	})
	return sourceResps
}

func (s *Server) getSourceRespsAfterOperation(ctx context.Context, taskName string, sources, workers []string, req interface{}) []*pb.CommonWorkerResponse {
	sourceRespCh := make(chan *pb.CommonWorkerResponse, len(sources))
	var wg sync.WaitGroup
	for i, source := range sources {
		wg.Add(1)
		var worker string
		if i < len(workers) {
			worker = workers[i]
		}
		go s.ap.Emit(ctx, 0, func(args ...interface{}) {
			defer wg.Done()
			source1, _ := args[0].(string)
			worker1, _ := args[1].(string)
			var workerCli *scheduler.Worker
			if worker1 != "" {
				workerCli = s.scheduler.GetWorkerByName(worker1)
			}
			if workerCli == nil {
				workerCli = s.scheduler.GetWorkerBySource(source1)
			}
			sourceResp := s.handleOperationResult(ctx, workerCli, taskName, source1, req)
			sourceResp.Source = source1 // may return other source's ID during stop worker
			sourceRespCh <- sourceResp
		}, func(args ...interface{}) {
			defer wg.Done()
			source1, _ := args[0].(string)
			worker1, _ := args[1].(string)
			sourceRespCh <- errorCommonWorkerResponse(terror.ErrMasterNoEmitToken.Generate(source1).Error(), source1, worker1)
		}, source, worker)
	}
	wg.Wait()
	return sortCommonWorkerResults(sourceRespCh)
}

func (s *Server) listMemberMaster(ctx context.Context, names []string) (*pb.Members_Master, error) {
	resp := &pb.Members_Master{
		Master: &pb.ListMasterMember{},
	}

	memberList, err := s.etcdClient.MemberList(ctx)
	if err != nil {
		resp.Master.Msg = err.Error()
		// nolint:nilerr
		return resp, nil
	}

	all := len(names) == 0
	set := make(map[string]bool)
	for _, name := range names {
		set[name] = true
	}

	etcdMembers := memberList.Members
	masters := make([]*pb.MasterInfo, 0, len(etcdMembers))

	client := &http.Client{}
	if len(s.cfg.SSLCA) != 0 {
		inner, err := toolutils.ToTLSConfigWithVerify(s.cfg.SSLCA, s.cfg.SSLCert, s.cfg.SSLKey, s.cfg.CertAllowedCN)
		if err != nil {
			return resp, err
		}
		client = toolutils.ClientWithTLS(inner)
	}
	client.Timeout = 1 * time.Second

	for _, etcdMember := range etcdMembers {
		if !all && !set[etcdMember.Name] {
			continue
		}

		alive := true
		if len(etcdMember.ClientURLs) == 0 {
			alive = false
		} else {
			// nolint:noctx, bodyclose
			_, err := client.Get(etcdMember.ClientURLs[0] + "/health")
			if err != nil {
				alive = false
			}
		}

		masters = append(masters, &pb.MasterInfo{
			Name:       etcdMember.Name,
			MemberID:   etcdMember.ID,
			Alive:      alive,
			ClientURLs: etcdMember.ClientURLs,
			PeerURLs:   etcdMember.PeerURLs,
		})
	}

	sort.Slice(masters, func(lhs, rhs int) bool {
		return masters[lhs].Name < masters[rhs].Name
	})
	resp.Master.Masters = masters
	return resp, nil
}

func (s *Server) listMemberWorker(names []string) *pb.Members_Worker {
	resp := &pb.Members_Worker{
		Worker: &pb.ListWorkerMember{},
	}

	workerAgents, err := s.scheduler.GetAllWorkers()
	if err != nil {
		resp.Worker.Msg = err.Error()
		return resp
	}

	all := len(names) == 0
	set := make(map[string]bool)
	for _, name := range names {
		set[name] = true
	}

	workers := make([]*pb.WorkerInfo, 0, len(workerAgents))

	for _, workerAgent := range workerAgents {
		if !all && !set[workerAgent.BaseInfo().Name] {
			continue
		}

		workers = append(workers, &pb.WorkerInfo{
			Name:   workerAgent.BaseInfo().Name,
			Addr:   workerAgent.BaseInfo().Addr,
			Stage:  string(workerAgent.Stage()),
			Source: workerAgent.Bound().Source,
		})
	}

	sort.Slice(workers, func(lhs, rhs int) bool {
		return workers[lhs].Name < workers[rhs].Name
	})
	resp.Worker.Workers = workers
	return resp
}

func (s *Server) listMemberLeader(ctx context.Context, names []string) *pb.Members_Leader {
	resp := &pb.Members_Leader{
		Leader: &pb.ListLeaderMember{},
	}

	all := len(names) == 0
	set := make(map[string]bool)
	for _, name := range names {
		set[name] = true
	}

	_, name, addr, err := s.election.LeaderInfo(ctx)
	if err != nil {
		resp.Leader.Msg = err.Error()
		return resp
	}

	if !all && !set[name] {
		return resp
	}

	resp.Leader.Name = name
	resp.Leader.Addr = addr
	return resp
}

// ListMember list member information.
func (s *Server) ListMember(ctx context.Context, req *pb.ListMemberRequest) (*pb.ListMemberResponse, error) {
	var (
		resp2 *pb.ListMemberResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	if !req.Leader && !req.Master && !req.Worker {
		req.Leader = true
		req.Master = true
		req.Worker = true
	}

	resp := &pb.ListMemberResponse{}
	members := make([]*pb.Members, 0)

	if req.Leader {
		res := s.listMemberLeader(ctx, req.Names)
		members = append(members, &pb.Members{
			Member: res,
		})
	}

	if req.Master {
		res, err := s.listMemberMaster(ctx, req.Names)
		if err != nil {
			resp.Msg = err.Error()
			// nolint:nilerr
			return resp, nil
		}
		members = append(members, &pb.Members{
			Member: res,
		})
	}

	if req.Worker {
		res := s.listMemberWorker(req.Names)
		members = append(members, &pb.Members{
			Member: res,
		})
	}

	resp.Result = true
	resp.Members = members
	return resp, nil
}

// OperateSchema operates schema of an upstream table.
func (s *Server) OperateSchema(ctx context.Context, req *pb.OperateSchemaRequest) (*pb.OperateSchemaResponse, error) {
	var (
		resp2 *pb.OperateSchemaResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	if len(req.Sources) == 0 {
		return &pb.OperateSchemaResponse{
			Result: false,
			Msg:    "must specify at least one source",
		}, nil
	}

	workerRespCh := make(chan *pb.CommonWorkerResponse, len(req.Sources))
	var wg sync.WaitGroup
	for _, source := range req.Sources {
		wg.Add(1)
		go func(source string) {
			defer wg.Done()
			worker := s.scheduler.GetWorkerBySource(source)
			if worker == nil {
				workerRespCh <- errorCommonWorkerResponse(fmt.Sprintf("source %s relevant worker-client not found", source), source, "")
				return
			}
			workerReq := workerrpc.Request{
				Type: workerrpc.CmdOperateSchema,
				OperateSchema: &pb.OperateWorkerSchemaRequest{
					Op:         req.Op,
					Task:       req.Task,
					Source:     source,
					Database:   req.Database,
					Table:      req.Table,
					Schema:     req.Schema,
					Flush:      req.Flush,
					Sync:       req.Sync,
					FromSource: req.FromSource,
					FromTarget: req.FromTarget,
				},
			}

			var workerResp *pb.CommonWorkerResponse
			resp, err := worker.SendRequest(ctx, &workerReq, s.cfg.RPCTimeout)
			if err != nil {
				workerResp = errorCommonWorkerResponse(err.Error(), source, worker.BaseInfo().Name)
			} else {
				workerResp = resp.OperateSchema
			}
			workerResp.Source = source
			workerRespCh <- workerResp
		}(source)
	}
	wg.Wait()

	workerRespMap := make(map[string]*pb.CommonWorkerResponse, len(req.Sources))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerRespMap[workerResp.Source] = workerResp
	}

	sort.Strings(req.Sources)
	workerResps := make([]*pb.CommonWorkerResponse, 0, len(req.Sources))
	for _, worker := range req.Sources {
		workerResps = append(workerResps, workerRespMap[worker])
	}

	return &pb.OperateSchemaResponse{
		Result:  true,
		Sources: workerResps,
	}, nil
}

func (s *Server) createMasterClientByName(ctx context.Context, name string) (pb.MasterClient, *grpc.ClientConn, error) {
	listResp, err := s.etcdClient.MemberList(ctx)
	if err != nil {
		return nil, nil, err
	}
	clientURLs := []string{}
	for _, m := range listResp.Members {
		if name == m.Name {
			for _, url := range m.GetClientURLs() {
				clientURLs = append(clientURLs, utils.UnwrapScheme(url))
			}
			break
		}
	}
	if len(clientURLs) == 0 {
		return nil, nil, errors.New("master not found")
	}
	tls, err := toolutils.NewTLS(s.cfg.SSLCA, s.cfg.SSLCert, s.cfg.SSLKey, s.cfg.AdvertiseAddr, s.cfg.CertAllowedCN)
	if err != nil {
		return nil, nil, err
	}

	var conn *grpc.ClientConn
	for _, clientURL := range clientURLs {
		//nolint:staticcheck
		conn, err = grpc.Dial(clientURL, tls.ToGRPCDialOption(), grpc.WithBackoffMaxDelay(3*time.Second))
		if err == nil {
			masterClient := pb.NewMasterClient(conn)
			return masterClient, conn, nil
		}
		log.L().Error("can not dial to master", zap.String("name", name), zap.String("client url", clientURL), log.ShortError(err))
	}
	// return last err
	return nil, nil, err
}

// GetMasterCfg implements MasterServer.GetMasterCfg.
func (s *Server) GetMasterCfg(ctx context.Context, req *pb.GetMasterCfgRequest) (*pb.GetMasterCfgResponse, error) {
	log.L().Info("", zap.Any("payload", req), zap.String("request", "GetMasterCfg"))

	var err error
	resp := &pb.GetMasterCfgResponse{}
	resp.Cfg, err = s.cfg.Toml()
	return resp, err
}

// GetCfg implements MasterServer.GetCfg.
func (s *Server) GetCfg(ctx context.Context, req *pb.GetCfgRequest) (*pb.GetCfgResponse, error) {
	var (
		resp2 = &pb.GetCfgResponse{}
		err2  error
		cfg   string
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	formartAndSortTaskString := func(subCfgList []*config.SubTaskConfig) string {
		sort.Slice(subCfgList, func(i, j int) bool {
			return subCfgList[i].SourceID < subCfgList[j].SourceID
		})
		// For the get-config command, we want to filter out fields that are not easily readable by humans,
		// such as SSLXXBytes field in `Security` struct
		taskCfg := config.SubTaskConfigsToTaskConfig(subCfgList...)
		taskCfg.TargetDB.Password = config.ObfuscatedPasswordForFeedback
		if taskCfg.TargetDB.Security != nil {
			taskCfg.TargetDB.Security.ClearSSLBytesData()
		}
		return taskCfg.String()
	}
	switch req.Type {
	case pb.CfgType_TaskTemplateType:
		task, err := ha.GetOpenAPITaskTemplate(s.etcdClient, req.Name)
		if err != nil {
			resp2.Msg = err.Error()
			// nolint:nilerr
			return resp2, nil
		}
		if task == nil {
			resp2.Msg = "task not found"
			// nolint:nilerr
			return resp2, nil
		}
		toDBCfg := config.GetTargetDBCfgFromOpenAPITask(task)
		if adjustDBErr := AdjustTargetDB(ctx, toDBCfg); adjustDBErr != nil {
			if adjustDBErr != nil {
				resp2.Msg = adjustDBErr.Error()
				// nolint:nilerr
				return resp2, nil
			}
		}
		sourceCfgMap := make(map[string]*config.SourceConfig)
		for _, cfg := range task.SourceConfig.SourceConf {
			if sourceCfg := s.scheduler.GetSourceCfgByID(cfg.SourceName); sourceCfg != nil {
				sourceCfgMap[cfg.SourceName] = sourceCfg
			} else {
				resp2.Msg = fmt.Sprintf("the source: %s of task not found", cfg.SourceName)
				return resp2, nil
			}
		}
		subTaskConfigList, err := config.OpenAPITaskToSubTaskConfigs(task, toDBCfg, sourceCfgMap)
		if err != nil {
			resp2.Msg = err.Error()
			// nolint:nilerr
			return resp2, nil
		}
		cfg = formartAndSortTaskString(subTaskConfigList)
	case pb.CfgType_TaskType:
		subCfgMap := s.scheduler.GetSubTaskCfgsByTask(req.Name)
		if len(subCfgMap) == 0 {
			resp2.Msg = "task not found"
			return resp2, nil
		}
		subTaskConfigList := make([]*config.SubTaskConfig, 0, len(subCfgMap))
		for _, subCfg := range subCfgMap {
			subTaskConfigList = append(subTaskConfigList, subCfg)
		}
		cfg = formartAndSortTaskString(subTaskConfigList)
	case pb.CfgType_MasterType:
		if req.Name == s.cfg.Name {
			cfg, err2 = s.cfg.Toml()
			if err2 != nil {
				resp2.Msg = err2.Error()
			} else {
				resp2.Result = true
				resp2.Cfg = cfg
			}
			return resp2, nil
		}

		masterClient, grpcConn, err := s.createMasterClientByName(ctx, req.Name)
		if err != nil {
			resp2.Msg = err.Error()
			// nolint:nilerr
			return resp2, nil
		}
		defer grpcConn.Close()
		masterResp, err := masterClient.GetMasterCfg(ctx, &pb.GetMasterCfgRequest{})
		if err != nil {
			resp2.Msg = err.Error()
			// nolint:nilerr
			return resp2, nil
		}
		cfg = masterResp.Cfg
	case pb.CfgType_WorkerType:
		worker := s.scheduler.GetWorkerByName(req.Name)
		if worker == nil {
			resp2.Msg = "worker not found"
			return resp2, nil
		}
		workerReq := workerrpc.Request{
			Type:         workerrpc.CmdGetWorkerCfg,
			GetWorkerCfg: &pb.GetWorkerCfgRequest{},
		}
		workerResp, err := worker.SendRequest(ctx, &workerReq, s.cfg.RPCTimeout)
		if err != nil {
			resp2.Msg = err.Error()
			// nolint:nilerr
			return resp2, nil
		}
		cfg = workerResp.GetWorkerCfg.Cfg
	case pb.CfgType_SourceType:
		sourceCfg := s.scheduler.GetSourceCfgByID(req.Name)
		if sourceCfg == nil {
			resp2.Msg = "source not found"

			return resp2, nil
		}
		sourceCfg.From.Password = config.ObfuscatedPasswordForFeedback
		if sourceCfg.From.Security != nil {
			sourceCfg.From.Security.ClearSSLBytesData()
		}
		cfg, err2 = sourceCfg.Yaml()
		if err2 != nil {
			resp2.Msg = err2.Error()
			// nolint:nilerr
			return resp2, nil
		}
	default:
		resp2.Msg = fmt.Sprintf("invalid config op '%s'", req.Type)
		return resp2, nil
	}

	return &pb.GetCfgResponse{
		Result: true,
		Cfg:    cfg,
	}, nil
}

// HandleError implements MasterServer.HandleError.
func (s *Server) HandleError(ctx context.Context, req *pb.HandleErrorRequest) (*pb.HandleErrorResponse, error) {
	var (
		resp2 *pb.HandleErrorResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	sources := req.Sources
	if len(sources) == 0 {
		sources = s.getTaskSourceNameList(req.Task)
		log.L().Info(fmt.Sprintf("sources: %s", sources))
		if len(sources) == 0 {
			return &pb.HandleErrorResponse{
				Result: false,
				Msg:    fmt.Sprintf("task %s has no source or not exist, please check the task name and status", req.Task),
			}, nil
		}
	}

	workerReq := workerrpc.Request{
		Type: workerrpc.CmdHandleError,
		HandleError: &pb.HandleWorkerErrorRequest{
			Op:        req.Op,
			Task:      req.Task,
			BinlogPos: req.BinlogPos,
			Sqls:      req.Sqls,
		},
	}

	workerRespCh := make(chan *pb.CommonWorkerResponse, len(sources))
	var wg sync.WaitGroup
	for _, source := range sources {
		wg.Add(1)
		go func(source string) {
			defer wg.Done()
			worker := s.scheduler.GetWorkerBySource(source)
			if worker == nil {
				workerRespCh <- errorCommonWorkerResponse(fmt.Sprintf("source %s relevant worker-client not found", source), source, "")
				return
			}
			var workerResp *pb.CommonWorkerResponse
			resp, err := worker.SendRequest(ctx, &workerReq, s.cfg.RPCTimeout)
			if err != nil {
				workerResp = errorCommonWorkerResponse(err.Error(), source, worker.BaseInfo().Name)
			} else {
				workerResp = resp.HandleError
			}
			workerResp.Source = source
			workerRespCh <- workerResp
		}(source)
	}
	wg.Wait()

	workerResps := make([]*pb.CommonWorkerResponse, 0, len(sources))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerResps = append(workerResps, workerResp)
	}

	sort.Slice(workerResps, func(i, j int) bool {
		return workerResps[i].Source < workerResps[j].Source
	})

	return &pb.HandleErrorResponse{
		Result:  true,
		Sources: workerResps,
	}, nil
}

// TransferSource implements MasterServer.TransferSource.
func (s *Server) TransferSource(ctx context.Context, req *pb.TransferSourceRequest) (*pb.TransferSourceResponse, error) {
	var (
		resp2 = &pb.TransferSourceResponse{}
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	err := s.scheduler.TransferSource(ctx, req.Source, req.Worker)
	if err != nil {
		resp2.Msg = err.Error()
		// nolint:nilerr
		return resp2, nil
	}
	resp2.Result = true
	return resp2, nil
}

// OperateRelay implements MasterServer.OperateRelay.
func (s *Server) OperateRelay(ctx context.Context, req *pb.OperateRelayRequest) (*pb.OperateRelayResponse, error) {
	var (
		resp2 = &pb.OperateRelayResponse{}
		err   error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err)
	if shouldRet {
		return resp2, err
	}

	switch req.Op {
	case pb.RelayOpV2_StartRelayV2:
		err = s.scheduler.StartRelay(req.Source, req.Worker)
	case pb.RelayOpV2_StopRelayV2:
		err = s.scheduler.StopRelay(req.Source, req.Worker)
	default:
		// should not happen
		return resp2, fmt.Errorf("only support start-relay or stop-relay, op: %s", req.Op.String())
	}
	if err != nil {
		resp2.Msg = err.Error()
		// nolint:nilerr
		return resp2, nil
	}
	resp2.Result = true
	// TODO: now we make sure req.Source isn't empty and len(req.Worker)>=1 in dmctl,
	// we need refactor the logic here if this behavior changed in the future
	// TODO: if len(req.Worker)==0 for a quick path, we must adjust req.Worker here
	var workerToCheck []string
	for _, worker := range req.Worker {
		w := s.scheduler.GetWorkerByName(worker)
		if w != nil && w.Stage() != scheduler.WorkerOffline {
			workerToCheck = append(workerToCheck, worker)
		} else {
			resp2.Sources = append(resp2.Sources, &pb.CommonWorkerResponse{
				Result: true,
				Msg:    "source relay is operated but the bound worker is offline",
				Source: req.Source,
				Worker: worker,
			})
		}
	}
	if len(workerToCheck) > 0 {
		sources := make([]string, len(req.Worker))
		for i := range workerToCheck {
			sources[i] = req.Source
		}
		resp2.Sources = append(resp2.Sources, s.getSourceRespsAfterOperation(ctx, "", sources, workerToCheck, req)...)
	}
	return resp2, nil
}

// sharedLogic does some shared logic for each RPC implementation
// arguments with `Pointer` suffix should be pointer to that variable its name indicated
// return `true` means caller should return with variable that `xxPointer` modified.
func (s *Server) sharedLogic(ctx context.Context, req interface{}, respPointer interface{}, errPointer *error) bool {
	// nolint:dogsled
	pc, _, _, _ := runtime.Caller(1)
	fullMethodName := runtime.FuncForPC(pc).Name()
	methodName := fullMethodName[strings.LastIndexByte(fullMethodName, '.')+1:]

	log.L().Info("", zap.Any("payload", req), zap.String("request", methodName))

	// origin code:
	//  isLeader, needForward := s.isLeaderAndNeedForward()
	//	if !isLeader {
	//		if needForward {
	//			return s.leaderClient.ListMember(ctx, req)
	//		}
	//		return nil, terror.ErrMasterRequestIsNotForwardToLeader
	//	}
	isLeader, needForward := s.isLeaderAndNeedForward(ctx)
	if isLeader {
		return false
	}
	if needForward {
		log.L().Info("will forward after a short interval", zap.String("from", s.cfg.Name), zap.String("to", s.leader.Load()), zap.String("request", methodName))
		time.Sleep(100 * time.Millisecond)
		params := []reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(req)}
		results := reflect.ValueOf(s.leaderClient).MethodByName(methodName).Call(params)
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
	*errPointer = terror.ErrMasterRequestIsNotForwardToLeader
	return true
}

func (s *Server) checkStartValidationParams(
	subTaskCfgs map[string]map[string]config.SubTaskConfig,
	req *pb.StartValidationRequest,
) (string, bool) {
	explicitModeOrStartTime := req.Mode != nil || req.StartTime != nil
	if req.Mode != nil {
		mode := req.GetModeValue()
		if mode != config.ValidationFull && mode != config.ValidationFast {
			msg := fmt.Sprintf("validation mode should be either `%s` or `%s`",
				config.ValidationFull, config.ValidationFast)
			return msg, false
		}
	}

	startTime := req.GetStartTimeValue()
	if startTime != "" {
		if _, err := utils.ParseStartTime(startTime); err != nil {
			return "start-time should be in the format like '2006-01-02 15:04:05' or '2006-01-02T15:04:05'", false
		}
	}

	var enabledValidator string
	allEnabled, noneEnabled := true, true
	for taskName := range subTaskCfgs {
		for sourceID := range subTaskCfgs[taskName] {
			if s.scheduler.ValidatorEnabled(taskName, sourceID) {
				enabledValidator = taskName + " with source " + sourceID
				noneEnabled = false
			} else {
				allEnabled = false
			}
		}
	}

	// if user set mode or start-time explicitly, then we do enable operation.
	// none of the target validators should have enabled
	if explicitModeOrStartTime && !noneEnabled {
		msg := fmt.Sprintf("some of target validator(%s) has already enabled, so we can't enable them in one command",
			enabledValidator)
		if allEnabled {
			msg = "all target validator has enabled, cannot do 'validation start' with explicit mode or start-time"
		}
		return msg, false
	}
	// no explicit mode or start-time:
	// - do start when all have enabled
	// - do enable when none enabled
	if !explicitModeOrStartTime && !allEnabled && !noneEnabled {
		return fmt.Sprintf("some of target validator(%s) has already enabled, so we can't enable or start them in one command",
			enabledValidator), false
	}

	return "", true
}

func (s *Server) StartValidation(ctx context.Context, req *pb.StartValidationRequest) (*pb.StartValidationResponse, error) {
	var (
		resp2     *pb.StartValidationResponse
		err, err2 error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}
	resp := &pb.StartValidationResponse{}

	subTaskCfgs := s.scheduler.GetSubTaskCfgsByTaskAndSource(req.TaskName, req.Sources)
	if len(subTaskCfgs) == 0 {
		if req.TaskName == "" {
			resp.Msg = fmt.Sprintf("cannot get subtask by sources `%v`", req.Sources)
		} else {
			resp.Msg = fmt.Sprintf("cannot get subtask by task name `%s` and sources `%v`",
				req.TaskName, req.Sources)
		}
		return resp, nil
	}

	for taskName := range subTaskCfgs {
		release, err3 := s.scheduler.AcquireSubtaskLatch(taskName)
		if err3 != nil {
			resp.Msg = err3.Error()
			// nolint:nilerr
			return resp, nil
		}
		defer release()
	}

	msg, ok := s.checkStartValidationParams(subTaskCfgs, req)
	if !ok {
		resp.Msg = msg
		return resp, nil
	}

	mode := config.ValidationFull
	if req.Mode != nil {
		mode = req.GetModeValue()
	}
	startTime := req.GetStartTimeValue()
	changedSubtaskCfgs := make([]config.SubTaskConfig, 0)
	validatorStages := make([]ha.Stage, 0)
	for taskName := range subTaskCfgs {
		for sourceID := range subTaskCfgs[taskName] {
			cfg := subTaskCfgs[taskName][sourceID]
			if !s.scheduler.ValidatorEnabled(taskName, sourceID) {
				cfg.ValidatorCfg.Mode = mode
				cfg.ValidatorCfg.StartTime = startTime
				changedSubtaskCfgs = append(changedSubtaskCfgs, cfg)
				log.L().Info("enable validator",
					zap.String("task", taskName), zap.String("source", sourceID),
					zap.String("mode", mode), zap.String("start-time", startTime))
			} else {
				log.L().Info("start validator",
					zap.String("task", taskName), zap.String("source", sourceID))
			}
			subTaskCfgs[taskName][sourceID] = cfg
			validatorStages = append(validatorStages, ha.NewValidatorStage(pb.Stage_Running, cfg.SourceID, cfg.Name))
		}
	}

	err = s.scheduler.OperateValidationTask(validatorStages, changedSubtaskCfgs)
	if err != nil {
		resp.Msg = err.Error()
		// nolint:nilerr
		return resp, nil
	}
	resp.Result = true
	resp.Msg = "success"
	return resp, nil
}

func (s *Server) StopValidation(ctx context.Context, req *pb.StopValidationRequest) (*pb.StopValidationResponse, error) {
	var (
		resp2       *pb.StopValidationResponse
		err, err2   error
		subTaskCfgs map[string]map[string]config.SubTaskConfig // task-name->sourceID->*config.SubTaskConfig
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}
	resp := &pb.StopValidationResponse{}
	subTaskCfgs = s.scheduler.GetSubTaskCfgsByTaskAndSource(req.TaskName, req.Sources)
	if len(subTaskCfgs) == 0 {
		msg := fmt.Sprintf("cannot get subtask by task name `%s` and sources `%v`", req.TaskName, req.Sources)
		if req.TaskName == "" {
			msg = fmt.Sprintf("cannot get subtask by sources `%v`", req.Sources)
		}
		resp.Msg = msg
		// nolint:nilerr
		return resp, nil
	}

	for taskName := range subTaskCfgs {
		release, err3 := s.scheduler.AcquireSubtaskLatch(taskName)
		if err3 != nil {
			resp.Msg = err3.Error()
			// nolint:nilerr
			return resp, nil
		}
		defer release()
	}

	var unEnabledValidator string
	allEnabled := true
	for taskName := range subTaskCfgs {
		for sourceID := range subTaskCfgs[taskName] {
			if !s.scheduler.ValidatorEnabled(taskName, sourceID) {
				unEnabledValidator = taskName + " with source " + sourceID
				allEnabled = false
			}
		}
	}
	if !allEnabled {
		resp.Msg = fmt.Sprintf("some target validator(%s) is not enabled", unEnabledValidator)
		return resp, nil
	}

	validatorStages := make([]ha.Stage, 0)
	for taskName := range subTaskCfgs {
		for _, cfg := range subTaskCfgs[taskName] {
			log.L().Info("stop validator", zap.String("task", taskName), zap.String("source", cfg.SourceID))
			validatorStages = append(validatorStages, ha.NewValidatorStage(pb.Stage_Stopped, cfg.SourceID, cfg.Name))
		}
	}
	err = s.scheduler.OperateValidationTask(validatorStages, []config.SubTaskConfig{})
	if err != nil {
		resp.Result = false
		resp.Msg = err.Error()
		// nolint:nilerr
		return resp, nil
	}
	resp.Result = true
	resp.Msg = "success"
	return resp, nil
}

func (s *Server) GetValidationStatus(ctx context.Context, req *pb.GetValidationStatusRequest) (*pb.GetValidationStatusResponse, error) {
	var (
		resp2       *pb.GetValidationStatusResponse
		err         error
		subTaskCfgs map[string]map[string]config.SubTaskConfig
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err)
	if shouldRet {
		return resp2, err
	}
	resp := &pb.GetValidationStatusResponse{
		Result: true,
	}
	if req.TaskName == "" {
		resp.Result = false
		resp.Msg = "task name should be specified"
		return resp, nil
	}
	if req.FilterStatus != pb.Stage_InvalidStage && req.FilterStatus != pb.Stage_Running && req.FilterStatus != pb.Stage_Stopped {
		resp.Result = false
		resp.Msg = fmt.Sprintf("filtering stage should be either `%s`, `%s`, or empty", strings.ToLower(pb.Stage_Running.String()), strings.ToLower(pb.Stage_Stopped.String()))
		// nolint:nilerr
		return resp, nil
	}
	subTaskCfgs = s.scheduler.GetSubTaskCfgsByTaskAndSource(req.TaskName, []string{})
	if len(subTaskCfgs) == 0 {
		resp.Result = false
		resp.Msg = fmt.Sprintf("cannot get subtask by task name `%s`", req.TaskName)
		// nolint:nilerr
		return resp, nil
	}
	log.L().Info("query validation status", zap.Reflect("subtask", subTaskCfgs))
	var (
		workerResps  = make([]*pb.GetValidationStatusResponse, 0)
		workerRespMu sync.Mutex
		wg           sync.WaitGroup
	)
	for taskName, mSource := range subTaskCfgs {
		for sourceID := range mSource {
			newReq := &workerrpc.Request{
				GetValidationStatus: &pb.GetValidationStatusRequest{},
				Type:                workerrpc.CmdGetValidationStatus,
			}
			*newReq.GetValidationStatus = *req
			newReq.GetValidationStatus.TaskName = taskName
			sendValidationRequest(ctx, s, newReq, sourceID, &wg, &workerRespMu, &workerResps, "get validation status")
		}
	}
	wg.Wait()
	// todo: sort?
	for _, wresp := range workerResps {
		if !wresp.Result {
			resp.Result = wresp.Result
			resp.Msg += wresp.Msg + "; "
			continue
		}
		resp.Validators = append(resp.Validators, wresp.Validators...)
		resp.TableStatuses = append(resp.TableStatuses, wresp.TableStatuses...)
	}
	// nolint:nilerr
	return resp, nil
}

func (s *Server) GetValidationError(ctx context.Context, req *pb.GetValidationErrorRequest) (*pb.GetValidationErrorResponse, error) {
	var (
		resp2       *pb.GetValidationErrorResponse
		err2, err   error
		subTaskCfgs map[string]map[string]config.SubTaskConfig
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}
	resp := &pb.GetValidationErrorResponse{
		Result: true,
	}
	if req.TaskName == "" {
		resp.Result = false
		resp.Msg = "task name should be specified"
		return resp, nil
	}
	if req.ErrState == pb.ValidateErrorState_ResolvedErr {
		resp.Result = false
		resp.Msg = "only support querying `all`, `unprocessed`, and `ignored` error"
		return resp, nil
	}
	subTaskCfgs = s.scheduler.GetSubTaskCfgsByTaskAndSource(req.TaskName, []string{})
	if len(subTaskCfgs) == 0 {
		resp.Result = false
		resp.Msg = fmt.Sprintf("cannot get subtask by task name `%s`", req.TaskName)
		// nolint:nilerr
		return resp, nil
	}
	log.L().Info("query validation error", zap.Reflect("subtask", subTaskCfgs))
	var (
		workerResps  = make([]*pb.GetValidationErrorResponse, 0)
		workerRespMu sync.Mutex
		wg           sync.WaitGroup
	)
	for taskName, mSource := range subTaskCfgs {
		for sourceID := range mSource {
			newReq := workerrpc.Request{
				GetValidationError: &pb.GetValidationErrorRequest{},
				Type:               workerrpc.CmdGetValidationError,
			}
			*newReq.GetValidationError = *req
			newReq.GetValidationError.TaskName = taskName
			sendValidationRequest(ctx, s, &newReq, sourceID, &wg, &workerRespMu, &workerResps, "get validator error")
		}
	}
	wg.Wait()
	// todo: sort?
	for _, wresp := range workerResps {
		if !wresp.Result {
			resp.Result = wresp.Result
			resp.Msg += wresp.Msg + "; "
			continue
		}
		resp.Error = append(resp.Error, wresp.Error...)
	}
	return resp, err
}

func (s *Server) OperateValidationError(ctx context.Context, req *pb.OperateValidationErrorRequest) (*pb.OperateValidationErrorResponse, error) {
	var (
		resp2       *pb.OperateValidationErrorResponse
		err         error
		subTaskCfgs map[string]map[string]config.SubTaskConfig
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err)
	if shouldRet {
		return resp2, err
	}
	resp := &pb.OperateValidationErrorResponse{
		Result: true,
	}
	if req.TaskName == "" {
		resp.Result = false
		resp.Msg = "task name should be specified"
		return resp, nil
	}
	subTaskCfgs = s.scheduler.GetSubTaskCfgsByTaskAndSource(req.TaskName, []string{})
	if len(subTaskCfgs) == 0 {
		resp.Result = false
		resp.Msg = fmt.Sprintf("cannot get subtask by task name `%s`", req.TaskName)
		// nolint:nilerr
		return resp, nil
	}
	log.L().Info("operate validation error", zap.Reflect("subtask", subTaskCfgs))
	var (
		workerResps  = make([]*pb.OperateValidationErrorResponse, 0)
		workerRespMu sync.Mutex
		wg           sync.WaitGroup
	)
	for taskName, mSource := range subTaskCfgs {
		for sourceID := range mSource {
			newReq := workerrpc.Request{
				Type:                   workerrpc.CmdOperateValidationError,
				OperateValidationError: &pb.OperateValidationErrorRequest{},
			}
			*newReq.OperateValidationError = *req
			newReq.OperateValidationError.TaskName = taskName
			sendValidationRequest(ctx, s, &newReq, sourceID, &wg, &workerRespMu, &workerResps, "operate validator")
		}
	}
	wg.Wait()
	// todo: sort?
	for _, wresp := range workerResps {
		if !wresp.Result {
			resp.Result = wresp.Result
			resp.Msg += wresp.Msg + "; "
		}
	}
	// nolint:nilerr
	return resp, nil
}

func appendWorkerResp[T any](workerRespMu *sync.Mutex, workerResps *[]T, resp T) {
	workerRespMu.Lock()
	*workerResps = append(*workerResps, resp)
	workerRespMu.Unlock()
}

func sendValidationRequest[T any](
	ctx context.Context,
	s *Server,
	req *workerrpc.Request,
	sourceID string,
	wg *sync.WaitGroup,
	workerRespMu *sync.Mutex,
	workerResps *[]T,
	logMsg string,
) {
	worker := s.scheduler.GetWorkerBySource(sourceID)
	if worker == nil {
		err := terror.ErrMasterWorkerArgsExtractor.Generatef("%s relevant worker-client not found", sourceID)
		resp := genValidationWorkerErrorResp(req, err, logMsg, "", sourceID)
		appendWorkerResp(workerRespMu, workerResps, resp.(T))
		return
	}
	wg.Add(1)
	go s.ap.Emit(ctx, 0, func(args ...interface{}) {
		// send request in parallel
		defer wg.Done()
		workerResp, err := worker.SendRequest(ctx, req, s.cfg.RPCTimeout)
		if err != nil {
			resp := genValidationWorkerErrorResp(req, err, logMsg, worker.BaseInfo().Name, sourceID)
			appendWorkerResp(workerRespMu, workerResps, resp.(T))
		} else {
			resp := getValidationWorkerResp(req, workerResp)
			appendWorkerResp(workerRespMu, workerResps, resp.(T))
		}
	}, func(args ...interface{}) {
		defer wg.Done()
		err := terror.ErrMasterNoEmitToken.Generate(sourceID)
		resp := genValidationWorkerErrorResp(req, err, logMsg, worker.BaseInfo().Name, sourceID)
		appendWorkerResp(workerRespMu, workerResps, resp.(T))
	})
}

func getValidationWorkerResp(req *workerrpc.Request, resp *workerrpc.Response) interface{} {
	switch req.Type {
	case workerrpc.CmdGetValidationStatus:
		return resp.GetValidationStatus
	case workerrpc.CmdGetValidationError:
		return resp.GetValidationError
	case workerrpc.CmdOperateValidationError:
		return resp.OperateValidationError
	default:
		return nil
	}
}

func genValidationWorkerErrorResp(req *workerrpc.Request, err error, logMsg, workerID, sourceID string) interface{} {
	log.L().Error(logMsg, zap.Error(err), zap.String("source", sourceID), zap.String("worker", workerID))
	switch req.Type {
	case workerrpc.CmdGetValidationStatus:
		return &pb.GetValidationStatusResponse{
			Result: false,
			Msg:    err.Error(),
		}
	case workerrpc.CmdGetValidationError:
		return &pb.GetValidationErrorResponse{
			Result: false,
			Msg:    err.Error(),
		}
	case workerrpc.CmdOperateValidationError:
		return &pb.OperateValidationErrorResponse{
			Result: false,
			Msg:    err.Error(),
		}
	default:
		return nil
	}
}

func (s *Server) UpdateValidation(ctx context.Context, req *pb.UpdateValidationRequest) (*pb.UpdateValidationResponse, error) {
	var (
		resp2 *pb.UpdateValidationResponse
		err   error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err)
	if shouldRet {
		return resp2, err
	}
	resp := &pb.UpdateValidationResponse{
		Result: false,
	}
	subTaskCfgs := s.scheduler.GetSubTaskCfgsByTaskAndSource(req.TaskName, req.Sources)
	if len(subTaskCfgs) == 0 {
		if len(req.Sources) > 0 {
			resp.Msg = fmt.Sprintf("cannot get subtask by task name `%s` and sources `%v`",
				req.TaskName, req.Sources)
		} else {
			resp.Msg = fmt.Sprintf("cannot get subtask by task name `%s`", req.TaskName)
		}
		return resp, nil
	}

	workerReq := workerrpc.Request{
		Type: workerrpc.CmdUpdateValidation,
		UpdateValidation: &pb.UpdateValidationWorkerRequest{
			TaskName:   req.TaskName,
			BinlogPos:  req.BinlogPos,
			BinlogGTID: req.BinlogGTID,
		},
	}

	sourcesLen := 0
	for _, subTaskCfg := range subTaskCfgs {
		sourcesLen += len(subTaskCfg)
	}
	workerRespCh := make(chan *pb.CommonWorkerResponse, sourcesLen)
	var wg sync.WaitGroup
	for _, subTaskCfg := range subTaskCfgs {
		for sourceID := range subTaskCfg {
			wg.Add(1)
			go func(source string) {
				defer wg.Done()
				sourceCfg := s.scheduler.GetSourceCfgByID(source)
				// can't directly use subtaskCfg here, because it will be overwritten by sourceCfg
				if sourceCfg.EnableGTID {
					if len(req.BinlogGTID) == 0 {
						workerRespCh <- errorCommonWorkerResponse(fmt.Sprintf("source %s didn't specify cutover-binlog-gtid when enableGTID is true", source), source, "")
						return
					}
				} else if len(req.BinlogPos) == 0 {
					workerRespCh <- errorCommonWorkerResponse(fmt.Sprintf("source %s didn't specify cutover-binlog-pos when enableGTID is false", source), source, "")
					return
				}
				worker := s.scheduler.GetWorkerBySource(source)
				if worker == nil {
					workerRespCh <- errorCommonWorkerResponse(fmt.Sprintf("source %s relevant worker-client not found", source), source, "")
					return
				}
				var workerResp *pb.CommonWorkerResponse
				resp, err := worker.SendRequest(ctx, &workerReq, s.cfg.RPCTimeout)
				if err != nil {
					workerResp = errorCommonWorkerResponse(err.Error(), source, worker.BaseInfo().Name)
				} else {
					workerResp = resp.UpdateValidation
				}
				workerResp.Source = source
				workerRespCh <- workerResp
			}(sourceID)
		}
	}
	wg.Wait()

	workerResps := make([]*pb.CommonWorkerResponse, 0, sourcesLen)
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerResps = append(workerResps, workerResp)
	}

	sort.Slice(workerResps, func(i, j int) bool {
		return workerResps[i].Source < workerResps[j].Source
	})

	return &pb.UpdateValidationResponse{
		Result:  true,
		Sources: workerResps,
	}, nil
}
