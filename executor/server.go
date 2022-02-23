package executor

import (
	"context"
	"strings"
	"time"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/executor/runtime"
	"github.com/hanfei1991/microcosm/executor/worker"
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/lib/registry"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/config"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/hanfei1991/microcosm/pkg/serverutils"
	"github.com/hanfei1991/microcosm/test"
	"github.com/hanfei1991/microcosm/test/mock"
	"github.com/pingcap/tiflow/dm/pkg/log"
	p2pImpl "github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/tcpserver"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

type Server struct {
	cfg     *Config
	testCtx *test.Context

	tcpServer   tcpserver.TCPServer
	grpcSrv     *grpc.Server
	cli         client.MasterClient
	cliUpdateCh chan []string
	sch         *runtime.Runtime
	workerRtm   *worker.Runtime
	msgServer   *p2p.MessageRPCService
	info        *model.NodeInfo

	lastHearbeatTime time.Time

	mockSrv mock.GrpcServer

	// etcdCli connects to server master embed etcd, it should be used in service
	// discovery only.
	etcdCli         *clientv3.Client
	metastore       metadata.MetaKV
	p2pMsgRouter    p2pImpl.MessageRouter
	discoveryKeeper *serverutils.DiscoveryKeepaliver
}

func NewServer(cfg *Config, ctx *test.Context) *Server {
	s := Server{
		cfg:         cfg,
		testCtx:     ctx,
		cliUpdateCh: make(chan []string),
	}
	return &s
}

// SubmitBatchTasks implements the pb interface.
func (s *Server) SubmitBatchTasks(ctx context.Context, req *pb.SubmitBatchTasksRequest) (*pb.SubmitBatchTasksResponse, error) {
	tasks := make([]*model.Task, 0, len(req.Tasks))
	for _, pbTask := range req.Tasks {
		task := &model.Task{
			ID:   model.ID(pbTask.Id),
			Op:   pbTask.Op,
			OpTp: model.OperatorType(pbTask.OpTp),
		}
		for _, id := range pbTask.Inputs {
			task.Inputs = append(task.Inputs, model.ID(id))
		}
		for _, id := range pbTask.Outputs {
			task.Outputs = append(task.Outputs, model.ID(id))
		}
		tasks = append(tasks, task)
	}
	log.L().Logger.Info("executor receive submit sub job", zap.Int("task", len(tasks)))
	resp := &pb.SubmitBatchTasksResponse{}
	err := s.sch.SubmitTasks(tasks)
	if err != nil {
		log.L().Logger.Error("submit subjob error", zap.Error(err))
		resp.Err = errors.ToPBError(err)
	}
	return resp, nil
}

// CancelBatchTasks implements pb interface.
func (s *Server) CancelBatchTasks(ctx context.Context, req *pb.CancelBatchTasksRequest) (*pb.CancelBatchTasksResponse, error) {
	log.L().Info("cancel tasks", zap.String("req", req.String()))
	err := s.sch.Stop(req.TaskIdList)
	if err != nil {
		return &pb.CancelBatchTasksResponse{
			Err: &pb.Error{
				Message: err.Error(),
			},
		}, nil
	}
	return &pb.CancelBatchTasksResponse{}, nil
}

// PauseBatchTasks implements pb interface.
func (s *Server) PauseBatchTasks(ctx context.Context, req *pb.PauseBatchTasksRequest) (*pb.PauseBatchTasksResponse, error) {
	log.L().Info("pause tasks", zap.String("req", req.String()))
	err := s.sch.Pause(req.TaskIdList)
	if err != nil {
		return &pb.PauseBatchTasksResponse{
			Err: &pb.Error{
				Message: err.Error(),
			},
		}, nil
	}
	return &pb.PauseBatchTasksResponse{}, nil
}

// ResumeBatchTasks implements pb interface.
func (s *Server) ResumeBatchTasks(ctx context.Context, req *pb.PauseBatchTasksRequest) (*pb.PauseBatchTasksResponse, error) {
	log.L().Info("resume tasks", zap.String("req", req.String()))
	s.sch.Continue(req.TaskIdList)
	return &pb.PauseBatchTasksResponse{}, nil
}

func (s *Server) DispatchTask(ctx context.Context, req *pb.DispatchTaskRequest) (*pb.DispatchTaskResponse, error) {
	log.L().Info("dispatch task", zap.String("req", req.String()))

	// TODO better dependency management
	dctx := dcontext.Background()
	dctx.Dependencies = dcontext.RuntimeDependencies{
		MessageHandlerManager: s.msgServer.MakeHandlerManager(),
		MessageRouter:         p2p.NewMessageSender(s.p2pMsgRouter),
		MetaKVClient:          s.metastore,
		ExecutorClientManager: client.NewClientManager(),
		ServerMasterClient:    s.cli,
	}
	dctx.Environ.NodeID = p2p.NodeID(s.info.ID)
	dctx.Environ.Addr = s.info.Addr
	masterMeta := &lib.MasterMetaExt{
		// GetWorkerId here returns id of current unit
		ID:     req.GetWorkerId(),
		Tp:     lib.WorkerType(req.GetTaskTypeId()),
		Config: req.GetTaskConfig(),
	}
	metaBytes, err := masterMeta.Marshal()
	if err != nil {
		return nil, err
	}
	dctx.Environ.MasterMetaExt = metaBytes

	newWorker, err := registry.GlobalWorkerRegistry().CreateWorker(
		dctx,
		lib.WorkerType(req.GetTaskTypeId()),
		req.GetWorkerId(),
		req.GetMasterId(),
		req.GetTaskConfig())
	if err != nil {
		log.L().Error("Failed to create worker", zap.Error(err))
		// TODO better error handling
		return nil, err
	}

	if err := s.workerRtm.SubmitTask(newWorker); err != nil {
		errCode := pb.DispatchTaskErrorCode_Other
		if errors.ErrRuntimeReachedCapacity.Equal(err) {
			errCode = pb.DispatchTaskErrorCode_NoResource
		}

		return &pb.DispatchTaskResponse{
			ErrorCode:    errCode,
			ErrorMessage: err.Error(),
			WorkerId:     req.GetWorkerId(),
		}, nil
	}

	return &pb.DispatchTaskResponse{
		ErrorCode: pb.DispatchTaskErrorCode_OK,
		WorkerId:  req.GetWorkerId(),
	}, nil
}

func (s *Server) Stop() {
	if s.grpcSrv != nil {
		s.grpcSrv.Stop()
	}

	if s.tcpServer != nil {
		err := s.tcpServer.Close()
		if err != nil {
			log.L().Error("close tcp server", zap.Error(err))
		}
	}

	if s.metastore != nil {
		// clear executor info in metastore to accelerate service discovery. If
		// not delete actively, the session will be timeout after TTL.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := s.metastore.Delete(ctx, s.info.EtcdKey())
		if err != nil {
			log.L().Warn("failed to delete executor info", zap.Error(err))
		}
	}

	if s.mockSrv != nil {
		s.mockSrv.Stop()
	}
}

func (s *Server) startForTest(ctx context.Context) (err error) {
	s.mockSrv, err = mock.NewExecutorServer(s.cfg.WorkerAddr, s)
	if err != nil {
		return err
	}
	s.sch = runtime.NewRuntime(s.testCtx)
	go func() {
		s.sch.Run(ctx, 10)
	}()

	err = s.selfRegister(ctx)
	if err != nil {
		return err
	}
	go func() {
		err := s.keepHeartbeat(ctx)
		log.L().Info("heartbeat quits", zap.Error(err))
	}()
	return nil
}

func (s *Server) startMsgService(ctx context.Context, wg *errgroup.Group) (err error) {
	s.msgServer, err = p2p.NewDependentMessageRPCService(string(s.info.ID), nil, s.grpcSrv)
	if err != nil {
		return err
	}
	wg.Go(func() error {
		// TODO refactor this
		return s.msgServer.Serve(ctx, nil)
	})
	return nil
}

const (
	defaultRuntimeCapacity = 65536 // TODO make this configurable
)

func (s *Server) Run(ctx context.Context) error {
	if test.GetGlobalTestFlag() {
		return s.startForTest(ctx)
	}

	registerMetrics()

	wg, ctx := errgroup.WithContext(ctx)

	s.sch = runtime.NewRuntime(nil)
	wg.Go(func() error {
		s.sch.Run(ctx, 10)
		return nil
	})

	pollCon := s.cfg.PollConcurrency
	s.workerRtm = worker.NewRuntime(ctx, defaultRuntimeCapacity)

	wg.Go(func() error {
		s.workerRtm.Start(ctx, pollCon)
		return nil
	})

	err := s.selfRegister(ctx)
	if err != nil {
		return err
	}

	s.p2pMsgRouter = p2p.NewMessageRouter(p2p.NodeID(s.info.ID), s.info.Addr)

	s.grpcSrv = grpc.NewServer()
	err = s.startMsgService(ctx, wg)
	if err != nil {
		return err
	}

	err = s.startTCPService(ctx, wg)
	if err != nil {
		return err
	}

	err = s.connectToMetaStore(ctx)
	if err != nil {
		return err
	}

	s.discoveryKeeper = serverutils.NewDiscoveryKeepaliver(
		s.info, s.etcdCli, s.cfg.SessionTTL, defaultDiscoverTicker,
		s.p2pMsgRouter,
	)
	// connects to metastore and maintains a etcd session
	wg.Go(func() error {
		return s.discoveryKeeper.Keepalive(ctx)
	})

	wg.Go(func() error {
		return s.keepHeartbeat(ctx)
	})

	wg.Go(func() error {
		return s.reportTaskResc(ctx)
	})

	wg.Go(func() error {
		return s.bgUpdateServerMasterClients(ctx)
	})

	wg.Go(func() error {
		return s.collectMetricLoop(ctx, defaultMetricInterval)
	})

	return wg.Wait()
}

// startTCPService starts grpc server and http server
func (s *Server) startTCPService(ctx context.Context, wg *errgroup.Group) error {
	tcpServer, err := tcpserver.NewTCPServer(s.cfg.WorkerAddr, &security.Credential{})
	if err != nil {
		return err
	}
	s.tcpServer = tcpServer
	pb.RegisterExecutorServer(s.grpcSrv, s)
	log.L().Logger.Info("listen address", zap.String("addr", s.cfg.WorkerAddr))

	wg.Go(func() error {
		return s.tcpServer.Run(ctx)
	})

	wg.Go(func() error {
		return s.grpcSrv.Serve(s.tcpServer.GrpcListener())
	})

	wg.Go(func() error {
		return httpHandler(s.tcpServer.HTTP1Listener())
	})
	return nil
}

// current the metastore is an embed etcd underlying
func (s *Server) connectToMetaStore(ctx context.Context) error {
	// query service discovery metastore to fetch metastore connection endpoint
	resp, err := s.cli.QueryMetaStore(
		ctx,
		&pb.QueryMetaStoreRequest{Tp: pb.StoreType_ServiceDiscovery},
		s.cfg.RPCTimeout,
	)
	if err != nil {
		return err
	}
	log.L().Info("update service discovery metastore", zap.String("addr", resp.Address))

	logConfig := logutil.DefaultZapLoggerConfig
	logConfig.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:        strings.Split(resp.GetAddress(), ","),
		Context:          ctx,
		LogConfig:        &logConfig,
		DialTimeout:      config.ServerMasterEtcdDialTimeout,
		AutoSyncInterval: config.ServerMasterEtcdSyncInterval,
		DialOptions: []grpc.DialOption{
			grpc.WithInsecure(),
			grpc.WithBlock(),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  time.Second,
					Multiplier: 1.1,
					Jitter:     0.1,
					MaxDelay:   3 * time.Second,
				},
				MinConnectTimeout: 3 * time.Second,
			}),
		},
	})
	if err != nil {
		return err
	}

	// TODO: we share system metastore with service discovery etcd, in the future
	// we will separate them
	s.metastore = metadata.NewMetaEtcd(etcdCli)
	// TODO: after metastore is separted from server master embed etcd, this etcdCli
	// should be another one.
	s.etcdCli = etcdCli
	return err
}

func (s *Server) selfRegister(ctx context.Context) (err error) {
	// Register myself
	s.cli, err = client.NewMasterClient(ctx, getJoinURLs(s.cfg.Join))
	if err != nil {
		return err
	}
	log.L().Logger.Info("master client init successful")
	registerReq := &pb.RegisterExecutorRequest{
		Address:    s.cfg.AdvertiseAddr,
		Capability: defaultCapability,
	}

	resp, err := s.cli.RegisterExecutor(ctx, registerReq, s.cfg.RPCTimeout)
	if err != nil {
		return err
	}
	s.info = &model.NodeInfo{
		Type:       model.NodeTypeExecutor,
		ID:         model.ExecutorID(resp.ExecutorId),
		Addr:       s.cfg.AdvertiseAddr,
		Capability: int(defaultCapability),
	}
	log.L().Logger.Info("register successful", zap.Any("info", s.info))
	return nil
}

// TODO: Right now heartbeat maintainace is too simple. We should look into
// what other frameworks do or whether we can use grpc heartbeat.
func (s *Server) keepHeartbeat(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.KeepAliveInterval)
	s.lastHearbeatTime = time.Now()
	defer func() {
		if test.GetGlobalTestFlag() {
			s.testCtx.NotifyExecutorChange(&test.ExecutorChangeEvent{
				Tp:   test.Delete,
				Time: time.Now(),
			})
		}
	}()
	rl := rate.NewLimiter(rate.Every(time.Second*5), 1 /*burst*/)
	for {
		select {
		case <-ctx.Done():
			return nil
		case t := <-ticker.C:
			if s.lastHearbeatTime.Add(s.cfg.KeepAliveTTL).Before(time.Now()) {
				return errors.ErrHeartbeat.GenWithStack("heartbeat timeout")
			}
			req := &pb.HeartbeatRequest{
				ExecutorId: string(s.info.ID),
				Status:     int32(model.Running),
				Timestamp:  uint64(t.Unix()),
				// We set longer ttl for master, which is "ttl + rpc timeout", to avoid that
				// executor actually wait for a timeout when ttl is nearly up.
				Ttl: uint64(s.cfg.KeepAliveTTL.Milliseconds() + s.cfg.RPCTimeout.Milliseconds()),
			}
			resp, err := s.cli.Heartbeat(ctx, req, s.cfg.RPCTimeout)
			if err != nil {
				log.L().Error("heartbeat rpc meet error", zap.Error(err))
				if s.lastHearbeatTime.Add(s.cfg.KeepAliveTTL).Before(time.Now()) {
					return errors.Wrap(errors.ErrHeartbeat, err, "rpc")
				}
				continue
			}
			if resp.Err != nil {
				log.L().Warn("heartbeat response meet error", zap.Stringer("code", resp.Err.GetCode()))
				switch resp.Err.Code {
				case pb.ErrorCode_UnknownExecutor, pb.ErrorCode_TombstoneExecutor:
					return errors.ErrHeartbeat.GenWithStack("logic error: %s", resp.Err.GetMessage())
				}
				continue
			}
			// We aim to keep lastHbTime of executor consistent with lastHbTime of Master.
			// If we set the heartbeat time of executor to the start time of rpc, it will
			// be a little bit earlier than the heartbeat time of master, which is safe.
			// In contrast, if we set it to the end time of rpc, it might be a little bit
			// later than master's, which might cause that master wait for less time than executor.
			// This gap is unsafe.
			s.lastHearbeatTime = t
			if rl.Allow() {
				log.L().Info("heartbeat success", zap.String("leader", resp.Leader), zap.Strings("members", resp.Addrs))
			}
			// update master client could cost long time, we make it a background
			// job and if there is running update task, we ignore once since more
			// heartbeats will be called later.
			select {
			case s.cliUpdateCh <- resp.Addrs:
			default:
			}
		}
	}
}

func getJoinURLs(addrs string) []string {
	return strings.Split(addrs, ",")
}

func (s *Server) reportTaskRescOnce(ctx context.Context) error {
	rescs := s.sch.Resource()
	req := &pb.ExecWorkloadRequest{
		// TODO: use which field as ExecutorId is more accurate
		ExecutorId: s.cfg.WorkerAddr,
		Workloads:  make([]*pb.ExecWorkload, 0, len(rescs)),
	}
	for tp, resc := range rescs {
		req.Workloads = append(req.Workloads, &pb.ExecWorkload{
			Tp:    pb.JobType(tp),
			Usage: int32(resc),
		})
	}
	resp, err := s.cli.ReportExecutorWorkload(ctx, req)
	if err != nil {
		return err
	}
	if resp.Err != nil {
		log.L().Warn("report executor workload error", zap.String("err", resp.Err.String()))
	}
	return nil
}

// reportTaskResc reports tasks resource usage to resource manager periodically
func (s *Server) reportTaskResc(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err := s.reportTaskRescOnce(ctx)
			if err != nil {
				return err
			}
		}
	}
}

func (s *Server) bgUpdateServerMasterClients(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case urls := <-s.cliUpdateCh:
			s.cli.UpdateClients(ctx, urls)
		}
	}
}

func (s *Server) collectMetricLoop(ctx context.Context, tickInterval time.Duration) error {
	metricRunningTask := executorTaskNumGauge.WithLabelValues("running")
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			metricRunningTask.Set(float64(s.workerRtm.TaskCount()))
		}
	}
}
