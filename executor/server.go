package executor

import (
	"context"
	"strings"
	"time"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/executor/runtime"
	"github.com/hanfei1991/microcosm/executor/worker"
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/lib/fake"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/autoid"
	"github.com/hanfei1991/microcosm/pkg/config"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/hanfei1991/microcosm/pkg/srvdiscovery"
	"github.com/hanfei1991/microcosm/test"
	"github.com/hanfei1991/microcosm/test/mock"
	"github.com/pingcap/tiflow/dm/pkg/log"
	p2pImpl "github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/tcpserver"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/pkg/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

type metaStoreSession interface {
	Done() <-chan struct{}
}

type discoveryConnectFn func(ctx context.Context) (metaStoreSession, error)

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
	info        *model.ExecutorInfo
	idAllocator *autoid.UUIDAllocator

	lastHearbeatTime time.Time

	mockSrv mock.GrpcServer

	metastore          metadata.MetaKV
	discovery          srvdiscovery.Discovery
	discoveryConnector discoveryConnectFn
	p2pMsgRouter       p2pImpl.MessageRouter
}

func NewServer(cfg *Config, ctx *test.Context) *Server {
	s := Server{
		cfg:         cfg,
		testCtx:     ctx,
		cliUpdateCh: make(chan []string),
		idAllocator: autoid.NewUUIDAllocator(),
	}
	s.discoveryConnector = s.connectToEtcdDiscovery
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
	workerID := s.idAllocator.AllocID()
	worker := lib.NewBaseWorker(fake.NewDummyWorkerImpl(dcontext.Context{Ctx: ctx}), s.msgServer.MakeHandlerManager(), p2p.NewMessageSender(p2p.NewMessageRouter(string(s.info.ID), s.cfg.AdvertiseAddr)), s.metastore, lib.WorkerID(workerID), lib.MasterID(req.MasterId))
	s.workerRtm.AddWorker(worker)
	return &pb.DispatchTaskResponse{
		ErrorCode: pb.DispatchTaskErrorCode_OK,
		WorkerId:  workerID,
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
	s.msgServer, err = p2p.NewMessageRPCService(string(s.info.ID), nil)
	if err != nil {
		return err
	}
	wg.Go(func() error {
		return s.msgServer.Serve(ctx, s.tcpServer.GrpcListener())
	})
	return nil
}

func (s *Server) Run(ctx context.Context) error {
	if test.GetGlobalTestFlag() {
		return s.startForTest(ctx)
	}

	wg, ctx := errgroup.WithContext(ctx)

	err := s.startTCPService(ctx, wg)
	if err != nil {
		return err
	}

	s.sch = runtime.NewRuntime(nil)
	wg.Go(func() error {
		s.sch.Run(ctx, 10)
		return nil
	})

	pollCon := s.cfg.PollConcurrency
	s.workerRtm = worker.NewRuntime(ctx)
	s.workerRtm.Start(pollCon)

	err = s.startMsgService(ctx, wg)
	if err != nil {
		return err
	}

	err = s.selfRegister(ctx)
	if err != nil {
		return err
	}

	// connects to metastore and maintains a etcd session
	wg.Go(func() error {
		return s.discoveryKeepalive(ctx)
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

	return wg.Wait()
}

// startTCPService starts grpc server and http server
func (s *Server) startTCPService(ctx context.Context, wg *errgroup.Group) error {
	tcpServer, err := tcpserver.NewTCPServer(s.cfg.WorkerAddr, &security.Credential{})
	if err != nil {
		return err
	}
	s.tcpServer = tcpServer
	s.grpcSrv = grpc.NewServer()
	pb.RegisterExecutorServer(s.grpcSrv, s)
	log.L().Logger.Info("listen address", zap.String("addr", s.cfg.WorkerAddr))

	wg.Go(func() error {
		return s.tcpServer.Run(ctx)
	})

	wg.Go(func() error {
		return s.grpcSrv.Serve(s.tcpServer.GrpcListener())
	})

	wg.Go(func() error {
		return debugHandler(s.tcpServer.HTTP1Listener())
	})

	return nil
}

func (s *Server) connectToEtcdDiscovery(ctx context.Context) (metaStoreSession, error) {
	// query service discovery metastore, which is an embed etcd underlying
	resp, err := s.cli.QueryMetaStore(
		ctx,
		&pb.QueryMetaStoreRequest{Tp: pb.StoreType_ServiceDiscovery},
		s.cfg.RPCTimeout,
	)
	if err != nil {
		return nil, err
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
		return nil, err
	}
	session, err := s.createSession(ctx, etcdCli)
	if err != nil {
		return nil, err
	}

	// initialize a new service discovery, if old discovery exists, clones its
	// snapshot to the new one.
	old := s.discovery
	s.discovery = srvdiscovery.NewEtcdSrvDiscovery(
		etcdCli, adapter.ExecutorInfoKeyAdapter, defaultDiscoverTicker)
	if old != nil {
		s.discovery.CopySnapshot(old.SnapshotClone())
	}

	return session, nil
}

func (s *Server) createSession(ctx context.Context, etcdCli *clientv3.Client) (metaStoreSession, error) {
	session, err := concurrency.NewSession(etcdCli, concurrency.WithTTL(s.cfg.SessionTTL))
	if err != nil {
		return nil, err
	}
	// TODO: we share system metastore with service discovery etcd, in the future
	// we could separate them
	s.metastore = metadata.NewMetaEtcd(etcdCli)
	value, err := s.info.ToJSON()
	if err != nil {
		return nil, err
	}
	_, err = s.metastore.Put(ctx, s.info.EtcdKey(), value, clientv3.WithLease(session.Lease()))
	if err != nil {
		return nil, err
	}
	return session, nil
}

func (s *Server) discoveryKeepalive(ctx context.Context) error {
	var (
		session metaStoreSession
		err     error
	)
	session, err = s.discoveryConnector(ctx)
	if err != nil {
		return err
	}
	executors, err := s.discovery.Snapshot(ctx)
	if err != nil {
		return err
	}
	for uuid, exec := range executors {
		if s.p2pMsgRouter != nil {
			s.p2pMsgRouter.AddPeer(uuid, exec.Addr)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-session.Done():
			log.L().Warn("metastore session is done", zap.String("executor-id", string(s.info.ID)))
			session, err = s.discoveryConnector(ctx)
			if err != nil {
				return err
			}
		case resp := <-s.discovery.Watch(ctx):
			if resp.Err != nil {
				log.L().Warn("discovery watch met error", zap.Error(resp.Err))
				session, err = s.discoveryConnector(ctx)
				if err != nil {
					return err
				}
				continue
			}
			for uuid, add := range resp.AddSet {
				if s.p2pMsgRouter != nil {
					s.p2pMsgRouter.AddPeer(uuid, add.Addr)
				}
			}
			for uuid := range resp.DelSet {
				if s.p2pMsgRouter != nil {
					s.p2pMsgRouter.RemovePeer(uuid)
				}
			}
		}
	}
}

func (s *Server) selfRegister(ctx context.Context) (err error) {
	// Register myself
	s.cli, err = client.NewMasterClient(ctx, getJoinURLs(s.cfg.Join))
	if err != nil {
		return err
	}
	log.L().Logger.Info("master client init successful")
	registerReq := &pb.RegisterExecutorRequest{
		Address:    s.cfg.WorkerAddr,
		Capability: 100,
	}

	resp, err := s.cli.RegisterExecutor(ctx, registerReq, s.cfg.RPCTimeout)
	if err != nil {
		return err
	}
	s.info = &model.ExecutorInfo{
		ID:   model.ExecutorID(resp.ExecutorId),
		Addr: s.cfg.WorkerAddr,
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
			log.L().Info("heartbeat success", zap.String("leader", resp.Leader), zap.Strings("members", resp.Addrs))
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
