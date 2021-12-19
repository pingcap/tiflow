package executor

import (
	"context"
	"net"
	"strings"
	"time"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/executor/runtime"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/test"
	"github.com/hanfei1991/microcosm/test/mock"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/pkg/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

type Server struct {
	cfg     *Config
	testCtx *test.Context

	srv  *grpc.Server
	cli  *client.MasterClient
	sch  *runtime.Runtime
	info *model.ExecutorInfo

	lastHearbeatTime time.Time

	mockSrv mock.GrpcServer

	cancel  func()
	closeCh chan bool

	metastore metadata.MetaKV
}

func NewServer(cfg *Config, ctx *test.Context) *Server {
	s := Server{
		cfg:     cfg,
		testCtx: ctx,
		closeCh: make(chan bool, 1),
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

func (s *Server) Stop() {
	if s.srv != nil {
		s.srv.Stop()
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
		defer s.cancel()
		s.sch.Run(ctx, 10)
	}()

	err = s.selfRegister(ctx)
	if err != nil {
		return err
	}
	go func() {
		defer s.cancel()
		err := s.keepHeartbeat(ctx)
		log.L().Info("heartbeat quits", zap.Error(err))
	}()
	return nil
}

func (s *Server) Start(ctx context.Context) error {
	ctx1, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	if test.GlobalTestFlag {
		return s.startForTest(ctx1)
	}
	// Start grpc server
	rootLis, err := net.Listen("tcp", s.cfg.WorkerAddr)
	if err != nil {
		return err
	}

	log.L().Logger.Info("listen address", zap.String("addr", s.cfg.WorkerAddr))

	s.srv = grpc.NewServer()
	pb.RegisterExecutorServer(s.srv, s)

	exitCh := make(chan struct{}, 1)

	go func() {
		defer s.close()
		err1 := s.srv.Serve(rootLis)
		if err1 != nil {
			log.L().Logger.Error("start grpc server failed", zap.Error(err))
		}
		exitCh <- struct{}{}
	}()

	s.sch = runtime.NewRuntime(nil)
	go func() {
		defer s.close()
		s.sch.Run(ctx1, 10)
	}()

	err = s.selfRegister(ctx1)
	if err != nil {
		return err
	}

	// connects to metastore and maintains a etcd session
	go func() {
		err := s.keepalive(ctx)
		if err != nil {
			log.L().Error("master keepalive failed", zap.Error(err))
			s.close()
		}
	}()

	// Start Heartbeat
	go func() {
		defer s.close()
		err := s.keepHeartbeat(ctx1)
		log.L().Info("heartbeat quits", zap.Error(err))
	}()
	return nil
}

// close the executor server, this function can be executed reentrantly
func (s *Server) close() {
	s.cancel()
	select {
	case s.closeCh <- true:
	default:
	}
}

// CloseCh returns a bool chan to notify server closed event
func (s *Server) CloseCh() chan bool {
	return s.closeCh
}

func (s *Server) keepalive(ctx context.Context) error {
	// query service discovery metastore, which is an embed etcd underlying
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
		Endpoints:   strings.Split(resp.GetAddress(), ","),
		Context:     ctx,
		LogConfig:   &logConfig,
		DialTimeout: 5 * time.Second,
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
	session, err := concurrency.NewSession(etcdCli, concurrency.WithTTL(s.cfg.SessionTTL))
	if err != nil {
		return err
	}
	// TODO: we share system metastore with service discovery etcd, in the future
	// we could separate them
	s.metastore = metadata.NewMetaEtcd(etcdCli)
	value, err := s.info.ToJSON()
	if err != nil {
		return err
	}
	_, err = s.metastore.Put(ctx, s.info.EtcdKey(), value, clientv3.WithLease(session.Lease()))
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-session.Done():
		return errors.ErrExecutorSessionDone.GenWithStackByArgs(s.info.ID)
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
		if test.GlobalTestFlag {
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
			resp, err := s.cli.SendHeartbeat(ctx, req, s.cfg.RPCTimeout)
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
			// We aim to keep lastHbTime of executor consistant with lastHbTime of Master.
			// If we set the heartbeat time of executor to the start time of rpc, it will
			// be a little bit earlier than the heartbeat time of master, which is safe.
			// In contrast, if we set it to the end time of rpc, it might be a little bit
			// later than master's, which might cause that master wait for less time than executor.
			// This gap is unsafe.
			s.lastHearbeatTime = t
			log.L().Info("heartbeat success")
		}
	}
}

func getJoinURLs(addrs string) []string {
	return strings.Split(addrs, ",")
}
