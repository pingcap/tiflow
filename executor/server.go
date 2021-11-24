package executor

import (
	"context"
	"net"
	"strings"
	"time"

	"github.com/hanfei1991/microcosm/executor/runtime"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/test"
	"github.com/hanfei1991/microcosm/test/mock"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Server struct {
	cfg     *Config
	testCtx *test.Context

	srv *grpc.Server
	cli *MasterClient
	sch *runtime.Runtime
	ID  model.ExecutorID

	lastHearbeatTime time.Time

	mockSrv mock.GrpcServer
}

func NewServer(cfg *Config, ctx *test.Context) *Server {
	s := Server{
		cfg:     cfg,
		testCtx: ctx,
	}
	return &s
}

// SubmitBatchTasks implements the pb interface.
func (s *Server) SubmitBatchTasks(ctx context.Context, req *pb.SubmitBatchTasksRequest) (*pb.SubmitBatchTasksResponse, error) {
	tasks := make([]*model.Task, 0, len(req.Tasks))
	for _, pbTask := range req.Tasks {
		task := &model.Task{
			ID:   model.TaskID(pbTask.Id),
			Op:   pbTask.Op,
			OpTp: model.OperatorType(pbTask.OpTp),
		}
		for _, id := range pbTask.Inputs {
			task.Inputs = append(task.Inputs, model.TaskID(id))
		}
		for _, id := range pbTask.Outputs {
			task.Outputs = append(task.Outputs, model.TaskID(id))
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
	exitCh := make(chan struct{}, 1)

	s.sch = runtime.NewRuntime()
	go func() {
		s.sch.Run(ctx)
		exitCh <- struct{}{}
	}()

	err = s.selfRegister(ctx)
	if err != nil {
		return err
	}
	return s.listenHeartbeat(ctx, exitCh)
}

func (s *Server) Start(ctx context.Context) error {
	if test.GlobalTestFlag {
		return s.startForTest(ctx)
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
		err1 := s.srv.Serve(rootLis)
		if err1 != nil {
			log.L().Logger.Error("start grpc server failed", zap.Error(err))
		}
		exitCh <- struct{}{}
	}()

	s.sch = runtime.NewRuntime()
	go func() {
		s.sch.Run(ctx)
		exitCh <- struct{}{}
	}()

	err = s.selfRegister(ctx)
	if err != nil {
		return err
	}

	// Start Heartbeat
	return s.listenHeartbeat(ctx, exitCh)
}

func (s *Server) selfRegister(ctx context.Context) (err error) {
	// Register myself
	s.cli, err = NewMasterClient(ctx, getJoinURLs(s.cfg.Join))
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
	s.ID = model.ExecutorID(resp.ExecutorId)
	log.L().Logger.Info("register successful", zap.Int32("id", int32(s.ID)))
	return nil
}

func (s *Server) listenHeartbeat(ctx context.Context, exitCh chan struct{}) error {
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
		case <-exitCh:
			return nil
		case t := <-ticker.C:
			if s.lastHearbeatTime.Add(s.cfg.KeepAliveTTL).Before(time.Now()) {
				return errors.ErrHeartbeat.GenWithStack("heartbeat timeout")
			}
			req := &pb.HeartbeatRequest{
				ExecutorId: int32(s.ID),
				Status:     int32(model.Running),
				Timestamp:  uint64(t.Unix()),
				// We set longer ttl for master, which is "ttl + rpc timeout", to avoid that
				// executor actually wait for a timeout when ttl is nearly up.
				Ttl: uint64(s.cfg.KeepAliveTTL.Milliseconds() + s.cfg.RPCTimeout.Milliseconds()),
			}
			resp, err := s.cli.SendHeartbeat(ctx, req, s.cfg.RPCTimeout)
			if err != nil {
				log.L().Error("heartbeat meet error", zap.Error(err))
				if s.lastHearbeatTime.Add(s.cfg.KeepAliveTTL).Before(time.Now()) {
					return errors.Wrap(errors.ErrHeartbeat, err, "rpc")
				}
				continue
			}
			if resp.Err != nil {
				log.L().Error("heartbeat meet error")
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
