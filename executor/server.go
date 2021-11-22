package executor

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/hanfei1991/microcosom/executor/runtime"
	"github.com/hanfei1991/microcosom/model"
	"github.com/hanfei1991/microcosom/pb"
	"github.com/hanfei1991/microcosom/pkg/terror"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Server struct {
	cfg *Config

	srv *grpc.Server
	cli *MasterClient
	sch *runtime.Runtime
	ID  model.ExecutorID

	lastHearbeatTime time.Time
}

func NewServer(cfg *Config) *Server {
	s := Server{
		cfg: cfg,
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
		resp.Err = terror.ToPBError(err)
	}
	return resp, nil
}

// CancelBatchTasks implements pb interface.
func (s *Server) CancelBatchTasks(ctx context.Context, req *pb.CancelBatchTasksRequest) (*pb.CancelBatchTasksResponse, error) {
	return &pb.CancelBatchTasksResponse{}, nil
}

func (s *Server) Start(ctx context.Context) error {
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

	// Register myself
	s.cli, err = NewMasterClient(ctx, s.cfg)
	if err != nil {
		return err
	}
	log.L().Logger.Info("master client init successful")
	registerReq := &pb.RegisterExecutorRequest{
		Address:    s.cfg.WorkerAddr,
		Capability: 100,
	}

	resp, err := s.cli.RegisterExecutor(ctx, registerReq)
	if err != nil {
		return err
	}
	s.ID = model.ExecutorID(resp.ExecutorId)
	log.L().Logger.Info("register successful", zap.Int32("id", int32(s.ID)))

	// Start Heartbeat
	ticker := time.NewTicker(time.Duration(s.cfg.KeepAliveInterval) * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-exitCh:
			return nil
		case t := <-ticker.C:
			req := &pb.HeartbeatRequest{
				ExecutorId: int32(s.ID),
				Status:     int32(model.Running),
				Timestamp:  uint64(t.Unix()),
				Ttl:        uint64(s.cfg.KeepAliveTTL),
			}
			// FIXME: set timeout.
			resp, err := s.cli.SendHeartbeat(ctx, req)
			if err != nil {
				log.L().Error("heartbeat meet error", zap.Error(err))
				if s.lastHearbeatTime.Add(time.Duration(s.cfg.KeepAliveTTL) * time.Millisecond).Before(time.Now()) {
					return err
				}
				continue
			}
			if resp.Err != nil {
				log.L().Error("heartbeat meet error")
				switch resp.Err.Code {
				case pb.ErrorCode_UnknownExecutor, pb.ErrorCode_TombstoneExecutor:
					return errors.New(resp.Err.Message)
				}
				continue
			}
			s.lastHearbeatTime = t
			log.L().Info("heartbeat success")
		}
	}
}
