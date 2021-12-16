package cluster

import (
	"context"
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/master/resource"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/autoid"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/ha"
	"github.com/hanfei1991/microcosm/test"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"go.uber.org/zap"
)

// ExecutorManager holds all the executors info, including liveness, status, resource usage.
type ExecutorManager struct {
	testContext *test.Context

	mu          sync.Mutex
	executors   map[model.ExecutorID]*Executor
	offExecutor chan model.ExecutorID

	idAllocator       *autoid.UUIDAllocator
	initHeartbeatTTL  time.Duration
	keepAliveInterval time.Duration

	// TODO: complete ha store.
	haStore ha.HAStore // nolint:structcheck,unused

	rescMgr resource.RescMgr
}

func NewExecutorManager(offExec chan model.ExecutorID, initHeartbeatTTL, keepAliveInterval time.Duration, ctx *test.Context) *ExecutorManager {
	return &ExecutorManager{
		testContext:       ctx,
		executors:         make(map[model.ExecutorID]*Executor),
		idAllocator:       autoid.NewUUIDAllocator(),
		offExecutor:       offExec,
		initHeartbeatTTL:  initHeartbeatTTL,
		keepAliveInterval: keepAliveInterval,
		rescMgr:           resource.NewCapRescMgr(),
	}
}

func (e *ExecutorManager) removeExecutorImpl(id model.ExecutorID) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	log.L().Logger.Info("begin to remove executor", zap.String("id", string(id)))
	_, ok := e.executors[id]
	if !ok {
		// This executor has been removed
		return errors.ErrUnknownExecutorID.GenWithStackByArgs(id)
	}
	delete(e.executors, id)
	e.rescMgr.Unregister(id)
	//err := e.haStore.Del(exec.EtcdKey())
	//if err != nil {
	//	return err
	//}
	e.offExecutor <- id
	log.L().Logger.Info("notify to offline exec")
	if test.GlobalTestFlag {
		e.testContext.NotifyExecutorChange(&test.ExecutorChangeEvent{
			Tp:   test.Delete,
			Time: time.Now(),
		})
	}
	return nil
}

// HandleHeartbeat implements pb interface,
func (e *ExecutorManager) HandleHeartbeat(req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	log.L().Logger.Info("handle heart beat", zap.String("id", req.ExecutorId))
	e.mu.Lock()
	exec, ok := e.executors[model.ExecutorID(req.ExecutorId)]

	// executor not exists
	if !ok {
		e.mu.Unlock()
		err := errors.ErrUnknownExecutorID.FastGenByArgs(req.ExecutorId)
		return &pb.HeartbeatResponse{Err: errors.ToPBError(err)}, nil
	}
	e.mu.Unlock()

	// exists and apply the resource usage.
	exec.mu.Lock()
	defer exec.mu.Unlock()
	if exec.Status == model.Tombstone {
		err := errors.ErrTombstoneExecutor.FastGenByArgs(req.ExecutorId)
		return &pb.HeartbeatResponse{Err: errors.ToPBError(err)}, nil
	}
	exec.lastUpdateTime = time.Now()
	exec.heartbeatTTL = time.Duration(req.Ttl) * time.Millisecond
	exec.Status = model.ExecutorStatus(req.Status)
	usage := resource.RescUnit(req.GetResourceUsage())
	err := e.rescMgr.Update(exec.ID, usage, exec.Status)
	if err != nil {
		return nil, err
	}
	resp := &pb.HeartbeatResponse{}
	return resp, nil
}

// AddExecutor processes the `RegisterExecutorRequest`.
func (e *ExecutorManager) AddExecutor(req *pb.RegisterExecutorRequest) (*model.ExecutorInfo, error) {
	log.L().Logger.Info("add executor", zap.String("addr", req.Address))
	e.mu.Lock()
	info := &model.ExecutorInfo{
		ID:         model.ExecutorID(e.idAllocator.AllocID()),
		Addr:       req.Address,
		Capability: int(req.Capability),
	}
	if _, ok := e.executors[info.ID]; ok {
		e.mu.Unlock()
		return nil, errors.ErrExecutorDupRegister.GenWithStackByArgs()
	}
	e.mu.Unlock()

	// Following part is to bootstrap the executor.
	exec := &Executor{
		ExecutorInfo: *info,

		lastUpdateTime: time.Now(),
		heartbeatTTL:   e.initHeartbeatTTL,
		Status:         model.Initing,
	}

	// Persistant
	//value, err := info.ToJSON()
	//if err != nil {
	//return nil, err
	//}
	//	e.haStore.Put(info.EtcdKey(), value)

	e.mu.Lock()
	e.executors[info.ID] = exec
	e.mu.Unlock()
	e.rescMgr.Register(exec.ID, exec.Addr, resource.RescUnit(exec.Capability))
	return info, nil
}

func (e *ExecutorManager) Allocate(tasks []*pb.ScheduleTask) (bool, *pb.TaskSchedulerResponse) {
	return e.rescMgr.Allocate(tasks)
}

// Executor records the status of an executor instance.
type Executor struct {
	model.ExecutorInfo
	Status model.ExecutorStatus

	mu sync.Mutex
	// Last heartbeat
	lastUpdateTime time.Time
	heartbeatTTL   time.Duration
}

func (e *Executor) checkAlive() bool {
	log.L().Logger.Info("check alive", zap.String("exec", string(e.ExecutorInfo.ID)))

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.Status == model.Tombstone {
		return false
	}
	if e.lastUpdateTime.Add(e.heartbeatTTL).Before(time.Now()) {
		e.Status = model.Tombstone
		return false
	}
	return true
}

// Start check alive goroutine.
func (e *ExecutorManager) Start(ctx context.Context) {
	go e.checkAlive(ctx)
}

// checkAlive goroutine checks whether all the executors are alive periodically.
func (e *ExecutorManager) checkAlive(ctx context.Context) {
	tick := time.NewTicker(1 * time.Second)
	defer func() { log.L().Logger.Info("check alive finished") }()
	for {
		select {
		case <-tick.C:
			err := e.checkAliveImpl()
			if err != nil {
				log.L().Logger.Info("check alive meet error", zap.Error(err))
			}
		case <-ctx.Done():
		}
	}
}

func (e *ExecutorManager) checkAliveImpl() error {
	e.mu.Lock()
	for id, exec := range e.executors {
		if !exec.checkAlive() {
			e.mu.Unlock()
			err := e.removeExecutorImpl(id)
			return err
		}
	}
	e.mu.Unlock()
	return nil
}
