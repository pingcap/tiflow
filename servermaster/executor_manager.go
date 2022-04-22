package servermaster

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/autoid"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/ha"
	"github.com/hanfei1991/microcosm/servermaster/resource"
	"github.com/hanfei1991/microcosm/servermaster/scheduler"
	"github.com/hanfei1991/microcosm/test"
)

// ExecutorManager defines an interface to manager all executors
type ExecutorManager interface {
	HandleHeartbeat(req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error)
	AllocateNewExec(req *pb.RegisterExecutorRequest) (*model.NodeInfo, error)
	RegisterExec(info *model.NodeInfo)
	Start(ctx context.Context)
	// Count returns executor count with given status
	ExecutorCount(status model.ExecutorStatus) int
	HasExecutor(executorID string) bool
	ListExecutors() []string
	CapacityProvider() scheduler.CapacityProvider
	GetAddr(executorID model.ExecutorID) (string, bool)
}

// ExecutorManagerImpl holds all the executors info, including liveness, status, resource usage.
type ExecutorManagerImpl struct {
	testContext *test.Context

	mu        sync.Mutex
	executors map[model.ExecutorID]*Executor

	idAllocator       *autoid.UUIDAllocator
	initHeartbeatTTL  time.Duration
	keepAliveInterval time.Duration

	// TODO: complete ha store.
	haStore ha.HAStore // nolint:structcheck,unused

	rescMgr resource.RescMgr
	logRL   *rate.Limiter
}

func NewExecutorManagerImpl(initHeartbeatTTL, keepAliveInterval time.Duration, ctx *test.Context) *ExecutorManagerImpl {
	return &ExecutorManagerImpl{
		testContext:       ctx,
		executors:         make(map[model.ExecutorID]*Executor),
		idAllocator:       autoid.NewUUIDAllocator(),
		initHeartbeatTTL:  initHeartbeatTTL,
		keepAliveInterval: keepAliveInterval,
		rescMgr:           resource.NewCapRescMgr(),
		logRL:             rate.NewLimiter(rate.Every(time.Second*5), 1 /*burst*/),
	}
}

func (e *ExecutorManagerImpl) removeExecutorImpl(id model.ExecutorID) error {
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
	log.L().Logger.Info("notify to offline exec")
	if test.GetGlobalTestFlag() {
		e.testContext.NotifyExecutorChange(&test.ExecutorChangeEvent{
			Tp:   test.Delete,
			Time: time.Now(),
		})
	}
	return nil
}

// HandleHeartbeat implements pb interface,
func (e *ExecutorManagerImpl) HandleHeartbeat(req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if e.logRL.Allow() {
		log.L().Logger.Info("handle heart beat", zap.Stringer("req", req))
	}
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
	usage := model.RescUnit(req.GetResourceUsage())
	// TODO: update reserve resources by heartbeats.
	err := e.rescMgr.Update(exec.ID, usage, usage, exec.Status)
	if err != nil {
		return nil, err
	}
	resp := &pb.HeartbeatResponse{}
	return resp, nil
}

// RegisterExec registers executor to both executor manager and resource manager
func (e *ExecutorManagerImpl) RegisterExec(info *model.NodeInfo) {
	log.L().Info("register executor", zap.Any("info", info))
	exec := &Executor{
		NodeInfo:       *info,
		lastUpdateTime: time.Now(),
		heartbeatTTL:   e.initHeartbeatTTL,
		Status:         model.Initing,
		logRL:          rate.NewLimiter(rate.Every(time.Second*5), 1 /*burst*/),
	}
	e.mu.Lock()
	e.executors[info.ID] = exec
	e.mu.Unlock()
	e.rescMgr.Register(exec.ID, exec.Addr, model.RescUnit(exec.Capability))
}

// AllocateNewExec allocates new executor info to a give RegisterExecutorRequest
// and then registers the executor.
func (e *ExecutorManagerImpl) AllocateNewExec(req *pb.RegisterExecutorRequest) (*model.NodeInfo, error) {
	log.L().Logger.Info("allocate new executor", zap.Stringer("req", req))

	e.mu.Lock()
	info := &model.NodeInfo{
		ID:         model.ExecutorID(e.idAllocator.AllocID()),
		Addr:       req.Address,
		Capability: int(req.Capability),
	}
	if _, ok := e.executors[info.ID]; ok {
		e.mu.Unlock()
		return nil, errors.ErrExecutorDupRegister.GenWithStackByArgs()
	}
	e.mu.Unlock()

	e.RegisterExec(info)
	return info, nil
}

func (e *ExecutorManagerImpl) HasExecutor(executorID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.executors[model.ExecutorID(executorID)]
	return ok
}

func (e *ExecutorManagerImpl) ListExecutors() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	ret := make([]string, 0, len(e.executors))
	for id := range e.executors {
		ret = append(ret, string(id))
	}
	return ret
}

// Executor records the status of an executor instance.
type Executor struct {
	model.NodeInfo
	Status model.ExecutorStatus

	mu sync.Mutex
	// Last heartbeat
	lastUpdateTime time.Time
	heartbeatTTL   time.Duration
	logRL          *rate.Limiter
}

func (e *Executor) checkAlive() bool {
	if e.logRL.Allow() {
		log.L().Logger.Info("check alive", zap.String("exec", string(e.NodeInfo.ID)))
	}

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
func (e *ExecutorManagerImpl) Start(ctx context.Context) {
	go e.checkAlive(ctx)
}

// checkAlive goroutine checks whether all the executors are alive periodically.
func (e *ExecutorManagerImpl) checkAlive(ctx context.Context) {
	ticker := time.NewTicker(e.keepAliveInterval)
	defer func() {
		ticker.Stop()
		log.L().Logger.Info("check alive finished")
	}()
	for {
		select {
		case <-ticker.C:
			err := e.checkAliveImpl()
			if err != nil {
				log.L().Logger.Info("check alive meet error", zap.Error(err))
			}
		case <-ctx.Done():
		}
	}
}

func (e *ExecutorManagerImpl) checkAliveImpl() error {
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

// ExecutorCount implements ExecutorManager.ExecutorCount
func (e *ExecutorManagerImpl) ExecutorCount(status model.ExecutorStatus) (count int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, executor := range e.executors {
		if executor.Status == status {
			count++
		}
	}
	return
}

// CapacityProvider returns the internal rescMgr as a scheduler.CapacityProvider.
func (e *ExecutorManagerImpl) CapacityProvider() scheduler.CapacityProvider {
	return e.rescMgr
}

func (e *ExecutorManagerImpl) GetAddr(executorID model.ExecutorID) (string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	executor, exists := e.executors[executorID]
	if !exists {
		return "", false
	}

	return executor.Addr, true
}
