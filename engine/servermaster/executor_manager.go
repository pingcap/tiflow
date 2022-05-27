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
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pb"
	"github.com/pingcap/tiflow/engine/pkg/errors"
	"github.com/pingcap/tiflow/engine/pkg/notifier"
	"github.com/pingcap/tiflow/engine/pkg/uuid"
	"github.com/pingcap/tiflow/engine/servermaster/resource"
	"github.com/pingcap/tiflow/engine/servermaster/scheduler"
	"github.com/pingcap/tiflow/engine/test"
)

// ExecutorManager defines an interface to manager all executors
type ExecutorManager interface {
	HandleHeartbeat(req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error)
	AllocateNewExec(req *pb.RegisterExecutorRequest) (*model.NodeInfo, error)
	RegisterExec(info *model.NodeInfo)
	Start(ctx context.Context)
	// ExecutorCount returns executor count with given status
	ExecutorCount(status model.ExecutorStatus) int
	HasExecutor(executorID string) bool
	ListExecutors() []string
	CapacityProvider() scheduler.CapacityProvider
	GetAddr(executorID model.ExecutorID) (string, bool)

	// WatchExecutors returns a snapshot of all online executors plus
	// a stream of events describing changes that happen to the executors
	// after the snapshot is taken.
	WatchExecutors(ctx context.Context) (
		snap []model.ExecutorID, stream *notifier.Receiver[model.ExecutorStatusChange], err error,
	)
}

// ExecutorManagerImpl holds all the executors info, including liveness, status, resource usage.
type ExecutorManagerImpl struct {
	testContext *test.Context

	mu        sync.Mutex
	executors map[model.ExecutorID]*Executor

	idAllocator       uuid.Generator
	initHeartbeatTTL  time.Duration
	keepAliveInterval time.Duration

	rescMgr resource.RescMgr
	logRL   *rate.Limiter

	notifier *notifier.Notifier[model.ExecutorStatusChange]
}

// NewExecutorManagerImpl creates a new ExecutorManagerImpl instance
func NewExecutorManagerImpl(initHeartbeatTTL, keepAliveInterval time.Duration, ctx *test.Context) *ExecutorManagerImpl {
	return &ExecutorManagerImpl{
		testContext:       ctx,
		executors:         make(map[model.ExecutorID]*Executor),
		idAllocator:       uuid.NewGenerator(),
		initHeartbeatTTL:  initHeartbeatTTL,
		keepAliveInterval: keepAliveInterval,
		rescMgr:           resource.NewCapRescMgr(),
		logRL:             rate.NewLimiter(rate.Every(time.Second*5), 1 /*burst*/),
		notifier:          notifier.NewNotifier[model.ExecutorStatusChange](),
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
	log.L().Logger.Info("notify to offline exec")
	if test.GetGlobalTestFlag() {
		e.testContext.NotifyExecutorChange(&test.ExecutorChangeEvent{
			Tp:   test.Delete,
			Time: time.Now(),
		})
	}

	e.notifier.Notify(model.ExecutorStatusChange{
		ID: id,
		Tp: model.EventExecutorOffline,
	})
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
	e.notifier.Notify(model.ExecutorStatusChange{
		ID: info.ID,
		Tp: model.EventExecutorOnline,
	})
	e.mu.Unlock()
	e.rescMgr.Register(exec.ID, exec.Addr, model.RescUnit(exec.Capability))
}

// AllocateNewExec allocates new executor info to a give RegisterExecutorRequest
// and then registers the executor.
func (e *ExecutorManagerImpl) AllocateNewExec(req *pb.RegisterExecutorRequest) (*model.NodeInfo, error) {
	log.L().Logger.Info("allocate new executor", zap.Stringer("req", req))

	e.mu.Lock()
	info := &model.NodeInfo{
		ID:         model.ExecutorID(e.idAllocator.NewString()),
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

// HasExecutor implements ExecutorManager.HasExecutor
func (e *ExecutorManagerImpl) HasExecutor(executorID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.executors[model.ExecutorID(executorID)]
	return ok
}

// ListExecutors implements ExecutorManager.ListExecutors
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

// GetAddr implements ExecutorManager.GetAddr
func (e *ExecutorManagerImpl) GetAddr(executorID model.ExecutorID) (string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	executor, exists := e.executors[executorID]
	if !exists {
		return "", false
	}

	return executor.Addr, true
}

// WatchExecutors implements the ExecutorManager interface.
func (e *ExecutorManagerImpl) WatchExecutors(
	ctx context.Context,
) (snap []model.ExecutorID, receiver *notifier.Receiver[model.ExecutorStatusChange], err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for executorID := range e.executors {
		snap = append(snap, executorID)
	}

	if err := e.notifier.Flush(ctx); err != nil {
		return nil, nil, err
	}

	receiver = e.notifier.NewReceiver()
	return
}
