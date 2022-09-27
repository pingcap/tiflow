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

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/log"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/notifier"
	"github.com/pingcap/tiflow/engine/servermaster/executormeta"
	execModel "github.com/pingcap/tiflow/engine/servermaster/executormeta/model"
	"github.com/pingcap/tiflow/engine/servermaster/resource"
	schedModel "github.com/pingcap/tiflow/engine/servermaster/scheduler/model"
	"github.com/pingcap/tiflow/engine/test"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/label"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// ExecutorManager defines an interface to manager all executors
type ExecutorManager interface {
	HandleHeartbeat(req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error)
	AllocateNewExec(ctx context.Context, req *pb.RegisterExecutorRequest) (*execModel.Executor, error)
	// ExecutorCount returns executor count with given status
	ExecutorCount(status model.ExecutorStatus) int
	HasExecutor(executorID string) bool
	ListExecutors() []*execModel.Executor
	GetAddr(executorID model.ExecutorID) (string, bool)
	Start(ctx context.Context) error
	Stop()

	// WatchExecutors returns a snapshot of all online executors plus
	// a stream of events describing changes that happen to the executors
	// after the snapshot is taken.
	WatchExecutors(ctx context.Context) (
		snap map[model.ExecutorID]string, stream *notifier.Receiver[model.ExecutorStatusChange], err error,
	)

	// GetExecutorInfos implements the interface scheduler.executorInfoProvider.
	// It is called by the scheduler as the source of truth for executors.
	GetExecutorInfos() map[model.ExecutorID]schedModel.ExecutorInfo
}

// ExecutorManagerImpl holds all the executors' info, including liveness, status, resource usage.
type ExecutorManagerImpl struct {
	testContext *test.Context
	wg          sync.WaitGroup
	metaClient  executormeta.Client

	mu        sync.Mutex
	executors map[model.ExecutorID]*Executor

	initHeartbeatTTL  time.Duration
	keepAliveInterval time.Duration

	rescMgr resource.RescMgr
	logRL   *rate.Limiter

	notifier *notifier.Notifier[model.ExecutorStatusChange]
}

// NewExecutorManagerImpl creates a new ExecutorManagerImpl instance
func NewExecutorManagerImpl(metaClient executormeta.Client, initHeartbeatTTL, keepAliveInterval time.Duration, ctx *test.Context) *ExecutorManagerImpl {
	return &ExecutorManagerImpl{
		testContext:       ctx,
		metaClient:        metaClient,
		executors:         make(map[model.ExecutorID]*Executor),
		initHeartbeatTTL:  initHeartbeatTTL,
		keepAliveInterval: keepAliveInterval,
		rescMgr:           resource.NewCapRescMgr(),
		logRL:             rate.NewLimiter(rate.Every(time.Second*5), 1 /*burst*/),
		notifier:          notifier.NewNotifier[model.ExecutorStatusChange](),
	}
}

// removeExecutorLocked removes an executor from the manager.
// Note that this method must be called with the lock held.
func (e *ExecutorManagerImpl) removeExecutorLocked(id model.ExecutorID) error {
	log.Info("begin to remove executor", zap.String("id", string(id)))
	exec, ok := e.executors[id]
	if !ok {
		// This executor has been removed
		return errors.ErrUnknownExecutorID.GenWithStackByArgs(id)
	}
	addr := exec.Address
	delete(e.executors, id)
	e.rescMgr.Unregister(id)
	log.Info("notify to offline exec")

	if test.GetGlobalTestFlag() {
		e.testContext.NotifyExecutorChange(&test.ExecutorChangeEvent{
			Tp:   test.Delete,
			Time: time.Now(),
		})
	}

	e.notifier.Notify(model.ExecutorStatusChange{
		ID:   id,
		Tp:   model.EventExecutorOffline,
		Addr: addr,
	})

	// Delete the executor from the database. This operation may take a long time,
	// and it may fail if the database is unavailable. So we don't want to hold the lock.
	// Instead, we do the deletion in a goroutine.
	//
	// We use ttl mechanism to manage the executor's life cycle. So we can tolerate
	// that a tombstone executor may be left in the database.
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		if err := e.metaClient.DeleteExecutor(ctx, id); err != nil {
			log.Warn("failed to delete executor from database", zap.String("id", string(id)), zap.Error(err))
		}
	}()

	return nil
}

// HandleHeartbeat implements pb interface,
func (e *ExecutorManagerImpl) HandleHeartbeat(req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if e.logRL.Allow() {
		log.Info("handle heart beat", zap.Stringer("req", req))
	}
	e.mu.Lock()
	execID := model.ExecutorID(req.ExecutorId)
	exec, ok := e.executors[execID]

	// executor not exists
	if !ok {
		e.mu.Unlock()
		err := errors.ErrUnknownExecutorID.FastGenByArgs(req.ExecutorId)
		return &pb.HeartbeatResponse{Err: errors.ToPBError(err)}, nil
	}
	e.mu.Unlock()

	status := model.ExecutorStatus(req.Status)
	if err := exec.heartbeat(req.Ttl, status); err != nil {
		return &pb.HeartbeatResponse{Err: errors.ToPBError(err)}, nil
	}
	usage := model.RescUnit(req.GetResourceUsage())
	if err := e.rescMgr.Update(execID, usage, usage, status); err != nil {
		return nil, err
	}
	resp := &pb.HeartbeatResponse{}
	return resp, nil
}

// registerExec registers executor to both executor manager and resource manager.
// Note that this method must be called with the lock held.
func (e *ExecutorManagerImpl) registerExecLocked(executorMeta *execModel.Executor) {
	log.Info("register executor", zap.Any("executor", executorMeta))
	exec := &Executor{
		Executor:       *executorMeta,
		lastUpdateTime: time.Now(),
		heartbeatTTL:   e.initHeartbeatTTL,
		status:         model.Initing,
		logRL:          rate.NewLimiter(rate.Every(time.Second*5), 1 /*burst*/),
	}
	e.executors[executorMeta.ID] = exec
	e.notifier.Notify(model.ExecutorStatusChange{
		ID:   executorMeta.ID,
		Tp:   model.EventExecutorOnline,
		Addr: executorMeta.Address,
	})
	e.rescMgr.Register(exec.ID, exec.Address, model.RescUnit(exec.Capability))
}

// AllocateNewExec allocates new executor info to a give RegisterExecutorRequest
// and then registers the executor.
func (e *ExecutorManagerImpl) AllocateNewExec(ctx context.Context, req *pb.RegisterExecutorRequest) (*execModel.Executor, error) {
	pbExecutor := req.Executor
	log.Info("allocate new executor", zap.Stringer("executor", pbExecutor))

	e.mu.Lock()
	var executorID model.ExecutorID
	for {
		executorID = model.ExecutorID(generateNodeID(pbExecutor.GetName()))
		if _, ok := e.executors[executorID]; !ok {
			break
		}
	}

	labelSet, err := label.NewSetFromMap(req.GetExecutor().GetLabels())
	if err != nil {
		return nil, err
	}
	executorMeta := &execModel.Executor{
		ID:         executorID,
		Name:       pbExecutor.GetName(),
		Address:    pbExecutor.GetAddress(),
		Capability: int(pbExecutor.GetCapability()),
		Labels:     execModel.LabelSet(labelSet),
	}
	e.registerExecLocked(executorMeta)
	e.mu.Unlock()

	// Store the executor info to database.
	// If any error occurs, client shouldn't use the executor.
	// The executor in the map will be removed after the ttl expires.
	if err := e.metaClient.CreateExecutor(ctx, executorMeta); err != nil {
		return nil, perrors.Trace(err)
	}

	return executorMeta, nil
}

// HasExecutor implements ExecutorManager.HasExecutor
func (e *ExecutorManagerImpl) HasExecutor(executorID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.executors[model.ExecutorID(executorID)]
	return ok
}

// ListExecutors implements ExecutorManager.ListExecutors
func (e *ExecutorManagerImpl) ListExecutors() []*execModel.Executor {
	e.mu.Lock()
	defer e.mu.Unlock()
	ret := make([]*execModel.Executor, 0, len(e.executors))
	for _, exec := range e.executors {
		execMeta := exec.Executor
		ret = append(ret, &execMeta)
	}
	return ret
}

// Executor records the status of an executor instance.
type Executor struct {
	execModel.Executor
	status model.ExecutorStatus

	mu sync.RWMutex
	// Last heartbeat
	lastUpdateTime time.Time
	heartbeatTTL   time.Duration
	logRL          *rate.Limiter
}

func (e *Executor) checkAlive() bool {
	if e.logRL.Allow() {
		log.Info("check alive", zap.String("exec", string(e.ID)))
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.status == model.Tombstone {
		return false
	}
	if e.lastUpdateTime.Add(e.heartbeatTTL).Before(time.Now()) {
		e.status = model.Tombstone
		return false
	}
	return true
}

func (e *Executor) heartbeat(ttl uint64, status model.ExecutorStatus) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.status == model.Tombstone {
		return errors.ErrTombstoneExecutor.FastGenByArgs(e.ID)
	}
	e.lastUpdateTime = time.Now()
	e.heartbeatTTL = time.Duration(ttl) * time.Millisecond
	e.status = status
	return nil
}

func (e *Executor) statusEqual(status model.ExecutorStatus) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.status == status
}

// Start implements ExecutorManager.Start. It starts a background goroutine to
// check whether all executors are alive periodically.
func (e *ExecutorManagerImpl) Start(ctx context.Context) error {
	if err := e.resetExecutors(ctx); err != nil {
		return perrors.Errorf("failed to reset executors: %v", err)
	}

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		ticker := time.NewTicker(e.keepAliveInterval)
		defer func() {
			ticker.Stop()
			log.Info("check executor alive finished")
		}()
		for {
			select {
			case <-ticker.C:
				err := e.checkAliveImpl()
				if err != nil {
					log.Info("check alive meet error", zap.Error(err))
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

// Stop implements ExecutorManager.Stop
func (e *ExecutorManagerImpl) Stop() {
	e.wg.Wait()
	e.notifier.Close()
}

func (e *ExecutorManagerImpl) checkAliveImpl() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	for id, exec := range e.executors {
		if !exec.checkAlive() {
			err := e.removeExecutorLocked(id)
			return err
		}
	}
	return nil
}

// ExecutorCount implements ExecutorManager.ExecutorCount
func (e *ExecutorManagerImpl) ExecutorCount(status model.ExecutorStatus) (count int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, executor := range e.executors {
		if executor.statusEqual(status) {
			count++
		}
	}
	return
}

// GetAddr implements ExecutorManager.GetAddr
func (e *ExecutorManagerImpl) GetAddr(executorID model.ExecutorID) (string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	executor, exists := e.executors[executorID]
	if !exists {
		return "", false
	}

	return executor.Address, true
}

func (e *ExecutorManagerImpl) resetExecutors(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	orphanExecutorIDs := make(map[model.ExecutorID]struct{})
	for id := range e.executors {
		orphanExecutorIDs[id] = struct{}{}
	}

	executors, err := e.metaClient.QueryExecutors(ctx)
	if err != nil {
		return perrors.Trace(err)
	}
	for _, executor := range executors {
		e.registerExecLocked(executor)
		delete(orphanExecutorIDs, executor.ID)
	}

	// Clean up executors that are not in the meta.
	for id := range orphanExecutorIDs {
		if err := e.removeExecutorLocked(id); err != nil {
			return perrors.Trace(err)
		}
	}

	return nil
}

// WatchExecutors implements the ExecutorManager interface.
func (e *ExecutorManagerImpl) WatchExecutors(
	ctx context.Context,
) (snap map[model.ExecutorID]string, receiver *notifier.Receiver[model.ExecutorStatusChange], err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	snap = make(map[model.ExecutorID]string, len(e.executors))
	for executorID, exec := range e.executors {
		snap[executorID] = exec.Address
	}

	if err := e.notifier.Flush(ctx); err != nil {
		return nil, nil, err
	}

	receiver = e.notifier.NewReceiver()
	return
}

// GetExecutorInfos returns necessary information on the executor that
// is needed for scheduling.
func (e *ExecutorManagerImpl) GetExecutorInfos() map[model.ExecutorID]schedModel.ExecutorInfo {
	e.mu.Lock()
	defer e.mu.Unlock()

	ret := make(map[model.ExecutorID]schedModel.ExecutorInfo, len(e.executors))
	for id, exec := range e.executors {
		resStatus, ok := e.rescMgr.CapacityForExecutor(id)
		if !ok {
			continue
		}
		schedInfo := schedModel.ExecutorInfo{
			ID:             id,
			ResourceStatus: *resStatus,
			Labels:         label.Set(exec.Labels),
		}
		ret[id] = schedInfo
	}
	return ret
}
