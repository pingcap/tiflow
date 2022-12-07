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

package master

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tiflow/engine/framework/config"
	"github.com/pingcap/tiflow/engine/framework/metadata"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/framework/statusutil"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/pingcap/tiflow/engine/pkg/errctx"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

type (
	// Callback alias to worker callback function when there is no error along with.
	Callback = func(ctx context.Context, handle WorkerHandle) error
	// CallbackWithError alias to worker callback function when there could be an error along with.
	CallbackWithError = func(ctx context.Context, handle WorkerHandle, err error) error
)

// WorkerManager manages all workers belonging to a job master
type WorkerManager struct {
	mu            sync.Mutex
	workerEntries map[frameModel.WorkerID]*workerEntry
	state         workerManagerState

	workerMetaClient *metadata.WorkerStatusClient
	messageSender    p2p.MessageSender

	masterID frameModel.MasterID
	epoch    frameModel.Epoch

	onWorkerOnlined       Callback
	onWorkerOfflined      CallbackWithError
	onWorkerStatusUpdated Callback
	onWorkerDispatched    CallbackWithError

	eventQueue chan *masterEvent
	closeCh    chan struct{}
	errCenter  *errctx.ErrCenter
	// allWorkersReady is **closed** when a heartbeat has been received
	// from all workers recorded in meta.
	allWorkersReady chan struct{}
	logger          *zap.Logger

	clock clock.Clock

	timeouts config.TimeoutConfig

	wg sync.WaitGroup
}

type workerManagerState int32

const (
	workerManagerReady = workerManagerState(iota + 1)
	workerManagerLoadingMeta
	workerManagerWaitingHeartbeat
)

// NewWorkerManager creates a new WorkerManager instance
func NewWorkerManager(
	masterID frameModel.MasterID,
	epoch frameModel.Epoch,
	meta pkgOrm.Client,
	messageSender p2p.MessageSender,
	onWorkerOnline Callback,
	onWorkerOffline CallbackWithError,
	onWorkerStatusUpdated Callback,
	onWorkerDispatched CallbackWithError,
	isInit bool,
	timeoutConfig config.TimeoutConfig,
	clock clock.Clock,
) *WorkerManager {
	state := workerManagerReady
	if !isInit {
		state = workerManagerLoadingMeta
	}

	ret := &WorkerManager{
		workerEntries: make(map[frameModel.WorkerID]*workerEntry),
		state:         state,

		workerMetaClient: metadata.NewWorkerStatusClient(masterID, meta),
		messageSender:    messageSender,

		masterID: masterID,
		epoch:    epoch,

		onWorkerOnlined:       onWorkerOnline,
		onWorkerOfflined:      onWorkerOffline,
		onWorkerStatusUpdated: onWorkerStatusUpdated,
		onWorkerDispatched:    onWorkerDispatched,

		eventQueue:      make(chan *masterEvent, 1024),
		closeCh:         make(chan struct{}),
		errCenter:       errctx.NewErrCenter(),
		allWorkersReady: make(chan struct{}),

		clock:    clock,
		timeouts: timeoutConfig,
	}

	ret.wg.Add(1)
	go func() {
		defer ret.wg.Done()
		if err := ret.runBackgroundChecker(); err != nil {
			ret.errCenter.OnError(err)
		}
	}()

	return ret
}

// Close closes the WorkerManager and waits all resource released.
func (m *WorkerManager) Close() {
	close(m.closeCh)
	m.wg.Wait()
}

// InitAfterRecover should be called after the master has failed over.
// This method will block until a timeout period for heartbeats has passed.
func (m *WorkerManager) InitAfterRecover(ctx context.Context) (retErr error) {
	defer func() {
		if retErr != nil {
			m.errCenter.OnError(retErr)
		}
	}()

	ctx, cancel := m.errCenter.WithCancelOnFirstError(ctx)
	defer cancel()

	m.mu.Lock()
	if m.state != workerManagerLoadingMeta {
		// InitAfterRecover should only be called if
		// NewWorkerManager has been called with isInit as false.
		m.logger.Panic("Unreachable", zap.String("master-id", m.masterID))
	}

	// Unlock here because loading meta involves I/O, which can be long.
	m.mu.Unlock()

	allPersistedWorkers, err := m.workerMetaClient.LoadAllWorkers(ctx)
	if err != nil {
		return err
	}

	m.mu.Lock()
	for workerID, status := range allPersistedWorkers {
		entry := newWaitingWorkerEntry(workerID, status)
		// TODO: refine mapping from worker status to worker entry state
		if status.State == frameModel.WorkerStateFinished {
			continue
		}
		m.workerEntries[workerID] = entry
	}

	if len(m.workerEntries) == 0 {
		// Fast path when there is no active worker.
		m.state = workerManagerReady
		m.mu.Unlock()
		return nil
	}

	m.state = workerManagerWaitingHeartbeat
	m.mu.Unlock()

	timeoutInterval := m.timeouts.WorkerTimeoutDuration + m.timeouts.WorkerTimeoutGracefulDuration

	timer := m.clock.Timer(timeoutInterval)
	defer timer.Stop()

	startTime := m.clock.Now()
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case <-m.allWorkersReady:
		m.logger.Info("All workers have sent heartbeats after master failover. Resuming right now.",
			zap.Duration("duration", m.clock.Since(startTime)))
	case <-timer.C:
		// Wait for the worker timeout to expire
	}

	m.mu.Lock()
	for _, entry := range m.workerEntries {
		if entry.State() == workerEntryWait || entry.IsFinished() {
			entry.MarkAsTombstone()
		}
	}
	m.state = workerManagerReady
	m.mu.Unlock()

	return nil
}

// HandleHeartbeat handles heartbeat ping message from a worker
func (m *WorkerManager) HandleHeartbeat(msg *frameModel.HeartbeatPingMessage, fromNode p2p.NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.state == workerManagerLoadingMeta {
		return
	}

	if !m.checkMasterEpochMatch(msg.Epoch) {
		return
	}

	entry, exists := m.workerEntries[msg.FromWorkerID]
	if !exists {
		m.logger.Info("Message from stale worker dropped",
			zap.String("master-id", m.masterID),
			zap.Any("message", msg),
			zap.String("from-node", fromNode))
		return
	}

	epoch := entry.Status().Epoch
	if !m.checkWorkerEpochMatch(epoch, msg.WorkerEpoch) {
		return
	}

	if msg.IsFinished {
		entry.SetFinished()
	}

	entry.SetExpireTime(m.nextExpireTime())

	if m.state == workerManagerWaitingHeartbeat {
		if entry.State() != workerEntryWait {
			// We should allow multiple heartbeats during the
			// workerManagerWaitingHeartbeat stage.
			return
		}

		m.logger.Info("Worker discovered", zap.String("master-id", m.masterID),
			zap.Any("worker-entry", entry))
		entry.MarkAsOnline(model.ExecutorID(fromNode), m.nextExpireTime())

		allReady := true
		for _, e := range m.workerEntries {
			if e.State() == workerEntryWait {
				allReady = false
				break
			}
		}
		if allReady {
			close(m.allWorkersReady)
			m.logger.Info("All workers have sent heartbeats, sending signal to resume the master",
				zap.String("master-id", m.masterID))
		}
	} else {
		if entry.State() != workerEntryCreated {
			// Return if it is not the first heartbeat.
			return
		}

		entry.MarkAsOnline(model.ExecutorID(fromNode), m.nextExpireTime())

		err := m.enqueueEvent(&masterEvent{
			Tp:       workerOnlineEvent,
			WorkerID: msg.FromWorkerID,
			Handle: &runningHandleImpl{
				workerID:   msg.FromWorkerID,
				executorID: model.ExecutorID(fromNode),
				manager:    m,
			},
		})
		if err != nil {
			m.errCenter.OnError(err)
		}
	}
}

// Tick should be called by the BaseMaster so that the callbacks can be
// run in the main goroutine.
func (m *WorkerManager) Tick(ctx context.Context) error {
	if err := m.errCenter.CheckError(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ctx, cancel = m.errCenter.WithCancelOnFirstError(ctx)
	defer cancel()

	for {
		var event *masterEvent
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case event = <-m.eventQueue:
		default:
			return nil
		}

		if event.beforeHook != nil {
			if ok := event.beforeHook(); !ok {
				// Continue to the next event.
				continue
			}
		}

		switch event.Tp {
		case workerOnlineEvent:
			if err := m.onWorkerOnlined(ctx, event.Handle); err != nil {
				return err
			}
		case workerOfflineEvent:
			if err := m.onWorkerOfflined(ctx, event.Handle, event.Err); err != nil {
				return err
			}
		case workerStatusUpdatedEvent:
			if err := m.onWorkerStatusUpdated(ctx, event.Handle); err != nil {
				return err
			}
		case workerDispatchFailedEvent:
			if err := m.onWorkerDispatched(ctx, event.Handle, event.Err); err != nil {
				return err
			}
		}
	}
}

// BeforeStartingWorker is called by the BaseMaster BEFORE the executor runs the worker,
// but after the executor records the time at which the worker is submitted.
func (m *WorkerManager) BeforeStartingWorker(
	workerID frameModel.WorkerID, executorID model.ExecutorID, epoch frameModel.Epoch,
) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.workerEntries[workerID]; exists {
		m.logger.Panic("worker already exists", zap.String("worker-id", workerID))
	}

	m.workerEntries[workerID] = newWorkerEntry(
		workerID,
		executorID,
		m.nextExpireTime(),
		workerEntryCreated,
		&frameModel.WorkerStatus{
			State: frameModel.WorkerStateCreated,
			Epoch: epoch,
		},
	)
}

// AbortCreatingWorker is called by BaseMaster if starting the worker has failed for sure.
// NOTE: If the RPC used to start the worker returns errors such as Canceled or DeadlineExceeded,
// it has NOT failed FOR SURE.
func (m *WorkerManager) AbortCreatingWorker(workerID frameModel.WorkerID, errIn error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	event := &masterEvent{
		Tp:       workerDispatchFailedEvent,
		WorkerID: workerID,
		Handle: &tombstoneHandleImpl{
			workerID: workerID,
			manager:  m,
		},
		Err: errIn,
		beforeHook: func() bool {
			m.mu.Lock()
			defer m.mu.Unlock()

			delete(m.workerEntries, workerID)
			return true
		},
	}

	err := m.enqueueEvent(event)
	if err != nil {
		m.errCenter.OnError(err)
	}
}

// OnWorkerStatusUpdateMessage should be called in the message handler for WorkerStatusMessage.
func (m *WorkerManager) OnWorkerStatusUpdateMessage(msg *statusutil.WorkerStatusMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.checkMasterEpochMatch(msg.MasterEpoch) {
		return
	}

	entry, exists := m.workerEntries[msg.Worker]
	if !exists {
		m.logger.Info("WorkerStatusMessage dropped for unknown worker",
			zap.String("master-id", m.masterID),
			zap.Any("message", msg))
		return
	}

	event := &masterEvent{
		Tp: workerStatusUpdatedEvent,
		Handle: &runningHandleImpl{
			workerID:   msg.Worker,
			executorID: entry.executorID,
			manager:    m,
		},
		WorkerID: msg.Worker,
		beforeHook: func() bool {
			if entry.IsTombstone() {
				// Cancel the event
				return false
			}
			entry.UpdateStatus(msg.Status)
			return true
		},
	}

	if err := m.enqueueEvent(event); err != nil {
		m.errCenter.OnError(err)
		return
	}
}

// GetWorkers gets all workers maintained by WorkerManager, including both running
// workers and dead workers.
func (m *WorkerManager) GetWorkers() map[frameModel.WorkerID]WorkerHandle {
	m.mu.Lock()
	defer m.mu.Unlock()

	ret := make(map[frameModel.WorkerID]WorkerHandle, len(m.workerEntries))
	for workerID, entry := range m.workerEntries {
		if entry.IsTombstone() {
			ret[workerID] = &tombstoneHandleImpl{
				workerID: workerID,
				manager:  m,
			}
			continue
		}

		ret[workerID] = &runningHandleImpl{
			workerID:   workerID,
			executorID: entry.executorID,
			manager:    m,
		}
	}
	return ret
}

// IsInitialized returns true after the worker manager has checked all tombstone
// workers are online or dead.
func (m *WorkerManager) IsInitialized() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.state == workerManagerReady
}

// WithLogger passes a logger.
func (m *WorkerManager) WithLogger(logger *zap.Logger) *WorkerManager {
	m.logger = logger
	return m
}

func (m *WorkerManager) checkWorkerEntriesOnce() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.state != workerManagerReady {
		// We should not check for timeout during the waiting period,
		// because timeouts during the waiting period is handled inside
		// InitAfterRecover.
		return nil
	}

	for workerID, entry := range m.workerEntries {
		entry := entry
		state := entry.State()
		if state == workerEntryOffline || state == workerEntryTombstone {
			// Prevent repeated delivery of the workerOffline event.
			continue
		}

		hasTimedOut := entry.ExpireTime().Before(m.clock.Now())
		shouldGoOffline := hasTimedOut || entry.IsFinished()
		if !shouldGoOffline {
			continue
		}

		// The worker has timed out, or has received a heartbeat
		// with IsFinished == true.
		entry.MarkAsOffline()

		var offlineError error
		if status := entry.Status(); status != nil {
			switch status.State {
			case frameModel.WorkerStateFinished:
				offlineError = errors.ErrWorkerFinish.FastGenByArgs()
			case frameModel.WorkerStateStopped:
				offlineError = errors.ErrWorkerCancel.FastGenByArgs()
			case frameModel.WorkerStateError:
				offlineError = errors.ErrWorkerFailed.FastGenByArgs()
			default:
				offlineError = errors.ErrWorkerOffline.FastGenByArgs(workerID)
			}
		}

		err := m.enqueueEvent(&masterEvent{
			Tp:       workerOfflineEvent,
			WorkerID: workerID,
			Handle: &tombstoneHandleImpl{
				workerID: workerID,
				manager:  m,
			},
			Err: offlineError,
			beforeHook: func() bool {
				entry.MarkAsTombstone()
				return true
			},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *WorkerManager) runBackgroundChecker() error {
	ticker := m.clock.Ticker(m.timeouts.MasterHeartbeatCheckLoopInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.closeCh:
			m.logger.Info("timeout checker exited", zap.String("master-id", m.masterID))
			return nil
		case <-ticker.C:
			if err := m.checkWorkerEntriesOnce(); err != nil {
				return err
			}
		}
	}
}

func (m *WorkerManager) nextExpireTime() time.Time {
	timeoutInterval := m.timeouts.WorkerTimeoutDuration + m.timeouts.WorkerTimeoutGracefulDuration
	return m.clock.Now().Add(timeoutInterval)
}

func (m *WorkerManager) checkMasterEpochMatch(msgEpoch frameModel.Epoch) (ok bool) {
	if msgEpoch > m.epoch {
		// If there is a worker reporting to a master with a larger epoch, then
		// we shouldn't be running.
		// TODO We need to do some chaos testing to determining whether and how to
		// handle this situation.
		m.logger.Panic("We are a stale master still running",
			zap.String("master-id", m.masterID),
			zap.Int64("msg-epoch", msgEpoch),
			zap.Int64("own-epoch", m.epoch))
	}

	if msgEpoch < m.epoch {
		m.logger.Info("Message from smaller epoch dropped",
			zap.String("master-id", m.masterID),
			zap.Int64("msg-epoch", msgEpoch),
			zap.Int64("own-epoch", m.epoch))
		return false
	}
	return true
}

func (m *WorkerManager) checkWorkerEpochMatch(curEpoch, msgEpoch frameModel.Epoch) bool {
	if msgEpoch > curEpoch {
		m.logger.Panic("We are a stale master still running",
			zap.String("master-id", m.masterID), zap.Int64("own-epoch", m.epoch),
			zap.Int64("own-worker-epoch", curEpoch),
			zap.Int64("msg-worker-epoch", msgEpoch),
		)
	}
	if msgEpoch < curEpoch {
		m.logger.Info("Message from small worker epoch dropped",
			zap.String("master-id", m.masterID),
			zap.Int64("own-worker-epoch", curEpoch),
			zap.Int64("msg-worker-epoch", msgEpoch),
		)
		return false
	}
	return true
}

func (m *WorkerManager) enqueueEvent(event *masterEvent) error {
	timer := time.NewTimer(1 * time.Second)
	defer timer.Stop()

	select {
	case <-timer.C:
		return errors.ErrMasterTooManyPendingEvents.GenWithStackByArgs()
	case m.eventQueue <- event:
	}

	return nil
}

// removeTombstoneEntry removes a tombstone workerEntry from the in-memory map.
// NOTE: removeTombstoneEntry is expected to be used by tombstoneHandleImpl only,
// and it should NOT be called with m.mu taken.
func (m *WorkerManager) removeTombstoneEntry(id frameModel.WorkerID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Checks precondition.
	entry, exists := m.workerEntries[id]
	if !exists {
		// Return here. We intend this method to be idempotent.
		return
	}

	if !entry.IsTombstone() {
		m.logger.Panic("Unreachable: not a tombstone", zap.Stringer("entry", entry))
	}

	delete(m.workerEntries, id)
}
