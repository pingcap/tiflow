package lib

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/clock"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

// workerManager is for private use by BaseMaster.
type workerManager interface {
	IsInitialized(ctx context.Context) (bool, error)
	Tick(ctx context.Context, sender p2p.MessageSender) (offlinedWorkers []*WorkerInfo, onlinedWorkers []*WorkerInfo)
	HandleHeartbeat(msg *HeartbeatPingMessage, fromNode p2p.NodeID) error
	UpdateStatus(msg *StatusUpdateMessage)
	GetWorkerInfo(id WorkerID) (*WorkerInfo, bool)
	PutWorkerInfo(info *WorkerInfo) bool
	AddWorker(id WorkerID, exeuctorNodeID p2p.NodeID, statusCode WorkerStatusCode) error
	MessageSender() p2p.MessageSender
	GetWorkerHandle(id WorkerID) WorkerHandle
	GetWorkers() map[WorkerID]WorkerHandle
}

type workerManagerImpl struct {
	mu            sync.Mutex
	initialized   bool
	initStartTime time.Time
	workerInfos   map[WorkerID]*WorkerInfo
	tombstones    map[WorkerID]*WorkerStatus

	// read-only
	masterEpoch   Epoch
	masterID      MasterID
	timeoutConfig TimeoutConfig

	// to help unit testing
	clock clock.Clock

	messageSender p2p.MessageSender
}

func newWorkerManager(id MasterID, needWait bool, curEpoch Epoch) workerManager {
	return &workerManagerImpl{
		initialized: !needWait,
		workerInfos: make(map[WorkerID]*WorkerInfo),
		tombstones:  make(map[WorkerID]*WorkerStatus),

		masterEpoch:   curEpoch,
		masterID:      id,
		timeoutConfig: defaultTimeoutConfig,

		clock: clock.New(),
	}
}

func (m *workerManagerImpl) IsInitialized(ctx context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.initialized {
		return true, nil
	}

	if m.initStartTime.IsZero() {
		m.initStartTime = m.clock.Now()
	}

	thresholdDuration := m.timeoutConfig.workerTimeoutDuration + m.timeoutConfig.workerTimeoutGracefulDuration
	if m.clock.Since(m.initStartTime) > thresholdDuration {
		m.initialized = true
		return true, nil
	}
	return false, nil
}

func (m *workerManagerImpl) Tick(
	ctx context.Context,
	sender p2p.MessageSender,
) (offlinedWorkers []*WorkerInfo, onlinedWorkers []*WorkerInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// respond to worker heartbeats
	for workerID, workerInfo := range m.workerInfos {
		// `justOnlined` indicates that the online event has not been notified,
		// and `hasPendingHeartbeat` indicates that we have received a heartbeat and
		// has not sent the Pong yet.
		if workerInfo.justOnlined && workerInfo.hasPendingHeartbeat {
			workerInfo.justOnlined = false
			workerInfo.status.Code = WorkerStatusInit
			onlinedWorkers = append(onlinedWorkers, workerInfo)
		}

		if workerInfo.hasTimedOut(m.clock, &m.timeoutConfig) {
			offlinedWorkers = append(offlinedWorkers, workerInfo)
			delete(m.workerInfos, workerID)

			statusCloned := workerInfo.status
			statusCloned.Code = WorkerStatusError
			m.tombstones[workerID] = &statusCloned
		}

		if !workerInfo.hasPendingHeartbeat {
			// No heartbeat to respond to.
			continue
		}
		reply := &HeartbeatPongMessage{
			SendTime:   workerInfo.lastHeartbeatSendTime,
			ReplyTime:  m.clock.Now(),
			ToWorkerID: workerID,
			Epoch:      m.masterEpoch,
		}
		workerNodeID := workerInfo.NodeID
		log.L().Debug("Sending heartbeat response to worker",
			zap.Any("worker-id", workerInfo.ID),
			zap.String("worker-node-id", workerNodeID),
			zap.Any("message", reply))

		ok, err := sender.SendToNode(ctx, workerNodeID, HeartbeatPongTopic(m.masterID, workerID), reply)
		if err != nil {
			log.L().Error("Failed to send heartbeat", zap.Error(err))
		}
		if !ok {
			log.L().Info("Sending heartbeat would block, will try again",
				zap.Any("message", reply))
			continue
		}
		// We have sent the Pong, mark it as such.
		workerInfo.hasPendingHeartbeat = false
	}
	return
}

func (m *workerManagerImpl) HandleHeartbeat(msg *HeartbeatPingMessage, fromNode p2p.NodeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.L().Debug("received heartbeat", zap.Any("msg", msg))
	workerInfo, ok := m.workerInfos[msg.FromWorkerID]
	if !ok {
		if m.initialized {
			log.L().Info("discarding heartbeat from non-existing worker, probably zombie?",
				zap.Any("msg", msg),
				zap.String("node-id", fromNode))
			return nil
		}
		// We are still waiting to take over workers created in previous epochs, so
		// it is possible to encounter a worker that is not created by us. In this case,
		// we need to add the worker.
		if err := m.addWorker(msg.FromWorkerID, fromNode, WorkerStatusInit); err != nil {
			return errors.Trace(err)
		}
		workerInfo, ok = m.getWorkerInfo(msg.FromWorkerID)
		if !ok {
			log.L().Panic("unreachable",
				zap.Any("msg", msg),
				zap.String("node-id", fromNode))
		}
	}
	workerInfo.lastHeartbeatReceiveTime = m.clock.Now()
	workerInfo.lastHeartbeatSendTime = msg.SendTime
	workerInfo.hasPendingHeartbeat = true
	return nil
}

func (m *workerManagerImpl) UpdateStatus(msg *StatusUpdateMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()

	info, ok := m.getWorkerInfo(msg.WorkerID)
	if !ok {
		log.L().Info("received status update for non-existing worker",
			zap.String("master-id", m.masterID),
			zap.Any("msg", msg))
		return
	}
	info.status = msg.Status
	log.L().Debug("worker status updated",
		zap.String("master-id", m.masterID),
		zap.Any("msg", msg))
}

func (m *workerManagerImpl) GetWorkerInfo(id WorkerID) (*WorkerInfo, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.getWorkerInfo(id)
}

func (m *workerManagerImpl) getWorkerInfo(id WorkerID) (*WorkerInfo, bool) {
	value, ok := m.workerInfos[id]
	if !ok {
		return nil, false
	}
	return value, true
}

func (m *workerManagerImpl) PutWorkerInfo(info *WorkerInfo) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.putWorkerInfo(info)
}

func (m *workerManagerImpl) putWorkerInfo(info *WorkerInfo) bool {
	id := info.ID
	_, exists := m.workerInfos[id]
	m.workerInfos[id] = info
	return !exists
}

func (m *workerManagerImpl) AddWorker(id WorkerID, exeuctorNodeID p2p.NodeID, statusCode WorkerStatusCode) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.addWorker(id, exeuctorNodeID, statusCode)
}

func (m *workerManagerImpl) addWorker(id WorkerID, exeuctorNodeID p2p.NodeID, statusCode WorkerStatusCode) error {
	ok := m.putWorkerInfo(&WorkerInfo{
		ID:                       id,
		NodeID:                   exeuctorNodeID,
		lastHeartbeatReceiveTime: m.clock.Now(),
		status: WorkerStatus{
			Code: statusCode,
		},
		// TODO fix workload
		workload:    10, // 10 is the initial workload for now.
		justOnlined: true,
	})
	if !ok {
		// The worker with the ID already exists, so we just update the statusCode.
		info, ok := m.getWorkerInfo(id)
		if !ok {
			log.L().Panic("unreachable", zap.Any("worker-id", id))
		}
		if info.NodeID != exeuctorNodeID {
			// We expect the worker ID to be globally unique, and the worker ID should
			// change if a worker is recreated. So two workers with the same name on
			// different executors are NOT POSSIBLE.
			log.L().Panic("unreachable", zap.Any("worker-id", id))
		}
		info.status.Code = statusCode
	}
	return nil
}

func (m *workerManagerImpl) MessageSender() p2p.MessageSender {
	return m.messageSender
}

func (m *workerManagerImpl) GetWorkerHandle(id WorkerID) WorkerHandle {
	return &workerHandleImpl{
		manager: m,
		id:      id,
	}
}

// GetWorkers returns a map from WorkerID to WorkerHandle for all known workers.
// NOTE this is a preliminary implementation and is likely to have performance problem
// if called too frequently.
// TODO cache the returned map, or rethink how to manage the list.
func (m *workerManagerImpl) GetWorkers() map[WorkerID]WorkerHandle {
	m.mu.Lock()
	defer m.mu.Unlock()

	ret := make(map[WorkerID]WorkerHandle)
	for workerID := range m.workerInfos {
		ret[workerID] = m.GetWorkerHandle(workerID)
	}

	for workerID, status := range m.tombstones {
		ret[workerID] = NewTombstoneWorkerHandle(workerID, *status)
	}
	return ret
}

type WorkerInfo struct {
	ID     WorkerID
	NodeID p2p.NodeID

	// fields for internal use by the Master.
	lastHeartbeatReceiveTime time.Time
	lastHeartbeatSendTime    clock.MonotonicTime
	hasPendingHeartbeat      bool
	justOnlined              bool

	status   WorkerStatus
	workload model.RescUnit
}

func (w *WorkerInfo) hasTimedOut(clock clock.Clock, config *TimeoutConfig) bool {
	return clock.Since(w.lastHeartbeatReceiveTime) > config.workerTimeoutDuration
}

type WorkerHandle interface {
	SendMessage(ctx context.Context, topic p2p.Topic, message interface{}) error
	Status() *WorkerStatus
	ID() WorkerID
	IsTombStone() bool
}

type workerHandleImpl struct {
	manager *workerManagerImpl

	// TODO think about how to handle the situation where the workerID has been removed from `manager`.
	id WorkerID
}

func (w *workerHandleImpl) SendMessage(ctx context.Context, topic p2p.Topic, message interface{}) error {
	info, ok := w.manager.GetWorkerInfo(w.id)
	if !ok {
		return derror.ErrWorkerNotFound.GenWithStackByArgs(w.id)
	}

	executorNodeID := info.NodeID
	// TODO the worker should have a way to register a handle for this topic.
	// TODO maybe we need a TopicEncoder
	prefixedTopic := fmt.Sprintf("worker-message/%s/%s", w.id, topic)
	_, err := w.manager.MessageSender().SendToNode(ctx, executorNodeID, prefixedTopic, message)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (w *workerHandleImpl) Status() *WorkerStatus {
	info, ok := w.manager.GetWorkerInfo(w.id)
	if !ok {
		return nil
	}
	return &info.status
}

func (w *workerHandleImpl) ID() WorkerID {
	return w.id
}

func (w *workerHandleImpl) IsTombStone() bool {
	return false
}

type tombstoneWorkerHandleImpl struct {
	id     WorkerID
	status WorkerStatus
}

func NewTombstoneWorkerHandle(id WorkerID, status WorkerStatus) WorkerHandle {
	return &tombstoneWorkerHandleImpl{
		id:     id,
		status: status,
	}
}

func (h *tombstoneWorkerHandleImpl) SendMessage(ctx context.Context, topic p2p.Topic, message interface{}) error {
	return derror.ErrWorkerOffline.GenWithStackByArgs(h.id)
}

func (h *tombstoneWorkerHandleImpl) Status() *WorkerStatus {
	return &h.status
}

func (h *tombstoneWorkerHandleImpl) Workload() model.RescUnit {
	return 0
}

func (h *tombstoneWorkerHandleImpl) ID() WorkerID {
	return h.id
}

func (h *tombstoneWorkerHandleImpl) IsTombStone() bool {
	return true
}
