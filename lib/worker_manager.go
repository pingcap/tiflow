package lib

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/pkg/workerpool"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/clock"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

// workerManager is for private use by BaseMaster.
type workerManager interface {
	IsInitialized(ctx context.Context) (bool, error)
	Tick(ctx context.Context, sender p2p.MessageSender) (offlinedWorkers []*WorkerInfo, onlinedWorkers []*WorkerInfo)
	HandleHeartbeat(msg *HeartbeatPingMessage, fromNode p2p.NodeID) error
	OnWorkerCreated(ctx context.Context, id WorkerID, exeuctorNodeID p2p.NodeID) error
	GetWorkerInfo(id WorkerID) (*WorkerInfo, bool)
	PutWorkerInfo(info *WorkerInfo) bool
	MessageSender() p2p.MessageSender
	GetWorkerHandle(id WorkerID) WorkerHandle
	GetWorkers() map[WorkerID]WorkerHandle
	GetStatus(id WorkerID) (*WorkerStatus, bool)
	CheckStatusUpdate(ctx context.Context) error
}

type workerManagerImpl struct {
	mu              sync.Mutex
	initialized     bool
	initStartTime   time.Time
	workerInfos     map[WorkerID]*WorkerInfo
	tombstones      map[WorkerID]*WorkerStatus
	statusReceivers map[WorkerID]*StatusReceiver

	// read-only
	masterEpoch   Epoch
	masterID      MasterID
	timeoutConfig TimeoutConfig

	// to help unit testing
	clock clock.Clock

	messageSender        p2p.MessageSender
	messageHandleManager p2p.MessageHandlerManager
	metaClient           metadata.MetaKV
	pool                 workerpool.AsyncPool
}

func newWorkerManager(
	id MasterID,
	needWait bool,
	curEpoch Epoch,
	messageSender p2p.MessageSender,
	messageHandleManager p2p.MessageHandlerManager,
	metaClient metadata.MetaKV,
	pool workerpool.AsyncPool,
	timeoutConfig *TimeoutConfig,
) workerManager {
	return &workerManagerImpl{
		initialized:     !needWait,
		workerInfos:     make(map[WorkerID]*WorkerInfo),
		tombstones:      make(map[WorkerID]*WorkerStatus),
		statusReceivers: make(map[WorkerID]*StatusReceiver),

		masterEpoch:   curEpoch,
		masterID:      id,
		timeoutConfig: *timeoutConfig,

		clock: clock.New(),

		messageSender:        messageSender,
		messageHandleManager: messageHandleManager,
		metaClient:           metaClient,
		pool:                 pool,
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

func (m *workerManagerImpl) CheckStatusUpdate(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, receiver := range m.statusReceivers {
		if err := receiver.Tick(ctx); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (m *workerManagerImpl) Tick(
	ctx context.Context,
	sender p2p.MessageSender,
) (offlinedWorkers []*WorkerInfo, onlinedWorkers []*WorkerInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// TODO gracefully handle all errors that could occur in this function
	// respond to worker heartbeats
	for workerID, workerInfo := range m.workerInfos {
		// `justOnlined` indicates that the online event has not been notified,
		// and `hasPendingHeartbeat` indicates that we have received a heartbeat and
		// has not sent the Pong yet.
		if workerInfo.justOnlined && workerInfo.hasPendingHeartbeat && workerInfo.statusInitialized.Load() {
			workerInfo.justOnlined = false
			onlinedWorkers = append(onlinedWorkers, workerInfo)
		}

		if workerInfo.hasTimedOut(m.clock, &m.timeoutConfig) {
			offlinedWorkers = append(offlinedWorkers, workerInfo)
			status := m.statusReceivers[workerID].Status()
			m.tombstones[workerID] = &status
			delete(m.workerInfos, workerID)
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
		if err := m.addWorker(msg.FromWorkerID, fromNode); err != nil {
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

func (m *workerManagerImpl) GetStatus(id WorkerID) (*WorkerStatus, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	receiver, exists := m.statusReceivers[id]
	if !exists {
		return nil, false
	}

	// TODO evaluate whether we need an object pool to mitigate allocation burden.
	ret := receiver.Status()
	return &ret, true
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

func (m *workerManagerImpl) addWorker(id WorkerID, executorNodeID p2p.NodeID) error {
	if _, exists := m.workerInfos[id]; exists {
		// TODO determine whether it is appropriate to panic here.
		log.L().Panic("duplicate worker ID",
			zap.String("worker-id", id))
	}

	m.workerInfos[id] = &WorkerInfo{
		ID:                       id,
		NodeID:                   executorNodeID,
		lastHeartbeatReceiveTime: m.clock.Now(),
		// TODO fix workload
		workload:    10, // 10 is the initial workload for now.
		justOnlined: true,
	}

	workerMetaClient := NewWorkerMetadataClient(m.masterID, m.metaClient)
	receiver := NewStatusReceiver(
		id,
		workerMetaClient,
		m.messageHandleManager,
		m.masterEpoch,
		m.pool,
		m.clock,
	)
	m.statusReceivers[id] = receiver

	// TODO refine AsyncPool or refactor this function to avoid
	// possible deadlocking when the pool's pending queue is full.
	//
	// TODO figure out what context to use here.
	err := m.pool.Go(context.TODO(), func() {
		if err := receiver.Init(context.TODO()); err != nil {
			// TODO handle the error
			log.L().Warn("failed to init StatusReceiver",
				zap.String("master-id", m.masterID),
				zap.String("worker-id", id),
				zap.Error(err))
		}
		info, ok := m.GetWorkerInfo(id)
		if !ok {
			log.L().Warn("worker has been removed",
				zap.String("master-id", m.masterID),
				zap.String("worker-id", id))
		}
		if old := info.statusInitialized.Swap(true); old {
			log.L().Panic("worker is initialized twice. Report a bug",
				zap.String("master-id", m.masterID),
				zap.String("worker-id", id))
		}
	})
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (m *workerManagerImpl) OnWorkerCreated(ctx context.Context, id WorkerID, executorID p2p.NodeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	workerMetaClient := NewWorkerMetadataClient(m.masterID, m.metaClient)
	err := workerMetaClient.Store(ctx, id, &WorkerStatus{
		Code: WorkerStatusCreated,
	})
	if err != nil {
		return errors.Trace(err)
	}

	if err := m.addWorker(id, executorID); err != nil {
		return errors.Trace(err)
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

	// marks whether the status has been asynchronously
	// loaded from the metastore.
	statusInitialized atomic.Bool

	workload model.RescUnit
}

func (w *WorkerInfo) hasTimedOut(clock clock.Clock, config *TimeoutConfig) bool {
	duration := clock.Since(w.lastHeartbeatReceiveTime)
	if duration > config.workerTimeoutDuration {
		// TODO add details about the worker.
		log.L().Warn("Worker timed out", zap.Duration("duration", duration))
		return true
	}
	return false
}

type WorkerHandle interface {
	SendMessage(ctx context.Context, topic p2p.Topic, message interface{}) error
	Status() *WorkerStatus
	ID() WorkerID
	IsTombStone() bool
	ToPB() (*pb.WorkerInfo, error)
}

type workerHandleImpl struct {
	manager *workerManagerImpl

	// TODO think about how to handle the situation where the workerID has been removed from `manager`.
	id WorkerID
}

func (w *workerHandleImpl) ToPB() (*pb.WorkerInfo, error) {
	statusBytes, err := w.Status().Marshal()
	if err != nil {
		return nil, err
	}

	info, ok := w.manager.GetWorkerInfo(w.id)
	if !ok {
		// TODO add an appropriate error
		return nil, nil
	}

	ret := &pb.WorkerInfo{
		Id:         w.ID(),
		ExecutorId: info.NodeID,
		Status:     statusBytes,
		LastHbTime: info.lastHeartbeatReceiveTime.Unix(),
		Workload:   int64(info.workload),
	}
	return ret, nil
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

func (w *workerHandleImpl) GetWorkerInfo(id WorkerID) (*WorkerInfo, bool) {
	return w.manager.GetWorkerInfo(id)
}

func (w *workerHandleImpl) Status() *WorkerStatus {
	// TODO come up with a better solution when the status does not exist
	status, exists := w.manager.GetStatus(w.id)
	if !exists {
		return nil
	}
	return status
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

func (h *tombstoneWorkerHandleImpl) ToPB() (*pb.WorkerInfo, error) {
	// TODO add an appropriate error
	return nil, nil
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
