package lib

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/model"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/p2p"
	"go.uber.org/zap"
)

// workerManager is for private use by BaseMaster.
type workerManager struct {
	mu                   sync.Mutex
	needWaitForHeartBeat bool
	initStartTime        time.Time
	workerInfos          map[WorkerID]*WorkerInfo
	tombstones           map[WorkerID]*WorkerStatus

	// read-only
	masterEpoch Epoch
	masterID    MasterID

	messageRouter p2p.MessageRouter
}

func newWorkerManager(id MasterID, needWait bool, curEpoch Epoch) *workerManager {
	return &workerManager{
		needWaitForHeartBeat: needWait,
		workerInfos:          make(map[WorkerID]*WorkerInfo),
		masterEpoch:          curEpoch,
		masterID:             id,
	}
}

func (m *workerManager) Initialized(ctx context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.needWaitForHeartBeat {
		return true, nil
	}

	if m.initStartTime.IsZero() {
		m.initStartTime = time.Now()
	}
	if time.Since(m.initStartTime) > workerTimeoutDuration+workerTimeoutGracefulDuration {
		return true, nil
	}
	return false, nil
}

func (m *workerManager) Tick(ctx context.Context, router p2p.MessageRouter) (timedOutWorkers []*WorkerInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// respond to worker heartbeats
	for workerID, workerInfo := range m.workerInfos {
		if !workerInfo.hasPendingHeartbeat {
			if workerInfo.hasTimedOut() {
				timedOutWorkers = append(timedOutWorkers, workerInfo)
				delete(m.workerInfos, workerID)
				m.tombstones[workerID] = &workerInfo.status
			}
			continue
		}
		reply := &HeartbeatPongMessage{
			SendTime:  workerInfo.lastHeartBeatSendTime,
			ReplyTime: time.Now(),
			Epoch:     m.masterEpoch,
		}
		workerNodeID := workerInfo.NodeID
		log.L().Debug("Sending heartbeat response to worker",
			zap.String("worker-id", string(workerInfo.ID)),
			zap.String("worker-node-id", workerNodeID),
			zap.Any("message", reply))

		msgClient := router.GetClient(workerNodeID)
		if msgClient == nil {
			// Retry on the next tick
			continue
		}
		_, err := msgClient.TrySendMessage(ctx, HeartbeatPongTopic(m.masterID), reply)
		if cerror.ErrPeerMessageSendTryAgain.Equal(err) {
			log.L().Warn("Sending heartbeat response to worker failed: message client congested",
				zap.String("worker-id", string(workerInfo.ID)),
				zap.String("worker-node-id", workerNodeID),
				zap.Any("message", reply))
			// Retry on the next tick
			continue
		}
		workerInfo.hasPendingHeartbeat = false
	}
	return nil
}

func (m *workerManager) HandleHeartBeat(msg *HeartbeatPingMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.L().Debug("received heartbeat", zap.Any("msg", msg))
	workerInfo, ok := m.workerInfos[msg.FromWorkerID]
	if !ok {
		log.L().Info("discarding heartbeat for non-existing worker",
			zap.Any("msg", msg))
		return
	}
	workerInfo.lastHeartBeatReceiveTime = time.Now()
	workerInfo.lastHeartBeatSendTime = msg.SendTime
	workerInfo.hasPendingHeartbeat = true
}

func (m *workerManager) UpdateWorkload(msg *WorkloadReportMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()

	info, ok := m.getWorkerInfo(msg.WorkerID)
	if !ok {
		log.L().Info("received workload update for non-existing worker",
			zap.String("master-id", string(m.masterID)),
			zap.Any("msg", msg))
		return
	}
	info.workload = msg.Workload
	log.L().Debug("workload updated",
		zap.String("master-id", string(m.masterID)),
		zap.Any("msg", msg))
}

func (m *workerManager) UpdateStatus(msg *StatusUpdateMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()

	info, ok := m.getWorkerInfo(msg.WorkerID)
	if !ok {
		log.L().Info("received status update for non-existing worker",
			zap.String("master-id", string(m.masterID)),
			zap.Any("msg", msg))
		return
	}
	info.status = msg.Status
	log.L().Debug("worker status updated",
		zap.String("master-id", string(m.masterID)),
		zap.Any("msg", msg))
}

func (m *workerManager) GetWorkerInfo(id WorkerID) (*WorkerInfo, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	value, ok := m.workerInfos[id]
	if !ok {
		return nil, false
	}
	return value, true
}

func (m *workerManager) getWorkerInfo(id WorkerID) (*WorkerInfo, bool) {
	value, ok := m.workerInfos[id]
	if !ok {
		return nil, false
	}
	return value, true
}

func (m *workerManager) putWorkerInfo(info *WorkerInfo) bool {
	id := info.ID
	_, exists := m.workerInfos[id]
	m.workerInfos[id] = info
	return !exists
}

func (m *workerManager) OnWorkerCreated(id WorkerID, exeuctorNodeID p2p.NodeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	ok := m.putWorkerInfo(&WorkerInfo{
		ID:                       id,
		NodeID:                   exeuctorNodeID,
		lastHeartBeatReceiveTime: time.Now(),
		status: WorkerStatus{
			Code: WorkerStatusCreated,
		},
		// TODO fix workload
		workload: 10, // 10 is the initial workload for now.
	})
	if !ok {
		return derror.ErrDuplicateWorkerID.GenWithStackByArgs(id)
	}
	return nil
}

func (m *workerManager) getWorkerHandle(id WorkerID) WorkerHandle {
	return &workerHandleImpl{
		manager: m,
		id:      id,
	}
}

type WorkerInfo struct {
	ID     WorkerID
	NodeID p2p.NodeID

	// fields for internal use by the Master.
	lastHeartBeatReceiveTime time.Time
	lastHeartBeatSendTime    monotonicTime
	hasPendingHeartbeat      bool

	status   WorkerStatus
	workload model.RescUnit
}

func (w *WorkerInfo) hasTimedOut() bool {
	return time.Since(w.lastHeartBeatReceiveTime) > workerTimeoutDuration
}

type WorkerHandle interface {
	SendMessage(ctx context.Context, topic p2p.Topic, message interface{}) error
	Status() *WorkerStatus
	Workload() model.RescUnit
	ID() WorkerID
	IsTombStone() bool
}

type workerHandleImpl struct {
	manager *workerManager

	// TODO think about how to handle the situation where the workerID has been removed from `manager`.
	id WorkerID
}

func (w *workerHandleImpl) SendMessage(ctx context.Context, topic p2p.Topic, message interface{}) error {
	info, ok := w.manager.GetWorkerInfo(w.id)
	if !ok {
		return derror.ErrWorkerNotFound.GenWithStackByArgs(w.id)
	}

	executorNodeID := info.NodeID
	messageClient := w.manager.messageRouter.GetClient(executorNodeID)
	if messageClient == nil {
		return derror.ErrMessageClientNotFoundForWorker.GenWithStackByArgs(w.id)
	}

	prefixedTopic := fmt.Sprintf("worker-message/%s/%s", w.id, topic)
	_, err := messageClient.TrySendMessage(ctx, prefixedTopic, message)
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

func (w *workerHandleImpl) Workload() model.RescUnit {
	info, ok := w.manager.GetWorkerInfo(w.id)
	if !ok {
		return 0
	}

	return info.workload
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
