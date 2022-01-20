package lib

import (
	"context"
	"sync"
	"time"

	"github.com/gavv/monotime"
	"github.com/hanfei1991/microcosm/model"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

type Worker interface {
	Init(ctx context.Context) error
	Poll(ctx context.Context) error
	ID() WorkerID
	Workload() model.RescUnit
	Close()
}

type WorkerImpl interface {
	// InitImpl provides customized logic for the business logic to initialize.
	InitImpl(ctx context.Context) error

	// Tick is called on a fixed interval.
	Tick(ctx context.Context) error

	// Status returns a short worker status to be periodically sent to the master.
	Status() (WorkerStatus, error)

	// Workload returns the current workload of the worker.
	Workload() (model.RescUnit, error)

	// OnMasterFailover is called when the master is failed over.
	OnMasterFailover(reason MasterFailoverReason) error

	// CloseImpl tells the WorkerImpl to quitrunStatusWorker and release resources.
	CloseImpl()
}

type BaseWorker struct {
	Impl WorkerImpl

	messageHandlerManager p2p.MessageHandlerManager
	messageRouter         p2p.MessageSender
	metaKVClient          metadata.MetaKV

	masterClient *masterClient

	id            WorkerID
	timeoutConfig TimeoutConfig

	wg    sync.WaitGroup
	errCh chan error
}

func NewBaseWorker(
	impl WorkerImpl,
	messageHandlerManager p2p.MessageHandlerManager,
	messageSender p2p.MessageSender,
	metaKVClient metadata.MetaKV,
	workerID WorkerID,
	masterID MasterID,
) *BaseWorker {
	masterManager := newMasterManager(masterID, workerID, messageSender)
	return &BaseWorker{
		Impl:                  impl,
		messageHandlerManager: messageHandlerManager,
		messageRouter:         messageSender,
		metaKVClient:          metaKVClient,
		masterClient:          masterManager,

		id:            workerID,
		timeoutConfig: defaultTimeoutConfig,

		errCh: make(chan error, 1),
	}
}

func (w *BaseWorker) ID() WorkerID {
	return w.id
}

func (w *BaseWorker) Workload() model.RescUnit {
	wl, err := w.Impl.Workload()
	if err != nil {
		log.L().Panic("workload meet error: " + err.Error())
	}
	return wl
}

func (w *BaseWorker) Init(ctx context.Context) error {
	if err := w.initMessageHandlers(ctx); err != nil {
		return errors.Trace(err)
	}

	if err := w.masterClient.refreshMasterInfo(ctx); err != nil {
		return errors.Trace(err)
	}

	if err := w.Impl.InitImpl(ctx); err != nil {
		return errors.Trace(err)
	}

	w.startBackgroundTasks(ctx)
	return nil
}

func (w *BaseWorker) Poll(ctx context.Context) error {
	if err := w.messageHandlerManager.CheckError(ctx); err != nil {
		return errors.Trace(err)
	}

	select {
	case err := <-w.errCh:
		if err != nil {
			return errors.Trace(err)
		}
	default:
	}

	if err := w.Impl.Tick(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (w *BaseWorker) Close() {
	w.Impl.CloseImpl()

	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	if err := w.messageHandlerManager.Clean(closeCtx); err != nil {
		log.L().Warn("cleaning message handlers failed",
			zap.Error(err))
	}
}

func (w *BaseWorker) MetaKVClient() metadata.MetaKV {
	return w.metaKVClient
}

func (w *BaseWorker) startBackgroundTasks(ctx context.Context) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		if err := w.runStatusWorker(ctx); err != nil {
			w.onError(err)
		}
	}()

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		if err := w.runHeartbeatWorker(ctx); err != nil {
			w.onError(err)
		}
	}()

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		if err := w.runWatchDog(ctx); err != nil {
			w.onError(err)
		}
	}()
}

func (w *BaseWorker) runHeartbeatWorker(ctx context.Context) error {
	ticker := time.NewTicker(w.timeoutConfig.workerHeartbeatInterval)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			if err := w.masterClient.SendHeartBeat(ctx); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (w *BaseWorker) runStatusWorker(ctx context.Context) error {
	ticker := time.NewTicker(w.timeoutConfig.workerReportStatusInterval)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
		}

		status, err := w.Impl.Status()
		if err != nil {
			return errors.Trace(err)
		}
		workload, err := w.Impl.Workload()
		if err != nil {
			return errors.Trace(err)
		}
		if err := w.masterClient.SendStatus(ctx, status, workload); err != nil {
			return errors.Trace(err)
		}
	}
}

func (w *BaseWorker) runWatchDog(ctx context.Context) error {
	ticker := time.NewTicker(w.timeoutConfig.workerTimeoutGracefulDuration / 2)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
		}

		hasTimedOut, err := w.masterClient.CheckMasterTimeout(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if hasTimedOut {
			return derror.ErrWorkerSuicide.GenWithStackByArgs()
		}
	}
}

func (w *BaseWorker) initMessageHandlers(ctx context.Context) error {
	topic := HeartbeatPongTopic(w.masterClient.MasterID())
	ok, err := w.messageHandlerManager.RegisterHandler(
		ctx,
		topic,
		&HeartbeatPongMessage{},
		func(sender p2p.NodeID, value p2p.MessageValue) error {
			msg := value.(*HeartbeatPongMessage)
			log.L().Debug("heartbeat pong received",
				zap.Any("msg", msg))
			w.masterClient.HandleHeartbeat(msg)
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	if !ok {
		log.L().Panic("duplicate handler",
			zap.String("topic", topic))
	}
	return nil
}

func (w *BaseWorker) onError(err error) {
	select {
	case w.errCh <- err:
	default:
	}
}

type masterClient struct {
	mu          sync.RWMutex
	masterID    MasterID
	masterNode  p2p.NodeID
	masterEpoch Epoch

	workerID WorkerID

	messageSender           p2p.MessageSender
	metaKVClient            metadata.MetaKV
	lastMasterAckedPingTime monotonicTime

	timeoutConfig TimeoutConfig
}

func newMasterManager(masterID MasterID, workerID WorkerID, messageRouter p2p.MessageSender) *masterClient {
	return &masterClient{
		masterID:      masterID,
		workerID:      workerID,
		messageSender: messageRouter,

		timeoutConfig: defaultTimeoutConfig,
	}
}

func (m *masterClient) refreshMasterInfo(ctx context.Context) error {
	metaClient := NewMetadataClient(m.masterID, m.metaKVClient)
	masterMeta, err := metaClient.Load(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.masterNode = masterMeta.NodeID
	m.masterEpoch = masterMeta.Epoch

	// TODO call OnMasterFailover at appropriate time
	return nil
}

func (m *masterClient) MasterID() MasterID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.masterID
}

func (m *masterClient) HandleHeartbeat(msg *HeartbeatPongMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// TODO think about whether to distinct msg.Epoch > m.masterEpoch
	// and msg.Epoch < m.masterEpoch
	if msg.Epoch != m.masterEpoch {
		log.L().Info("epoch does not match",
			zap.Any("msg", msg),
			zap.Int64("master-epoch", m.masterEpoch))
		return
	}
	m.lastMasterAckedPingTime = msg.SendTime
}

func (m *masterClient) CheckMasterTimeout(ctx context.Context) (ok bool, err error) {
	m.mu.RLock()
	lastMasterAckedPingTime := m.lastMasterAckedPingTime
	m.mu.RUnlock()

	if lastMasterAckedPingTime <= 2*m.timeoutConfig.workerHeartbeatInterval {
		return true, nil
	}

	if lastMasterAckedPingTime > 2*m.timeoutConfig.workerHeartbeatInterval &&
		lastMasterAckedPingTime < m.timeoutConfig.workerTimeoutDuration {

		if err := m.refreshMasterInfo(ctx); err != nil {
			return false, errors.Trace(err)
		}
		return true, nil
	}

	return false, nil
}

func (m *masterClient) SendHeartBeat(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	heartbeatMsg := &HeartbeatPingMessage{
		// We use `monotime` because we would like to serialize a local timestamp.
		// The timestamp will be returned in a PONG for time-out check, so we need
		// the timestamp to be a local monotonic timestamp, which is not exposed by the
		// standard library `time`.
		SendTime:     monotime.Now(),
		FromWorkerID: m.workerID,
		Epoch:        m.masterEpoch,
	}

	ok, err := m.messageSender.SendToNode(ctx, m.masterNode, HeartbeatPingTopic(m.masterID), heartbeatMsg)
	if err != nil {
		return errors.Trace(err)
	}
	if !ok {
		log.L().Warn("sending heartbeat ping encountered ErrPeerMessageSendTryAgain")
	}
	return nil
}

func (m *masterClient) SendStatus(ctx context.Context, status WorkerStatus, workload model.RescUnit) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	statusUpdateMessage := &StatusUpdateMessage{
		WorkerID: m.workerID,
		Status:   status,
	}
	ok, err := m.messageSender.SendToNode(ctx, m.masterNode, StatusUpdateTopic(m.masterID), statusUpdateMessage)
	if err != nil {
		return errors.Trace(err)
	}
	if !ok {
		if cerror.ErrPeerMessageSendTryAgain.Equal(err) {
			log.L().Warn("sending status update encountered ErrPeerMessageSendTryAgain")
			return nil
		}
	}

	workloadReportMessage := &WorkloadReportMessage{
		WorkerID: m.workerID,
		Workload: workload,
	}
	ok, err = m.messageSender.SendToNode(ctx, m.masterNode, WorkloadReportTopic(m.masterID), workloadReportMessage)
	if err != nil {
		return errors.Trace(err)
	}
	if !ok {
		if cerror.ErrPeerMessageSendTryAgain.Equal(err) {
			log.L().Warn("sending status update encountered ErrPeerMessageSendTryAgain")
			return nil
		}
	}

	return nil
}
