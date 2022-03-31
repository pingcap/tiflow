package lib

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/pkg/workerpool"
	"go.uber.org/dig"
	"go.uber.org/zap"

	runtime "github.com/hanfei1991/microcosm/executor/worker"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/lib/statusutil"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/clock"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/externalresource/broker"
	"github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta"
	extKV "github.com/hanfei1991/microcosm/pkg/meta/extension"
	"github.com/hanfei1991/microcosm/pkg/meta/kvclient"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/hanfei1991/microcosm/pkg/tenant"
)

type Worker interface {
	Init(ctx context.Context) error
	Poll(ctx context.Context) error
	ID() runtime.RunnableID
	Workload() model.RescUnit

	runtime.Closer
}

// WorkerImpl is the implementation of a worker of dataflow engine.
// the implementation struct must embed the lib.BaseWorker interface, this
// interface will be initialized by the framework.
type WorkerImpl interface {
	// InitImpl provides customized logic for the business logic to initialize.
	InitImpl(ctx context.Context) error

	// Tick is called on a fixed interval. When an error is returned, the worker
	// will be stopped.
	Tick(ctx context.Context) error

	// Workload returns the current workload of the worker.
	Workload() model.RescUnit

	// OnMasterFailover is called when the master is failed over.
	OnMasterFailover(reason MasterFailoverReason) error

	// OnMasterMessage is called when worker receives master message
	OnMasterMessage(topic p2p.Topic, message p2p.MessageValue) error

	// CloseImpl tells the WorkerImpl to quit running StatusWorker and release resources.
	CloseImpl(ctx context.Context) error
}

type BaseWorker interface {
	Workload() model.RescUnit
	Init(ctx context.Context) error
	Poll(ctx context.Context) error
	Close(ctx context.Context) error
	ID() runtime.RunnableID
	MetaKVClient() metaclient.KVClient
	UpdateStatus(ctx context.Context, status libModel.WorkerStatus) error
	SendMessage(ctx context.Context, topic p2p.Topic, message interface{}) (bool, error)
	OpenStorage(ctx context.Context, resourcePath resourcemeta.ResourceID) (broker.Handle, error)
	// Exit should be called when worker (in user logic) wants to exit.
	// When `err` is not nil, the status code is assigned WorkerStatusError.
	// Otherwise worker should set its status code to a meaningful value.
	Exit(ctx context.Context, status libModel.WorkerStatus, err error) error
}

type DefaultBaseWorker struct {
	Impl WorkerImpl

	messageHandlerManager p2p.MessageHandlerManager
	messageSender         p2p.MessageSender
	// framework metastore prefix kvclient
	metaKVClient metaclient.KVClient
	// user metastore raw kvclient
	userRawKVClient extKV.KVClientEx
	resourceBroker  broker.Broker

	masterClient *masterClient
	masterID     MasterID

	workerMetaClient *WorkerMetadataClient
	statusSender     *statusutil.Writer
	messageRouter    *MessageRouter

	id            WorkerID
	timeoutConfig TimeoutConfig

	pool workerpool.AsyncPool

	wg    sync.WaitGroup
	errCh chan error

	cancelMu      sync.Mutex
	cancelBgTasks context.CancelFunc
	cancelPool    context.CancelFunc

	clock clock.Clock

	// user metastore prefix kvclient
	// Don't close it. It's just a prefix wrapper for underlying userRawKVClient
	userMetaKVClient metaclient.KVClient
}

type workerParams struct {
	dig.In

	MessageHandlerManager p2p.MessageHandlerManager
	MessageSender         p2p.MessageSender
	MetaKVClient          metaclient.KVClient
	UserRawKVClient       extKV.KVClientEx
	ResourceBroker        broker.Broker
}

func NewBaseWorker(
	ctx *dcontext.Context,
	impl WorkerImpl,
	workerID WorkerID,
	masterID MasterID,
) BaseWorker {
	var params workerParams
	if err := ctx.Deps().Fill(&params); err != nil {
		log.L().Panic("Failed to fill dependencies for BaseWorker",
			zap.Error(err))
	}

	return &DefaultBaseWorker{
		Impl:                  impl,
		messageHandlerManager: params.MessageHandlerManager,
		messageSender:         params.MessageSender,
		metaKVClient:          params.MetaKVClient,
		userRawKVClient:       params.UserRawKVClient,
		resourceBroker:        params.ResourceBroker,

		masterID:      masterID,
		id:            workerID,
		timeoutConfig: defaultTimeoutConfig,

		pool: workerpool.NewDefaultAsyncPool(1),

		errCh: make(chan error, 1),
		clock: clock.New(),
		// [TODO] use tenantID if support multi-tenant
		userMetaKVClient: kvclient.NewPrefixKVClient(params.UserRawKVClient, tenant.DefaultUserTenantID),
	}
}

func (w *DefaultBaseWorker) Workload() model.RescUnit {
	return w.Impl.Workload()
}

func (w *DefaultBaseWorker) Init(ctx context.Context) error {
	if err := w.doPreInit(ctx); err != nil {
		return errors.Trace(err)
	}

	if err := w.Impl.InitImpl(ctx); err != nil {
		return errors.Trace(err)
	}

	if err := w.doPostInit(ctx); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (w *DefaultBaseWorker) doPreInit(ctx context.Context) error {
	// TODO refine this part
	poolCtx, cancelPool := context.WithCancel(context.TODO())
	w.cancelMu.Lock()
	w.cancelPool = cancelPool
	w.cancelMu.Unlock()

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		err := w.pool.Run(poolCtx)
		log.L().Info("workerpool exited",
			zap.String("worker-id", w.id),
			zap.Error(err))
	}()

	w.masterClient = newMasterClient(
		w.masterID,
		w.id,
		w.messageSender,
		w.metaKVClient,
		w.clock.Mono(),
		func() error {
			return errors.Trace(w.Impl.OnMasterFailover(MasterFailoverReason{
				// TODO support other fail-over reasons
				Code: MasterTimedOut,
			}))
		})

	w.workerMetaClient = NewWorkerMetadataClient(w.masterID, w.metaKVClient)

	w.statusSender = statusutil.NewWriter(
		w.metaKVClient, w.messageSender, w.masterClient, w.id)
	w.messageRouter = NewMessageRouter(w.id, w.pool, defaultMessageRouterBufferSize,
		func(topic p2p.Topic, msg p2p.MessageValue) error {
			return w.Impl.OnMasterMessage(topic, msg)
		},
	)

	if err := w.initMessageHandlers(ctx); err != nil {
		return errors.Trace(err)
	}

	if err := w.masterClient.InitMasterInfoFromMeta(ctx); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (w *DefaultBaseWorker) doPostInit(ctx context.Context) error {
	if err := w.statusSender.UpdateStatus(
		ctx, &libModel.WorkerStatus{Code: libModel.WorkerStatusInit}); err != nil {
		return errors.Trace(err)
	}

	w.startBackgroundTasks()
	return nil
}

func (w *DefaultBaseWorker) doPoll(ctx context.Context) error {
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

	if err := w.messageRouter.Tick(ctx); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (w *DefaultBaseWorker) Poll(ctx context.Context) error {
	if err := w.doPoll(ctx); err != nil {
		return errors.Trace(err)
	}

	if err := w.Impl.Tick(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (w *DefaultBaseWorker) doClose() {
	w.cancelMu.Lock()
	if w.cancelBgTasks != nil {
		w.cancelBgTasks()
	}
	if w.cancelPool != nil {
		w.cancelPool()
	}
	w.cancelMu.Unlock()

	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	if err := w.messageHandlerManager.Clean(closeCtx); err != nil {
		log.L().Warn("cleaning message handlers failed",
			zap.Error(err))
	}

	w.wg.Wait()
}

func (w *DefaultBaseWorker) Close(ctx context.Context) error {
	if err := w.Impl.CloseImpl(ctx); err != nil {
		log.L().Error("Failed to close WorkerImpl", zap.Error(err))
		return errors.Trace(err)
	}

	w.doClose()
	return nil
}

func (w *DefaultBaseWorker) ID() runtime.RunnableID {
	return w.id
}

func (w *DefaultBaseWorker) MetaKVClient() metaclient.KVClient {
	return w.userMetaKVClient
}

// UpdateStatus updates the worker's status and tries to notify the master.
// The status is persisted if Code or ErrorMessage has changed. Refer to (*WorkerStatus).HasSignificantChange.
//
// If UpdateStatus returns without an error, then the status must have been persisted,
// but there is no guarantee that the master has received a notification.
// Note that if the master cannot handle the notifications fast enough, notifications
// can be lost.
func (w *DefaultBaseWorker) UpdateStatus(ctx context.Context, status libModel.WorkerStatus) error {
	err := w.statusSender.UpdateStatus(ctx, &status)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (w *DefaultBaseWorker) SendMessage(
	ctx context.Context,
	topic p2p.Topic,
	message interface{},
) (bool, error) {
	return w.messageSender.SendToNode(ctx, w.masterClient.MasterNode(), topic, message)
}

func (w *DefaultBaseWorker) OpenStorage(ctx context.Context, resourcePath resourcemeta.ResourceID) (broker.Handle, error) {
	return w.resourceBroker.OpenStorage(ctx, w.id, w.masterID, resourcePath)
}

func (w *DefaultBaseWorker) Exit(ctx context.Context, status libModel.WorkerStatus, err error) error {
	if err != nil {
		status.Code = libModel.WorkerStatusError
	}

	if err1 := w.statusSender.UpdateStatus(ctx, &status); err1 != nil {
		return err1
	}

	return derror.ErrWorkerFinish.FastGenByArgs()
}

func (w *DefaultBaseWorker) startBackgroundTasks() {
	ctx, cancel := context.WithCancel(context.Background())

	w.cancelMu.Lock()
	w.cancelBgTasks = cancel
	w.cancelMu.Unlock()

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

func (w *DefaultBaseWorker) runHeartbeatWorker(ctx context.Context) error {
	ticker := w.clock.Ticker(w.timeoutConfig.workerHeartbeatInterval)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			if err := w.masterClient.SendHeartBeat(ctx, w.clock); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (w *DefaultBaseWorker) runWatchDog(ctx context.Context) error {
	ticker := w.clock.Ticker(w.timeoutConfig.workerHeartbeatInterval)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
		}

		isNormal, err := w.masterClient.CheckMasterTimeout(ctx, w.clock)
		if err != nil {
			return errors.Trace(err)
		}
		if !isNormal {
			return derror.ErrWorkerSuicide.GenWithStackByArgs(w.masterClient.MasterID())
		}
	}
}

func (w *DefaultBaseWorker) initMessageHandlers(ctx context.Context) (retErr error) {
	defer func() {
		if retErr != nil {
			if err := w.messageHandlerManager.Clean(context.Background()); err != nil {
				log.L().Warn("Failed to clean up message handlers",
					zap.String("master-id", w.masterID),
					zap.String("worker-id", w.id))
			}
		}
	}()
	topic := HeartbeatPongTopic(w.masterClient.MasterID(), w.id)
	ok, err := w.messageHandlerManager.RegisterHandler(
		ctx,
		topic,
		&HeartbeatPongMessage{},
		func(sender p2p.NodeID, value p2p.MessageValue) error {
			msg := value.(*HeartbeatPongMessage)
			log.L().Debug("heartbeat pong received",
				zap.Any("msg", msg))
			w.masterClient.HandleHeartbeat(sender, msg)
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	if !ok {
		log.L().Panic("duplicate handler",
			zap.String("topic", topic))
	}

	topic = WorkerStatusChangeRequestTopic(w.masterID, w.id)
	ok, err = w.messageHandlerManager.RegisterHandler(
		ctx,
		topic,
		&StatusChangeRequest{},
		func(sender p2p.NodeID, value p2p.MessageValue) error {
			msg, ok := value.(*StatusChangeRequest)
			if !ok {
				return derror.ErrInvalidMasterMessage.GenWithStackByArgs(value)
			}
			w.messageRouter.AppendMessage(topic, msg)
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	if !ok {
		log.L().Panic("duplicate handler", zap.String("topic", topic))
	}

	return nil
}

func (w *DefaultBaseWorker) onError(err error) {
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
	metaKVClient            metaclient.KVClient
	lastMasterAckedPingTime clock.MonotonicTime

	timeoutConfig TimeoutConfig

	onMasterFailOver func() error
}

func newMasterClient(
	masterID MasterID,
	workerID WorkerID,
	messageRouter p2p.MessageSender,
	metaKV metaclient.KVClient,
	initTime clock.MonotonicTime,
	onMasterFailOver func() error,
) *masterClient {
	return &masterClient{
		masterID:                masterID,
		workerID:                workerID,
		messageSender:           messageRouter,
		metaKVClient:            metaKV,
		lastMasterAckedPingTime: initTime,
		timeoutConfig:           defaultTimeoutConfig,
		onMasterFailOver:        onMasterFailOver,
	}
}

func (m *masterClient) InitMasterInfoFromMeta(ctx context.Context) error {
	metaClient := NewMasterMetadataClient(m.masterID, m.metaKVClient)
	masterMeta, err := metaClient.Load(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.masterNode = masterMeta.NodeID
	m.masterEpoch = masterMeta.Epoch
	return nil
}

func (m *masterClient) MasterNodeID() p2p.NodeID {
	return m.masterID
}

func (m *masterClient) refreshMasterInfo(ctx context.Context, clock clock.Clock) error {
	metaClient := NewMasterMetadataClient(m.masterID, m.metaKVClient)
	masterMeta, err := metaClient.Load(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	m.mu.Lock()
	m.masterNode = masterMeta.NodeID
	if m.masterEpoch < masterMeta.Epoch {
		log.L().Info("refresh master info", zap.String("masterID", m.masterID),
			zap.Int64("oldEpoch", m.masterEpoch), zap.Int64("newEpoch", masterMeta.Epoch),
		)
		m.masterEpoch = masterMeta.Epoch
		// if worker finds master is transferred, reset the master last ack time
		// to now in case of a false positive detection of master timeout.
		m.lastMasterAckedPingTime = clock.Mono()
		m.mu.Unlock()
		if err := m.onMasterFailOver(); err != nil {
			return errors.Trace(err)
		}
	} else {
		m.mu.Unlock()
	}
	return nil
}

func (m *masterClient) MasterID() MasterID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.masterID
}

func (m *masterClient) MasterNode() p2p.NodeID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.masterNode
}

func (m *masterClient) Epoch() Epoch {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.masterEpoch
}

func (m *masterClient) HandleHeartbeat(sender p2p.NodeID, msg *HeartbeatPongMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if msg.Epoch < m.masterEpoch {
		log.L().Info("epoch does not match, ignore stale heartbeat",
			zap.Any("msg", msg),
			zap.Int64("master-epoch", m.masterEpoch))
		return
	}

	if msg.Epoch > m.masterEpoch {
		// We received a heartbeat from a restarted master, we need to record
		// its information.
		// TODO refine the logic of this part
		m.masterEpoch = msg.Epoch
		m.masterNode = sender
	}
	m.lastMasterAckedPingTime = msg.SendTime
}

func (m *masterClient) CheckMasterTimeout(ctx context.Context, clock clock.Clock) (ok bool, err error) {
	m.mu.RLock()
	lastMasterAckedPingTime := m.lastMasterAckedPingTime
	m.mu.RUnlock()

	sinceLastAcked := clock.Mono().Sub(lastMasterAckedPingTime)
	if sinceLastAcked <= 2*m.timeoutConfig.workerHeartbeatInterval {
		return true, nil
	}

	if sinceLastAcked > 2*m.timeoutConfig.workerHeartbeatInterval &&
		sinceLastAcked < m.timeoutConfig.workerTimeoutDuration {

		if err := m.refreshMasterInfo(ctx, clock); err != nil {
			return false, errors.Trace(err)
		}
		return true, nil
	}

	return false, nil
}

func (m *masterClient) SendHeartBeat(ctx context.Context, clock clock.Clock) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// We use the monotonic time because we would like to serialize a local timestamp.
	// The timestamp will be returned in a PONG for time-out check, so we need
	// the timestamp to be a local monotonic timestamp, which is not exposed by the
	// standard library `time`.
	sendTime := clock.Mono()
	heartbeatMsg := &HeartbeatPingMessage{
		SendTime:     sendTime,
		FromWorkerID: m.workerID,
		Epoch:        m.masterEpoch,
	}

	log.L().Debug("sending heartbeat", zap.String("worker", m.workerID))
	ok, err := m.messageSender.SendToNode(ctx, m.masterNode, HeartbeatPingTopic(m.masterID), heartbeatMsg)
	if err != nil {
		return errors.Trace(err)
	}
	log.L().Debug("sending heartbeat success", zap.String("worker", m.workerID))
	if !ok {
		log.L().Warn("sending heartbeat ping encountered ErrPeerMessageSendTryAgain")
	}
	return nil
}

// used in unit test only
func (m *masterClient) getLastMasterAckedPingTime() clock.MonotonicTime {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastMasterAckedPingTime
}
