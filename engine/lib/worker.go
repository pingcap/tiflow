package lib

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/pkg/workerpool"
	"go.uber.org/atomic"
	"go.uber.org/dig"
	"go.uber.org/zap"

	runtime "github.com/hanfei1991/microcosm/executor/worker"
	"github.com/hanfei1991/microcosm/lib/config"
	"github.com/hanfei1991/microcosm/lib/metadata"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/lib/statusutil"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/clock"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/errctx"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/externalresource/broker"
	resourcemeta "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
	extkv "github.com/hanfei1991/microcosm/pkg/meta/extension"
	"github.com/hanfei1991/microcosm/pkg/meta/kvclient"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
	pkgOrm "github.com/hanfei1991/microcosm/pkg/orm"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/hanfei1991/microcosm/pkg/tenant"
)

// Worker defines an interface that provides all methods that will be used in
// runtime(runner container)
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

// BaseWorker defines the worker interface, it embeds a Worker interface and adds
// more utility methods
type BaseWorker interface {
	Worker

	MetaKVClient() metaclient.KVClient
	UpdateStatus(ctx context.Context, status libModel.WorkerStatus) error
	SendMessage(ctx context.Context, topic p2p.Topic, message interface{}) (bool, error)
	OpenStorage(ctx context.Context, resourcePath resourcemeta.ResourceID) (broker.Handle, error)
	// Exit should be called when worker (in user logic) wants to exit.
	// When `err` is not nil, the status code is assigned WorkerStatusError.
	// Otherwise worker should set its status code to a meaningful value.
	Exit(ctx context.Context, status libModel.WorkerStatus, err error) error
}

type workerExitFsmState = int32

const (
	workerNormal = workerExitFsmState(iota + 1)
	workerHalfExit
	workerExited
)

// DefaultBaseWorker implements BaseWorker interface, it also embeds an Impl
// which implements the WorkerImpl interface and passed from business logic.
type DefaultBaseWorker struct {
	Impl WorkerImpl

	messageHandlerManager p2p.MessageHandlerManager
	messageSender         p2p.MessageSender
	// framework metastore client
	frameMetaClient pkgOrm.Client
	// user metastore raw kvclient
	userRawKVClient extkv.KVClientEx
	resourceBroker  broker.Broker

	masterClient *masterClient
	masterID     libModel.MasterID

	workerMetaClient *metadata.WorkerMetadataClient
	statusSender     *statusutil.Writer
	workerStatus     *libModel.WorkerStatus
	messageRouter    *MessageRouter

	id            libModel.WorkerID
	timeoutConfig config.TimeoutConfig

	pool workerpool.AsyncPool

	wg        sync.WaitGroup
	errCenter *errctx.ErrCenter

	cancelMu      sync.Mutex
	cancelBgTasks context.CancelFunc
	cancelPool    context.CancelFunc

	exitController *workerExitController

	clock clock.Clock

	// user metastore prefix kvclient
	// Don't close it. It's just a prefix wrapper for underlying userRawKVClient
	userMetaKVClient metaclient.KVClient
}

type workerParams struct {
	dig.In

	MessageHandlerManager p2p.MessageHandlerManager
	MessageSender         p2p.MessageSender
	FrameMetaClient       pkgOrm.Client
	UserRawKVClient       extkv.KVClientEx
	ResourceBroker        broker.Broker
}

// NewBaseWorker creates a new BaseWorker instance
func NewBaseWorker(
	ctx *dcontext.Context,
	impl WorkerImpl,
	workerID libModel.WorkerID,
	masterID libModel.MasterID,
	// tp libModel.WorkerType,
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
		frameMetaClient:       params.FrameMetaClient,
		userRawKVClient:       params.UserRawKVClient,
		resourceBroker:        params.ResourceBroker,

		masterID: masterID,
		id:       workerID,
		workerStatus: &libModel.WorkerStatus{
			// TODO ProjectID
			JobID: masterID,
			ID:    workerID,
			// TODO: worker_type
		},
		timeoutConfig: config.DefaultTimeoutConfig(),

		pool: workerpool.NewDefaultAsyncPool(1),

		errCenter: errctx.NewErrCenter(),
		clock:     clock.New(),
		// [TODO] use tenantID if support multi-tenant
		userMetaKVClient: kvclient.NewPrefixKVClient(params.UserRawKVClient, tenant.DefaultUserTenantID),
	}
}

// Workload implements BaseWorker.Workload
func (w *DefaultBaseWorker) Workload() model.RescUnit {
	return w.Impl.Workload()
}

// Init implements BaseWorker.Init
func (w *DefaultBaseWorker) Init(ctx context.Context) error {
	ctx = w.errCenter.WithCancelOnFirstError(ctx)

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

	w.startBackgroundTasks()

	initTime := w.clock.Mono()
	rctx, ok := runtime.ToRuntimeCtx(ctx)
	if ok {
		initTime = clock.ToMono(rctx.SubmitTime())
	}

	w.masterClient = newMasterClient(
		w.masterID,
		w.id,
		w.messageSender,
		w.frameMetaClient,
		initTime,
		func() error {
			return errors.Trace(w.Impl.OnMasterFailover(MasterFailoverReason{
				// TODO support other fail-over reasons
				Code: MasterTimedOut,
			}))
		})

	w.exitController = newWorkerExitController(w.masterClient, w.errCenter, w.clock)
	w.workerMetaClient = metadata.NewWorkerMetadataClient(w.masterID, w.frameMetaClient)

	w.statusSender = statusutil.NewWriter(
		w.frameMetaClient, w.messageSender, w.masterClient, w.id)
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
	// Upsert the worker to ensure we have created the worker info
	if err := w.frameMetaClient.UpsertWorker(ctx, w.workerStatus); err != nil {
		return errors.Trace(err)
	}

	w.workerStatus.Code = libModel.WorkerStatusInit
	if err := w.statusSender.UpdateStatus(ctx, w.workerStatus); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (w *DefaultBaseWorker) doPoll(ctx context.Context) error {
	err := w.exitController.PollExit()
	if err != nil {
		return err
	}

	if err := w.messageHandlerManager.CheckError(ctx); err != nil {
		return err
	}

	return w.messageRouter.Tick(ctx)
}

// Poll implements BaseWorker.Poll
func (w *DefaultBaseWorker) Poll(ctx context.Context) error {
	ctx = w.errCenter.WithCancelOnFirstError(ctx)

	if err := w.doPoll(ctx); err != nil {
		if derror.ErrWorkerHalfExit.NotEqual(err) {
			return err
		}
		return nil
	}

	if err := w.Impl.Tick(ctx); err != nil {
		w.errCenter.OnError(err)
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

// Close implements BaseWorker.Close
func (w *DefaultBaseWorker) Close(ctx context.Context) error {
	if err := w.Impl.CloseImpl(ctx); err != nil {
		log.L().Error("Failed to close WorkerImpl", zap.Error(err))
		return errors.Trace(err)
	}

	w.doClose()
	return nil
}

// ID implements BaseWorker.ID
func (w *DefaultBaseWorker) ID() runtime.RunnableID {
	return w.id
}

// MetaKVClient implements BaseWorker.MetaKVClient
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
	ctx = w.errCenter.WithCancelOnFirstError(ctx)

	w.workerStatus.Code = status.Code
	w.workerStatus.ErrorMessage = status.ErrorMessage
	w.workerStatus.ExtBytes = status.ExtBytes
	err := w.statusSender.UpdateStatus(ctx, w.workerStatus)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// SendMessage implements BaseWorker.SendMessage
func (w *DefaultBaseWorker) SendMessage(
	ctx context.Context,
	topic p2p.Topic,
	message interface{},
) (bool, error) {
	ctx = w.errCenter.WithCancelOnFirstError(ctx)
	return w.messageSender.SendToNode(ctx, w.masterClient.MasterNode(), topic, message)
}

// OpenStorage implements BaseWorker.OpenStorage
func (w *DefaultBaseWorker) OpenStorage(ctx context.Context, resourcePath resourcemeta.ResourceID) (broker.Handle, error) {
	ctx = w.errCenter.WithCancelOnFirstError(ctx)
	return w.resourceBroker.OpenStorage(ctx, w.id, w.masterID, resourcePath)
}

// Exit implements BaseWorker.Exit
func (w *DefaultBaseWorker) Exit(ctx context.Context, status libModel.WorkerStatus, err error) error {
	if err != nil {
		status.Code = libModel.WorkerStatusError
	}

	w.workerStatus.Code = status.Code
	w.workerStatus.ErrorMessage = status.ErrorMessage
	w.workerStatus.ExtBytes = status.ExtBytes
	if err1 := w.statusSender.UpdateStatus(ctx, w.workerStatus); err1 != nil {
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
	ticker := w.clock.Ticker(w.timeoutConfig.WorkerHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			isFinished := false
			if w.exitController.IsExiting() {
				// If we are in the state workerHalfExit,
				// we need to notify the master so that the master
				// marks us as exited.
				isFinished = true
			}
			if err := w.masterClient.SendHeartBeat(ctx, w.clock, isFinished); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (w *DefaultBaseWorker) runWatchDog(ctx context.Context) error {
	ticker := w.clock.Ticker(w.timeoutConfig.WorkerHeartbeatInterval)
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
			errOut := derror.ErrWorkerSuicide.GenWithStackByArgs(w.masterClient.MasterID())
			w.exitController.ForceExit(errOut)
			return errOut
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
	topic := libModel.HeartbeatPongTopic(w.masterClient.MasterID(), w.id)
	ok, err := w.messageHandlerManager.RegisterHandler(
		ctx,
		topic,
		&libModel.HeartbeatPongMessage{},
		func(sender p2p.NodeID, value p2p.MessageValue) error {
			msg := value.(*libModel.HeartbeatPongMessage)
			log.L().Info("heartbeat pong received",
				zap.String("master-id", w.masterID),
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

	topic = libModel.WorkerStatusChangeRequestTopic(w.masterID, w.id)
	ok, err = w.messageHandlerManager.RegisterHandler(
		ctx,
		topic,
		&libModel.StatusChangeRequest{},
		func(sender p2p.NodeID, value p2p.MessageValue) error {
			msg, ok := value.(*libModel.StatusChangeRequest)
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
	w.errCenter.OnError(err)
}

type masterClient struct {
	mu          sync.RWMutex
	masterID    libModel.MasterID
	masterNode  p2p.NodeID
	masterEpoch libModel.Epoch

	workerID libModel.WorkerID

	messageSender           p2p.MessageSender
	frameMetaClient         pkgOrm.Client
	lastMasterAckedPingTime clock.MonotonicTime

	// masterSideClosed records whether the master
	// has marked us as closed
	masterSideClosed atomic.Bool

	timeoutConfig config.TimeoutConfig

	onMasterFailOver func() error
}

func newMasterClient(
	masterID libModel.MasterID,
	workerID libModel.WorkerID,
	messageRouter p2p.MessageSender,
	metaCli pkgOrm.Client,
	initTime clock.MonotonicTime,
	onMasterFailOver func() error,
) *masterClient {
	return &masterClient{
		masterID:                masterID,
		workerID:                workerID,
		messageSender:           messageRouter,
		frameMetaClient:         metaCli,
		lastMasterAckedPingTime: initTime,
		timeoutConfig:           config.DefaultTimeoutConfig(),
		onMasterFailOver:        onMasterFailOver,
	}
}

func (m *masterClient) InitMasterInfoFromMeta(ctx context.Context) error {
	metaClient := metadata.NewMasterMetadataClient(m.masterID, m.frameMetaClient)
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

func (m *masterClient) RefreshMasterInfo(ctx context.Context) error {
	metaClient := metadata.NewMasterMetadataClient(m.masterID, m.frameMetaClient)
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
		m.mu.Unlock()
		if err := m.onMasterFailOver(); err != nil {
			return errors.Trace(err)
		}
	} else {
		m.mu.Unlock()
	}
	return nil
}

func (m *masterClient) MasterID() libModel.MasterID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.masterID
}

func (m *masterClient) MasterNode() p2p.NodeID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.masterNode
}

func (m *masterClient) Epoch() libModel.Epoch {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.masterEpoch
}

func (m *masterClient) HandleHeartbeat(sender p2p.NodeID, msg *libModel.HeartbeatPongMessage) {
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

	if msg.IsFinished {
		m.masterSideClosed.Store(true)
	}
	m.lastMasterAckedPingTime = msg.SendTime
}

func (m *masterClient) CheckMasterTimeout(ctx context.Context, clock clock.Clock) (ok bool, err error) {
	m.mu.RLock()
	lastMasterAckedPingTime := m.lastMasterAckedPingTime
	m.mu.RUnlock()

	sinceLastAcked := clock.Mono().Sub(lastMasterAckedPingTime)
	if sinceLastAcked <= 2*m.timeoutConfig.WorkerHeartbeatInterval {
		return true, nil
	}

	if sinceLastAcked > 2*m.timeoutConfig.WorkerHeartbeatInterval &&
		sinceLastAcked < m.timeoutConfig.WorkerTimeoutDuration {

		if err := m.RefreshMasterInfo(ctx); err != nil {
			return false, errors.Trace(err)
		}
		return true, nil
	}

	return false, nil
}

func (m *masterClient) SendHeartBeat(ctx context.Context, clock clock.Clock, isFinished bool) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// We use the monotonic time because we would like to serialize a local timestamp.
	// The timestamp will be returned in a PONG for time-out check, so we need
	// the timestamp to be a local monotonic timestamp, which is not exposed by the
	// standard library `time`.
	sendTime := clock.Mono()
	heartbeatMsg := &libModel.HeartbeatPingMessage{
		SendTime:     sendTime,
		FromWorkerID: m.workerID,
		Epoch:        m.masterEpoch,
		IsFinished:   isFinished,
	}

	log.L().Debug("sending heartbeat", zap.String("worker", m.workerID))
	ok, err := m.messageSender.SendToNode(ctx, m.masterNode, libModel.HeartbeatPingTopic(m.masterID), heartbeatMsg)
	if err != nil {
		return errors.Trace(err)
	}
	log.L().Info("sending heartbeat success", zap.String("worker", m.workerID),
		zap.String("master-id", m.masterID))
	if !ok {
		log.L().Warn("sending heartbeat ping encountered ErrPeerMessageSendTryAgain")
	}
	return nil
}

func (m *masterClient) IsMasterSideClosed() bool {
	return m.masterSideClosed.Load()
}

// used in unit test only
func (m *masterClient) getLastMasterAckedPingTime() clock.MonotonicTime {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastMasterAckedPingTime
}

const (
	workerExitWaitForMasterTimeout = time.Second * 15
)

// workerExitController implements the exit sequence of
// a worker. This object is thread-safe.
// TODO move this to a separate file or package.
type workerExitController struct {
	workerExitFsm atomic.Int32
	halfExitTime  atomic.Time
	errCenter     *errctx.ErrCenter
	masterClient  *masterClient

	// clock is to facilitate unit testing.
	clock clock.Clock
}

func newWorkerExitController(
	masterClient *masterClient,
	errCenter *errctx.ErrCenter,
	clock clock.Clock,
) *workerExitController {
	return &workerExitController{
		workerExitFsm: *atomic.NewInt32(workerNormal),
		errCenter:     errCenter,
		masterClient:  masterClient,
		clock:         clock,
	}
}

// PollExit is called in each tick of the worker.
// Returning an error other than ErrWorkerHalfExit
// means that the worker is ready to exit.
func (c *workerExitController) PollExit() error {
	err := c.errCenter.CheckError()
	if err == nil {
		return nil
	}

	switch c.workerExitFsm.Load() {
	case workerNormal:
		c.workerExitFsm.CAS(workerNormal, workerHalfExit)
		c.halfExitTime.Store(c.clock.Now())
		return derror.ErrWorkerHalfExit.FastGenByArgs()
	case workerHalfExit:
		if c.masterClient.IsMasterSideClosed() {
			c.workerExitFsm.Store(workerExited)
			return err
		}
		sinceStartExiting := c.clock.Since(c.halfExitTime.Load())
		if sinceStartExiting > workerExitWaitForMasterTimeout {
			// TODO log worker ID and master ID.
			log.L().Warn("Exiting worker cannot get acknowledgement from master")
			return err
		}
		return derror.ErrWorkerHalfExit.FastGenByArgs()
	case workerExited:
		return err
	default:
		log.L().Panic("unreachable")
	}
	return nil
}

// ForceExit forces a quick exit without notifying the
// master. It should be used when suicide is required when
// we have lost contact with the master.
func (c *workerExitController) ForceExit(errIn error) {
	c.errCenter.OnError(errIn)
	c.workerExitFsm.Store(workerExited)
}

// IsExiting indicates whether the worker is performing
// an exit sequence.
func (c *workerExitController) IsExiting() bool {
	return c.workerExitFsm.Load() == workerHalfExit
}
