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

package framework

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	runtime "github.com/pingcap/tiflow/engine/executor/worker"
	"github.com/pingcap/tiflow/engine/framework/config"
	frameErrors "github.com/pingcap/tiflow/engine/framework/internal/errors"
	"github.com/pingcap/tiflow/engine/framework/internal/worker"
	frameLog "github.com/pingcap/tiflow/engine/framework/logutil"
	"github.com/pingcap/tiflow/engine/framework/metadata"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/framework/statusutil"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/errctx"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/broker"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/engine/pkg/meta"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/pingcap/tiflow/pkg/workerpool"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

// Worker defines an interface that provides all methods that will be used in
// runtime(runner container)
type Worker interface {
	Init(ctx context.Context) error
	Poll(ctx context.Context) error
	ID() runtime.RunnableID
	Close(ctx context.Context) error
	Stop(ctx context.Context) error
	NotifyExit(ctx context.Context, errIn error) error
}

// WorkerImpl is the implementation of a worker of dataflow engine.
// the implementation struct must embed the framework.BaseWorker interface, this
// interface will be initialized by the framework.
type WorkerImpl interface {
	// InitImpl is called as the consequence of CreateWorker from jobmaster or failover,
	// business logic is expected to do initialization here.
	// Return:
	// - error to let the framework call CloseImpl.
	// Concurrent safety:
	// - this function is called as the first callback function of an WorkerImpl
	//   instance, and it's not concurrent with other callbacks.
	InitImpl(ctx context.Context) error

	// Tick is called on a fixed interval after WorkerImpl is initialized, business
	// logic can do some periodic tasks here.
	// Return:
	// - error to let the framework call CloseImpl.
	// Concurrent safety:
	// - this function may be concurrently called with OnMasterMessage.
	Tick(ctx context.Context) error

	// OnMasterMessage is called when worker receives master message, business developer
	// does not need to implement it.
	// TODO: move it out of WorkerImpl and should not be concurrent with CloseImpl.
	OnMasterMessage(ctx context.Context, topic p2p.Topic, message p2p.MessageValue) error

	// CloseImpl is called as the consequence of returning error from InitImpl or
	// Tick, the Tick will be stopped after entering this function. Business logic
	// is expected to release resources here, but business developer should be aware
	// that when the runtime is crashed, CloseImpl has no time to be called.
	// CloseImpl will only be called for once.
	// TODO: no other callbacks will be called after CloseImpl
	// Concurrent safety:
	// - this function may be concurrently called with OnMasterMessage.
	CloseImpl(ctx context.Context)
}

// BaseWorker defines the worker interface, it embeds a Worker interface and adds
// more utility methods
// TODO: decouple the BaseWorker and WorkerService(for business)
type BaseWorker interface {
	Worker

	// MetaKVClient return business metastore kv client with job-level isolation
	MetaKVClient() metaModel.KVClient

	// MetricFactory return a promethus factory with some underlying labels(e.g. job-id, work-id)
	MetricFactory() promutil.Factory

	// Logger return a zap logger with some underlying fields(e.g. job-id)
	Logger() *zap.Logger

	// UpdateStatus persists the status to framework metastore if worker status is changed and
	// sends 'status updated message' to master.
	UpdateStatus(ctx context.Context, status frameModel.WorkerStatus) error

	// SendMessage sends a message of specific topic to master in a blocking or nonblocking way
	SendMessage(ctx context.Context, topic p2p.Topic, message interface{}, nonblocking bool) error

	// OpenStorage creates a resource and return the resource handle
	OpenStorage(
		ctx context.Context, resourcePath resModel.ResourceID, opts ...broker.OpenStorageOption,
	) (broker.Handle, error)

	// GetEnabledBucketStorage returns whether the bucket storage is enabled and
	// the resource type if bucket exists
	GetEnabledBucketStorage() (bool, resModel.ResourceType)

	// Exit should be called when worker (in user logic) wants to exit.
	// exitReason: ExitReasonFinished/ExitReasonCanceled/ExitReasonFailed
	Exit(ctx context.Context, exitReason ExitReason, err error, extBytes []byte) error
}

// DefaultBaseWorker implements BaseWorker interface, it also embeds an Impl
// which implements the WorkerImpl interface and passed from business logic.
type DefaultBaseWorker struct {
	Impl WorkerImpl

	messageHandlerManager p2p.MessageHandlerManager
	messageSender         p2p.MessageSender
	// framework metastore client
	frameMetaClient pkgOrm.Client
	resourceBroker  broker.Broker

	masterClient *worker.MasterClient
	masterID     frameModel.MasterID

	workerMetaClient *metadata.WorkerStatusClient
	statusSender     *statusutil.Writer
	workerStatus     *frameModel.WorkerStatus
	messageRouter    *MessageRouter

	id            frameModel.WorkerID
	timeoutConfig config.TimeoutConfig

	pool workerpool.AsyncPool

	wg        sync.WaitGroup
	errCenter *errctx.ErrCenter

	cancelMu      sync.Mutex
	cancelBgTasks context.CancelFunc
	cancelPool    context.CancelFunc

	closeImplOnce sync.Once

	clock clock.Clock

	// business metastore kvclient with namespace
	businessMetaKVClient metaModel.KVClient

	// metricFactory can produce metric with underlying project info and job info
	metricFactory promutil.Factory

	// logger is the zap logger with underlying project info and job info
	logger *zap.Logger

	// tenant/project information
	projectInfo tenant.ProjectInfo
}

type workerParams struct {
	dig.In

	MessageHandlerManager p2p.MessageHandlerManager
	MessageSender         p2p.MessageSender
	FrameMetaClient       pkgOrm.Client
	BusinessClientConn    metaModel.ClientConn
	ResourceBroker        broker.Broker
}

// NewBaseWorker creates a new BaseWorker instance
func NewBaseWorker(
	ctx *dcontext.Context,
	impl WorkerImpl,
	workerID frameModel.WorkerID,
	masterID frameModel.MasterID,
	tp frameModel.WorkerType,
	epoch frameModel.Epoch,
) BaseWorker {
	var params workerParams
	if err := ctx.Deps().Fill(&params); err != nil {
		log.Panic("Failed to fill dependencies for BaseWorker",
			zap.Error(err))
	}

	logger := logutil.FromContext(*ctx)

	cli, err := meta.NewKVClientWithNamespace(params.BusinessClientConn, ctx.ProjectInfo.UniqueID(), masterID)
	if err != nil {
		// TODO more elegant error handling
		log.Panic("failed to create business kvclient", zap.Error(err))
	}

	return &DefaultBaseWorker{
		Impl:                  impl,
		messageHandlerManager: params.MessageHandlerManager,
		messageSender:         params.MessageSender,
		frameMetaClient:       params.FrameMetaClient,
		resourceBroker:        params.ResourceBroker,

		masterID:    masterID,
		id:          workerID,
		projectInfo: ctx.ProjectInfo,
		workerStatus: &frameModel.WorkerStatus{
			ProjectID: ctx.ProjectInfo.UniqueID(),
			JobID:     masterID,
			ID:        workerID,
			Type:      tp,
			Epoch:     epoch,
		},
		timeoutConfig: config.DefaultTimeoutConfig(),

		pool: workerpool.NewDefaultAsyncPool(1),

		errCenter:            errctx.NewErrCenter(),
		clock:                clock.New(),
		businessMetaKVClient: cli,
		metricFactory:        promutil.NewFactory4Worker(ctx.ProjectInfo, MustConvertWorkerType2JobType(tp), masterID, workerID),
		logger:               frameLog.WithWorkerID(frameLog.WithMasterID(logger, masterID), workerID),
	}
}

// Init implements BaseWorker.Init
func (w *DefaultBaseWorker) Init(ctx context.Context) error {
	// Note this context must not be held in any resident goroutine.
	ctx, cancel := w.errCenter.WithCancelOnFirstError(ctx)
	defer cancel()

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

// NotifyExit implements BaseWorker.NotifyExit
func (w *DefaultBaseWorker) NotifyExit(ctx context.Context, errIn error) (retErr error) {
	w.closeImplOnce.Do(func() {
		// Must ensure that the business logic is
		// notified before closing.
		w.callCloseImpl()
	})

	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		w.logger.Info("worker finished exiting",
			zap.NamedError("caused", errIn),
			zap.Duration("duration", duration),
			logutil.ShortError(retErr))
	}()

	w.logger.Info("worker start exiting", zap.NamedError("cause", errIn))
	return w.masterClient.WaitClosed(ctx)
}

func (w *DefaultBaseWorker) doPreInit(ctx context.Context) (retErr error) {
	defer func() {
		if retErr != nil {
			// Wraps the error as FailFast because errors
			// that occurred during doPreInit may indicate
			// a failure that's severe enough that it is not
			// possible for the worker to correctly communicate
			// with the master.
			retErr = frameErrors.FailFast(retErr)
		}
	}()
	// poolCtx will be held in background goroutines, and it won't be canceled
	// until DefaultBaseWorker.Close is called.
	poolCtx, cancelPool := context.WithCancel(context.Background())
	w.cancelMu.Lock()
	w.cancelPool = cancelPool
	w.cancelMu.Unlock()

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		err := w.pool.Run(poolCtx)
		w.Logger().Info("workerpool exited",
			zap.String("worker-id", w.id),
			zap.Error(err))
	}()

	initTime := w.clock.Mono()
	rctx, ok := runtime.ToRuntimeCtx(ctx)
	if ok {
		initTime = rctx.SubmitTime()
	}

	w.masterClient = worker.NewMasterClient(
		w.masterID,
		w.id,
		w.messageSender,
		w.frameMetaClient,
		initTime,
		w.clock,
		w.workerStatus.Epoch,
	)

	w.workerMetaClient = metadata.NewWorkerStatusClient(w.masterID, w.frameMetaClient)

	w.statusSender = statusutil.NewWriter(
		w.frameMetaClient, w.messageSender, w.masterClient, w.id)
	w.messageRouter = NewMessageRouter(w.id, w.pool, defaultMessageRouterBufferSize,
		func(topic p2p.Topic, msg p2p.MessageValue) error {
			return w.Impl.OnMasterMessage(poolCtx, topic, msg)
		},
	)

	w.startBackgroundTasks()

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

	w.workerStatus.State = frameModel.WorkerStateInit
	if err := w.statusSender.UpdateStatus(ctx, w.workerStatus); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (w *DefaultBaseWorker) doPoll(ctx context.Context) error {
	if err := w.errCenter.CheckError(); err != nil {
		return err
	}

	if err := w.messageHandlerManager.CheckError(ctx); err != nil {
		return err
	}

	return w.messageRouter.Tick(ctx)
}

// Poll implements BaseWorker.Poll
func (w *DefaultBaseWorker) Poll(ctx context.Context) error {
	ctx, cancel := w.errCenter.WithCancelOnFirstError(ctx)
	defer cancel()

	if err := w.doPoll(ctx); err != nil {
		return err
	}

	if err := w.Impl.Tick(ctx); err != nil {
		w.errCenter.OnError(err)
		return err
	}
	return nil
}

func (w *DefaultBaseWorker) doClose() {
	if w.resourceBroker != nil {
		// Closing the resource broker here will release all temporary file
		// resources created by the worker.
		w.resourceBroker.OnWorkerClosed(context.Background(), w.id, w.masterID)
	}

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
		w.Logger().Warn("cleaning message handlers failed",
			zap.Error(err))
	}

	w.wg.Wait()
	promutil.UnregisterWorkerMetrics(w.id)
	w.businessMetaKVClient.Close()
}

// Close implements BaseWorker.Close
// TODO remove the return value from the signature.
func (w *DefaultBaseWorker) Close(ctx context.Context) error {
	w.closeImplOnce.Do(func() {
		w.callCloseImpl()
	})

	w.doClose()
	return nil
}

func (w *DefaultBaseWorker) callCloseImpl() {
	closeCtx, cancel := context.WithTimeout(
		context.Background(), w.timeoutConfig.CloseWorkerTimeout)
	defer cancel()

	w.Impl.CloseImpl(closeCtx)
}

// Stop implements Worker.Stop, works the same as Worker.Close
func (w *DefaultBaseWorker) Stop(ctx context.Context) error {
	return w.Close(ctx)
}

// ID implements BaseWorker.ID
func (w *DefaultBaseWorker) ID() runtime.RunnableID {
	return w.id
}

// MetaKVClient implements BaseWorker.MetaKVClient
func (w *DefaultBaseWorker) MetaKVClient() metaModel.KVClient {
	return w.businessMetaKVClient
}

// MetricFactory implements BaseWorker.MetricFactory
func (w *DefaultBaseWorker) MetricFactory() promutil.Factory {
	return w.metricFactory
}

// Logger implements BaseMaster.Logger
func (w *DefaultBaseWorker) Logger() *zap.Logger {
	return w.logger
}

// UpdateStatus updates the worker's status and tries to notify the master.
// The status is persisted if State or ErrorMsg has changed. Refer to (*WorkerState).HasSignificantChange.
//
// If UpdateStatus returns without an error, then the status must have been persisted,
// but there is no guarantee that the master has received a notification.
// Note that if the master cannot handle the notifications fast enough, notifications
// can be lost.
func (w *DefaultBaseWorker) UpdateStatus(ctx context.Context, status frameModel.WorkerStatus) error {
	ctx, cancel := w.errCenter.WithCancelOnFirstError(ctx)
	defer cancel()

	w.workerStatus.State = status.State
	w.workerStatus.ErrorMsg = status.ErrorMsg
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
	nonblocking bool,
) error {
	var err error
	ctx, cancel := w.errCenter.WithCancelOnFirstError(ctx)
	defer cancel()
	if nonblocking {
		_, err = w.messageSender.SendToNode(ctx, w.masterClient.MasterNode(), topic, message)
	} else {
		err = w.messageSender.SendToNodeB(ctx, w.masterClient.MasterNode(), topic, message)
	}
	return err
}

// OpenStorage implements BaseWorker.OpenStorage
func (w *DefaultBaseWorker) OpenStorage(
	ctx context.Context, resourcePath resModel.ResourceID, opts ...broker.OpenStorageOption,
) (broker.Handle, error) {
	ctx, cancel := w.errCenter.WithCancelOnFirstError(ctx)
	defer cancel()
	return w.resourceBroker.OpenStorage(ctx, w.projectInfo, w.id, w.masterID, resourcePath, opts...)
}

// GetEnabledBucketStorage implements BaseWorker.GetEnabledBucketStorage
func (w *DefaultBaseWorker) GetEnabledBucketStorage() (bool, resModel.ResourceType) {
	return w.resourceBroker.GetEnabledBucketStorage()
}

// Exit implements BaseWorker.Exit
func (w *DefaultBaseWorker) Exit(ctx context.Context, exitReason ExitReason, err error, extBytes []byte) (errRet error) {
	// Set the errCenter to prevent user from forgetting to return directly after calling 'Exit'
	defer func() {
		// keep the original error or ErrWorkerFinish in error center
		if err == nil {
			err = errors.ErrWorkerFinish.FastGenByArgs()
		}
		w.onError(err)
	}()

	switch exitReason {
	case ExitReasonFinished:
		w.workerStatus.State = frameModel.WorkerStateFinished
	case ExitReasonCanceled:
		// TODO: replace stop with cancel
		w.workerStatus.State = frameModel.WorkerStateStopped
	case ExitReasonFailed:
		// TODO: replace error with failed
		w.workerStatus.State = frameModel.WorkerStateError
	default:
		w.workerStatus.State = frameModel.WorkerStateError
	}

	if err != nil {
		w.workerStatus.ErrorMsg = err.Error()
	}
	w.workerStatus.ExtBytes = extBytes
	return w.statusSender.UpdateStatus(ctx, w.workerStatus)
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
			if err := w.masterClient.SendHeartBeat(ctx); err != nil {
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

		isNormal, err := w.masterClient.CheckMasterTimeout()
		if err != nil {
			return errors.Trace(err)
		}
		if !isNormal {
			errOut := errors.ErrWorkerSuicide.GenWithStackByArgs(w.masterClient.MasterID())
			return errOut
		}
	}
}

func (w *DefaultBaseWorker) initMessageHandlers(ctx context.Context) (retErr error) {
	defer func() {
		if retErr != nil {
			if err := w.messageHandlerManager.Clean(context.Background()); err != nil {
				w.Logger().Warn("Failed to clean up message handlers",
					zap.String("master-id", w.masterID),
					zap.String("worker-id", w.id))
			}
		}
	}()
	topic := frameModel.HeartbeatPongTopic(w.masterClient.MasterID(), w.id)
	ok, err := w.messageHandlerManager.RegisterHandler(
		ctx,
		topic,
		&frameModel.HeartbeatPongMessage{},
		func(sender p2p.NodeID, value p2p.MessageValue) error {
			msg := value.(*frameModel.HeartbeatPongMessage)
			w.Logger().Info("heartbeat pong received",
				zap.String("master-id", w.masterID),
				zap.Any("msg", msg))
			w.masterClient.HandleHeartbeat(sender, msg)
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	if !ok {
		w.Logger().Panic("duplicate handler",
			zap.String("topic", topic))
	}

	topic = frameModel.WorkerStatusChangeRequestTopic(w.masterID, w.id)
	ok, err = w.messageHandlerManager.RegisterHandler(
		ctx,
		topic,
		&frameModel.StatusChangeRequest{},
		func(sender p2p.NodeID, value p2p.MessageValue) error {
			msg, ok := value.(*frameModel.StatusChangeRequest)
			if !ok {
				return errors.ErrInvalidMasterMessage.GenWithStackByArgs(value)
			}
			w.messageRouter.AppendMessage(topic, msg)
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	if !ok {
		w.Logger().Panic("duplicate handler", zap.String("topic", topic))
	}

	return nil
}

func (w *DefaultBaseWorker) onError(err error) {
	w.errCenter.OnError(err)
}
