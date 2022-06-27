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

	"go.uber.org/dig"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	runtime "github.com/pingcap/tiflow/engine/executor/worker"
	"github.com/pingcap/tiflow/engine/framework/config"
	frameErrors "github.com/pingcap/tiflow/engine/framework/internal/errors"
	"github.com/pingcap/tiflow/engine/framework/internal/worker"
	frameLog "github.com/pingcap/tiflow/engine/framework/logutil"
	"github.com/pingcap/tiflow/engine/framework/metadata"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/framework/statusutil"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/errctx"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/broker"
	resourcemeta "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	extkv "github.com/pingcap/tiflow/engine/pkg/meta/extension"
	"github.com/pingcap/tiflow/engine/pkg/meta/kvclient"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	derror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/pingcap/tiflow/pkg/workerpool"
)

// Worker defines an interface that provides all methods that will be used in
// runtime(runner container)
type Worker interface {
	Init(ctx context.Context) error
	Poll(ctx context.Context) error
	ID() runtime.RunnableID
	Workload() model.RescUnit
	Close(ctx context.Context) error
	NotifyExit(ctx context.Context, errIn error) error
}

// WorkerImpl is the implementation of a worker of dataflow engine.
// the implementation struct must embed the framework.BaseWorker interface, this
// interface will be initialized by the framework.
type WorkerImpl interface {
	// InitImpl provides customized logic for the business logic to initialize.
	InitImpl(ctx context.Context) error

	// Tick is called on a fixed interval. When an error is returned, the worker
	// will be stopped.
	Tick(ctx context.Context) error

	// Workload returns the current workload of the worker.
	Workload() model.RescUnit

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
	MetricFactory() promutil.Factory
	Logger() *zap.Logger
	UpdateStatus(ctx context.Context, status frameModel.WorkerStatus) error
	SendMessage(ctx context.Context, topic p2p.Topic, message interface{}, nonblocking bool) error
	OpenStorage(ctx context.Context, resourcePath resourcemeta.ResourceID) (broker.Handle, error)
	// Exit should be called when worker (in user logic) wants to exit.
	// When `err` is not nil, the status code is assigned WorkerStatusError.
	// Otherwise worker should set its status code to a meaningful value.
	Exit(ctx context.Context, status frameModel.WorkerStatus, err error) error
	NotifyExit(ctx context.Context, errIn error) error
}

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

	masterClient *worker.MasterClient
	masterID     frameModel.MasterID

	workerMetaClient *metadata.WorkerMetadataClient
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

	// user metastore prefix kvclient
	// Don't close it. It's just a prefix wrapper for underlying userRawKVClient
	userMetaKVClient metaclient.KVClient

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
	UserRawKVClient       extkv.KVClientEx
	ResourceBroker        broker.Broker
}

// NewBaseWorker creates a new BaseWorker instance
func NewBaseWorker(
	ctx *dcontext.Context,
	impl WorkerImpl,
	workerID frameModel.WorkerID,
	masterID frameModel.MasterID,
	tp frameModel.WorkerType,
) BaseWorker {
	var params workerParams
	if err := ctx.Deps().Fill(&params); err != nil {
		log.L().Panic("Failed to fill dependencies for BaseWorker",
			zap.Error(err))
	}

	logger := logutil.FromContext(*ctx)

	return &DefaultBaseWorker{
		Impl:                  impl,
		messageHandlerManager: params.MessageHandlerManager,
		messageSender:         params.MessageSender,
		frameMetaClient:       params.FrameMetaClient,
		userRawKVClient:       params.UserRawKVClient,
		resourceBroker:        params.ResourceBroker,

		masterID:    masterID,
		id:          workerID,
		projectInfo: ctx.ProjectInfo,
		workerStatus: &frameModel.WorkerStatus{
			ProjectID: ctx.ProjectInfo.UniqueID(),
			JobID:     masterID,
			ID:        workerID,
			Type:      tp,
		},
		timeoutConfig: config.DefaultTimeoutConfig(),

		pool: workerpool.NewDefaultAsyncPool(1),

		errCenter:        errctx.NewErrCenter(),
		clock:            clock.New(),
		userMetaKVClient: kvclient.NewPrefixKVClient(params.UserRawKVClient, ctx.ProjectInfo.UniqueID()),
		metricFactory:    promutil.NewFactory4Worker(ctx.ProjectInfo, MustConvertWorkerType2JobType(tp), masterID, workerID),
		logger:           frameLog.WithWorkerID(frameLog.WithMasterID(logger, masterID), workerID),
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
	// TODO refine this part
	poolCtx, cancelPool := context.WithCancel(context.TODO())
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
		w.clock)

	w.workerMetaClient = metadata.NewWorkerMetadataClient(w.masterID, w.frameMetaClient)

	w.statusSender = statusutil.NewWriter(
		w.frameMetaClient, w.messageSender, w.masterClient, w.id)
	w.messageRouter = NewMessageRouter(w.id, w.pool, defaultMessageRouterBufferSize,
		func(topic p2p.Topic, msg p2p.MessageValue) error {
			return w.Impl.OnMasterMessage(topic, msg)
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

	w.workerStatus.Code = frameModel.WorkerStatusInit
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
	ctx = w.errCenter.WithCancelOnFirstError(ctx)

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
		// Closing the resource broker here will
		// release all temporary file resources created by the worker.
		// Since we only support local files for now, deletion is fast,
		// so this method will block until it finishes.
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

	err := w.Impl.CloseImpl(closeCtx)
	if err != nil {
		w.Logger().Warn("Failed to close worker",
			zap.String("worker-id", w.id),
			logutil.ShortError(err))
	}
}

// ID implements BaseWorker.ID
func (w *DefaultBaseWorker) ID() runtime.RunnableID {
	return w.id
}

// MetaKVClient implements BaseWorker.MetaKVClient
func (w *DefaultBaseWorker) MetaKVClient() metaclient.KVClient {
	return w.userMetaKVClient
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
// The status is persisted if Code or ErrorMessage has changed. Refer to (*WorkerStatus).HasSignificantChange.
//
// If UpdateStatus returns without an error, then the status must have been persisted,
// but there is no guarantee that the master has received a notification.
// Note that if the master cannot handle the notifications fast enough, notifications
// can be lost.
func (w *DefaultBaseWorker) UpdateStatus(ctx context.Context, status frameModel.WorkerStatus) error {
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
	nonblocking bool,
) error {
	var err error
	ctx = w.errCenter.WithCancelOnFirstError(ctx)
	if nonblocking {
		_, err = w.messageSender.SendToNode(ctx, w.masterClient.MasterNode(), topic, message)
	} else {
		err = w.messageSender.SendToNodeB(ctx, w.masterClient.MasterNode(), topic, message)
	}
	return err
}

// OpenStorage implements BaseWorker.OpenStorage
func (w *DefaultBaseWorker) OpenStorage(ctx context.Context, resourcePath resourcemeta.ResourceID) (broker.Handle, error) {
	ctx = w.errCenter.WithCancelOnFirstError(ctx)
	return w.resourceBroker.OpenStorage(ctx, w.id, w.masterID, resourcePath)
}

// Exit implements BaseWorker.Exit
func (w *DefaultBaseWorker) Exit(ctx context.Context, status frameModel.WorkerStatus, err error) (errRet error) {
	// Set the errCenter to prevent user from forgetting to return directly after calling 'Exit'
	defer func() {
		w.onError(errRet)
	}()

	if err != nil {
		status.Code = frameModel.WorkerStatusError
	}

	w.workerStatus.Code = status.Code
	w.workerStatus.ErrorMessage = status.ErrorMessage
	w.workerStatus.ExtBytes = status.ExtBytes
	if errRet = w.statusSender.UpdateStatus(ctx, w.workerStatus); errRet != nil {
		return
	}

	errRet = derror.ErrWorkerFinish.FastGenByArgs()
	return
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
			errOut := derror.ErrWorkerSuicide.GenWithStackByArgs(w.masterClient.MasterID())
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
				return derror.ErrInvalidMasterMessage.GenWithStackByArgs(value)
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
