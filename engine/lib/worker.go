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

package lib

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"go.uber.org/atomic"
	"go.uber.org/dig"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/pkg/log"
	runtime "github.com/pingcap/tiflow/engine/executor/worker"
	"github.com/pingcap/tiflow/engine/lib/config"
	"github.com/pingcap/tiflow/engine/lib/metadata"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/lib/statusutil"
	"github.com/pingcap/tiflow/engine/lib/worker"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/errctx"
	derror "github.com/pingcap/tiflow/engine/pkg/errors"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/broker"
	resourcemeta "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	extkv "github.com/pingcap/tiflow/engine/pkg/meta/extension"
	"github.com/pingcap/tiflow/engine/pkg/meta/kvclient"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/pingcap/tiflow/pkg/workerpool"
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

	masterClient *worker.MasterClient
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
	metricFactory    promutil.Factory
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
	tp libModel.WorkerType,
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
		// TODO: tenant info and job type
		metricFactory: promutil.NewFactory4Worker(tenant.ProjectInfo{
			TenantID:  tenant.DefaultUserTenantID,
			ProjectID: "TODO",
		}, WorkerTypeForMetric(tp), masterID, workerID),
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

	w.masterClient = worker.NewMasterClient(
		w.masterID,
		w.id,
		w.messageSender,
		w.frameMetaClient,
		initTime)

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

// MetricFactory implements BaseWorker.MetricFactory
func (w *DefaultBaseWorker) MetricFactory() promutil.Factory {
	return w.metricFactory
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

		isNormal, err := w.masterClient.CheckMasterTimeout(w.clock)
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
	masterClient  *worker.MasterClient

	// clock is to facilitate unit testing.
	clock clock.Clock
}

func newWorkerExitController(
	masterClient *worker.MasterClient,
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
