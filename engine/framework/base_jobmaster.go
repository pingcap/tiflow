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

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	runtime "github.com/pingcap/tiflow/engine/executor/worker"
	"github.com/pingcap/tiflow/engine/framework/internal/eventloop"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/errctx"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/logutil"
	"go.uber.org/zap"
)

// BaseJobMaster defines an interface that can work as a job master, it embeds
// a Worker interface which can run on dataflow engine runtime, and also provides
// some utility methods.
type BaseJobMaster interface {
	Worker

	// MetaKVClient return business metastore kv client with job-level isolation
	MetaKVClient() metaModel.KVClient

	// MetricFactory return a promethus factory with some underlying labels(e.g. job-id, work-id)
	MetricFactory() promutil.Factory

	// Logger return a zap logger with some underlying fields(e.g. job-id)
	Logger() *zap.Logger

	// GetWorkers return the handle of all workers, from which we can get the worker status、worker id and
	// the method for sending message to specific worker
	GetWorkers() map[frameModel.WorkerID]WorkerHandle

	// CreateWorker requires the framework to dispatch a new worker.
	// If the worker needs to access certain file system resources, it must pass
	// resource ID via CreateWorkerOpt
	CreateWorker(
		workerType frameModel.WorkerType,
		config WorkerConfig,
		opts ...CreateWorkerOpt,
	) (frameModel.WorkerID, error)

	// UpdateJobStatus updates jobmaster(worker of jobmanager) status and
	// sends a 'status updated' message to jobmanager
	UpdateJobStatus(ctx context.Context, status frameModel.WorkerStatus) error

	// CurrentEpoch return the epoch of current job
	CurrentEpoch() frameModel.Epoch

	// SendMessage sends a message of specific topic to jobmanager in a blocking or nonblocking way
	SendMessage(ctx context.Context, topic p2p.Topic, message interface{}, nonblocking bool) error

	// Exit should be called when jobmaster (in user logic) wants to exit.
	// exitReason: ExitReasonFinished/ExitReasonCanceled/ExitReasonFailed
	Exit(ctx context.Context, exitReason ExitReason, err error, detail []byte) error

	// IsMasterReady returns whether the master has received heartbeats for all
	// workers after a fail-over. If this is the first time the JobMaster started up,
	// the return value is always true.
	IsMasterReady() bool

	// IsBaseJobMaster is an empty function used to prevent accidental implementation
	// of this interface.
	IsBaseJobMaster()

	// IsS3StorageEnabled returns whether the s3 storage is enabled
	IsS3StorageEnabled() bool
}

// BaseJobMasterExt extends BaseJobMaster with some extra methods.
// These methods are used by framework and is not visible to JobMasterImpl.
type BaseJobMasterExt interface {
	// TriggerOpenAPIInitialize is used to trigger the initialization of openapi handler.
	// It just delegates to the JobMasterImpl.OnOpenAPIInitialized.
	TriggerOpenAPIInitialize(apiGroup *gin.RouterGroup)

	// IsBaseJobMasterExt is an empty function used to prevent accidental implementation
	// of this interface.
	IsBaseJobMasterExt()
}

var (
	_ BaseJobMaster    = (*DefaultBaseJobMaster)(nil)
	_ BaseJobMasterExt = (*DefaultBaseJobMaster)(nil)
)

// DefaultBaseJobMaster implements BaseJobMaster interface
type DefaultBaseJobMaster struct {
	master    *DefaultBaseMaster
	worker    *DefaultBaseWorker
	impl      JobMasterImpl
	errCenter *errctx.ErrCenter
	closeOnce sync.Once
}

// JobMasterImpl is the implementation of a job master of dataflow engine.
// the implementation struct must embed the framework.BaseJobMaster interface, this
// interface will be initialized by the framework.
type JobMasterImpl interface {
	MasterImpl

	// OnCancel is triggered when a cancel message is received. It can be
	// triggered multiple times.
	// TODO: when it returns error, framework should close this jobmaster.
	OnCancel(ctx context.Context) error
	// OnOpenAPIInitialized is called as the first callback function of the JobMasterImpl
	// instance, the business logic should only register the OpenAPI handler in it.
	// The implementation must not retain the apiGroup.
	// Note: this function is called before Init().
	// Concurrent safety:
	// - this function is called as the first callback function of an JobMasterImpl
	//   instance, and it's not concurrent with other callbacks.
	OnOpenAPIInitialized(apiGroup *gin.RouterGroup)

	// IsJobMasterImpl is an empty function used to prevent accidental implementation
	// of this interface.
	IsJobMasterImpl()
}

// NewBaseJobMaster creates a new DefaultBaseJobMaster instance
func NewBaseJobMaster(
	ctx *dcontext.Context,
	jobMasterImpl JobMasterImpl,
	masterID frameModel.MasterID,
	workerID frameModel.WorkerID,
	tp frameModel.WorkerType,
	workerEpoch frameModel.Epoch,
) BaseJobMaster {
	// master-worker pair: job manager <-> job master(`baseWorker` following)
	// master-worker pair: job master(`baseMaster` following) <-> real workers
	// `masterID` here is the ID of `JobManager`
	// `workerID` here is the ID of Job. It remains unchanged in the job lifecycle.
	baseMaster := NewBaseMaster(
		ctx, &jobMasterImplAsMasterImpl{jobMasterImpl}, workerID, tp)
	baseWorker := NewBaseWorker(
		ctx, &jobMasterImplAsWorkerImpl{inner: jobMasterImpl}, workerID, masterID, tp, workerEpoch)
	errCenter := errctx.NewErrCenter()
	baseMaster.(*DefaultBaseMaster).errCenter = errCenter
	baseWorker.(*DefaultBaseWorker).errCenter = errCenter
	return &DefaultBaseJobMaster{
		master:    baseMaster.(*DefaultBaseMaster),
		worker:    baseWorker.(*DefaultBaseWorker),
		impl:      jobMasterImpl,
		errCenter: errCenter,
	}
}

// MetaKVClient implements BaseJobMaster.MetaKVClient
func (d *DefaultBaseJobMaster) MetaKVClient() metaModel.KVClient {
	return d.master.MetaKVClient()
}

// MetricFactory implements BaseJobMaster.MetricFactory
func (d *DefaultBaseJobMaster) MetricFactory() promutil.Factory {
	return d.master.MetricFactory()
}

// Logger implements BaseJobMaster.Logger
func (d *DefaultBaseJobMaster) Logger() *zap.Logger {
	return d.master.logger
}

// Init implements BaseJobMaster.Init
func (d *DefaultBaseJobMaster) Init(ctx context.Context) error {
	// Note this context must not be held in any resident goroutine.
	ctx, cancel := d.errCenter.WithCancelOnFirstError(ctx)
	defer cancel()

	if err := d.worker.doPreInit(ctx); err != nil {
		return errors.Trace(err)
	}

	isFirstStartUp, err := d.master.doInit(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	if isFirstStartUp {
		if err := d.impl.InitImpl(ctx); err != nil {
			// Currently we only pass error to error center when calling busniess
			// API returns error. Business API is also known as XxxImpl.
			d.errCenter.OnError(err)
			return errors.Trace(err)
		}
		if err := d.master.markStateInMetadata(ctx, frameModel.MasterStateInit); err != nil {
			return errors.Trace(err)
		}
	} else {
		if err := d.impl.OnMasterRecovered(ctx); err != nil {
			d.errCenter.OnError(err)
			return errors.Trace(err)
		}
	}

	if err := d.worker.doPostInit(ctx); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Poll implements BaseJobMaster.Poll
func (d *DefaultBaseJobMaster) Poll(ctx context.Context) error {
	ctx, cancel := d.errCenter.WithCancelOnFirstError(ctx)
	defer cancel()

	if err := d.master.doPoll(ctx); err != nil {
		return errors.Trace(err)
	}
	if err := d.worker.doPoll(ctx); err != nil {
		if !errors.Is(err, errors.ErrWorkerHalfExit) {
			return errors.Trace(err)
		}
		return nil
	}
	if err := d.impl.Tick(ctx); err != nil {
		d.errCenter.OnError(err)
		return errors.Trace(err)
	}
	return nil
}

// GetWorkers implements BaseJobMaster.GetWorkers
func (d *DefaultBaseJobMaster) GetWorkers() map[frameModel.WorkerID]WorkerHandle {
	return d.master.GetWorkers()
}

// Close implements BaseJobMaster.Close
func (d *DefaultBaseJobMaster) Close(ctx context.Context) error {
	d.closeOnce.Do(func() {
		d.impl.CloseImpl(ctx)
	})

	d.master.persistMetaError()
	d.master.doClose()
	d.worker.doClose()
	return nil
}

// Stop implements BaseJobMaster.Stop
func (d *DefaultBaseJobMaster) Stop(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	d.impl.StopImpl(ctx)
	d.master.doClose()
	d.worker.doClose()
	return nil
}

// NotifyExit implements BaseJobMaster interface
func (d *DefaultBaseJobMaster) NotifyExit(ctx context.Context, errIn error) (retErr error) {
	if eventloop.IsTerminatedError(errIn) {
		// In terminate scenario job master should call StopImpl, and we don't
		// call NotifyExit to advance the calling of StopImpl. The drawback of
		// this choice is job manager has to hearbeat timeout of this job master.
		return nil
	}

	d.closeOnce.Do(func() {
		d.impl.CloseImpl(ctx)
	})

	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		d.Logger().Info("job master finished exiting",
			zap.NamedError("caused", errIn),
			zap.Duration("duration", duration),
			logutil.ShortError(retErr))
	}()

	d.Logger().Info("worker start exiting", zap.NamedError("cause", errIn))
	return d.worker.masterClient.WaitClosed(ctx)
}

// CreateWorker implements BaseJobMaster.CreateWorker
func (d *DefaultBaseJobMaster) CreateWorker(
	workerType frameModel.WorkerType,
	config WorkerConfig,
	opts ...CreateWorkerOpt,
) (frameModel.WorkerID, error) {
	return d.master.CreateWorker(workerType, config, opts...)
}

// UpdateStatus delegates the UpdateStatus of inner worker
func (d *DefaultBaseJobMaster) UpdateStatus(ctx context.Context, status frameModel.WorkerStatus) error {
	ctx, cancel := d.errCenter.WithCancelOnFirstError(ctx)
	defer cancel()

	return d.worker.UpdateStatus(ctx, status)
}

// ID delegates the ID of inner worker
func (d *DefaultBaseJobMaster) ID() runtime.RunnableID {
	// JobMaster is a combination of 'master' and 'worker'
	// d.master.MasterID() == d.worker.ID() == JobID
	return d.worker.ID()
}

// UpdateJobStatus implements BaseJobMaster.UpdateJobStatus
func (d *DefaultBaseJobMaster) UpdateJobStatus(ctx context.Context, status frameModel.WorkerStatus) error {
	ctx, cancel := d.errCenter.WithCancelOnFirstError(ctx)
	defer cancel()

	return d.worker.UpdateStatus(ctx, status)
}

// CurrentEpoch implements BaseJobMaster.CurrentEpoch
func (d *DefaultBaseJobMaster) CurrentEpoch() frameModel.Epoch {
	return d.master.currentEpoch.Load()
}

// IsBaseJobMaster implements BaseJobMaster.IsBaseJobMaster
func (d *DefaultBaseJobMaster) IsBaseJobMaster() {
}

// IsS3StorageEnabled implements BaseJobMaster.IsS3StorageEnabled
func (d *DefaultBaseJobMaster) IsS3StorageEnabled() bool {
	return d.worker.IsS3StorageEnabled()
}

// SendMessage delegates the SendMessage or inner worker
func (d *DefaultBaseJobMaster) SendMessage(ctx context.Context, topic p2p.Topic, message interface{}, nonblocking bool) error {
	ctx, cancel := d.errCenter.WithCancelOnFirstError(ctx)
	defer cancel()

	// master will use WorkerHandle to send message
	return d.worker.SendMessage(ctx, topic, message, nonblocking)
}

// IsMasterReady implements BaseJobMaster.IsMasterReady
func (d *DefaultBaseJobMaster) IsMasterReady() bool {
	return d.master.IsMasterReady()
}

// Exit implements BaseJobMaster.Exit
func (d *DefaultBaseJobMaster) Exit(ctx context.Context, exitReason ExitReason, err error, detail []byte) error {
	ctx, cancel := d.errCenter.WithCancelOnFirstError(ctx)
	defer cancel()

	// Don't set error center for master to make worker.Exit work well
	if errTmp := d.master.exitWithoutSetErrCenter(ctx, exitReason, err, detail); errTmp != nil {
		return errTmp
	}

	return d.worker.Exit(ctx, exitReason, err, detail)
}

// TriggerOpenAPIInitialize implements BaseJobMasterExt.TriggerOpenAPIInitialize.
func (d *DefaultBaseJobMaster) TriggerOpenAPIInitialize(apiGroup *gin.RouterGroup) {
	d.impl.OnOpenAPIInitialized(apiGroup)
}

// IsBaseJobMasterExt implements BaseJobMaster.IsBaseJobMasterExt.
func (d *DefaultBaseJobMaster) IsBaseJobMasterExt() {}

type jobMasterImplAsWorkerImpl struct {
	inner          JobMasterImpl
	onCancelCalled bool
}

func (j *jobMasterImplAsWorkerImpl) InitImpl(ctx context.Context) error {
	log.Panic("unexpected Init call")
	return nil
}

func (j *jobMasterImplAsWorkerImpl) Tick(ctx context.Context) error {
	log.Panic("unexpected Poll call")
	return nil
}

func (j *jobMasterImplAsWorkerImpl) OnMasterMessage(
	ctx context.Context, topic p2p.Topic, message interface{},
) error {
	switch msg := message.(type) {
	case *frameModel.StatusChangeRequest:
		switch msg.ExpectState {
		case frameModel.WorkerStateStopped:
			if !j.onCancelCalled {
				j.onCancelCalled = true
				return j.inner.OnCancel(ctx)
			}
		default:
			log.Info("Ignore status change state", zap.Int32("state", int32(msg.ExpectState)))
		}
	default:
		log.Info("unsupported message", zap.Any("message", message))
	}
	return nil
}

func (j *jobMasterImplAsWorkerImpl) CloseImpl(ctx context.Context) {
	log.Panic("unexpected Close call")
}

type jobMasterImplAsMasterImpl struct {
	inner JobMasterImpl
}

func (j *jobMasterImplAsMasterImpl) OnWorkerStatusUpdated(worker WorkerHandle, newStatus *frameModel.WorkerStatus) error {
	return j.inner.OnWorkerStatusUpdated(worker, newStatus)
}

func (j *jobMasterImplAsMasterImpl) Tick(ctx context.Context) error {
	log.Panic("unexpected poll call")
	return nil
}

func (j *jobMasterImplAsMasterImpl) InitImpl(ctx context.Context) error {
	log.Panic("unexpected init call")
	return nil
}

func (j *jobMasterImplAsMasterImpl) OnMasterRecovered(ctx context.Context) error {
	return j.inner.OnMasterRecovered(ctx)
}

func (j *jobMasterImplAsMasterImpl) OnWorkerDispatched(worker WorkerHandle, result error) error {
	return j.inner.OnWorkerDispatched(worker, result)
}

func (j *jobMasterImplAsMasterImpl) OnWorkerOnline(worker WorkerHandle) error {
	return j.inner.OnWorkerOnline(worker)
}

func (j *jobMasterImplAsMasterImpl) OnWorkerOffline(worker WorkerHandle, reason error) error {
	return j.inner.OnWorkerOffline(worker, reason)
}

func (j *jobMasterImplAsMasterImpl) OnWorkerMessage(worker WorkerHandle, topic p2p.Topic, message interface{}) error {
	return j.inner.OnWorkerMessage(worker, topic, message)
}

func (j *jobMasterImplAsMasterImpl) CloseImpl(ctx context.Context) {
	log.Panic("unexpected Close call")
}

func (j *jobMasterImplAsMasterImpl) StopImpl(ctx context.Context) {
	log.Panic("unexpected StopImpl call")
}
