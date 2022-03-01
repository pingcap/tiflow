package lib

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"

	"github.com/hanfei1991/microcosm/executor/worker"
	"github.com/hanfei1991/microcosm/model"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type BaseJobMaster interface {
	Init(ctx context.Context) error
	Poll(ctx context.Context) error
	Close(ctx context.Context) error
	OnError(err error)

	MetaKVClient() metadata.MetaKV

	GetWorkers() map[WorkerID]WorkerHandle

	RegisterWorker(ctx context.Context, workerID WorkerID) error
	CreateWorker(workerType WorkerType, config WorkerConfig, cost model.RescUnit) (WorkerID, error)

	Workload() model.RescUnit

	JobMasterID() MasterID
	ID() worker.RunnableID
	UpdateJobStatus(ctx context.Context, status WorkerStatus) error

	// IsMasterReady returns whether the master has received heartbeats for all
	// workers after a fail-over. If this is the first time the JobMaster started up,
	// the return value is always true.
	IsMasterReady() bool

	// IsBaseJobMaster is an empty function used to prevent accidental implementation
	// of this interface.
	IsBaseJobMaster()
}

type DefaultBaseJobMaster struct {
	master *DefaultBaseMaster
	worker *DefaultBaseWorker
	impl   JobMasterImpl
}

// JobMasterImpl is the implementation of a job master of dataflow engine.
// the implementation struct must embed the lib.BaseJobMaster interface, this
// interface will be initialized by the framework.
type JobMasterImpl interface {
	InitImpl(ctx context.Context) error
	// Tick is called on a fixed interval. When an error is returned, the worker
	// will be stopped.
	Tick(ctx context.Context) error
	CloseImpl(ctx context.Context) error

	OnMasterRecovered(ctx context.Context) error
	OnWorkerDispatched(worker WorkerHandle, result error) error
	OnWorkerOnline(worker WorkerHandle) error
	OnWorkerOffline(worker WorkerHandle, reason error) error
	OnWorkerMessage(worker WorkerHandle, topic p2p.Topic, message interface{}) error

	Workload() model.RescUnit
	OnJobManagerFailover(reason MasterFailoverReason) error

	// IsJobMasterImpl is an empty function used to prevent accidental implementation
	// of this interface.
	IsJobMasterImpl()
}

func NewBaseJobMaster(
	ctx *dcontext.Context,
	jobMasterImpl JobMasterImpl,
	masterID MasterID,
	workerID WorkerID,
) BaseJobMaster {
	// master-worker pair: job manager <-> job master(`baseWorker` following)
	// master-worker pair: job master(`baseMaster` following) <-> real workers
	// `masterID` is always the ID of master role, against current object
	// `workerID` is the ID of current object
	baseMaster := NewBaseMaster(
		ctx, &jobMasterImplAsMasterImpl{jobMasterImpl}, workerID)
	baseWorker := NewBaseWorker(
		ctx, &jobMasterImplAsWorkerImpl{jobMasterImpl}, workerID, masterID)
	return &DefaultBaseJobMaster{
		master: baseMaster.(*DefaultBaseMaster),
		worker: baseWorker.(*DefaultBaseWorker),
		impl:   jobMasterImpl,
	}
}

func (d *DefaultBaseJobMaster) MetaKVClient() metadata.MetaKV {
	return d.master.MetaKVClient()
}

func (d *DefaultBaseJobMaster) Init(ctx context.Context) error {
	if err := d.worker.doPreInit(ctx); err != nil {
		return errors.Trace(err)
	}

	isFirstStartUp, err := d.master.doInit(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	if isFirstStartUp {
		if err := d.impl.InitImpl(ctx); err != nil {
			return errors.Trace(err)
		}
		if err := d.master.markInitializedInMetadata(ctx); err != nil {
			return errors.Trace(err)
		}
	} else {
		if err := d.impl.OnMasterRecovered(ctx); err != nil {
			return errors.Trace(err)
		}
	}

	if err := d.worker.doPostInit(ctx); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (d *DefaultBaseJobMaster) Poll(ctx context.Context) error {
	if err := d.master.doPoll(ctx); err != nil {
		return errors.Trace(err)
	}
	if err := d.worker.doPoll(ctx); err != nil {
		return errors.Trace(err)
	}
	if err := d.impl.Tick(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (d *DefaultBaseJobMaster) MasterID() MasterID {
	return d.master.MasterID()
}

func (d *DefaultBaseJobMaster) GetWorkers() map[WorkerID]WorkerHandle {
	return d.master.GetWorkers()
}

func (d *DefaultBaseJobMaster) Close(ctx context.Context) error {
	if err := d.impl.CloseImpl(ctx); err != nil {
		return errors.Trace(err)
	}

	d.master.doClose()
	d.worker.doClose()
	return nil
}

func (d *DefaultBaseJobMaster) OnError(err error) {
	// TODO refine the OnError logic.
	d.master.OnError(err)
}

func (d *DefaultBaseJobMaster) RegisterWorker(ctx context.Context, workerID WorkerID) error {
	return d.master.RegisterWorker(ctx, workerID)
}

func (d *DefaultBaseJobMaster) CreateWorker(workerType WorkerType, config WorkerConfig, cost model.RescUnit) (WorkerID, error) {
	return d.master.CreateWorker(workerType, config, cost)
}

func (d *DefaultBaseJobMaster) UpdateStatus(ctx context.Context, status WorkerStatus) error {
	return d.worker.UpdateStatus(ctx, status)
}

func (d *DefaultBaseJobMaster) Workload() model.RescUnit {
	return d.worker.Workload()
}

func (d *DefaultBaseJobMaster) ID() worker.RunnableID {
	return d.worker.ID()
}

func (d *DefaultBaseJobMaster) JobMasterID() MasterID {
	return d.master.MasterID()
}

func (d *DefaultBaseJobMaster) UpdateJobStatus(ctx context.Context, status WorkerStatus) error {
	return d.worker.UpdateStatus(ctx, status)
}

func (d *DefaultBaseJobMaster) IsBaseJobMaster() {
}

func (d *DefaultBaseJobMaster) SendMessage(ctx context.Context, topic p2p.Topic, message interface{}) (bool, error) {
	// master will use WorkerHandle to send message
	return d.worker.SendMessage(ctx, topic, message)
}

func (d *DefaultBaseJobMaster) IsMasterReady() bool {
	return d.master.IsMasterReady()
}

type jobMasterImplAsWorkerImpl struct {
	inner JobMasterImpl
}

func (j *jobMasterImplAsWorkerImpl) InitImpl(ctx context.Context) error {
	log.L().Panic("unexpected Init call")
	return nil
}

func (j *jobMasterImplAsWorkerImpl) Tick(ctx context.Context) error {
	log.L().Panic("unexpected Poll call")
	return nil
}

func (j *jobMasterImplAsWorkerImpl) Workload() model.RescUnit {
	return j.inner.Workload()
}

func (j *jobMasterImplAsWorkerImpl) OnMasterFailover(reason MasterFailoverReason) error {
	return j.inner.OnJobManagerFailover(reason)
}

func (j *jobMasterImplAsWorkerImpl) CloseImpl(ctx context.Context) error {
	log.L().Panic("unexpected Close call")
	return nil
}

type jobMasterImplAsMasterImpl struct {
	inner JobMasterImpl
}

func (j *jobMasterImplAsMasterImpl) Tick(ctx context.Context) error {
	log.L().Panic("unexpected poll call")
	return nil
}

func (j *jobMasterImplAsMasterImpl) InitImpl(ctx context.Context) error {
	log.L().Panic("unexpected init call")
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

func (j *jobMasterImplAsMasterImpl) CloseImpl(ctx context.Context) error {
	log.L().Panic("unexpected Close call")
	return nil
}
