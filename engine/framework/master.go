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
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/framework/config"
	"github.com/pingcap/tiflow/engine/framework/internal/master"
	frameLog "github.com/pingcap/tiflow/engine/framework/logutil"
	"github.com/pingcap/tiflow/engine/framework/metadata"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/framework/statusutil"
	"github.com/pingcap/tiflow/engine/pkg/client"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/deps"
	"github.com/pingcap/tiflow/engine/pkg/errctx"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/engine/pkg/meta"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/pingcap/tiflow/engine/pkg/quota"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/label"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/pingcap/tiflow/pkg/uuid"
	"go.uber.org/atomic"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

// Master defines a basic interface that can run in dataflow engine runtime
type Master interface {
	Init(ctx context.Context) error
	Poll(ctx context.Context) error
	MasterID() frameModel.MasterID
	Close(ctx context.Context) error
	Stop(ctx context.Context) error
	NotifyExit(ctx context.Context, errIn error) error
}

// MasterImpl defines the interface to implement a master, business logic can be
// added in the functions of this interface
type MasterImpl interface {
	// InitImpl is called at the first time the MasterImpl instance is initialized
	// after OnOpenAPIInitialized. When InitImpl returns without error, framework
	// will try to persist an internal state so further failover will call OnMasterRecovered
	// rather than InitImpl.
	// Return:
	// - error to let the framework call CloseImpl, and framework may retry InitImpl
	//   later for some times. For non-retryable failure, business logic should
	//   call Exit.
	// Concurrent safety:
	// - this function is not concurrent with other callbacks.
	InitImpl(ctx context.Context) error

	// OnMasterRecovered is called when the MasterImpl instance has failover from
	// error by framework. For this MasterImpl instance, it's called after OnOpenAPIInitialized.
	// Return:
	// - error to let the framework call CloseImpl.
	// Concurrent safety:
	// - this function is not concurrent with other callbacks.
	OnMasterRecovered(ctx context.Context) error

	// Tick is called on a fixed interval after MasterImpl's InitImpl or OnMasterRecovered,
	// business logic can do some periodic tasks here.
	// Return:
	// - error to let the framework call CloseImpl.
	// Concurrent safety:
	// - this function may be concurrently called with other callbacks except for
	//   Tick itself, OnOpenAPIInitialized, InitImpl, OnMasterRecovered, CloseImpl,
	//   StopImpl.
	Tick(ctx context.Context) error

	// OnWorkerDispatched is called when the asynchronized action of CreateWorker
	// is finished. Only after OnWorkerDispatched, OnWorkerOnline and OnWorkerStatusUpdated
	// of the same worker may be called.
	// Return:
	// - error to let the framework call CloseImpl.
	// Concurrent safety:
	// - this function may be concurrently called with another worker's OnWorkerXXX,
	//   Tick, CloseImpl, StopImpl, OnCancel.
	OnWorkerDispatched(worker WorkerHandle, result error) error

	// OnWorkerOnline is called when the first heartbeat for a worker is received.
	// NOTE: OnWorkerOffline can appear without OnWorkerOnline
	// Return:
	// - error to let the framework call CloseImpl.
	// Concurrent safety:
	// - this function may be concurrently called with another worker's OnWorkerXXX,
	//   Tick, CloseImpl, StopImpl, OnCancel, the same worker's OnWorkerStatusUpdated.
	OnWorkerOnline(worker WorkerHandle) error

	// OnWorkerOffline is called as the consequence of worker's Exit or heartbeat
	// timed out. It's the last callback function among OnWorkerXXX for a worker.
	// Return:
	// - error to let the framework call CloseImpl.
	// Concurrent safety:
	// - this function may be concurrently called with another worker's OnWorkerXXX,
	//   Tick, CloseImpl, StopImpl, OnCancel.
	OnWorkerOffline(worker WorkerHandle, reason error) error

	// OnWorkerMessage is called when a customized message is received.
	OnWorkerMessage(worker WorkerHandle, topic p2p.Topic, message interface{}) error

	// OnWorkerStatusUpdated is called as the consequence of worker's UpdateStatus.
	// Return:
	// - error to let the framework call CloseImpl.
	// Concurrent safety:
	// - this function may be concurrently called with another worker's OnWorkerXXX,
	//   Tick, CloseImpl, StopImpl, OnCancel, the same worker's OnWorkerOnline.
	OnWorkerStatusUpdated(worker WorkerHandle, newStatus *frameModel.WorkerStatus) error

	// CloseImpl is called as the consequence of returning error from InitImpl,
	// OnMasterRecovered or Tick, the Tick will be stopped after entering this function.
	// And framework may try to create a new masterImpl instance afterwards.
	// Business logic is expected to release resources here, but business developer
	// should be aware that when the runtime is crashed, CloseImpl has no time to
	// be called.
	// TODO: no other callbacks will be called after and concurrent with CloseImpl
	// Concurrent safety:
	// - this function may be concurrently called with OnWorkerMessage, OnCancel,
	//   OnWorkerDispatched, OnWorkerOnline, OnWorkerOffline, OnWorkerStatusUpdated.
	CloseImpl(ctx context.Context)

	// StopImpl is called the consequence of business logic calls Exit. Tick will
	// be stopped after entering this function, and framework will treat this MasterImpl
	// as non-recoverable,
	// There's at most one invocation to StopImpl after Exit. If the runtime is
	// crashed, StopImpl has no time to be called.
	// Concurrent safety:
	// - this function may be concurrently called with OnWorkerMessage, OnCancel,
	//   OnWorkerDispatched, OnWorkerOnline, OnWorkerOffline, OnWorkerStatusUpdated.
	StopImpl(ctx context.Context)
}

const (
	createWorkerWaitQuotaTimeout = 5 * time.Second
	createWorkerTimeout          = 10 * time.Second
	maxCreateWorkerConcurrency   = 100
)

// CreateWorkerOpt specifies an option for creating a worker.
type CreateWorkerOpt = master.CreateWorkerOpt

// CreateWorkerWithResourceRequirements specifies the resource requirement of a worker.
func CreateWorkerWithResourceRequirements(resources ...resModel.ResourceID) CreateWorkerOpt {
	return master.CreateWorkerWithResourceRequirements(resources...)
}

// CreateWorkerWithSelectors specifies the selectors used to dispatch the worker.
func CreateWorkerWithSelectors(selectors ...*label.Selector) CreateWorkerOpt {
	return master.CreateWorkerWithSelectors(selectors...)
}

// BaseMaster defines the master interface, it embeds the Master interface and
// contains more core logic of a master
type BaseMaster interface {
	Master

	// MetaKVClient return business metastore kv client with job-level isolation
	MetaKVClient() metaModel.KVClient

	// MetricFactory return a promethus factory with some underlying labels(e.g. job-id, work-id)
	MetricFactory() promutil.Factory

	// Logger return a zap logger with some underlying fields(e.g. job-id)
	Logger() *zap.Logger

	// MasterMeta return the meta data of master
	MasterMeta() *frameModel.MasterMeta

	// GetWorkers return the handle of all workers, from which we can get the worker status、worker id and
	// the method for sending message to specific worker
	GetWorkers() map[frameModel.WorkerID]WorkerHandle

	// IsMasterReady returns whether the master has received heartbeats for all
	// workers after a fail-over. If this is the first time the JobMaster started up,
	// the return value is always true.
	IsMasterReady() bool

	// Exit should be called when master (in user logic) wants to exit.
	// exitReason: ExitReasonFinished/ExitReasonCanceled/ExitReasonFailed
	// NOTE: Currently, no implement has used this method, but we still keep it to make the interface intact
	Exit(ctx context.Context, exitReason ExitReason, err error, detail []byte) error

	// CreateWorker is the latest version of CreateWorker, but with
	// a more flexible way of passing options.
	// If the worker needs to access certain file system resources, it must pass
	// resource ID via CreateWorkerOpt
	CreateWorker(
		workerType frameModel.WorkerType,
		config WorkerConfig,
		opts ...CreateWorkerOpt,
	) (frameModel.WorkerID, error)
}

// DefaultBaseMaster implements BaseMaster interface
type DefaultBaseMaster struct {
	Impl MasterImpl

	// dependencies
	messageHandlerManager p2p.MessageHandlerManager
	messageSender         p2p.MessageSender
	// framework metastore client
	frameMetaClient    pkgOrm.Client
	executorGroup      client.ExecutorGroup
	serverMasterClient client.ServerMasterClient

	clock clock.Clock

	// workerManager maintains the list of all workers and
	// their statuses.
	workerManager *master.WorkerManager

	currentEpoch atomic.Int64

	wg        sync.WaitGroup
	errCenter *errctx.ErrCenter

	// closeCh is closed when the BaseMaster is exiting
	closeCh chan struct{}

	id            frameModel.MasterID // id of this master itself
	advertiseAddr string
	nodeID        p2p.NodeID
	timeoutConfig config.TimeoutConfig
	masterMeta    *frameModel.MasterMeta

	workerCreator *master.WorkerCreator

	// workerProjectMap keep the <WorkerID, ProjectInfo> map
	// It's only used by JobManager who has workers(jobmaster) with different project info
	// [NOTICE]: When JobManager failover, we need to load all workers(jobmaster)'s project info
	workerProjectMap sync.Map
	// masterProjectInfo is the projectInfo of itself
	masterProjectInfo tenant.ProjectInfo

	// business kvclient with namespace
	businessMetaKVClient metaModel.KVClient

	// metricFactory can produce metric with underlying project info and job info
	metricFactory promutil.Factory

	// logger is the zap logger with underlying project info and job info
	logger *zap.Logger

	// components for easier unit testing
	uuidGen uuid.Generator

	// TODO use a shared quota for all masters.
	createWorkerQuota quota.ConcurrencyQuota

	// deps is a container for injected dependencies
	deps *deps.Deps
}

// NotifyExit implements BaseWorker.NotifyExit
func (m *DefaultBaseMaster) NotifyExit(ctx context.Context, errIn error) error {
	// no-op for now.
	return nil
}

type masterParams struct {
	dig.In

	MessageHandlerManager p2p.MessageHandlerManager
	MessageSender         p2p.MessageSender
	// framework metastore client
	FrameMetaClient    pkgOrm.Client
	BusinessClientConn metaModel.ClientConn
	ExecutorGroup      client.ExecutorGroup
	ServerMasterClient client.ServerMasterClient
}

// NewBaseMaster creates a new DefaultBaseMaster instance
func NewBaseMaster(
	ctx *dcontext.Context,
	impl MasterImpl,
	id frameModel.MasterID,
	tp frameModel.WorkerType,
) BaseMaster {
	var (
		nodeID        p2p.NodeID
		advertiseAddr string
		masterMeta    = &frameModel.MasterMeta{}
		params        masterParams
	)
	if ctx != nil {
		nodeID = ctx.Environ.NodeID
		advertiseAddr = ctx.Environ.Addr
		metaBytes := ctx.Environ.MasterMetaBytes
		err := errors.Trace(masterMeta.Unmarshal(metaBytes))
		if err != nil {
			log.Warn("invalid master meta", zap.ByteString("data", metaBytes), zap.Error(err))
		}
	}

	if err := ctx.Deps().Fill(&params); err != nil {
		// TODO more elegant error handling
		log.Panic("failed to provide dependencies", zap.Error(err))
	}

	logger := logutil.FromContext(*ctx)

	cli, err := meta.NewKVClientWithNamespace(params.BusinessClientConn, ctx.ProjectInfo.UniqueID(), id)
	if err != nil {
		// TODO more elegant error handling
		log.Panic("failed to create business kvclient", zap.Error(err))
	}

	return &DefaultBaseMaster{
		Impl:                  impl,
		messageHandlerManager: params.MessageHandlerManager,
		messageSender:         params.MessageSender,
		frameMetaClient:       params.FrameMetaClient,
		executorGroup:         params.ExecutorGroup,
		serverMasterClient:    params.ServerMasterClient,
		id:                    id,
		clock:                 clock.New(),

		timeoutConfig: config.DefaultTimeoutConfig(),
		masterMeta:    masterMeta,

		closeCh: make(chan struct{}),

		errCenter: errctx.NewErrCenter(),

		uuidGen: uuid.NewGenerator(),

		nodeID:            nodeID,
		advertiseAddr:     advertiseAddr,
		masterProjectInfo: ctx.ProjectInfo,

		createWorkerQuota:    quota.NewConcurrencyQuota(maxCreateWorkerConcurrency),
		businessMetaKVClient: cli,
		metricFactory:        promutil.NewFactory4Master(ctx.ProjectInfo, MustConvertWorkerType2JobType(tp), id),
		logger:               frameLog.WithMasterID(logger, id),

		deps: ctx.Deps(),
	}
}

// MetaKVClient returns the business space metaclient
func (m *DefaultBaseMaster) MetaKVClient() metaModel.KVClient {
	return m.businessMetaKVClient
}

// MetricFactory implements BaseMaster.MetricFactory
func (m *DefaultBaseMaster) MetricFactory() promutil.Factory {
	return m.metricFactory
}

// Logger implements BaseMaster.Logger
func (m *DefaultBaseMaster) Logger() *zap.Logger {
	return m.logger
}

// Init implements BaseMaster.Init
func (m *DefaultBaseMaster) Init(ctx context.Context) error {
	// Note this context must not be held in any resident goroutine.
	ctx, cancel := m.errCenter.WithCancelOnFirstError(ctx)
	defer cancel()

	isInit, err := m.doInit(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	if isInit {
		if err := m.Impl.InitImpl(ctx); err != nil {
			m.errCenter.OnError(err)
			return errors.Trace(err)
		}
	} else {
		if err := m.Impl.OnMasterRecovered(ctx); err != nil {
			m.errCenter.OnError(err)
			return errors.Trace(err)
		}
	}

	if err := m.markStateInMetadata(ctx, frameModel.MasterStateInit); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (m *DefaultBaseMaster) doInit(ctx context.Context) (isFirstStartUp bool, err error) {
	isInit, epoch, err := m.refreshMetadata(ctx)
	if err != nil {
		return false, errors.Trace(err)
	}
	m.currentEpoch.Store(epoch)

	m.workerManager = master.NewWorkerManager(
		m.id,
		epoch,
		m.frameMetaClient,
		m.messageSender,
		func(_ context.Context, handle master.WorkerHandle) error {
			return m.Impl.OnWorkerOnline(handle)
		},
		func(_ context.Context, handle master.WorkerHandle, err error) error {
			return m.Impl.OnWorkerOffline(handle, err)
		},
		func(_ context.Context, handle master.WorkerHandle) error {
			return m.Impl.OnWorkerStatusUpdated(handle, handle.Status())
		},
		func(_ context.Context, handle master.WorkerHandle, err error) error {
			return m.Impl.OnWorkerDispatched(handle, err)
		}, isInit, m.timeoutConfig, m.clock)

	inheritedSelectors := m.masterMeta.Ext.Selectors
	workerCreator := master.NewWorkerCreatorBuilder().
		WithMasterID(m.id).
		WithHooks(&master.WorkerCreationHooks{BeforeStartingWorker: m.workerManager.BeforeStartingWorker}).
		WithExecutorGroup(m.executorGroup).
		WithServerMasterClient(m.serverMasterClient).
		WithFrameMetaClient(m.frameMetaClient).
		WithLogger(m.Logger()).
		WithInheritedSelectors(inheritedSelectors...).
		Build()
	m.workerCreator = workerCreator

	if err := m.registerMessageHandlers(ctx); err != nil {
		return false, errors.Trace(err)
	}

	if !isInit {
		if err := m.workerManager.InitAfterRecover(ctx); err != nil {
			return false, err
		}
	}
	return isInit, nil
}

func (m *DefaultBaseMaster) registerMessageHandlers(ctx context.Context) error {
	ok, err := m.messageHandlerManager.RegisterHandler(
		ctx,
		frameModel.HeartbeatPingTopic(m.id),
		&frameModel.HeartbeatPingMessage{},
		func(sender p2p.NodeID, value p2p.MessageValue) error {
			msg := value.(*frameModel.HeartbeatPingMessage)
			m.Logger().Info("Heartbeat Ping received",
				zap.Any("msg", msg),
				zap.String("master-id", m.id))

			replyMsg := &frameModel.HeartbeatPongMessage{
				SendTime:   msg.SendTime,
				ReplyTime:  m.clock.Now(),
				ToWorkerID: msg.FromWorkerID,
				Epoch:      m.currentEpoch.Load(),
				IsFinished: msg.IsFinished,
			}
			ok, err := m.messageSender.SendToNode(
				ctx,
				sender,
				frameModel.HeartbeatPongTopic(m.id, msg.FromWorkerID),
				replyMsg)
			if err != nil {
				return err
			}
			if !ok {
				log.Warn("Sending Heartbeat Pong failed",
					zap.Any("reply", replyMsg))
				return nil
			}
			m.workerManager.HandleHeartbeat(msg, sender)
			return nil
		})
	if err != nil {
		return err
	}
	if !ok {
		m.Logger().Panic("duplicate handler", zap.String("topic", frameModel.HeartbeatPingTopic(m.id)))
	}

	ok, err = m.messageHandlerManager.RegisterHandler(
		ctx,
		statusutil.WorkerStatusTopic(m.id),
		&statusutil.WorkerStatusMessage{},
		func(sender p2p.NodeID, value p2p.MessageValue) error {
			msg := value.(*statusutil.WorkerStatusMessage)
			m.workerManager.OnWorkerStatusUpdateMessage(msg)
			return nil
		})
	if err != nil {
		return err
	}
	if !ok {
		m.Logger().Panic("duplicate handler", zap.String("topic", statusutil.WorkerStatusTopic(m.id)))
	}

	return nil
}

// Poll implements BaseMaster.Poll
func (m *DefaultBaseMaster) Poll(ctx context.Context) error {
	ctx, cancel := m.errCenter.WithCancelOnFirstError(ctx)
	defer cancel()

	if err := m.doPoll(ctx); err != nil {
		return errors.Trace(err)
	}

	if err := m.Impl.Tick(ctx); err != nil {
		m.errCenter.OnError(err)
		return errors.Trace(err)
	}

	return nil
}

func (m *DefaultBaseMaster) doPoll(ctx context.Context) error {
	if err := m.errCenter.CheckError(); err != nil {
		return err
	}

	select {
	case <-m.closeCh:
		return errors.ErrMasterClosed.GenWithStackByArgs()
	default:
	}

	if err := m.messageHandlerManager.CheckError(ctx); err != nil {
		return errors.Trace(err)
	}
	return m.workerManager.Tick(ctx)
}

// MasterMeta implements BaseMaster.MasterMeta
func (m *DefaultBaseMaster) MasterMeta() *frameModel.MasterMeta {
	return m.masterMeta
}

// MasterID implements BaseMaster.MasterID
func (m *DefaultBaseMaster) MasterID() frameModel.MasterID {
	return m.id
}

// GetWorkers implements BaseMaster.GetWorkers
func (m *DefaultBaseMaster) GetWorkers() map[frameModel.WorkerID]WorkerHandle {
	return m.workerManager.GetWorkers()
}

func (m *DefaultBaseMaster) doClose() {
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	close(m.closeCh)
	m.wg.Wait()
	if err := m.messageHandlerManager.Clean(closeCtx); err != nil {
		m.Logger().Warn("Failed to clean up message handlers",
			zap.String("master-id", m.id), zap.Error(err))
	}
	promutil.UnregisterWorkerMetrics(m.id)
	m.businessMetaKVClient.Close()
}

// Close implements BaseMaster.Close
func (m *DefaultBaseMaster) Close(ctx context.Context) error {
	m.Impl.CloseImpl(ctx)

	m.persistMetaError()
	m.doClose()
	return nil
}

// Stop implements Master.Stop
func (m *DefaultBaseMaster) Stop(ctx context.Context) error {
	m.Impl.StopImpl(ctx)
	return nil
}

// refreshMetadata load and update metadata by current epoch, nodeID, advertiseAddr, etc.
// master meta is persisted before it is created, in this function we update some
// fileds to the current value, including epoch, nodeID and advertiseAddr.
func (m *DefaultBaseMaster) refreshMetadata(ctx context.Context) (isInit bool, epoch frameModel.Epoch, err error) {
	metaClient := metadata.NewMasterMetadataClient(m.id, m.frameMetaClient)

	masterMeta, err := metaClient.Load(ctx)
	if err != nil {
		return false, 0, err
	}

	epoch, err = m.frameMetaClient.GenEpoch(ctx)
	if err != nil {
		return false, 0, err
	}

	// We should update the master data to reflect our current information
	masterMeta.Epoch = epoch
	masterMeta.Addr = m.advertiseAddr
	masterMeta.NodeID = m.nodeID

	if err := metaClient.Update(ctx, masterMeta.RefreshValues()); err != nil {
		return false, 0, errors.Trace(err)
	}

	m.masterMeta = masterMeta
	// isInit true means the master is created but has not been initialized.
	isInit = masterMeta.State == frameModel.MasterStateUninit

	return
}

func (m *DefaultBaseMaster) markStateInMetadata(
	ctx context.Context, code frameModel.MasterState,
) error {
	metaClient := metadata.NewMasterMetadataClient(m.id, m.frameMetaClient)
	m.masterMeta.State = code
	return metaClient.Update(ctx, m.masterMeta.UpdateStateValues())
}

func (m *DefaultBaseMaster) persistMetaError() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	if err := m.errCenter.CheckError(); err != nil {
		metaClient := metadata.NewMasterMetadataClient(m.id, m.frameMetaClient)
		m.masterMeta.ErrorMsg = err.Error()
		if err2 := metaClient.Update(ctx, m.masterMeta.UpdateErrorValues()); err2 != nil {
			m.Logger().Warn("Failed to update error message",
				zap.String("master-id", m.id), zap.Error(err2))
		}
	}
}

// PrepareWorkerConfig extracts information from WorkerConfig into detail fields.
//   - If workerType is master type, the config is a `*MasterMeta` struct and
//     contains pre allocated maseter ID, and json marshalled config.
//   - If workerType is worker type, the config is a user defined config struct, we
//     marshal it to byte slice as returned config, and generate a random WorkerID.
func (m *DefaultBaseMaster) PrepareWorkerConfig(
	workerType frameModel.WorkerType, config WorkerConfig,
) (rawConfig []byte, workerID frameModel.WorkerID, err error) {
	switch workerType {
	case frameModel.CvsJobMaster, frameModel.FakeJobMaster, frameModel.DMJobMaster:
		masterMeta, ok := config.(*frameModel.MasterMeta)
		if !ok {
			err = errors.ErrMasterInvalidMeta.GenWithStackByArgs(config)
			return
		}
		rawConfig = masterMeta.Config
		workerID = masterMeta.ID
	case frameModel.WorkerDMDump, frameModel.WorkerDMLoad, frameModel.WorkerDMSync:
		var b bytes.Buffer
		err = toml.NewEncoder(&b).Encode(config)
		if err != nil {
			return
		}
		rawConfig = b.Bytes()
		workerID = m.uuidGen.NewString()
	default:
		rawConfig, err = json.Marshal(config)
		if err != nil {
			return
		}
		workerID = m.uuidGen.NewString()
	}
	return
}

// CreateWorker implements BaseMaster.CreateWorker
func (m *DefaultBaseMaster) CreateWorker(
	workerType frameModel.WorkerType,
	config WorkerConfig,
	opts ...CreateWorkerOpt,
) (frameModel.WorkerID, error) {
	m.Logger().Info("CreateWorker",
		zap.Stringer("worker-type", workerType),
		zap.Any("worker-config", config),
		zap.String("master-id", m.id))

	rawConfig, workerID, err := m.PrepareWorkerConfig(workerType, config)
	if err != nil {
		return "", err
	}

	errCtx, cancel := m.errCenter.WithCancelOnFirstError(context.Background())
	defer cancel()
	quotaCtx, cancel := context.WithTimeout(errCtx, createWorkerWaitQuotaTimeout)
	defer cancel()
	if err := m.createWorkerQuota.Consume(quotaCtx); err != nil {
		return "", errors.WrapError(errors.ErrMasterConcurrencyExceeded, err)
	}

	go func() {
		defer func() {
			m.createWorkerQuota.Release()
		}()

		errCtx, cancelErrCtx := m.errCenter.WithCancelOnFirstError(context.Background())
		defer cancelErrCtx()

		requestCtx, cancelRequestCtx := context.WithTimeout(errCtx, createWorkerTimeout)
		defer cancelRequestCtx()

		err := m.workerCreator.CreateWorker(
			requestCtx, m.GetProjectInfo(workerID), workerType, workerID, rawConfig,
			opts...)
		if err != nil {
			m.workerManager.AbortCreatingWorker(workerID, err)
		}
	}()

	return workerID, nil
}

// IsMasterReady implements BaseMaster.IsMasterReady
func (m *DefaultBaseMaster) IsMasterReady() bool {
	return m.workerManager.IsInitialized()
}

// Exit implements BaseMaster.Exit
// NOTE: Currently, no implement has used this method, but we still keep it to make the interface intact
func (m *DefaultBaseMaster) Exit(ctx context.Context, exitReason ExitReason, err error, detail []byte) error {
	// Set the errCenter to prevent user from forgetting to return directly after calling 'Exit'
	// keep the original error in errCenter if possible
	defer func() {
		if err == nil {
			err = errors.ErrWorkerFinish.FastGenByArgs()
		}
		m.errCenter.OnError(err)
	}()

	return m.exitWithoutSetErrCenter(ctx, exitReason, err, detail)
}

func (m *DefaultBaseMaster) exitWithoutSetErrCenter(ctx context.Context, exitReason ExitReason, err error, detail []byte) (errRet error) {
	switch exitReason {
	case ExitReasonFinished:
		m.masterMeta.State = frameModel.MasterStateFinished
	case ExitReasonCanceled:
		// TODO: replace stop with cancel
		m.masterMeta.State = frameModel.MasterStateStopped
	case ExitReasonFailed:
		m.masterMeta.State = frameModel.MasterStateFailed
	default:
		m.masterMeta.State = frameModel.MasterStateFailed
	}

	if err != nil {
		m.masterMeta.ErrorMsg = err.Error()
	} else {
		m.masterMeta.ErrorMsg = ""
	}
	m.masterMeta.Detail = detail
	metaClient := metadata.NewMasterMetadataClient(m.id, m.frameMetaClient)
	return metaClient.Update(ctx, m.masterMeta.ExitValues())
}

// SetProjectInfo set the project info of specific worker
// [NOTICE]: Only used by JobManager to set project for different job(worker for jobmanager)
func (m *DefaultBaseMaster) SetProjectInfo(workerID frameModel.WorkerID, projectInfo tenant.ProjectInfo) {
	m.workerProjectMap.Store(workerID, projectInfo)
}

// DeleteProjectInfo delete the project info of specific worker
// NOTICEL Only used by JobMananger when stop job
func (m *DefaultBaseMaster) DeleteProjectInfo(workerID frameModel.WorkerID) {
	m.workerProjectMap.Delete(workerID)
}

// GetProjectInfo get the project info of the worker
// [WARN]: Once 'DeleteProjectInfo' is called, 'GetProjectInfo' may return unexpected project info
// For JobManager: It will set the <jobID, projectInfo> pair in advance.
// So if we call 'GetProjectInfo' before 'DeleteProjectInfo', we can expect a correct projectInfo.
// For JobMaster: Master and worker always have the same projectInfo and workerProjectMap is empty
func (m *DefaultBaseMaster) GetProjectInfo(masterID frameModel.MasterID) tenant.ProjectInfo {
	projectInfo, exists := m.workerProjectMap.Load(masterID)
	if !exists {
		return m.masterProjectInfo
	}

	return projectInfo.(tenant.ProjectInfo)
}

// InitProjectInfosAfterRecover set project infos for all worker after master recover
// NOTICE: Only used by JobMananger when failover
func (m *DefaultBaseMaster) InitProjectInfosAfterRecover(jobs []*frameModel.MasterMeta) {
	for _, meta := range jobs {
		// TODO: fix the TenantID
		m.workerProjectMap.Store(meta.ID, tenant.NewProjectInfo("", meta.ProjectID))
	}
}
