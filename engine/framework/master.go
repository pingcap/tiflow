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
	"github.com/pingcap/errors"
	"go.uber.org/atomic"
	"go.uber.org/dig"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/client"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	runtime "github.com/pingcap/tiflow/engine/executor/worker"
	"github.com/pingcap/tiflow/engine/framework/config"
	"github.com/pingcap/tiflow/engine/framework/master"
	"github.com/pingcap/tiflow/engine/framework/metadata"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/framework/statusutil"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/deps"
	"github.com/pingcap/tiflow/engine/pkg/errctx"
	derror "github.com/pingcap/tiflow/engine/pkg/errors"
	resourcemeta "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	"github.com/pingcap/tiflow/engine/pkg/logutil"
	extkv "github.com/pingcap/tiflow/engine/pkg/meta/extension"
	"github.com/pingcap/tiflow/engine/pkg/meta/kvclient"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/pingcap/tiflow/engine/pkg/quota"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/pingcap/tiflow/pkg/uuid"
)

// Master defines a basic interface that can run in dataflow engine runtime
type Master interface {
	Init(ctx context.Context) error
	Poll(ctx context.Context) error
	MasterID() frameModel.MasterID

	runtime.Closer
}

// MasterImpl defines the interface to implement a master, business logic can be
// added in the functions of this interface
type MasterImpl interface {
	// InitImpl provides customized logic for the business logic to initialize.
	InitImpl(ctx context.Context) error

	// Tick is called on a fixed interval.
	Tick(ctx context.Context) error

	// OnMasterRecovered is called when the master has recovered from an error.
	OnMasterRecovered(ctx context.Context) error

	// OnWorkerDispatched is called when a request to launch a worker is finished.
	OnWorkerDispatched(worker WorkerHandle, result error) error

	// OnWorkerOnline is called when the first heartbeat for a worker is received.
	OnWorkerOnline(worker WorkerHandle) error

	// OnWorkerOffline is called when a worker exits or has timed out.
	// Worker exit scenario contains normal finish and manually stop
	OnWorkerOffline(worker WorkerHandle, reason error) error

	// OnWorkerMessage is called when a customized message is received.
	OnWorkerMessage(worker WorkerHandle, topic p2p.Topic, message interface{}) error

	// OnWorkerStatusUpdated is called when a worker's status is updated.
	OnWorkerStatusUpdated(worker WorkerHandle, newStatus *frameModel.WorkerStatus) error

	// CloseImpl is called when the master is being closed
	CloseImpl(ctx context.Context) error
}

const (
	createWorkerWaitQuotaTimeout = 5 * time.Second
	createWorkerTimeout          = 10 * time.Second
	maxCreateWorkerConcurrency   = 100
)

// BaseMaster defines the master interface, it embeds the Master interface and
// contains more core logic of a master
type BaseMaster interface {
	Master

	// MetaKVClient return user metastore kv client
	MetaKVClient() metaclient.KVClient
	MetricFactory() promutil.Factory
	Logger() *zap.Logger
	MasterMeta() *frameModel.MasterMetaKVData
	GetWorkers() map[frameModel.WorkerID]WorkerHandle
	IsMasterReady() bool
	OnError(err error)

	// CreateWorker requires the framework to dispatch a new worker.
	// If the worker needs to access certain file system resources,
	// their ID's must be passed by `resources`.
	CreateWorker(
		workerType WorkerType,
		config WorkerConfig,
		cost model.RescUnit,
		resources ...resourcemeta.ResourceID,
	) (frameModel.WorkerID, error)
}

// DefaultBaseMaster implements BaseMaster interface
type DefaultBaseMaster struct {
	Impl MasterImpl

	// dependencies
	messageHandlerManager p2p.MessageHandlerManager
	messageSender         p2p.MessageSender
	// framework metastore client
	frameMetaClient pkgOrm.Client
	// user metastore raw kvclient
	userRawKVClient       extkv.KVClientEx
	executorClientManager client.ClientsManager
	serverMasterClient    client.MasterClient

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
	masterMeta    *frameModel.MasterMetaKVData

	// workerProjectMap keep the <WorkerID, ProjectInfo> map
	// It's only used by JobManager who has workers(jobmaster) with different project info
	// [NOTICE]: When JobManager failover, we need to load all workers(jobmaster)'s project info
	workerProjectMap sync.Map
	// masterProjectInfo is the projectInfo of itself
	masterProjectInfo tenant.ProjectInfo

	// user metastore prefix kvclient
	// Don't close it. It's just a prefix wrapper for underlying userRawKVClient
	userMetaKVClient metaclient.KVClient

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

type masterParams struct {
	dig.In

	MessageHandlerManager p2p.MessageHandlerManager
	MessageSender         p2p.MessageSender
	// framework metastore client
	FrameMetaClient pkgOrm.Client
	// user metastore raw kvclient
	UserRawKVClient       extkv.KVClientEx
	ExecutorClientManager client.ClientsManager
	ServerMasterClient    client.MasterClient
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
		masterMeta    = &frameModel.MasterMetaKVData{}
		params        masterParams
	)
	if ctx != nil {
		nodeID = ctx.Environ.NodeID
		advertiseAddr = ctx.Environ.Addr
		metaBytes := ctx.Environ.MasterMetaBytes
		err := errors.Trace(masterMeta.Unmarshal(metaBytes))
		if err != nil {
			log.L().Warn("invalid master meta", zap.ByteString("data", metaBytes), zap.Error(err))
		}
	}

	if err := ctx.Deps().Fill(&params); err != nil {
		// TODO more elegant error handling
		log.L().Panic("failed to provide dependencies", zap.Error(err))
	}

	return &DefaultBaseMaster{
		Impl:                  impl,
		messageHandlerManager: params.MessageHandlerManager,
		messageSender:         params.MessageSender,
		frameMetaClient:       params.FrameMetaClient,
		userRawKVClient:       params.UserRawKVClient,
		executorClientManager: params.ExecutorClientManager,
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

		createWorkerQuota: quota.NewConcurrencyQuota(maxCreateWorkerConcurrency),
		userMetaKVClient:  kvclient.NewPrefixKVClient(params.UserRawKVClient, ctx.ProjectInfo.UniqueID()),
		metricFactory:     promutil.NewFactory4Master(ctx.ProjectInfo, WorkerTypeForMetric(tp), id),
		logger:            logutil.NewLogger4Master(ctx.ProjectInfo, id),

		deps: ctx.Deps(),
	}
}

// MetaKVClient returns the user space metaclient
func (m *DefaultBaseMaster) MetaKVClient() metaclient.KVClient {
	return m.userMetaKVClient
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
	ctx = m.errCenter.WithCancelOnFirstError(ctx)

	isInit, err := m.doInit(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	if isInit {
		if err := m.Impl.InitImpl(ctx); err != nil {
			return errors.Trace(err)
		}
	} else {
		if err := m.Impl.OnMasterRecovered(ctx); err != nil {
			return errors.Trace(err)
		}
	}

	if err := m.markStatusCodeInMetadata(ctx, frameModel.MasterStatusInit); err != nil {
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
			log.L().Info("Heartbeat Ping received",
				zap.Any("msg", msg),
				zap.String("master-id", m.id))
			ok, err := m.messageSender.SendToNode(
				ctx,
				sender,
				frameModel.HeartbeatPongTopic(m.id, msg.FromWorkerID),
				&frameModel.HeartbeatPongMessage{
					SendTime:   msg.SendTime,
					ReplyTime:  m.clock.Now(),
					ToWorkerID: msg.FromWorkerID,
					Epoch:      m.currentEpoch.Load(),
					IsFinished: msg.IsFinished,
				})
			if err != nil {
				return err
			}
			if !ok {
				// TODO add a retry mechanism
				return nil
			}
			m.workerManager.HandleHeartbeat(msg, sender)
			return nil
		})
	if err != nil {
		return err
	}
	if !ok {
		log.L().Panic("duplicate handler", zap.String("topic", frameModel.HeartbeatPingTopic(m.id)))
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
		log.L().Panic("duplicate handler", zap.String("topic", statusutil.WorkerStatusTopic(m.id)))
	}

	return nil
}

// Poll implements BaseMaster.Poll
func (m *DefaultBaseMaster) Poll(ctx context.Context) error {
	ctx = m.errCenter.WithCancelOnFirstError(ctx)

	if err := m.doPoll(ctx); err != nil {
		return errors.Trace(err)
	}

	if err := m.Impl.Tick(ctx); err != nil {
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
		return derror.ErrMasterClosed.GenWithStackByArgs()
	default:
	}

	if err := m.messageHandlerManager.CheckError(ctx); err != nil {
		return errors.Trace(err)
	}
	return m.workerManager.Tick(ctx)
}

// MasterMeta implements BaseMaster.MasterMeta
func (m *DefaultBaseMaster) MasterMeta() *frameModel.MasterMetaKVData {
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
		log.L().Warn("Failed to clean up message handlers",
			zap.String("master-id", m.id))
	}
	promutil.UnregisterWorkerMetrics(m.id)
}

// Close implements BaseMaster.Close
func (m *DefaultBaseMaster) Close(ctx context.Context) error {
	err := m.Impl.CloseImpl(ctx)
	// We don't return here if CloseImpl return error to ensure
	// that we can close inner resources of the framework
	if err != nil {
		log.L().Error("Failed to close MasterImpl", zap.Error(err))
	}

	m.doClose()
	return errors.Trace(err)
}

// OnError implements BaseMaster.OnError
func (m *DefaultBaseMaster) OnError(err error) {
	m.errCenter.OnError(err)
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

	if err := metaClient.Update(ctx, masterMeta); err != nil {
		return false, 0, errors.Trace(err)
	}

	m.masterMeta = masterMeta
	// isInit true means the master is created but has not been initialized.
	isInit = masterMeta.StatusCode == frameModel.MasterStatusUninit

	return
}

func (m *DefaultBaseMaster) markStatusCodeInMetadata(
	ctx context.Context, code frameModel.MasterStatusCode,
) error {
	metaClient := metadata.NewMasterMetadataClient(m.id, m.frameMetaClient)
	masterMeta, err := metaClient.Load(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	masterMeta.StatusCode = code
	return metaClient.Update(ctx, masterMeta)
}

// prepareWorkerConfig extracts information from WorkerConfig into detail fields.
// - If workerType is master type, the config is a `*MasterMetaKVData` struct and
//   contains pre allocated maseter ID, and json marshalled config.
// - If workerType is worker type, the config is a user defined config struct, we
//   marshal it to byte slice as returned config, and generate a random WorkerID.
func (m *DefaultBaseMaster) prepareWorkerConfig(
	workerType frameModel.WorkerType, config WorkerConfig,
) (rawConfig []byte, workerID frameModel.WorkerID, err error) {
	switch workerType {
	case CvsJobMaster, FakeJobMaster, DMJobMaster:
		masterMeta, ok := config.(*frameModel.MasterMetaKVData)
		if !ok {
			err = derror.ErrMasterInvalidMeta.GenWithStackByArgs(config)
			return
		}
		rawConfig = masterMeta.Config
		workerID = masterMeta.ID
	case WorkerDMDump, WorkerDMLoad, WorkerDMSync:
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
	cost model.RescUnit,
	resources ...resourcemeta.ResourceID,
) (frameModel.WorkerID, error) {
	log.L().Info("CreateWorker",
		zap.Int64("worker-type", int64(workerType)),
		zap.Any("worker-config", config),
		zap.Int("cost", int(cost)),
		zap.Any("resources", resources),
		zap.String("master-id", m.id))

	ctx := m.errCenter.WithCancelOnFirstError(context.Background())
	quotaCtx, cancel := context.WithTimeout(ctx, createWorkerWaitQuotaTimeout)
	defer cancel()
	if err := m.createWorkerQuota.Consume(quotaCtx); err != nil {
		return "", derror.Wrap(derror.ErrMasterConcurrencyExceeded, err)
	}

	configBytes, workerID, err := m.prepareWorkerConfig(workerType, config)
	if err != nil {
		return "", err
	}

	go func() {
		defer func() {
			m.createWorkerQuota.Release()
		}()

		requestCtx, cancel := context.WithTimeout(ctx, createWorkerTimeout)
		defer cancel()

		resp, err := m.serverMasterClient.ScheduleTask(requestCtx, &pb.ScheduleTaskRequest{
			TaskId:               workerID,
			Cost:                 int64(cost),
			ResourceRequirements: resources,
		},
			// TODO (zixiong) remove this timeout.
			time.Second*10)
		if err != nil {
			// TODO log the gRPC errors from a lower level such as by an interceptor.
			log.L().Warn("ScheduleTask returned error", zap.Error(err))
			m.workerManager.AbortCreatingWorker(workerID, err)
			return
		}
		log.L().Debug("ScheduleTask succeeded", zap.Any("response", resp))

		executorID := model.ExecutorID(resp.ExecutorId)

		err = m.executorClientManager.AddExecutor(executorID, resp.ExecutorAddr)
		if err != nil {
			m.workerManager.AbortCreatingWorker(workerID, err)
			return
		}

		executorClient := m.executorClientManager.ExecutorClient(executorID)
		dispatchArgs := &client.DispatchTaskArgs{
			// [NOTICE]:
			// For JobManager, <JobID, ProjectInfo> pair is set in advance
			// For JobMaster, we always get the 'masterProjectInfo'
			ProjectInfo:  m.GetProjectInfo(workerID),
			WorkerID:     workerID,
			MasterID:     m.id,
			WorkerType:   int64(workerType),
			WorkerConfig: configBytes,
		}

		err = executorClient.DispatchTask(requestCtx, dispatchArgs, func() {
			m.workerManager.BeforeStartingWorker(workerID, executorID)
		}, func(err error) {
			m.workerManager.AbortCreatingWorker(workerID, err)
		})

		if err != nil {
			// All cleaning up should have been done in AbortCreatingWorker.
			log.L().Info("DispatchTask failed",
				zap.Error(err))
			return
		}

		log.L().Info("Dispatch Worker succeeded",
			zap.Any("args", dispatchArgs))
	}()

	return workerID, nil
}

// IsMasterReady implements BaseMaster.IsMasterReady
func (m *DefaultBaseMaster) IsMasterReady() bool {
	return m.workerManager.IsInitialized()
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
func (m *DefaultBaseMaster) InitProjectInfosAfterRecover(jobs []*frameModel.MasterMetaKVData) {
	for _, meta := range jobs {
		// TODO: fix the TenantID
		m.workerProjectMap.Store(meta.ID, tenant.NewProjectInfo("", meta.ProjectID))
	}
}
