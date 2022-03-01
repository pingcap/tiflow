package lib

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/pkg/workerpool"
	"go.uber.org/atomic"
	"go.uber.org/dig"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/client"
	runtime "github.com/hanfei1991/microcosm/executor/worker"
	"github.com/hanfei1991/microcosm/lib/quota"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/clock"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/hanfei1991/microcosm/pkg/uuid"
)

type Master interface {
	Init(ctx context.Context) error
	Poll(ctx context.Context) error
	MasterID() MasterID

	runtime.Closer
}

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
	OnWorkerOffline(worker WorkerHandle, reason error) error

	// OnWorkerMessage is called when a customized message is received.
	OnWorkerMessage(worker WorkerHandle, topic p2p.Topic, message interface{}) error

	// CloseImpl is called when the master is being closed
	CloseImpl(ctx context.Context) error
}

const (
	createWorkerTimeout        = 10 * time.Second
	maxCreateWorkerConcurrency = 100
)

type BaseMaster interface {
	MetaKVClient() metadata.MetaKV
	Init(ctx context.Context) error
	Poll(ctx context.Context) error
	MasterMeta() *MasterMetaKVData
	MasterID() MasterID
	GetWorkers() map[WorkerID]WorkerHandle
	Close(ctx context.Context) error
	OnError(err error)
	// RegisterWorker registers worker handler only, the worker is expected to be running
	RegisterWorker(ctx context.Context, workerID WorkerID) error
	// CreateWorker registers worker handler and dispatches worker to executor
	CreateWorker(workerType WorkerType, config WorkerConfig, cost model.RescUnit) (WorkerID, error)
}

type DefaultBaseMaster struct {
	Impl MasterImpl

	// dependencies
	messageHandlerManager p2p.MessageHandlerManager
	messageSender         p2p.MessageSender
	metaKVClient          metadata.MetaKV
	executorClientManager client.ClientsManager
	serverMasterClient    client.MasterClient
	pool                  workerpool.AsyncPool

	clock clock.Clock

	// workerManager maintains the list of all workers and
	// their statuses.
	workerManager workerManager

	currentEpoch atomic.Int64

	wg    sync.WaitGroup
	errCh chan error

	// closeCh is closed when the BaseMaster is exiting
	closeCh chan struct{}

	id            MasterID // id of this master itself
	advertiseAddr string
	nodeID        p2p.NodeID
	timeoutConfig TimeoutConfig
	masterMeta    *MasterMetaKVData

	// components for easier unit testing
	uuidGen uuid.Generator

	// TODO use a shared quota for all masters.
	createWorkerQuota quota.ConcurrencyQuota
}

type masterParams struct {
	dig.In

	MessageHandlerManager p2p.MessageHandlerManager
	MessageSender         p2p.MessageSender
	MetaKVClient          metadata.MetaKV
	ExecutorClientManager client.ClientsManager
	ServerMasterClient    client.MasterClient
}

func NewBaseMaster(
	ctx *dcontext.Context,
	impl MasterImpl,
	id MasterID,
) BaseMaster {
	var (
		nodeID        p2p.NodeID
		advertiseAddr string
		masterMeta    = &MasterMetaKVData{}
		params        masterParams
	)
	if ctx != nil {
		nodeID = ctx.Environ.NodeID
		advertiseAddr = ctx.Environ.Addr
		metaBytes := ctx.Environ.MasterMetaBytes
		err := masterMeta.Unmarshal(metaBytes)
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
		metaKVClient:          params.MetaKVClient,
		executorClientManager: params.ExecutorClientManager,
		serverMasterClient:    params.ServerMasterClient,
		pool:                  workerpool.NewDefaultAsyncPool(4),
		id:                    id,
		clock:                 clock.New(),

		timeoutConfig: defaultTimeoutConfig,
		masterMeta:    masterMeta,

		errCh:   make(chan error, 1),
		closeCh: make(chan struct{}),

		uuidGen: uuid.NewGenerator(),

		nodeID:        nodeID,
		advertiseAddr: advertiseAddr,

		createWorkerQuota: quota.NewConcurrencyQuota(maxCreateWorkerConcurrency),
	}
}

func (m *DefaultBaseMaster) MetaKVClient() metadata.MetaKV {
	return m.metaKVClient
}

func (m *DefaultBaseMaster) Init(ctx context.Context) error {
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

	if err := m.markInitializedInMetadata(ctx); err != nil {
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
	m.workerManager = newWorkerManager(
		m.id,
		!isInit,
		epoch,
		m.messageSender,
		m.messageHandlerManager,
		m.metaKVClient,
		m.pool,
		&m.timeoutConfig)

	m.startBackgroundTasks()

	return isInit, nil
}

func (m *DefaultBaseMaster) Poll(ctx context.Context) error {
	if err := m.doPoll(ctx); err != nil {
		return errors.Trace(err)
	}

	if err := m.Impl.Tick(ctx); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (m *DefaultBaseMaster) doPoll(ctx context.Context) error {
	select {
	case err := <-m.errCh:
		if err != nil {
			return errors.Trace(err)
		}
	case <-m.closeCh:
		return derror.ErrMasterClosed.GenWithStackByArgs()
	default:
	}

	if err := m.messageHandlerManager.CheckError(ctx); err != nil {
		return errors.Trace(err)
	}

	if err := m.workerManager.CheckStatusUpdate(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (m *DefaultBaseMaster) MasterMeta() *MasterMetaKVData {
	return m.masterMeta
}

func (m *DefaultBaseMaster) MasterID() MasterID {
	return m.id
}

func (m *DefaultBaseMaster) GetWorkers() map[WorkerID]WorkerHandle {
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
}

func (m *DefaultBaseMaster) Close(ctx context.Context) error {
	if err := m.Impl.CloseImpl(ctx); err != nil {
		return errors.Trace(err)
	}

	m.doClose()
	return nil
}

func (m *DefaultBaseMaster) startBackgroundTasks() {
	cctx, cancel := context.WithCancel(context.Background())
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		<-m.closeCh
		cancel()
	}()

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		if err := m.pool.Run(cctx); err != nil {
			m.OnError(err)
		}
	}()

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		if err := m.runWorkerCheck(cctx); err != nil {
			m.OnError(err)
		}
	}()
}

func (m *DefaultBaseMaster) runWorkerCheck(ctx context.Context) error {
	ticker := time.NewTicker(m.timeoutConfig.masterHeartbeatCheckLoopInterval)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
		}

		offlinedWorkers, onlinedWorkers := m.workerManager.Tick(ctx, m.messageSender)
		// It is logical to call `OnWorkerOnline` first and then call `OnWorkerOffline`.
		// In case that these two events for the same worker is detected in the same tick.
		for _, workerInfo := range onlinedWorkers {
			log.L().Info("worker is online", zap.Any("worker-info", workerInfo))

			handle := m.workerManager.GetWorkerHandle(workerInfo.ID)
			err := m.Impl.OnWorkerOnline(handle)
			if err != nil {
				return errors.Trace(err)
			}
		}

		for _, workerInfo := range offlinedWorkers {
			log.L().Info("worker is offline", zap.Any("worker-info", workerInfo))
			status, ok := m.workerManager.GetStatus(workerInfo.ID)
			if !ok {
				log.L().Panic(
					"offlined worker has no status found",
					zap.Any("worker-info", workerInfo),
				)
			}
			tombstoneHandle := NewTombstoneWorkerHandle(workerInfo.ID, *status)
			err := m.unregisterMessageHandler(ctx, workerInfo.ID)
			if err != nil {
				return err
			}
			err = m.Impl.OnWorkerOffline(tombstoneHandle, derror.ErrWorkerOffline.GenWithStackByArgs(workerInfo.ID))
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (m *DefaultBaseMaster) unregisterMessageHandler(ctx context.Context, workerID WorkerID) error {
	topic := HeartbeatPingTopic(m.id, workerID)
	removed, err := m.messageHandlerManager.UnregisterHandler(ctx, topic)
	if err != nil {
		return errors.Trace(err)
	}
	if !removed {
		log.L().Warn("heartbeat message handler is not removed", zap.String("topic", topic))
	}

	topic = StatusUpdateTopic(m.id, workerID)
	removed, err = m.messageHandlerManager.UnregisterHandler(ctx, topic)
	if err != nil {
		return errors.Trace(err)
	}
	if !removed {
		log.L().Warn("status update message handler is not removed", zap.String("topic", topic))
	}

	return m.workerManager.OnWorkerOffline(ctx, workerID)
}

func (m *DefaultBaseMaster) OnError(err error) {
	if errors.Cause(err) == context.Canceled {
		// TODO think about how to gracefully handle cancellation here.
		log.L().Warn("BaseMaster is being canceled", zap.Error(err))
		return
	}
	select {
	case m.errCh <- err:
	default:
	}
}

// master meta is persisted before it is created, in this function we update some
// fileds to the current value, including epoch, nodeID and advertiseAddr.
func (m *DefaultBaseMaster) refreshMetadata(ctx context.Context) (isInit bool, epoch Epoch, err error) {
	metaClient := NewMasterMetadataClient(m.id, m.metaKVClient)

	masterMeta, err := metaClient.Load(ctx)
	if err != nil {
		return false, 0, err
	}

	epoch, err = metaClient.GenerateEpoch(ctx)
	if err != nil {
		return false, 0, errors.Trace(err)
	}

	// We should update the master data to reflect our current information
	masterMeta.Addr = m.advertiseAddr
	masterMeta.NodeID = m.nodeID
	masterMeta.Epoch = epoch

	if err := metaClient.Store(ctx, masterMeta); err != nil {
		return false, 0, errors.Trace(err)
	}

	m.masterMeta = masterMeta
	// isInit true means the master is created but has not been initialized.
	// TODO: consider to combine the state machine of master status with initialized
	isInit = !masterMeta.Initialized

	return
}

func (m *DefaultBaseMaster) markInitializedInMetadata(ctx context.Context) error {
	metaClient := NewMasterMetadataClient(m.id, m.metaKVClient)
	masterMeta, err := metaClient.Load(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	masterMeta.Initialized = true
	if err := metaClient.Store(ctx, masterMeta); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (m *DefaultBaseMaster) registerHandlerForWorker(ctx context.Context, workerID WorkerID) error {
	topic := HeartbeatPingTopic(m.id, workerID)
	ok, err := m.messageHandlerManager.RegisterHandler(
		ctx,
		topic,
		&HeartbeatPingMessage{},
		func(sender p2p.NodeID, value p2p.MessageValue) error {
			heartBeatMsg := value.(*HeartbeatPingMessage)
			curEpoch := m.currentEpoch.Load()
			if heartBeatMsg.Epoch < curEpoch {
				log.L().Info("stale message dropped",
					zap.Any("message", heartBeatMsg),
					zap.Int64("cur-epoch", curEpoch))
				return nil
			}
			if err := m.workerManager.HandleHeartbeat(heartBeatMsg, sender); err != nil {
				log.L().Error("HandleHeartbeat failed", zap.Error(err))
				m.OnError(err)
			}
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

// prepareWorkerConfig extracts information from WorkerConfig into detail fields.
// - If workerType is master type, the config is a `*MasterMetaKVData` struct and
//   contains pre allocated maseter ID, and json marshalled config.
// - If workerType is worker type, the config is a user defined config struct, we
//   marshal it to byte slice as returned config, and generate a random WorkerID.
func (m *DefaultBaseMaster) prepareWorkerConfig(
	workerType WorkerType, config WorkerConfig,
) (rawConfig []byte, workerID string, err error) {
	switch workerType {
	case CvsJobMaster, FakeJobMaster, DMJobMaster:
		masterMeta, ok := config.(*MasterMetaKVData)
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

func (m *DefaultBaseMaster) RegisterWorker(ctx context.Context, workerID WorkerID) error {
	registerHandlerCtx, cancelRegisterHandler := context.WithTimeout(ctx, time.Second*1)
	defer cancelRegisterHandler()
	return m.registerHandlerForWorker(registerHandlerCtx, workerID)
}

func (m *DefaultBaseMaster) CreateWorker(workerType WorkerType, config WorkerConfig, cost model.RescUnit) (WorkerID, error) {
	log.L().Info("CreateWorker",
		zap.Int64("worker-type", int64(workerType)),
		zap.Any("worker-config", config))

	if !m.createWorkerQuota.TryConsume() {
		return "", derror.ErrMasterConcurrencyExceeded.GenWithStackByArgs()
	}

	configBytes, workerID, err := m.prepareWorkerConfig(workerType, config)
	if err != nil {
		return "", err
	}

	// register worker haneler before dispatching worker to executor.
	// Because dispatchTask could fail, we must ensure register handler happens
	// before unregister handler, in case of zombie handler.
	if err := m.RegisterWorker(context.TODO(), workerID); err != nil {
		return "", errors.Trace(err)
	}

	go func() {
		// TODO make the timeout configurable
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		needUnregisterWorkerHandler := false
		defer func() {
			m.createWorkerQuota.Release()
			if needUnregisterWorkerHandler {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := m.unregisterMessageHandler(ctx, workerID); err != nil {
					m.OnError(errors.Trace(err))
				}
			}
		}()

		// When CreateWorker failed, we need to pass the worker id to
		// OnWorkerDispatched, so we use a dummy WorkerHandle.
		dispatchFailedDummyHandler := NewTombstoneWorkerHandle(
			workerID, WorkerStatus{Code: WorkerStatusError})
		requestCtx, cancel := context.WithTimeout(context.Background(), createWorkerTimeout)
		defer cancel()
		// This following API should be refined.
		resp, err := m.serverMasterClient.ScheduleTask(requestCtx, &pb.TaskSchedulerRequest{Tasks: []*pb.ScheduleTask{{
			Task: &pb.TaskRequest{
				Id: 0,
			},
			Cost: int64(cost),
		}}},
			// TODO (zixiong) make the timeout configurable
			time.Second*10)
		if err != nil {
			needUnregisterWorkerHandler = true
			err1 := m.Impl.OnWorkerDispatched(dispatchFailedDummyHandler, errors.Trace(err))
			if err1 != nil {
				m.OnError(errors.Trace(err1))
			}
			return
		}
		schedule := resp.GetSchedule()
		if len(schedule) != 1 {
			log.L().Panic("unexpected schedule result", zap.Any("schedule", schedule))
		}
		executorID := model.ExecutorID(schedule[0].ExecutorId)

		err = m.executorClientManager.AddExecutor(executorID, schedule[0].Addr)
		if err != nil {
			needUnregisterWorkerHandler = true
			err1 := m.Impl.OnWorkerDispatched(dispatchFailedDummyHandler, errors.Trace(err))
			if err1 != nil {
				m.OnError(errors.Trace(err1))
			}
			return
		}
		executorClient := m.executorClientManager.ExecutorClient(executorID)
		executorResp, err := executorClient.Send(requestCtx, &client.ExecutorRequest{
			Cmd: client.CmdDispatchTask,
			Req: &pb.DispatchTaskRequest{
				TaskTypeId: int64(workerType),
				TaskConfig: configBytes,
				MasterId:   m.id,
				WorkerId:   workerID,
			},
		})
		if err != nil {
			needUnregisterWorkerHandler = true
			err1 := m.Impl.OnWorkerDispatched(dispatchFailedDummyHandler, errors.Trace(err))
			if err1 != nil {
				m.OnError(errors.Trace(err1))
			}
			return
		}
		dispatchTaskResp := executorResp.Resp.(*pb.DispatchTaskResponse)
		log.L().Info("Worker dispatched", zap.Any("response", dispatchTaskResp))
		errCode := dispatchTaskResp.GetErrorCode()
		if errCode != pb.DispatchTaskErrorCode_OK {
			needUnregisterWorkerHandler = true
			err1 := m.Impl.OnWorkerDispatched(dispatchFailedDummyHandler,
				errors.Errorf("dispatch worker failed with error code: %d", errCode))
			if err1 != nil {
				m.OnError(errors.Trace(err1))
			}
			return
		}

		if err := m.workerManager.OnWorkerCreated(ctx, workerID, p2p.NodeID(executorID)); err != nil {
			needUnregisterWorkerHandler = true
			m.OnError(errors.Trace(err))
		}
		handle := m.workerManager.GetWorkerHandle(workerID)

		if err := m.Impl.OnWorkerDispatched(handle, nil); err != nil {
			m.OnError(errors.Trace(err))
		}
	}()

	return workerID, nil
}
