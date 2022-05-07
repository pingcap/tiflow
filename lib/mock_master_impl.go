package lib

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/hanfei1991/microcosm/lib/master"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"
	"go.uber.org/dig"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/client"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/pb"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/deps"
	"github.com/hanfei1991/microcosm/pkg/externalresource/broker"
	extkv "github.com/hanfei1991/microcosm/pkg/meta/extension"
	mockkv "github.com/hanfei1991/microcosm/pkg/meta/kvclient/mock"
	pkgOrm "github.com/hanfei1991/microcosm/pkg/orm"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type MockMasterImpl struct {
	mu sync.Mutex
	mock.Mock

	*DefaultBaseMaster
	masterID libModel.MasterID
	id       libModel.MasterID

	tickCount         atomic.Int64
	onlineWorkerCount atomic.Int64

	dispatchedWorkers chan WorkerHandle
	dispatchedResult  chan error
	updatedStatuses   chan *libModel.WorkerStatus

	messageHandlerManager *p2p.MockMessageHandlerManager
	messageSender         p2p.MessageSender
	frameMetaClient       pkgOrm.Client
	userRawKVClient       *mockkv.MetaMock
	executorClientManager *client.Manager
	serverMasterClient    *client.MockServerMasterClient
}

func NewMockMasterImpl(masterID, id libModel.MasterID) *MockMasterImpl {
	ret := &MockMasterImpl{
		masterID:          masterID,
		id:                id,
		dispatchedWorkers: make(chan WorkerHandle, 1),
		dispatchedResult:  make(chan error, 1),
		updatedStatuses:   make(chan *libModel.WorkerStatus, 1024),
	}
	ret.DefaultBaseMaster = MockBaseMaster(id, ret)
	ret.messageHandlerManager = ret.DefaultBaseMaster.messageHandlerManager.(*p2p.MockMessageHandlerManager)
	ret.messageSender = ret.DefaultBaseMaster.messageSender
	ret.frameMetaClient = ret.DefaultBaseMaster.frameMetaClient
	ret.userRawKVClient = ret.DefaultBaseMaster.userRawKVClient.(*mockkv.MetaMock)
	ret.executorClientManager = ret.DefaultBaseMaster.executorClientManager.(*client.Manager)
	ret.serverMasterClient = ret.DefaultBaseMaster.serverMasterClient.(*client.MockServerMasterClient)

	return ret
}

type masterParamListForTest struct {
	dig.Out

	MessageHandlerManager p2p.MessageHandlerManager
	MessageSender         p2p.MessageSender
	FrameMetaClient       pkgOrm.Client
	UserRawKVClient       extkv.KVClientEx
	ExecutorClientManager client.ClientsManager
	ServerMasterClient    client.MasterClient
	ResourceBroker        broker.Broker
}

func (m *MockMasterImpl) GetFrameMetaClient() pkgOrm.Client {
	return m.frameMetaClient
}

func (m *MockMasterImpl) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.Mock.ExpectedCalls = nil
	m.Mock.Calls = nil

	ctx := dcontext.Background()
	dp := deps.NewDeps()
	err := dp.Provide(func() masterParamListForTest {
		return masterParamListForTest{
			MessageHandlerManager: m.messageHandlerManager,
			MessageSender:         m.messageSender,
			FrameMetaClient:       m.frameMetaClient,
			UserRawKVClient:       m.userRawKVClient,
			ExecutorClientManager: m.executorClientManager,
			ServerMasterClient:    m.serverMasterClient,
			ResourceBroker:        broker.NewBrokerForTesting("executor-1"),
		}
	})
	if err != nil {
		panic(err)
	}

	ctx = ctx.WithDeps(dp)
	m.DefaultBaseMaster = NewBaseMaster(
		ctx,
		m,
		m.id).(*DefaultBaseMaster)
}

func (m *MockMasterImpl) TickCount() int64 {
	return m.tickCount.Load()
}

func (m *MockMasterImpl) InitImpl(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockMasterImpl) OnMasterRecovered(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockMasterImpl) OnWorkerStatusUpdated(worker WorkerHandle, newStatus *libModel.WorkerStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	select {
	case m.updatedStatuses <- newStatus:
	default:
	}

	args := m.Called(worker, newStatus)
	return args.Error(0)
}

func (m *MockMasterImpl) Tick(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.tickCount.Add(1)
	log.L().Info("tick")

	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockMasterImpl) OnWorkerDispatched(worker WorkerHandle, result error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.dispatchedWorkers <- worker
	m.dispatchedResult <- result

	args := m.Called(worker, result)
	return args.Error(0)
}

func (m *MockMasterImpl) OnWorkerOnline(worker WorkerHandle) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.L().Info("OnWorkerOnline", zap.Any("worker-id", worker.ID()))
	m.onlineWorkerCount.Add(1)

	args := m.Called(worker)
	return args.Error(0)
}

func (m *MockMasterImpl) OnWorkerOffline(worker WorkerHandle, reason error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.onlineWorkerCount.Sub(1)

	args := m.Called(worker, reason)
	return args.Error(0)
}

func (m *MockMasterImpl) OnWorkerMessage(worker WorkerHandle, topic p2p.Topic, message interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(worker, topic, message)
	return args.Error(0)
}

func (m *MockMasterImpl) CloseImpl(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockMasterImpl) MasterClient() *client.MockServerMasterClient {
	return m.serverMasterClient
}

type dummyStatus struct {
	Val int
}

func (s *dummyStatus) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

func (s *dummyStatus) Unmarshal(data []byte) error {
	return json.Unmarshal(data, s)
}

type MockWorkerHandler struct {
	mock.Mock

	WorkerID libModel.WorkerID
}

func (m *MockWorkerHandler) GetTombstone() master.TombstoneHandle {
	if m.IsTombStone() {
		return m
	}
	return nil
}

func (m *MockWorkerHandler) Unwrap() master.RunningHandle {
	if !m.IsTombStone() {
		return m
	}
	return nil
}

func (m *MockWorkerHandler) SendMessage(ctx context.Context, topic p2p.Topic, message interface{}, nonblocking bool) error {
	args := m.Called(ctx, topic, message, nonblocking)
	return args.Error(0)
}

func (m *MockWorkerHandler) Status() *libModel.WorkerStatus {
	args := m.Called()
	return args.Get(0).(*libModel.WorkerStatus)
}

func (m *MockWorkerHandler) ID() libModel.WorkerID {
	return m.WorkerID
}

func (m *MockWorkerHandler) IsTombStone() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockWorkerHandler) ToPB() (*pb.WorkerInfo, error) {
	args := m.Called()
	return args.Get(0).(*pb.WorkerInfo), args.Error(1)
}

func (m *MockWorkerHandler) CleanTombstone(ctx context.Context) error {
	args := m.Called()
	return args.Error(0)
}
