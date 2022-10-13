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
	"encoding/json"
	"sync"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/framework/internal/master"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/pkg/client"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/deps"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/broker"
	metaMock "github.com/pingcap/tiflow/engine/pkg/meta/mock"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

// MockMasterImpl implements a mock MasterImpl
type MockMasterImpl struct {
	mu sync.Mutex
	mock.Mock

	*DefaultBaseMaster
	masterID frameModel.MasterID
	id       frameModel.MasterID
	tp       frameModel.WorkerType

	tickCount         atomic.Int64
	onlineWorkerCount atomic.Int64

	dispatchedWorkers chan WorkerHandle
	dispatchedResult  chan error
	updatedStatuses   chan *frameModel.WorkerStatus

	messageHandlerManager *p2p.MockMessageHandlerManager
	messageSender         p2p.MessageSender
	frameMetaClient       pkgOrm.Client
	businessMetaKVClient  *metaMock.MetaMock
	executorGroup         *client.MockExecutorGroup
	serverMasterClient    *client.MockServerMasterClient
}

// NewMockMasterImpl creates a new MockMasterImpl instance
func NewMockMasterImpl(t *testing.T, masterID, id frameModel.MasterID) *MockMasterImpl {
	ret := &MockMasterImpl{
		masterID:          masterID,
		id:                id,
		tp:                frameModel.FakeJobMaster,
		dispatchedWorkers: make(chan WorkerHandle, 1),
		dispatchedResult:  make(chan error, 1),
		updatedStatuses:   make(chan *frameModel.WorkerStatus, 1024),
	}
	ret.DefaultBaseMaster = MockBaseMaster(t, id, ret)
	ret.messageHandlerManager = ret.DefaultBaseMaster.messageHandlerManager.(*p2p.MockMessageHandlerManager)
	ret.messageSender = ret.DefaultBaseMaster.messageSender
	ret.frameMetaClient = ret.DefaultBaseMaster.frameMetaClient
	ret.businessMetaKVClient = ret.DefaultBaseMaster.businessMetaKVClient.(*metaMock.MetaMock)
	ret.executorGroup = ret.DefaultBaseMaster.executorGroup.(*client.MockExecutorGroup)
	ret.serverMasterClient = ret.DefaultBaseMaster.serverMasterClient.(*client.MockServerMasterClient)

	return ret
}

type masterParamListForTest struct {
	dig.Out

	MessageHandlerManager p2p.MessageHandlerManager
	MessageSender         p2p.MessageSender
	FrameMetaClient       pkgOrm.Client
	BusinessClientConn    metaModel.ClientConn
	ExecutorGroup         client.ExecutorGroup
	ServerMasterClient    client.ServerMasterClient
	ResourceBroker        broker.Broker
}

// GetFrameMetaClient returns the framework meta client.
func (m *MockMasterImpl) GetFrameMetaClient() pkgOrm.Client {
	return m.frameMetaClient
}

// Reset resets the mock data.
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
			BusinessClientConn:    metaMock.NewMockClientConn(),
			ExecutorGroup:         m.executorGroup,
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
		m.id,
		m.tp,
	).(*DefaultBaseMaster)
}

// TickCount returns tick invoke time
func (m *MockMasterImpl) TickCount() int64 {
	return m.tickCount.Load()
}

// InitImpl implements MasterImpl.InitImpl
func (m *MockMasterImpl) InitImpl(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(ctx)
	return args.Error(0)
}

// OnMasterRecovered implements MasterImpl.OnMasterRecovered
func (m *MockMasterImpl) OnMasterRecovered(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(ctx)
	return args.Error(0)
}

// OnWorkerStatusUpdated implements MasterImpl.OnWorkerStatusUpdated
func (m *MockMasterImpl) OnWorkerStatusUpdated(worker WorkerHandle, newStatus *frameModel.WorkerStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	select {
	case m.updatedStatuses <- newStatus:
	default:
	}

	args := m.Called(worker, newStatus)
	return args.Error(0)
}

// Tick implements MasterImpl.Tick
func (m *MockMasterImpl) Tick(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.tickCount.Add(1)
	log.Info("tick")

	args := m.Called(ctx)
	return args.Error(0)
}

// OnWorkerDispatched implements MasterImpl.OnWorkerDispatched
func (m *MockMasterImpl) OnWorkerDispatched(worker WorkerHandle, result error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.dispatchedWorkers <- worker
	m.dispatchedResult <- result

	args := m.Called(worker, result)
	return args.Error(0)
}

// OnWorkerOnline implements MasterImpl.OnWorkerOnline
func (m *MockMasterImpl) OnWorkerOnline(worker WorkerHandle) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Info("OnWorkerOnline", zap.Any("worker-id", worker.ID()))
	m.onlineWorkerCount.Add(1)

	args := m.Called(worker)
	return args.Error(0)
}

// OnWorkerOffline implements MasterImpl.OnWorkerOffline
func (m *MockMasterImpl) OnWorkerOffline(worker WorkerHandle, reason error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.onlineWorkerCount.Sub(1)

	args := m.Called(worker, reason)
	return args.Error(0)
}

// OnWorkerMessage implements MasterImpl.OnWorkerMessage
func (m *MockMasterImpl) OnWorkerMessage(worker WorkerHandle, topic p2p.Topic, message interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(worker, topic, message)
	return args.Error(0)
}

// CloseImpl implements MasterImpl.CloseImpl
func (m *MockMasterImpl) CloseImpl(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.Called(ctx)
}

// StopImpl implements MasterImpl.StopImpl
func (m *MockMasterImpl) StopImpl(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.Called(ctx)
}

// MasterClient returns internal server master client
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

// MockWorkerHandler implements WorkerHandle, RunningHandle and TombstoneHandle interface
type MockWorkerHandler struct {
	mock.Mock

	WorkerID frameModel.WorkerID
}

// GetTombstone implements WorkerHandle.GetTombstone
func (m *MockWorkerHandler) GetTombstone() master.TombstoneHandle {
	if m.IsTombStone() {
		return m
	}
	return nil
}

// Unwrap implements WorkerHandle.Unwrap
func (m *MockWorkerHandler) Unwrap() master.RunningHandle {
	if !m.IsTombStone() {
		return m
	}
	return nil
}

// SendMessage implements RunningHandle.SendMessage
func (m *MockWorkerHandler) SendMessage(ctx context.Context, topic p2p.Topic, message interface{}, nonblocking bool) error {
	args := m.Called(ctx, topic, message, nonblocking)
	return args.Error(0)
}

// Status implements WorkerHandle.Status
func (m *MockWorkerHandler) Status() *frameModel.WorkerStatus {
	args := m.Called()
	return args.Get(0).(*frameModel.WorkerStatus)
}

// ID implements WorkerHandle.ID
func (m *MockWorkerHandler) ID() frameModel.WorkerID {
	return m.WorkerID
}

// IsTombStone implements WorkerHandle.IsTombStone
func (m *MockWorkerHandler) IsTombStone() bool {
	args := m.Called()
	return args.Bool(0)
}

// CleanTombstone implements TombstoneHandle.CleanTombstone
func (m *MockWorkerHandler) CleanTombstone(ctx context.Context) error {
	args := m.Called()
	return args.Error(0)
}
