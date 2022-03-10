package lib

import (
	"context"
	"sync"

	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"
	"go.uber.org/dig"

	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/hanfei1991/microcosm/pkg/resource"
)

type mockWorkerImpl struct {
	mu sync.Mutex
	mock.Mock

	*DefaultBaseWorker
	id WorkerID

	messageHandlerManager *p2p.MockMessageHandlerManager
	messageSender         *p2p.MockMessageSender
	metaKVClient          *metadata.MetaMock

	failoverCount atomic.Int64
}

type workerParamListForTest struct {
	dig.Out

	MessageHandlerManager p2p.MessageHandlerManager
	MessageSender         p2p.MessageSender
	MetaKVClient          metadata.MetaKV
	ResourceProxy         resource.Proxy
}

//nolint:unparam
func newMockWorkerImpl(workerID WorkerID, masterID MasterID) *mockWorkerImpl {
	ret := &mockWorkerImpl{
		id: workerID,
	}

	ret.DefaultBaseWorker = MockBaseWorker(workerID, masterID, ret)
	ret.messageHandlerManager = ret.DefaultBaseWorker.messageHandlerManager.(*p2p.MockMessageHandlerManager)
	ret.messageSender = ret.DefaultBaseWorker.messageSender.(*p2p.MockMessageSender)
	ret.metaKVClient = ret.DefaultBaseWorker.metaKVClient.(*metadata.MetaMock)
	return ret
}

func (w *mockWorkerImpl) InitImpl(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	args := w.Called(ctx)
	return args.Error(0)
}

func (w *mockWorkerImpl) Tick(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	args := w.Called(ctx)
	return args.Error(0)
}

func (w *mockWorkerImpl) Status() WorkerStatus {
	w.mu.Lock()
	defer w.mu.Unlock()

	args := w.Called()
	return args.Get(0).(WorkerStatus)
}

func (w *mockWorkerImpl) OnMasterFailover(reason MasterFailoverReason) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.failoverCount.Add(1)

	args := w.Called(reason)
	return args.Error(0)
}

func (w *mockWorkerImpl) CloseImpl(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	args := w.Called()
	return args.Error(0)
}
