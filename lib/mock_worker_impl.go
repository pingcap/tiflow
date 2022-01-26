package lib

import (
	"context"
	"sync"

	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"
)

type mockWorkerImpl struct {
	mu sync.Mutex
	mock.Mock

	*BaseWorker
	id WorkerID

	messageHandlerManager *p2p.MockMessageHandlerManager
	messageSender         *p2p.MockMessageSender
	metaKVClient          *metadata.MetaMock

	failoverCount atomic.Int64
}

//nolint:unparam
func newMockWorkerImpl(workerID WorkerID, masterID MasterID) *mockWorkerImpl {
	ret := &mockWorkerImpl{
		id:                    workerID,
		messageHandlerManager: p2p.NewMockMessageHandlerManager(),
		messageSender:         p2p.NewMockMessageSender(),
		metaKVClient:          metadata.NewMetaMock(),
	}
	base := NewBaseWorker(
		ret,
		ret.messageHandlerManager,
		ret.messageSender,
		ret.metaKVClient,
		workerID,
		masterID)
	ret.BaseWorker = base
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
