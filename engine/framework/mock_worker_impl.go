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

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/broker"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"
	"go.uber.org/dig"
)

type mockWorkerImpl struct {
	mu sync.Mutex
	mock.Mock

	*DefaultBaseWorker
	id frameModel.WorkerID

	messageHandlerManager *p2p.MockMessageHandlerManager
	messageSender         *p2p.MockMessageSender
	metaClient            pkgOrm.Client

	closed atomic.Bool
}

type workerParamListForTest struct {
	dig.Out

	MessageHandlerManager p2p.MessageHandlerManager
	MessageSender         p2p.MessageSender
	FrameMetaClient       pkgOrm.Client
	BusinessClientConn    metaModel.ClientConn
	ResourceBroker        broker.Broker
}

//nolint:unparam
func newMockWorkerImpl(workerID frameModel.WorkerID, masterID frameModel.MasterID) *mockWorkerImpl {
	ret := &mockWorkerImpl{
		id: workerID,
	}

	ret.DefaultBaseWorker = MockBaseWorker(workerID, masterID, ret).DefaultBaseWorker
	ret.messageHandlerManager = ret.DefaultBaseWorker.messageHandlerManager.(*p2p.MockMessageHandlerManager)
	ret.messageSender = ret.DefaultBaseWorker.messageSender.(*p2p.MockMessageSender)
	ret.metaClient = ret.DefaultBaseWorker.frameMetaClient
	return ret
}

func (w *mockWorkerImpl) InitImpl(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	args := w.Called(ctx)
	return args.Error(0)
}

func (w *mockWorkerImpl) Tick(ctx context.Context) error {
	if w.closed.Load() {
		panic("Tick called after CloseImpl is called")
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	args := w.Called(ctx)
	return args.Error(0)
}

func (w *mockWorkerImpl) Status() frameModel.WorkerStatus {
	w.mu.Lock()
	defer w.mu.Unlock()

	args := w.Called()
	return args.Get(0).(frameModel.WorkerStatus)
}

func (w *mockWorkerImpl) OnMasterMessage(ctx context.Context, topic p2p.Topic, message p2p.MessageValue) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	args := w.Called(ctx, topic, message)
	return args.Error(0)
}

func (w *mockWorkerImpl) CloseImpl(ctx context.Context) {
	if w.closed.Swap(true) {
		panic("CloseImpl called twice")
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	w.Called()
}
