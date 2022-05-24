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

package lib

import (
	"context"
	"sync"

	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"
	"go.uber.org/dig"

	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/broker"
	extkv "github.com/pingcap/tiflow/engine/pkg/meta/extension"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
)

type mockWorkerImpl struct {
	mu sync.Mutex
	mock.Mock

	*DefaultBaseWorker
	id libModel.WorkerID

	messageHandlerManager *p2p.MockMessageHandlerManager
	messageSender         *p2p.MockMessageSender
	metaClient            pkgOrm.Client

	failoverCount atomic.Int64
}

type workerParamListForTest struct {
	dig.Out

	MessageHandlerManager p2p.MessageHandlerManager
	MessageSender         p2p.MessageSender
	FrameMetaClient       pkgOrm.Client
	UserRawKVClient       extkv.KVClientEx
	ResourceBroker        broker.Broker
}

//nolint:unparam
func newMockWorkerImpl(workerID libModel.WorkerID, masterID libModel.MasterID) *mockWorkerImpl {
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
	w.mu.Lock()
	defer w.mu.Unlock()

	args := w.Called(ctx)
	return args.Error(0)
}

func (w *mockWorkerImpl) Status() libModel.WorkerStatus {
	w.mu.Lock()
	defer w.mu.Unlock()

	args := w.Called()
	return args.Get(0).(libModel.WorkerStatus)
}

func (w *mockWorkerImpl) OnMasterMessage(topic p2p.Topic, message p2p.MessageValue) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	args := w.Called(topic, message)
	return args.Error(0)
}

func (w *mockWorkerImpl) CloseImpl(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	args := w.Called()
	return args.Error(0)
}
