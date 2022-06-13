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

// This file provides helper function to let the implementation of WorkerImpl
// can finish its unit tests.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/framework/statusutil"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/deps"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/broker"
	mockkv "github.com/pingcap/tiflow/engine/pkg/meta/kvclient/mock"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
)

// BaseWorkerForTesting mocks base worker
type BaseWorkerForTesting struct {
	*DefaultBaseWorker
	Broker *broker.LocalBroker
}

// MockBaseWorker creates a mock base worker for test
func MockBaseWorker(
	workerID frameModel.WorkerID,
	masterID frameModel.MasterID,
	workerImpl WorkerImpl,
) *BaseWorkerForTesting {
	ctx := dcontext.Background()
	dp := deps.NewDeps()
	cli, err := pkgOrm.NewMockClient()
	if err != nil {
		panic(err)
	}
	resourceBroker := broker.NewBrokerForTesting("executor-1")
	params := workerParamListForTest{
		MessageHandlerManager: p2p.NewMockMessageHandlerManager(),
		MessageSender:         p2p.NewMockMessageSender(),
		FrameMetaClient:       cli,
		UserRawKVClient:       mockkv.NewMetaMock(),
		ResourceBroker:        resourceBroker,
	}
	err = dp.Provide(func() workerParamListForTest {
		return params
	})
	if err != nil {
		panic(err)
	}
	ctx = ctx.WithDeps(dp)

	ret := NewBaseWorker(
		ctx,
		workerImpl,
		workerID,
		masterID,
		FakeTask)
	return &BaseWorkerForTesting{
		ret.(*DefaultBaseWorker),
		resourceBroker,
	}
}

// MockBaseWorkerCheckSendMessage checks can receive one message from mock message sender
func MockBaseWorkerCheckSendMessage(
	t *testing.T,
	worker *DefaultBaseWorker,
	topic p2p.Topic,
	message interface{},
) {
	masterNode := worker.masterClient.MasterNode()
	got, ok := worker.messageSender.(*p2p.MockMessageSender).TryPop(masterNode, topic)
	require.True(t, ok)
	require.Equal(t, message, got)
}

// MockBaseWorkerWaitUpdateStatus checks can receive a update status message from
// mock message sender
func MockBaseWorkerWaitUpdateStatus(
	t *testing.T,
	worker *DefaultBaseWorker,
) {
	topic := statusutil.WorkerStatusTopic(worker.masterClient.MasterID())
	masterNode := worker.masterClient.MasterNode()
	require.Eventually(t, func() bool {
		_, ok := worker.messageSender.(*p2p.MockMessageSender).TryPop(masterNode, topic)
		return ok
	}, time.Second, 100*time.Millisecond)
}
