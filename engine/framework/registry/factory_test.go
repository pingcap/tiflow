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

package registry

import (
	"context"
	"testing"

	"github.com/pingcap/tiflow/engine/framework/fake"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/deps"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/broker"
	"github.com/pingcap/tiflow/engine/pkg/meta/mock"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/stretchr/testify/require"
	"go.uber.org/dig"
)

type paramList struct {
	dig.Out

	MessageHandlerManager p2p.MessageHandlerManager
	MessageSender         p2p.MessageSender
	FrameMetaClient       pkgOrm.Client
	BusinessClientConn    metaModel.ClientConn
	ResourceBroker        broker.Broker
}

func makeCtxWithMockDeps(t *testing.T) (*dcontext.Context, context.CancelFunc) {
	dp := deps.NewDeps()
	cli, err := pkgOrm.NewMockClient()
	require.NoError(t, err)
	broker := broker.NewBrokerForTesting("executor-1")
	err = dp.Provide(func() paramList {
		return paramList{
			MessageHandlerManager: p2p.NewMockMessageHandlerManager(),
			MessageSender:         p2p.NewMockMessageSender(),
			FrameMetaClient:       cli,
			BusinessClientConn:    mock.NewMockClientConn(),
			ResourceBroker:        broker,
		}
	})
	require.NoError(t, err)

	cancelFn := func() {
		broker.Close()
	}
	return dcontext.Background().WithDeps(dp), cancelFn
}

func TestNewSimpleWorkerFactory(t *testing.T) {
	fac := NewSimpleWorkerFactory(fake.NewDummyWorker)
	config, err := fac.DeserializeConfig([]byte(`{"target-tick":100}`))
	require.NoError(t, err)
	require.Equal(t, &fake.WorkerConfig{TargetTick: 100}, config)

	ctx, cancel := makeCtxWithMockDeps(t)
	defer cancel()

	metaCli, err := ctx.Deps().Construct(
		func(cli pkgOrm.Client) (pkgOrm.Client, error) {
			return cli, nil
		},
	)
	require.NoError(t, err)
	defer metaCli.(pkgOrm.Client).Close()
	newWorker, err := fac.NewWorkerImpl(ctx, "my-worker", "my-master", &fake.WorkerConfig{TargetTick: 100})
	require.NoError(t, err)
	require.IsType(t, &fake.Worker{}, newWorker)
}

func TestDeserializeConfigError(t *testing.T) {
	fac := NewSimpleWorkerFactory(fake.NewDummyWorker)
	_, err := fac.DeserializeConfig([]byte(`{target-tick:100}`))
	require.Error(t, err)
	require.False(t, fac.IsRetryableError(err))
}
