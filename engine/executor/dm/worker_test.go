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

package dm

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	dmconfig "github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/lib"
	"github.com/pingcap/tiflow/engine/lib/registry"
	"github.com/pingcap/tiflow/engine/model"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/deps"
	engineErr "github.com/pingcap/tiflow/engine/pkg/errors"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/broker"
	extkv "github.com/pingcap/tiflow/engine/pkg/meta/extension"
	kvmock "github.com/pingcap/tiflow/engine/pkg/meta/kvclient/mock"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/stretchr/testify/require"
	"go.uber.org/dig"
)

type workerParamListForTest struct {
	dig.Out

	MessageHandlerManager p2p.MessageHandlerManager
	MessageSender         p2p.MessageSender
	FrameMetaClient       pkgOrm.Client
	UserRawKVClient       extkv.KVClientEx
	ResourceBroker        broker.Broker
}

// Init -> Poll -> Close
func TestFactory(t *testing.T) {
	cli, err := pkgOrm.NewMockClient()
	require.NoError(t, err)
	dctx := dcontext.Background()
	dp := deps.NewDeps()
	dctx = dctx.WithDeps(dp)
	messageHandlerManager := p2p.NewMockMessageHandlerManager()
	depsForTest := workerParamListForTest{
		MessageHandlerManager: messageHandlerManager,
		MessageSender:         p2p.NewMockMessageSender(),
		FrameMetaClient:       cli,
		UserRawKVClient:       kvmock.NewMetaMock(),
		ResourceBroker:        broker.NewBrokerForTesting("exector-id"),
	}
	require.NoError(t, dp.Provide(func() workerParamListForTest {
		return depsForTest
	}))

	// test factory
	subtaskCfg := &dmconfig.SubTaskConfig{Name: "job-id", SourceID: "task-id", Flavor: mysql.MySQLFlavor}
	content, err := subtaskCfg.Toml()
	require.NoError(t, err)
	RegisterWorker()
	_, err = registry.GlobalWorkerRegistry().CreateWorker(dctx, lib.WorkerDMDump, "worker-id", "dm-jobmaster-id", []byte(content))
	require.NoError(t, err)
	_, err = registry.GlobalWorkerRegistry().CreateWorker(dctx, lib.WorkerDMLoad, "worker-id", "dm-jobmaster-id", []byte(content))
	require.NoError(t, err)
	_, err = registry.GlobalWorkerRegistry().CreateWorker(dctx, lib.WorkerDMSync, "worker-id", "dm-jobmaster-id", []byte(content))
	require.NoError(t, err)
}

func TestWorker(t *testing.T) {
	dctx := dcontext.Background()
	dp := deps.NewDeps()
	dctx = dctx.WithDeps(dp)
	require.NoError(t, dp.Provide(func() p2p.MessageHandlerManager {
		return p2p.NewMockMessageHandlerManager()
	}))
	dmWorker := newDMWorker(dctx, "master-id", lib.WorkerDMDump, &dmconfig.SubTaskConfig{})
	require.NotNil(t, dmWorker.messageAgent)
	unitHolder := &mockUnitHolder{}
	dmWorker.unitHolder = unitHolder
	dmWorker.BaseWorker = lib.MockBaseWorker("worker-id", "master-id", dmWorker)
	require.NoError(t, dmWorker.Init(context.Background()))
	// tick
	unitHolder.On("Stage").Return(metadata.StageRunning, nil).Twice()
	require.NoError(t, dmWorker.Tick(context.Background()))
	unitHolder.On("Stage").Return(metadata.StageRunning, nil).Twice()
	require.NoError(t, dmWorker.Tick(context.Background()))

	// auto resume error
	unitHolder.On("Stage").Return(metadata.StageError, &pb.ProcessResult{Errors: []*pb.ProcessError{{ErrCode: 0}}}).Twice()
	require.NoError(t, dmWorker.Tick(context.Background()))
	time.Sleep(time.Second)
	unitHolder.On("Stage").Return(metadata.StageError, &pb.ProcessResult{Errors: []*pb.ProcessError{{ErrCode: 0}}}).Once()
	unitHolder.On("Resume").Return(errors.New("resume error")).Once()
	require.EqualError(t, dmWorker.Tick(context.Background()), "resume error")
	// auto resume normal
	unitHolder.On("Stage").Return(metadata.StageError, &pb.ProcessResult{Errors: []*pb.ProcessError{{ErrCode: 0}}}).Once()
	unitHolder.On("Stage").Return(metadata.StageRunning, nil).Once()
	unitHolder.On("Resume").Return(nil).Once()
	require.NoError(t, dmWorker.Tick(context.Background()))

	// placeholder
	require.Equal(t, model.RescUnit(0), dmWorker.Workload())
	require.NoError(t, dmWorker.OnMasterFailover(lib.MasterFailoverReason{}))
	require.NoError(t, dmWorker.OnMasterMessage("", nil))

	// Finished
	unitHolder.On("Stage").Return(metadata.StageFinished, nil).Twice()
	require.True(t, engineErr.ErrWorkerFinish.Equal(dmWorker.Tick(context.Background())))

	unitHolder.AssertExpectations(t)
}
