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

package integration

import (
	"context"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/client"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/broker"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/manager"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	rpcutilMock "github.com/pingcap/tiflow/engine/pkg/rpcutil/mock"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type mockCluster struct {
	executorInfo *manager.MockExecutorInfoProvider
	jobInfo      *manager.MockJobStatusProvider

	service       *manager.Service
	gcCoordinator manager.GCCoordinator
	gcRunner      manager.GCRunner
	executorGroup *client.MockExecutorGroup

	meta pkgOrm.Client

	brokerLock sync.RWMutex
	brokers    map[model.ExecutorID]*broker.DefaultBroker

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func newMockGCCluster(t *testing.T) (*mockCluster, *rpcutilMock.MockFeatureChecker) {
	meta, err := pkgOrm.NewMockClient()
	if err != nil {
		panic(err)
	}

	executorInfo := manager.NewMockExecutorInfoProvider()
	jobInfo := manager.NewMockJobStatusProvider()

	mockFeatureChecker := rpcutilMock.NewMockFeatureChecker(gomock.NewController(t))
	service := manager.NewService(meta)

	executorGroup := client.NewMockExecutorGroup()
	gcRunner := manager.NewGCRunner(meta, executorGroup, nil)
	gcCoordinator := manager.NewGCCoordinator(executorInfo, jobInfo, meta, gcRunner)

	return &mockCluster{
		executorInfo:  executorInfo,
		jobInfo:       jobInfo,
		service:       service,
		gcCoordinator: gcCoordinator,
		gcRunner:      gcRunner,
		brokers:       make(map[model.ExecutorID]*broker.DefaultBroker),
		executorGroup: executorGroup,
		meta:          meta,
	}, mockFeatureChecker
}

func (c *mockCluster) Start(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		err := c.gcCoordinator.Run(ctx)
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled))
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		err := c.gcRunner.Run(ctx)
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled))
	}()
}

func (c *mockCluster) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
}

func (c *mockCluster) AddBroker(id model.ExecutorID, baseDir string) {
	config := &resModel.Config{Local: resModel.LocalFileConfig{BaseDir: baseDir}}
	cli := &resourceClientStub{service: c.service}
	brk, err := broker.NewBrokerWithConfig(config, id, cli)
	if err != nil {
		log.Panic("create broker failed", zap.Error(err))
	}

	c.brokerLock.Lock()
	c.brokers[id] = brk
	c.brokerLock.Unlock()

	c.executorInfo.AddExecutor(string(id), "")
	c.executorGroup.AddClient(id, &executorClientStub{
		brk: brk,
	})
}

func (c *mockCluster) GetBroker(id model.ExecutorID) broker.Broker {
	c.brokerLock.RLock()
	defer c.brokerLock.RUnlock()

	return c.brokers[id]
}

func (c *mockCluster) MustGetBroker(t *testing.T, id model.ExecutorID) broker.Broker {
	brk := c.GetBroker(id)
	require.NotNil(t, brk)
	return brk
}
