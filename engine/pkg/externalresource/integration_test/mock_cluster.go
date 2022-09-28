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
	gerrors "errors"
	"sync"
	"testing"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/client"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/broker"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/manager"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/rpcutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"
)

type mockCluster struct {
	executorInfo *manager.MockExecutorInfoProvider
	jobInfo      *manager.MockJobStatusProvider

	service       *manager.Service
	gcCoordinator *manager.DefaultGCCoordinator
	gcRunner      *manager.DefaultGCRunner
	executorGroup *client.MockExecutorGroup

	meta pkgOrm.Client

	brokerLock sync.RWMutex
	brokers    map[model.ExecutorID]*broker.DefaultBroker

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func newMockGCCluster() *mockCluster {
	meta, err := pkgOrm.NewMockClient()
	if err != nil {
		panic(err)
	}

	executorInfo := manager.NewMockExecutorInfoProvider()
	jobInfo := manager.NewMockJobStatusProvider()

	id := "leader"
	leaderVal := &atomic.Value{}
	leaderVal.Store(&rpcutil.Member{Name: id})
	service := manager.NewService(meta, executorInfo, rpcutil.NewPreRPCHook[pb.ResourceManagerClient](
		id,
		leaderVal,
		&rpcutil.LeaderClientWithLock[pb.ResourceManagerClient]{},
		atomic.NewBool(true),
		&rate.Limiter{}, nil))

	executorGroup := client.NewMockExecutorGroup()
	gcRunner := manager.NewGCRunner(meta, executorGroup)
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
	}
}

func (c *mockCluster) Start(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		err := c.gcCoordinator.Run(ctx)
		require.Error(t, err)
		require.True(t, gerrors.Is(err, context.Canceled))
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		err := c.gcRunner.Run(ctx)
		require.Error(t, err)
		require.True(t, gerrors.Is(err, context.Canceled))
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
	brk := broker.NewBroker(config, id, cli)

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
