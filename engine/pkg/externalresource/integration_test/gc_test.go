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

package integration_test

import (
	"sync"

	"go.uber.org/atomic"
	"golang.org/x/time/rate"

	"github.com/pingcap/tiflow/engine/client"
	"github.com/pingcap/tiflow/engine/pb"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/broker"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/manager"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/rpcutil"
)

type mockGCCluster struct {
	executorInfo *manager.MockExecutorInfoProvider

	service        *manager.Service
	gcCoordinator  *manager.DefaultGCCoordinator
	gcRunner       *manager.DefaultGCRunner
	clientsManager *client.Manager

	meta pkgOrm.Client

	brokerLock sync.RWMutex
	brokers    []*broker.LocalBroker
}

func newMockGCCluster() *mockGCCluster {
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
		&rate.Limiter{}))

	gcRunner := manager.NewGCRunner(meta, nil)
	gcCoordinator := manager.NewGCCoordinator(executorInfo, jobInfo, meta, gcRunner)

	return &mockGCCluster{
		executorInfo:  executorInfo,
		service:       service,
		gcCoordinator: gcCoordinator,
		gcRunner:      gcRunner,
		brokers:       make([]*broker.LocalBroker, 0),
		meta:          meta,
	}
}
