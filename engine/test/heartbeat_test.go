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

package test_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/engine/executor"
	"github.com/pingcap/tiflow/engine/servermaster"
	"github.com/pingcap/tiflow/engine/test"
)

func TestHeartbeatExecutorCrush(t *testing.T) {
	test.SetGlobalTestFlag(true)
	defer test.SetGlobalTestFlag(false)

	addr, _, _, cleanFn := test.PrepareEtcd(t, "etcd0")
	defer cleanFn()

	const (
		keepAliveTTL      = 3 * time.Second
		keepAliveInterval = 500 * time.Millisecond
		rpcTimeout        = 6 * time.Second
	)

	masterCfg := &servermaster.Config{
		Addr:              "127.0.0.1:1991",
		ETCDEndpoints:     []string{addr},
		KeepAliveTTL:      keepAliveTTL,
		KeepAliveInterval: keepAliveInterval,
		RPCTimeout:        rpcTimeout,
	}
	// one master + one executor
	executorCfg := &executor.Config{
		Join:              "127.0.0.1:1991",
		Addr:              "127.0.0.1:1992",
		KeepAliveTTL:      keepAliveTTL,
		KeepAliveInterval: keepAliveInterval,
		RPCTimeout:        rpcTimeout,
	}

	cluster := new(MiniCluster)
	masterCtx, err := cluster.CreateMaster(masterCfg)
	require.NoError(t, err)
	executorCtx := cluster.CreateExecutor(executorCfg)
	// Start cluster
	err = cluster.AsyncStartMaster()
	require.NoError(t, err)

	err = cluster.AsyncStartExector()
	require.NoError(t, err)

	time.Sleep(2 * time.Second)
	cluster.StopExec()

	executorEvent := <-executorCtx.ExecutorChange()
	masterEvent := <-masterCtx.ExecutorChange()
	require.Less(t, executorEvent.Time.Add(keepAliveTTL), masterEvent.Time)
	cluster.StopMaster()
}
