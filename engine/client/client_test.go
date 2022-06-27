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

package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/engine/client"
	"github.com/pingcap/tiflow/engine/executor"
	"github.com/pingcap/tiflow/engine/pkg/etcdutil"
	"github.com/pingcap/tiflow/engine/servermaster"
	"github.com/pingcap/tiflow/engine/test"
)

func TestClientManager(t *testing.T) {
	t.Parallel()

	test.SetGlobalTestFlag(true)
	defer test.SetGlobalTestFlag(false)

	manager := client.NewClientManager()
	require.Nil(t, manager.MasterClient())
	require.Nil(t, manager.ExecutorClient("abc"))
	ctx := context.Background()
	err := manager.AddMasterClient(ctx, []string{"127.0.0.1:1992"})
	require.NotNil(t, err)
	require.Nil(t, manager.MasterClient())

	masterCfg := &servermaster.Config{
		Etcd: &etcdutil.ConfigParams{
			Name:    "master1",
			DataDir: "/tmp/df",
		},
		MasterAddr:        "127.0.0.1:1992",
		KeepAliveTTL:      20000000 * time.Second,
		KeepAliveInterval: 200 * time.Millisecond,
		RPCTimeout:        time.Second,
	}

	masterServer, err := servermaster.NewServer(masterCfg, test.NewContext())
	require.Nil(t, err)

	masterCtx, masterCancel := context.WithCancel(ctx)
	defer masterCancel()
	err = masterServer.Run(masterCtx)
	require.Nil(t, err)

	err = manager.AddMasterClient(ctx, []string{"127.0.0.1:1992"})
	require.Nil(t, err)
	require.NotNil(t, manager.MasterClient())

	executorCfg := &executor.Config{
		Join:              "127.0.0.1:1992",
		WorkerAddr:        "127.0.0.1:1993",
		KeepAliveTTL:      20000000 * time.Second,
		KeepAliveInterval: 200 * time.Millisecond,
		RPCTimeout:        time.Second,
	}

	execServer := executor.NewServer(executorCfg, test.NewContext())
	execCtx, execCancel := context.WithCancel(ctx)
	defer execCancel()
	err = execServer.Run(execCtx)
	require.Nil(t, err)

	err = manager.AddExecutor("executor", "127.0.0.1:1993")
	require.Nil(t, err)
	require.NotNil(t, manager.ExecutorClient("executor"))
}

func TestAddNonExistentExecutor(t *testing.T) {
	t.Parallel()

	manager := client.NewClientManager()

	startTime := time.Now()
	// We don't care about the error for now.
	// As long as it does not block, it's fine.
	_ = manager.AddExecutor("executor", "127.0.0.1:1111") // Bad address
	require.Less(t, time.Since(startTime), time.Second)
}
