package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/executor"
	"github.com/hanfei1991/microcosm/pkg/etcdutils"
	"github.com/hanfei1991/microcosm/servermaster"
	"github.com/hanfei1991/microcosm/test"
	"github.com/stretchr/testify/require"
)

func TestClientManager(t *testing.T) {
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
		Etcd: &etcdutils.ConfigParams{
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
