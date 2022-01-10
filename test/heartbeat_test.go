package test_test

import (
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/executor"
	"github.com/hanfei1991/microcosm/pkg/etcdutils"
	"github.com/hanfei1991/microcosm/servermaster"
	"github.com/hanfei1991/microcosm/test"
	. "github.com/pingcap/check"
	"github.com/pingcap/tiflow/dm/pkg/log"
)

func TestT(t *testing.T) {
	err := log.InitLogger(&log.Config{
		Level:  "debug",
		Format: "text",
	})
	if err != nil {
		panic(err)
	}

	test.SetGlobalTestFlag(true)
	defer test.SetGlobalTestFlag(false)
	TestingT(t)
}

var _ = SerialSuites(&testHeartbeatSuite{})

type testHeartbeatSuite struct {
	keepAliveTTL      time.Duration
	keepAliveInterval time.Duration
	rpcTimeout        time.Duration
}

func (t *testHeartbeatSuite) SetUpSuite(c *C) {
	t.keepAliveTTL = 3 * time.Second
	t.keepAliveInterval = 500 * time.Millisecond
	t.rpcTimeout = 6 * time.Second
}

func (t *testHeartbeatSuite) TestHeartbeatExecutorCrush(c *C) {
	masterCfg := &servermaster.Config{
		Etcd: &etcdutils.ConfigParams{
			Name:    "master1",
			DataDir: "/tmp/df",
		},
		MasterAddr:        "127.0.0.1:1991",
		KeepAliveTTL:      t.keepAliveTTL,
		KeepAliveInterval: t.keepAliveInterval,
		RPCTimeout:        t.rpcTimeout,
	}
	// one master + one executor
	executorCfg := &executor.Config{
		Join:              "127.0.0.1:1991",
		WorkerAddr:        "127.0.0.1:1992",
		KeepAliveTTL:      t.keepAliveTTL,
		KeepAliveInterval: t.keepAliveInterval,
		RPCTimeout:        t.rpcTimeout,
	}

	cluster := new(MiniCluster)
	masterCtx, err := cluster.CreateMaster(masterCfg)
	c.Assert(err, IsNil)
	executorCtx := cluster.CreateExecutor(executorCfg)
	// Start cluster
	err = cluster.AsyncStartMaster()
	c.Assert(err, IsNil)

	err = cluster.AsyncStartExector()
	c.Assert(err, IsNil)

	time.Sleep(2 * time.Second)
	cluster.StopExec()

	executorEvent := <-executorCtx.ExecutorChange()
	masterEvent := <-masterCtx.ExecutorChange()
	c.Assert(executorEvent.Time.Add(t.keepAliveTTL), Less, masterEvent.Time)
	cluster.StopMaster()
}
