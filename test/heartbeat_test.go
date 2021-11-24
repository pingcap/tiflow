package test_test

import (
	"context"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/executor"
	"github.com/hanfei1991/microcosm/master"
	"github.com/hanfei1991/microcosm/test"
	. "github.com/pingcap/check"
	"github.com/pingcap/ticdc/dm/pkg/log"
)

func TestT(t *testing.T) {
	log.InitLogger(&log.Config{
		Level:  "debug",
		Format: "text",
	})

	test.GlobalTestFlag = true
	TestingT(t)
}

var _ = SerialSuites(&testHeartbeatSuite{})

type testHeartbeatSuite struct {
	master   *master.Server
	executor *executor.Server

	masterCtx   *test.Context
	executorCtx *test.Context

	keepAliveTTL      time.Duration
	keepAliveInterval time.Duration
	rpcTimeout        time.Duration
}

func (t *testHeartbeatSuite) SetUpSuite(c *C) {
	t.keepAliveTTL = 3 * time.Second
	t.keepAliveInterval = 500 * time.Millisecond
	t.rpcTimeout = 6 * time.Second
}

func (t *testHeartbeatSuite) SetUpTest(c *C) {
	masterCfg := &master.Config{
		Name:              "master1",
		MasterAddr:        "127.0.0.1:1991",
		DataDir:           "/tmp/df",
		KeepAliveTTL:      t.keepAliveTTL,
		KeepAliveInterval: t.keepAliveInterval,
		RPCTimeout:        t.rpcTimeout,
	}
	var err error
	t.masterCtx = test.NewContext()
	t.master, err = master.NewServer(masterCfg, t.masterCtx)
	c.Assert(err, IsNil)
	// one master + one executor
	executorCfg := &executor.Config{
		Join:              "127.0.0.1:1991",
		WorkerAddr:        "127.0.0.1:1992",
		KeepAliveTTL:      t.keepAliveTTL,
		KeepAliveInterval: t.keepAliveInterval,
		RPCTimeout:        t.rpcTimeout,
	}
	t.executorCtx = test.NewContext()
	t.executor = executor.NewServer(executorCfg, t.executorCtx)
}

func (t *testHeartbeatSuite) TearDownTest(c *C) {
	t.master.Stop()
	t.executor.Stop()
}

func (t *testHeartbeatSuite) TestHeartbeatExecutorCrush(c *C) {
	ctx := context.Background()
	masterCtx, masterCancel := context.WithCancel(ctx)
	defer masterCancel()
	err := t.master.Start(masterCtx)
	c.Assert(err, IsNil)
	execCtx, execCancel := context.WithCancel(ctx)
	go func() {
		err = t.executor.Start(execCtx)
		c.Assert(err, IsNil)
	}()

	time.Sleep(2 * time.Second)
	execCancel()

	executorEvent := <-t.executorCtx.ExcutorChange()
	masterEvent := <-t.masterCtx.ExcutorChange()
	c.Assert(executorEvent.Time.Add(t.keepAliveTTL), Less, masterEvent.Time)
}
