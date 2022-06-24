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

	. "github.com/pingcap/check"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/executor"
	"github.com/pingcap/tiflow/engine/pkg/etcdutil"
	"github.com/pingcap/tiflow/engine/servermaster"
	"github.com/pingcap/tiflow/engine/test"
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
		Etcd: &etcdutil.ConfigParams{
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
