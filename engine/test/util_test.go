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
	"context"
	"fmt"
	"time"

	"github.com/phayes/freeport"
	. "github.com/pingcap/check"
	"github.com/pingcap/tiflow/engine/executor"
	"github.com/pingcap/tiflow/engine/servermaster"
	"github.com/pingcap/tiflow/engine/test"
	"github.com/pingcap/tiflow/engine/test/mock"
)

// TODO: support multi master / executor
type MiniCluster struct {
	master       *servermaster.Server
	masterCancel func()

	exec       *executor.Server
	execCancel func()
}

func (c *MiniCluster) CreateMaster(cfg *servermaster.Config) (*test.Context, error) {
	masterCtx := test.NewContext()
	master, err := servermaster.NewServer(cfg, masterCtx)
	c.master = master
	return masterCtx, err
}

func (c *MiniCluster) AsyncStartMaster() error {
	ctx := context.Background()
	masterCtx, masterCancel := context.WithCancel(ctx)
	err := c.master.Run(masterCtx)
	c.masterCancel = masterCancel
	return err
}

func (c *MiniCluster) CreateExecutor(cfg *executor.Config) *test.Context {
	execContext := test.NewContext()
	exec := executor.NewServer(cfg, execContext)
	c.exec = exec
	return execContext
}

func (c *MiniCluster) AsyncStartExector() error {
	ctx := context.Background()
	execCtx, execCancel := context.WithCancel(ctx)
	err := c.exec.Run(execCtx)
	c.execCancel = execCancel
	return err
}

func (c *MiniCluster) StopExec() {
	c.execCancel()
	c.exec.Stop()
}

func (c *MiniCluster) StopMaster() {
	c.masterCancel()
	c.master.Stop()
}

// Start 1 master 1 executor.
func (c *MiniCluster) Start1M1E(cc *C) (
	masterAddr string, workerAddr string,
	masterCtx *test.Context, workerCtx *test.Context,
) {
	ports, err := freeport.GetFreePorts(2)
	cc.Assert(err, IsNil)
	masterAddr = fmt.Sprintf("127.0.0.1:%d", ports[0])
	workerAddr = fmt.Sprintf("127.0.0.1:%d", ports[1])
	masterCfg := &servermaster.Config{
		Addr:              masterAddr,
		AdvertiseAddr:     masterAddr,
		KeepAliveTTL:      20000000 * time.Second,
		KeepAliveInterval: 200 * time.Millisecond,
		RPCTimeout:        time.Second,
	}
	// one master + one executor
	executorCfg := &executor.Config{
		Join:              masterAddr,
		Addr:              workerAddr,
		AdvertiseAddr:     workerAddr,
		KeepAliveTTL:      20000000 * time.Second,
		KeepAliveInterval: 200 * time.Millisecond,
		RPCTimeout:        time.Second,
	}

	masterCtx, err = c.CreateMaster(masterCfg)
	cc.Assert(err, IsNil)
	workerCtx = c.CreateExecutor(executorCfg)
	// Start cluster
	err = c.AsyncStartMaster()
	cc.Assert(err, IsNil)

	err = c.AsyncStartExector()
	cc.Assert(err, IsNil)

	time.Sleep(2 * time.Second)
	return
}

func (c *MiniCluster) StopCluster() {
	c.StopExec()
	c.StopMaster()
	mock.ResetGrpcCtx()
}
