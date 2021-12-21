package test_test

import (
	"context"

	"github.com/hanfei1991/microcosm/executor"
	"github.com/hanfei1991/microcosm/master"
	"github.com/hanfei1991/microcosm/test"
)

// TODO: support multi master / executor
type MiniCluster struct {
	master       *master.Server
	masterCancel func()

	exec       *executor.Server
	execCancel func()
}

func (c *MiniCluster) CreateMaster(cfg *master.Config) (*test.Context, error) {
	masterCtx := test.NewContext()
	master, err := master.NewServer(cfg, masterCtx)
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
	err := c.exec.Start(execCtx)
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
