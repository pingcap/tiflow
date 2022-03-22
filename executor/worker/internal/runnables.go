package internal

import (
	"context"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/model"
)

type Closer interface {
	Close(ctx context.Context) error
}

type Workloader interface {
	Workload() model.RescUnit
}

type RunnableID = string

type Runnable interface {
	Init(ctx context.Context) error
	Poll(ctx context.Context) error
	ID() RunnableID

	Closer
}

type RunnableStatus = int32

const (
	TaskSubmitted = RunnableStatus(iota + 1)
	TaskRunning
	TaskClosing
)

type RunnableContainer struct {
	Runnable
	status atomic.Int32
	info   RuntimeInfo
}

func WrapRunnable(runnable Runnable, submitTime time.Time) *RunnableContainer {
	return &RunnableContainer{
		Runnable: runnable,
		status:   *atomic.NewInt32(TaskSubmitted),
		info:     RuntimeInfo{SubmitTime: submitTime},
	}
}

func (c *RunnableContainer) Status() RunnableStatus {
	return c.status.Load()
}

func (c *RunnableContainer) Info() RuntimeInfo {
	return c.info
}

func (c *RunnableContainer) OnInitialized() {
	oldStatus := c.status.Swap(TaskRunning)
	if oldStatus != TaskSubmitted {
		log.L().Panic("unexpected status", zap.Int32("status", oldStatus))
	}
}

func (c *RunnableContainer) OnStopped() {
	oldStatus := c.status.Swap(TaskClosing)
	if oldStatus != TaskRunning && oldStatus != TaskSubmitted {
		log.L().Panic("unexpected status", zap.Int32("status", oldStatus))
	}
}
