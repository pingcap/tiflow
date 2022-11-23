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

package internal

import (
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// RunnableID is a unique id for the runnable
type RunnableID = string

// Runnable defines an interface that can be run in task runner
type Runnable interface {
	Run(ctx context.Context) error
	ID() RunnableID
}

// RunnableStatus is the runtime container status
type RunnableStatus = int32

// Defines all RunnableStatus
const (
	TaskSubmitted = RunnableStatus(iota + 1)
	TaskRunning
	TaskClosing
)

// RunnableContainer implements Runnable, and maintains some more running information
type RunnableContainer struct {
	Runnable
	status atomic.Int32
	info   RuntimeInfo
}

// WrapRunnable creates a new RunnableContainer from a Runnable interface
func WrapRunnable(runnable Runnable, submitTime clock.MonotonicTime) *RunnableContainer {
	return &RunnableContainer{
		Runnable: runnable,
		status:   *atomic.NewInt32(TaskSubmitted),
		info:     RuntimeInfo{SubmitTime: submitTime},
	}
}

// Status returns the status of RunnableContainer
func (c *RunnableContainer) Status() RunnableStatus {
	return c.status.Load()
}

// Info returns the info of RunnableContainer
func (c *RunnableContainer) Info() RuntimeInfo {
	return c.info
}

// OnLaunched is the callback when the runnable instance is launched.
func (c *RunnableContainer) OnLaunched() {
	oldStatus := c.status.Swap(TaskRunning)
	if oldStatus != TaskSubmitted {
		log.Panic("unexpected status", zap.Int32("status", oldStatus))
	}
}

// OnStopped is the callback when the runnable instance is stopped
func (c *RunnableContainer) OnStopped() {
	oldStatus := c.status.Swap(TaskClosing)
	if oldStatus != TaskRunning && oldStatus != TaskSubmitted {
		log.Panic("unexpected status", zap.Int32("status", oldStatus))
	}
}
