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

package worker

import (
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/executor/worker/internal"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const runTTLCheckerInterval = 1 * time.Second

type requestID = string

type requestEntry struct {
	RequestID requestID
	ExpireAt  time.Time

	// task is a RunnableContainer, which contains
	// the task's submit time.
	task *internal.RunnableContainer
}

func (r *requestEntry) TaskID() internal.RunnableID {
	return r.task.ID()
}

// WrappedTaskAdder is an interface used to abstract a TaskRunner.
type WrappedTaskAdder interface {
	addWrappedTask(task *internal.RunnableContainer) error
}

// TaskCommitter is used to implement two-phase task dispatching.
type TaskCommitter struct {
	runner WrappedTaskAdder

	mu               sync.Mutex
	pendingRequests  map[requestID]*requestEntry
	requestsByTaskID map[RunnableID]*requestEntry

	clock    clock.Clock
	wg       sync.WaitGroup
	cancelCh chan struct{}

	// for unit tests only
	requestCleanUpCount atomic.Int64

	requestTTL time.Duration
}

// NewTaskCommitter returns a TaskCommitter.
func NewTaskCommitter(runner WrappedTaskAdder, requestTTL time.Duration) *TaskCommitter {
	return newTaskCommitterWithClock(runner, requestTTL, clock.New())
}

func newTaskCommitterWithClock(
	runner WrappedTaskAdder,
	requestTTL time.Duration,
	clock clock.Clock,
) *TaskCommitter {
	committer := &TaskCommitter{
		runner: runner,

		pendingRequests:  make(map[requestID]*requestEntry),
		requestsByTaskID: make(map[RunnableID]*requestEntry),

		clock:      clock,
		cancelCh:   make(chan struct{}),
		requestTTL: requestTTL,
	}

	committer.wg.Add(1)
	go func() {
		defer committer.wg.Done()
		committer.runTTLChecker()
	}()

	return committer
}

// PreDispatchTask is the "prepare" stage of submitting a task.
func (c *TaskCommitter) PreDispatchTask(rID requestID, task Runnable) (ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.pendingRequests[rID]; exists {
		// Duplicate requests are not allowed.
		// As we use UUIDs as request IDs, there should not be duplications.
		// The calling convention is that you should NOT retry the pre-dispatch API call.
		return false
	}

	// We need to overwrite stale request for the same workerID.
	if request, exists := c.requestsByTaskID[task.ID()]; exists {
		log.Info("There exists a previous request with the same worker ID, overwriting it.",
			zap.Any("request", request))

		c.removeRequestByID(request.RequestID)
	}

	// We use the current time as the submit time of the task.
	taskWithSubmitTime := internal.WrapRunnable(task, c.clock.Mono())

	request := &requestEntry{
		RequestID: rID,
		ExpireAt:  c.clock.Now().Add(c.requestTTL),
		task:      taskWithSubmitTime,
	}
	c.pendingRequests[rID] = request
	c.requestsByTaskID[task.ID()] = request

	return true
}

// ConfirmDispatchTask is the "commit" stage of dispatching a task.
// Return values:
// - (true, nil) is returned on success.
// - (false, nil) is returned if the PreDispatchTask request is not found in memory.
// - (false, [error]) is returned for other unexpected situations.
func (c *TaskCommitter) ConfirmDispatchTask(rID requestID, taskID RunnableID) (ok bool, retErr error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	request, ok := c.pendingRequests[rID]
	if !ok {
		log.Warn("ConfirmDispatchTask: request not found",
			zap.String("request-id", rID),
			zap.String("task-id", taskID))
		return false, nil
	}

	c.removeRequestByID(rID)

	if err := c.runner.addWrappedTask(request.task); err != nil {
		return false, err
	}

	return true, nil
}

// Close terminates the background task of the TaskCommitter.
func (c *TaskCommitter) Close() {
	close(c.cancelCh)
	c.wg.Wait()
}

// cleanUpCount is for unit tests only.
func (c *TaskCommitter) cleanUpCount() int64 {
	return c.requestCleanUpCount.Load()
}

func (c *TaskCommitter) runTTLChecker() {
	ticker := c.clock.Ticker(runTTLCheckerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.cancelCh:
			return
		case <-ticker.C:
		}

		c.requestCleanUpCount.Add(1)
		c.checkTTLOnce()
	}
}

func (c *TaskCommitter) checkTTLOnce() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for rID, request := range c.pendingRequests {
		expireAt := request.ExpireAt
		if c.clock.Now().After(expireAt) {
			log.Info("Pending request has expired.",
				zap.Any("request", request),
				zap.String("task-id", request.TaskID()))
			c.removeRequestByID(rID)
		}
	}
}

// removeRequestByID should be called with c.mu taken.
func (c *TaskCommitter) removeRequestByID(id requestID) {
	request, ok := c.pendingRequests[id]
	if !ok {
		log.Panic("Unreachable", zap.String("request-id", id))
	}

	taskID := request.TaskID()
	if _, ok := c.requestsByTaskID[taskID]; !ok {
		log.Panic("Unreachable",
			zap.Any("request", request),
			zap.String("task-id", taskID))
	}

	delete(c.pendingRequests, id)
	delete(c.requestsByTaskID, taskID)
}
