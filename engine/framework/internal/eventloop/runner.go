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

package eventloop

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/log"
	frameErrors "github.com/pingcap/tiflow/engine/framework/internal/errors"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	defaultEventLoopTickInterval = 50 * time.Millisecond
)

// Runner runs a Task.
type Runner[T Task] struct {
	task T

	alreadyRun atomic.Bool

	clk clock.Clock
}

// NewRunner returns a new Runner.
func NewRunner[T Task](t T) *Runner[T] {
	return &Runner[T]{
		task: t,
		clk:  clock.New(),
	}
}

// Run is called by the runtime. Cancelling the
// ctx cancels the task immediately and forcefully.
func (r *Runner[R]) Run(ctx context.Context) error {
	if r.alreadyRun.Swap(true) {
		panic(fmt.Sprintf("duplicate calls to Run: %s", r.task.ID()))
	}

	err := r.doRun(ctx)
	if err == nil {
		panic(fmt.Sprintf("unexpected exiting with nil error: %s", r.task.ID()))
	}

	if !isForcefulExitError(err) {
		// Exit gracefully.
		r.doGracefulExit(ctx, err)
	}

	if IsTerminatedError(err) {
		if stopErr := r.task.Stop(context.Background()); stopErr != nil {
			log.Warn("Stop task returned error",
				zap.String("label", r.task.ID()), zap.Error(stopErr))
		}
	} else {
		if closeErr := r.task.Close(context.Background()); closeErr != nil {
			log.Warn("Closing task returned error", zap.String("label", r.task.ID()))
		}
	}

	return err
}

func (r *Runner[R]) doRun(ctx context.Context) error {
	if err := r.task.Init(ctx); err != nil {
		return errors.Trace(err)
	}

	ticker := r.clk.Ticker(defaultEventLoopTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			if err := r.task.Poll(ctx); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (r *Runner[R]) doGracefulExit(ctx context.Context, errIn error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	err := r.task.NotifyExit(timeoutCtx, errIn)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Error("an error is encountered when a task is already exiting",
			zap.Error(err), zap.NamedError("original-err", errIn))
	}
}

func isForcefulExitError(errIn error) bool {
	if errors.Is(errIn, context.Canceled) {
		// Cancellation should result in a forceful exit.
		return true
	}

	if frameErrors.IsFailFastError(errIn) {
		return true
	}

	// Suicides should result in a forceful exit.
	return errors.Is(errIn, errors.ErrWorkerSuicide)
}

// IsTerminatedError checks whether task enters a terminated state, which include
// finished, canceled
func IsTerminatedError(errIn error) bool {
	return errors.Is(errIn, errors.ErrWorkerCancel) ||
		errors.Is(errIn, errors.ErrWorkerFinish) ||
		errors.Is(errIn, errors.ErrWorkerFailed)
}
