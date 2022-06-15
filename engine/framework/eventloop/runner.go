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
	gerrors "errors"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	derrors "github.com/pingcap/tiflow/engine/pkg/errors"
)

const (
	defaultEventLoopTickInterval = 50 * time.Millisecond
)

// Runner runs a task.
type Runner[T task] struct {
	task T

	alreadyRun atomic.Bool
	label      string

	clk clock.Clock
}

// NewRunner returns a new Runner.
func NewRunner[T task](t T, label string) *Runner[T] {
	return &Runner[T]{
		task:  t,
		label: label,
		clk:   clock.New(),
	}
}

// Run is called by the runtime. Cancelling the
// ctx cancels the task immediately and forcefully.
func (r *Runner[R]) Run(ctx context.Context) error {
	if r.alreadyRun.Swap(true) {
		panic(fmt.Sprintf("duplicate calls to Run: %s", r.label))
	}

	err := r.doRun(ctx)
	if err == nil {
		panic(fmt.Sprintf("unexpected exiting with nil error: %s", r.label))
	}

	if !isForcefulExitError(err) {
		// Exit gracefully.
		r.doGracefulExit(ctx, err)
	}

	if closeErr := r.task.Close(context.Background()); closeErr != nil {
		log.L().Warn("Closing task returned error", zap.String("label", r.label))
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

	err := r.task.Exit(timeoutCtx, errIn)
	if !gerrors.Is(err, context.Canceled) {
		log.L().Error("an error is encountered when a task is already exiting",
			zap.Error(err), zap.NamedError("original-err", errIn))
	}
}

func isForcefulExitError(errIn error) bool {
	if gerrors.Is(errIn, context.Canceled) {
		// Cancellation should result in a forceful exit.
		return true
	}
	// Suicides should result in a forceful exit.
	return derrors.ErrWorkerSuicide.Equal(errIn)
}
