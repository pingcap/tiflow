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
	"context"

	"github.com/pingcap/tiflow/engine/executor/worker/internal"
	"github.com/pingcap/tiflow/engine/pkg/clock"
)

type runtimeInfoKeyType int

const (
	runtimeInfoKey = runtimeInfoKeyType(0)
)

// RuntimeContext is used to store information related to the Runtime.
type RuntimeContext struct {
	context.Context

	info *internal.RuntimeInfo
}

func newRuntimeCtx(ctx context.Context, info internal.RuntimeInfo) *RuntimeContext {
	// Note that info is passed by value to prevent accidental data sharing.
	infoPtr := &info
	valCtx := context.WithValue(ctx, runtimeInfoKey, infoPtr)
	return &RuntimeContext{
		Context: valCtx,
		info:    infoPtr,
	}
}

// NewRuntimeCtxWithSubmitTime creates a RuntimeContext with a given submit-time.
// This function is exposed for the purpose of unit-testing.
// There is NO NEED to use this function in production code.
func NewRuntimeCtxWithSubmitTime(ctx context.Context, submitTime clock.MonotonicTime) *RuntimeContext {
	return newRuntimeCtx(ctx, internal.RuntimeInfo{SubmitTime: submitTime})
}

// ToRuntimeCtx tries to convert a plain context.Context to RuntimeContext.
// Returns (nil, false) if the argument is not derived from a RuntimeContext.
func ToRuntimeCtx(ctx context.Context) (rctx *RuntimeContext, ok bool) {
	info := ctx.Value(runtimeInfoKey)
	if info == nil {
		return nil, false
	}

	return &RuntimeContext{Context: ctx, info: info.(*internal.RuntimeInfo)}, true
}

// SubmitTime returns the time at which a task is submitted to the runtime's queue.
func (c *RuntimeContext) SubmitTime() clock.MonotonicTime {
	return c.info.SubmitTime
}
