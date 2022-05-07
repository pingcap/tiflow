package worker

import (
	"context"
	"time"

	"github.com/hanfei1991/microcosm/executor/worker/internal"
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
func NewRuntimeCtxWithSubmitTime(ctx context.Context, submitTime time.Time) *RuntimeContext {
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
func (c *RuntimeContext) SubmitTime() time.Time {
	return c.info.SubmitTime
}
