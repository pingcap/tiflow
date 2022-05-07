package worker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/executor/worker/internal"
)

func TestToRuntimeCtxFromDerivedStdCtx(t *testing.T) {
	rtInfo := internal.RuntimeInfo{SubmitTime: time.Unix(1, 1)}
	rctx := newRuntimeCtx(context.Background(), rtInfo)

	ctx1, cancel := context.WithCancel(rctx)
	defer cancel()

	ctx2 := context.WithValue(ctx1, "fake-key", "fake-value")

	rctx1, ok := ToRuntimeCtx(ctx2)
	require.True(t, ok)
	require.Equal(t, &rtInfo, rctx1.info)
}

func TestToRuntimeCtxFalse(t *testing.T) {
	_, ok := ToRuntimeCtx(context.Background())
	require.False(t, ok)
}
