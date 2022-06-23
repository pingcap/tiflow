package logutil

import (
	"context"
	"testing"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestContextWithLogger(t *testing.T) {
	t.Parallel()

	ctx := context.TODO()
	require.Equal(t, FromContext(ctx), log.L())

	logger := log.L().With(zap.String("inner", "logger"))
	ctx = NewContextWithLogger(ctx, logger)
	require.Equal(t, FromContext(ctx), log.L().With(zap.String("inner", "logger")))
}
