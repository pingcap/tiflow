package quota

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConcurrencyQuota(t *testing.T) {
	t.Parallel()

	quota := NewConcurrencyQuota(5)
	require.True(t, quota.TryConsume())
	require.True(t, quota.TryConsume())
	require.True(t, quota.TryConsume())
	require.True(t, quota.TryConsume())
	require.True(t, quota.TryConsume())
	require.False(t, quota.TryConsume())
	quota.Release()
	require.True(t, quota.TryConsume())
	require.False(t, quota.TryConsume())
}

func TestConcurrencyQuotaBlocking(t *testing.T) {
	t.Parallel()

	quota := NewConcurrencyQuota(1)
	err := quota.Consume(context.Background())
	require.NoError(t, err)

	timeoutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err = quota.Consume(timeoutCtx)
	require.Error(t, err)
	require.Regexp(t, ".*context deadline exceeded.*", err.Error())
}
