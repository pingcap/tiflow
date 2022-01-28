package quota

import (
	"testing"

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
