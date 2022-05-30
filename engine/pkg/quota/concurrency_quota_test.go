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
