// Copyright 2026 PingCAP, Inc.
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

package util

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHangCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for range 100 {
		require.ErrorIs(t, Hang(ctx, 0), context.Canceled)
	}
}

func TestHangDeadlineExceeded(t *testing.T) {
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, 0))
	defer cancel()
	for range 100 {
		require.ErrorIs(t, Hang(ctx, 0), context.DeadlineExceeded)
	}
}

func TestHang(t *testing.T) {
	require.NoError(t, Hang(t.Context(), 0))
	require.NoError(t, Hang(t.Context(), time.Millisecond))
}
