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

package upstream

import (
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestUpstreamShouldClose(t *testing.T) {
	up := &Upstream{}
	up.isDefaultUpstream = false
	mockClock := clock.NewMock()
	require.False(t, up.shouldClose())
	up.clock = mockClock
	up.idleTime = mockClock.Now().Add(-2 * maxIdleDuration)
	require.True(t, up.shouldClose())
	up.isDefaultUpstream = true
	require.False(t, up.shouldClose())
}

func TestUpstreamError(t *testing.T) {
	up := &Upstream{}
	err := errors.New("test")
	up.err.Store(err)
	require.Equal(t, err, up.Error())
	up.err.Store(nil)
	require.Nil(t, up.Error())
}

func TestUpstreamIsNormal(t *testing.T) {
	up := &Upstream{}
	up.status = uninit
	require.False(t, up.IsNormal())
	up.status = normal
	require.True(t, up.IsNormal())
	up.err.Store(errors.New("test"))
	require.False(t, up.IsNormal())
}

func TestTrySetIdleTime(t *testing.T) {
	up := newUpstream(nil, nil)
	require.Equal(t, uninit, up.status)
	up.clock = clock.New()
	up.trySetIdleTime()
	require.False(t, up.idleTime.IsZero())
	idleTime := up.idleTime
	up.trySetIdleTime()
	require.Equal(t, idleTime, up.idleTime)
	up.resetIdleTime()
	require.True(t, up.idleTime.IsZero())
	up.resetIdleTime()
	require.True(t, up.idleTime.IsZero())
}
