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
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/stretchr/testify/require"
)

func TestUpstream(t *testing.T) {
	pdClient := &gc.MockPDClient{}
	manager := NewManager4Test(pdClient)

	up1, ok1 := manager.Get(testUpstreamID)
	require.True(t, ok1)
	require.NotNil(t, up1)

	// test Add
	manager.add(testUpstreamID, []string{}, nil)

	// test Get
	testID := uint64(21)
	up, ok := manager.Get(testID)
	require.Nil(t, up)
	require.False(t, ok)
	up2 := NewUpstream4Test(pdClient)
	up2.ID = testID
	mockClock := clock.NewMock()
	up2.clock = mockClock

	manager.ups.Store(testID, up2)
	up, ok = manager.Get(testID)
	require.True(t, ok)
	require.NotNil(t, up)

	// test Tick
	_ = manager.Tick(context.Background(), &orchestrator.GlobalReactorState{})
	mockClock.Add(maxIdleDuration * 2)
	manager.lastTickTime = time.Time{}
	_ = manager.Tick(context.Background(), &orchestrator.GlobalReactorState{})
	// wait until up2 is closed
	for !up2.IsClosed() {
	}
	_ = manager.Tick(context.Background(), &orchestrator.GlobalReactorState{})
	up, ok = manager.Get(testID)
	require.False(t, ok)
	require.Nil(t, up)
	up, ok = manager.Get(testUpstreamID)
	require.True(t, ok)
	require.NotNil(t, up)
}
