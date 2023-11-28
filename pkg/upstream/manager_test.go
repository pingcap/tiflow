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
	"sync"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
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
	manager.lastTickTime = atomic.Time{}
	_ = manager.Tick(context.Background(), &orchestrator.GlobalReactorState{})
	// wait until up2 is closed
	for !up2.IsClosed() {
	}
	manager.lastTickTime = atomic.Time{}
	_ = manager.Tick(context.Background(), &orchestrator.GlobalReactorState{})
	_ = manager.Tick(context.Background(), &orchestrator.GlobalReactorState{})
	up, ok = manager.Get(testID)
	require.False(t, ok)
	require.Nil(t, up)
	up, ok = manager.Get(testUpstreamID)
	require.True(t, ok)
	require.NotNil(t, up)
}

func TestRemoveErrorUpstream(t *testing.T) {
	pdClient := &gc.MockPDClient{}
	manager := NewManager4Test(pdClient)
	up1 := NewUpstream4Test(pdClient)
	manager.ups.Store(uint64(4), up1)
	up2 := NewUpstream4Test(pdClient)
	up2.err.Store(errors.New("test"))
	manager.ups.Store(testUpstreamID, up2)
	up, ok := manager.Get(testUpstreamID)
	require.True(t, ok)
	require.NotNil(t, up)
	_ = manager.Tick(context.Background(), &orchestrator.GlobalReactorState{
		Changefeeds: map[model.ChangeFeedID]*orchestrator.ChangefeedReactorState{
			model.DefaultChangeFeedID("1"): {
				Info: &model.ChangeFeedInfo{UpstreamID: testUpstreamID},
			},
			model.DefaultChangeFeedID("4"): {
				Info: &model.ChangeFeedInfo{UpstreamID: 4},
			},
		},
	})
	up, ok = manager.Get(testUpstreamID)
	require.False(t, ok)
	require.Nil(t, up)
}

func TestAddDefaultUpstream(t *testing.T) {
	m := NewManager(context.Background(), "id")
	m.initUpstreamFunc = func(ctx context.Context,
		up *Upstream, gcID string,
	) error {
		return errors.New("test")
	}
	pdClient := &gc.MockPDClient{}
	_, err := m.AddDefaultUpstream([]string{}, &security.Credential{}, pdClient)
	require.NotNil(t, err)
	up, err := m.GetDefaultUpstream()
	require.Nil(t, up)
	require.NotNil(t, err)
	m.initUpstreamFunc = func(ctx context.Context,
		up *Upstream, gcID string,
	) error {
		up.ID = uint64(2)
		return nil
	}
	_, err = m.AddDefaultUpstream([]string{}, &security.Credential{}, pdClient)
	require.Nil(t, err)
	up, err = m.GetDefaultUpstream()
	require.NotNil(t, up)
	require.Nil(t, err)
	up, ok := m.Get(uint64(2))
	require.NotNil(t, up)
	require.True(t, ok)
}

func TestAddUpstream(t *testing.T) {
	m := NewManager(context.Background(), "id")
	m.initUpstreamFunc = func(ctx context.Context,
		up *Upstream, gcID string,
	) error {
		return errors.New("test")
	}
	up := m.AddUpstream(&model.UpstreamInfo{ID: uint64(3)})
	require.NotNil(t, up)
	up1, ok := m.Get(uint64(3))
	require.NotNil(t, up1)
	require.True(t, ok)
	require.False(t, up1.IsNormal())
	for up.Error() == nil {
	}
	require.NotNil(t, up.Error())
}

func TestCloseManager(t *testing.T) {
	ctx := context.TODO()
	m := &Manager{
		ups: &sync.Map{},
	}
	ctx, cancel := context.WithCancel(ctx)
	m.ctx = ctx
	m.cancel = cancel
	up := &Upstream{
		wg: new(sync.WaitGroup),
	}
	_, cancel = context.WithCancel(ctx)
	up.cancel = cancel
	m.ups.Store(uint64(1), up)
	m.Close()
	require.Equal(t, closed, up.status)
	_, ok := m.ups.Load(uint64(1))
	require.False(t, ok)
}

func TestRemoveThenAddAgain(t *testing.T) {
	m := NewManager(context.Background(), "id")
	m.initUpstreamFunc = func(ctx context.Context,
		up *Upstream, gcID string,
	) error {
		return nil
	}
	up := m.AddUpstream(&model.UpstreamInfo{ID: uint64(3)})
	require.NotNil(t, up)
	// test Tick
	_ = m.Tick(context.Background(), &orchestrator.GlobalReactorState{})
	require.False(t, up.idleTime.IsZero())
	_ = m.AddUpstream(&model.UpstreamInfo{ID: uint64(3)})
	require.True(t, up.idleTime.IsZero())
}
