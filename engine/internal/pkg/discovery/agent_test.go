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

package discovery

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/srvdiscovery"
	"github.com/pingcap/tiflow/engine/test/mock"
	tmock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type agentTestHelper struct {
	agent           *agentImpl
	discoveryRunner *mock.MockDiscoveryRunner
	session         *mock.MockSession
	watchCh         chan srvdiscovery.WatchResp
	sessionDoneCh   chan struct{}
}

type mockRunnerFactory struct {
	tmock.Mock
}

func (f *mockRunnerFactory) Build(
	etcdCli *clientv3.Client,
	sessionTTL int,
	watchDur time.Duration,
	key string,
	value string,
) srvdiscovery.DiscoveryRunner {
	args := f.Called(etcdCli, sessionTTL, watchDur, key, value)
	return args.Get(0).(srvdiscovery.DiscoveryRunner)
}

func newAgentTestHelper(t *testing.T, selfInfo *model.NodeInfo) *agentTestHelper {
	runner := mock.NewMockDiscoveryRunner(gomock.NewController(t))
	session := mock.NewMockSession(gomock.NewController(t))
	watchCh := make(chan srvdiscovery.WatchResp, 1)
	sessionDoneCh := make(chan struct{})
	session.EXPECT().Done().AnyTimes().Return(sessionDoneCh)
	runner.
		EXPECT().
		ResetDiscovery(gomock.Any(), gomock.Eq(true)).
		Times(1).
		Return(session, nil)
	runner.EXPECT().
		GetWatcher().
		AnyTimes().
		Return(watchCh)

	factory := &mockRunnerFactory{}

	selfInfoStr, err := selfInfo.ToJSON()
	require.NoError(t, err)

	factory.On("Build", (*clientv3.Client)(nil), 5, 1*time.Second, selfInfo.EtcdKey(), selfInfoStr).
		Return(runner).Times(1)
	agent := NewAgent(selfInfo, nil, 5, 1*time.Second)
	agent.(*agentImpl).discoveryRunnerFactory = factory
	return &agentTestHelper{
		agent:           agent.(*agentImpl),
		discoveryRunner: runner,
		session:         session,
		watchCh:         watchCh,
		sessionDoneCh:   sessionDoneCh,
	}
}

func (h *agentTestHelper) ResetSession(t *testing.T) {
	newSession := mock.NewMockSession(gomock.NewController(t))
	newSessionDoneCh := make(chan struct{})
	newSession.EXPECT().Done().AnyTimes().Return(newSessionDoneCh)
	h.discoveryRunner.
		EXPECT().
		ResetDiscovery(gomock.Any(), gomock.Eq(true)).
		Times(1).
		Return(newSession, nil)
	close(h.sessionDoneCh)
	h.sessionDoneCh = newSessionDoneCh
	h.session = newSession
}

func TestAgent(t *testing.T) {
	t.Parallel()

	selfInfo := &model.NodeInfo{
		Type: model.NodeTypeExecutor,
		ID:   "executor-1",
		Addr: "dummy:2222",
	}
	helper := newAgentTestHelper(t, selfInfo)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := helper.agent.Run(ctx)
		require.ErrorIs(t, err, context.Canceled)
	}()

	expectedSnap := srvdiscovery.Snapshot{
		"executor-2": srvdiscovery.ServiceResource{
			Type: model.NodeTypeExecutor,
			ID:   "executor-2",
			Addr: "dummy-2:2222",
		},
		"executor-3": srvdiscovery.ServiceResource{
			Type: model.NodeTypeExecutor,
			ID:   "executor-3",
			Addr: "dummy-3:2222",
		},
	}
	helper.discoveryRunner.EXPECT().GetSnapshot().Times(1).Return(expectedSnap)

	snap, receiver, err := helper.agent.Subscribe(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedSnap, snap)

	select {
	case <-receiver.C:
		require.Fail(t, "unreachable")
	default:
	}
	resp := srvdiscovery.WatchResp{
		AddSet: map[srvdiscovery.UUID]srvdiscovery.ServiceResource{
			"executor-4": {
				Type: model.NodeTypeExecutor,
				ID:   "executor-4",
				Addr: "dummy-4:2222",
			},
		},
		DelSet: map[srvdiscovery.UUID]srvdiscovery.ServiceResource{
			"executor-3": {
				Type: model.NodeTypeExecutor,
				ID:   "executor-3",
				Addr: "dummy-3:2222",
			},
		},
	}
	helper.discoveryRunner.EXPECT().ApplyWatchResult(resp).Times(1)
	helper.watchCh <- resp

	select {
	case <-ctx.Done():
		require.Failf(t, "unreachable", "%s", ctx.Err().Error())
	case event := <-receiver.C:
		require.Equal(t, Event{
			Tp: EventTypeDel,
			Info: model.NodeInfo{
				Type: model.NodeTypeExecutor,
				ID:   "executor-3",
				Addr: "dummy-3:2222",
			},
		}, event)
	}

	select {
	case <-ctx.Done():
		require.Failf(t, "unreachable", "%s", ctx.Err().Error())
	case event := <-receiver.C:
		require.Equal(t, Event{
			Tp: EventTypeAdd,
			Info: model.NodeInfo{
				Type: model.NodeTypeExecutor,
				ID:   "executor-4",
				Addr: "dummy-4:2222",
			},
		}, event)
	}

	helper.session.EXPECT().Close().Times(1)
	cancel()
	wg.Wait()
}

func TestAgentSessionDone(t *testing.T) {
	t.Parallel()

	selfInfo := &model.NodeInfo{
		Type: model.NodeTypeExecutor,
		ID:   "executor-1",
		Addr: "dummy:2222",
	}
	helper := newAgentTestHelper(t, selfInfo)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := helper.agent.Run(ctx)
		require.ErrorIs(t, err, context.Canceled)
	}()

	for i := 0; i < 10; i++ {
		helper.ResetSession(t)
		time.Sleep(100 * time.Millisecond)
	}

	helper.session.EXPECT().Close().Times(1)
	cancel()
	wg.Wait()
}

func TestAgentRunnerError(t *testing.T) {
	t.Parallel()

	selfInfo := &model.NodeInfo{
		Type: model.NodeTypeExecutor,
		ID:   "executor-1",
		Addr: "dummy:2222",
	}
	helper := newAgentTestHelper(t, selfInfo)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := helper.agent.Run(ctx)
		require.ErrorContains(t, err, "runner error")
	}()

	helper.session.EXPECT().Close().Times(1)
	helper.watchCh <- srvdiscovery.WatchResp{Err: errors.New("runner error")}
	wg.Wait()
}
