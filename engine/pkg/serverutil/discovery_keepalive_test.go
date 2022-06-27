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

package serverutil

import (
	"context"
	stdErrors "errors"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/srvdiscovery"
	"github.com/pingcap/tiflow/engine/test/mock"
	p2pImpl "github.com/pingcap/tiflow/pkg/p2p"
	"github.com/stretchr/testify/require"
)

type mockMetaStoreSession struct {
	doneCh chan struct{}
}

func (ms *mockMetaStoreSession) Done() <-chan struct{} {
	return ms.doneCh
}

func (ms *mockMetaStoreSession) Close() error {
	return nil
}

type mockMessageRouter struct {
	mu    sync.RWMutex
	peers map[string]string
}

func (mr *mockMessageRouter) AddPeer(id p2pImpl.NodeID, addr string) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	mr.peers[id] = addr
}

func (mr *mockMessageRouter) RemovePeer(id p2pImpl.NodeID) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	delete(mr.peers, id)
}

func (mr *mockMessageRouter) GetPeers() map[string]string {
	mr.mu.RLock()
	defer mr.mu.RUnlock()
	peers := make(map[string]string, len(mr.peers))
	for k, v := range mr.peers {
		peers[k] = v
	}
	return peers
}

func (mr *mockMessageRouter) GetClient(target p2pImpl.NodeID) *p2pImpl.MessageClient {
	panic("not implemented")
}

func (mr *mockMessageRouter) Close()            { panic("not implemented ") }
func (mr *mockMessageRouter) Wait()             { panic("not implemented ") }
func (mr *mockMessageRouter) Err() <-chan error { panic("not implemented ") }

func TestDiscoveryKeepalive(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctrl := gomock.NewController(t)
	runner := mock.NewMockDiscoveryRunner(ctrl)
	doneCh := make(chan struct{}, 1)
	watchResp := make(chan srvdiscovery.WatchResp, 1)

	snapshot := map[srvdiscovery.UUID]srvdiscovery.ServiceResource{
		"uuid-1": {Addr: "127.0.0.1:10001"},
		"uuid-2": {Addr: "127.0.0.1:10002"},
	}

	// first initialization, will return new session
	runner.EXPECT().ResetDiscovery(ctx, true).Return(&mockMetaStoreSession{doneCh: doneCh}, nil)
	runner.EXPECT().GetSnapshot().Return(snapshot)

	// discovery watch returns an error, the discovery will be reset, but session keeps alive
	runner.EXPECT().ResetDiscovery(ctx, false).Return(nil, nil)

	// discovery session is done, reset discovery and session
	runner.EXPECT().ResetDiscovery(ctx, true).Return(&mockMetaStoreSession{doneCh: doneCh}, nil)

	runner.EXPECT().GetWatcher().Return(watchResp).AnyTimes()

	router := &mockMessageRouter{peers: map[p2pImpl.NodeID]string{}}
	info := &model.NodeInfo{
		Type: model.NodeTypeExecutor,
		ID:   "uuid-1",
	}
	keeper := NewDiscoveryKeepaliver(info, nil, 10, time.Millisecond*10, router)
	keeper.initDiscoveryRunner = func() error {
		keeper.discoveryRunner = runner
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := keeper.Keepalive(ctx)
		require.EqualError(t, err, context.Canceled.Error())
	}()

	var peers map[string]string
	// check snapshot can be load when discovery keepalive routine starts for the first time
	require.Eventually(t, func() bool {
		peers = router.GetPeers()
		return len(peers) == 2
	}, time.Second, time.Millisecond*20)
	require.Contains(t, peers, "uuid-1")
	require.Contains(t, peers, "uuid-2")

	// check discovery watch can work as expected
	nodeUpdate := srvdiscovery.WatchResp{
		AddSet: map[srvdiscovery.UUID]srvdiscovery.ServiceResource{
			"uuid-3": {Addr: "127.0.0.1:10003"},
			"uuid-4": {Addr: "127.0.0.1:10004"},
		},
		DelSet: map[srvdiscovery.UUID]srvdiscovery.ServiceResource{
			"uuid-2": {Addr: "127.0.0.1:10002"},
		},
	}
	runner.EXPECT().ApplyWatchResult(nodeUpdate)
	watchResp <- nodeUpdate
	require.Eventually(t, func() bool {
		peers = router.GetPeers()
		return len(peers) == 3
	}, time.Second, time.Millisecond*20)
	require.Contains(t, peers, "uuid-1")
	require.Contains(t, peers, "uuid-3")
	require.Contains(t, peers, "uuid-4")

	// check will reconnect to discovery metastore when watch meets error
	watchResp <- srvdiscovery.WatchResp{Err: stdErrors.New("mock discovery watch error")}

	// check will reconnect to discovery metastore when metastore session is done
	doneCh <- struct{}{}

	// check the watch channel can be reset after error happens
	nodeUpdate = srvdiscovery.WatchResp{
		AddSet: map[srvdiscovery.UUID]srvdiscovery.ServiceResource{
			"uuid-2": {Addr: "127.0.0.1:10002"},
		},
	}
	runner.EXPECT().ApplyWatchResult(nodeUpdate)
	watchResp <- nodeUpdate
	require.Eventually(t, func() bool {
		peers = router.GetPeers()
		return len(peers) == 4
	}, time.Second, time.Millisecond*20)
	require.Contains(t, peers, "uuid-1")
	require.Contains(t, peers, "uuid-2")
	require.Contains(t, peers, "uuid-3")
	require.Contains(t, peers, "uuid-4")

	cancel()
	wg.Wait()
}
