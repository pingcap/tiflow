package executor

import (
	"context"
	stdErrors "errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/srvdiscovery"
	"github.com/hanfei1991/microcosm/test/mock"
	"github.com/phayes/freeport"
	"github.com/pingcap/tiflow/dm/pkg/log"
	p2pImpl "github.com/pingcap/tiflow/pkg/p2p"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

func init() {
	err := log.InitLogger(&log.Config{Level: "warn"})
	if err != nil {
		panic(err)
	}
}

func TestStartTCPSrv(t *testing.T) {
	t.Parallel()

	cfg := NewConfig()
	port, err := freeport.GetFreePort()
	require.Nil(t, err)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	cfg.WorkerAddr = addr
	s := NewServer(cfg, nil)

	s.grpcSrv = grpc.NewServer()
	wg, ctx := errgroup.WithContext(context.Background())
	err = s.startTCPService(ctx, wg)
	require.Nil(t, err)

	testPprof(t, fmt.Sprintf("http://127.0.0.1:%d", port))
	s.Stop()
}

func testPprof(t *testing.T, addr string) {
	urls := []string{
		"/debug/pprof/",
		"/debug/pprof/cmdline",
		"/debug/pprof/symbol",
		// enable these two apis will make ut slow
		//"/debug/pprof/profile", http.MethodGet,
		//"/debug/pprof/trace", http.MethodGet,
		"/debug/pprof/threadcreate",
		"/debug/pprof/allocs",
		"/debug/pprof/block",
		"/debug/pprof/goroutine?debug=1",
		"/debug/pprof/mutex?debug=1",
	}
	for _, uri := range urls {
		resp, err := http.Get(addr + uri)
		require.Nil(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		_, err = ioutil.ReadAll(resp.Body)
		require.Nil(t, err)
	}
}

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
	s := &Server{
		info: &model.NodeInfo{
			Type: model.NodeTypeExecutor,
			ID:   "uuid-1",
		},
		p2pMsgRouter: router,
	}
	s.initDiscoveryRunner = func() error {
		s.discoveryRunner = runner
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := s.discoveryKeepalive(ctx)
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
