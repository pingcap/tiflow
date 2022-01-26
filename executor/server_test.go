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
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
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
	disc := mock.NewMockDiscovery(ctrl)

	snapshot := map[srvdiscovery.UUID]srvdiscovery.ServiceResource{
		"uuid-1": {Addr: "127.0.0.1:10001"},
		"uuid-2": {Addr: "127.0.0.1:10002"},
	}
	watchResp := make(chan srvdiscovery.WatchResp, 1)
	watchRespReset := make(chan srvdiscovery.WatchResp, 1)
	disc.EXPECT().Snapshot(ctx).Return(snapshot, nil)
	disc.EXPECT().Watch(ctx).Return(watchResp)
	disc.EXPECT().Watch(ctx).Return(watchRespReset).AnyTimes()

	router := &mockMessageRouter{peers: map[p2pImpl.NodeID]string{}}
	s := &Server{
		info: &model.ExecutorInfo{
			ID: "uuid-1",
		},
		p2pMsgRouter: router,
	}
	var doneCh chan struct{}
	discoveryConnectTime := atomic.NewInt64(0)
	s.discoveryConnector = func(ctx context.Context) (metaStoreSession, error) {
		doneCh = make(chan struct{}, 1)
		mockSession := &mockMetaStoreSession{doneCh: doneCh}
		s.discovery = disc
		discoveryConnectTime.Add(1)
		return mockSession, nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := s.discoveryKeepalive(ctx)
		require.EqualError(t, err, context.Canceled.Error())
	}()

	// check snapshot can be load when discovery keepalive routine starts for the first time
	time.Sleep(time.Millisecond * 50)
	peers := router.GetPeers()
	require.Equal(t, 2, len(peers))
	require.Contains(t, peers, "uuid-1")
	require.Contains(t, peers, "uuid-2")
	require.Equal(t, int64(1), discoveryConnectTime.Load())

	// check discovery watch can work as expected
	watchResp <- srvdiscovery.WatchResp{
		AddSet: map[srvdiscovery.UUID]srvdiscovery.ServiceResource{
			"uuid-3": {Addr: "127.0.0.1:10003"},
			"uuid-4": {Addr: "127.0.0.1:10004"},
		},
		DelSet: map[srvdiscovery.UUID]srvdiscovery.ServiceResource{
			"uuid-2": {Addr: "127.0.0.1:10002"},
		},
	}
	require.Eventually(t, func() bool {
		peers = router.GetPeers()
		return len(peers) == 3
	}, time.Second, time.Millisecond*20)
	require.Contains(t, peers, "uuid-1")
	require.Contains(t, peers, "uuid-3")
	require.Contains(t, peers, "uuid-4")

	// check will reconnect to discovery metastore when watch meets error
	watchResp <- srvdiscovery.WatchResp{Err: stdErrors.New("mock discovery watch error")}
	time.Sleep(time.Millisecond * 50)
	require.Equal(t, int64(2), discoveryConnectTime.Load())

	// check will reconnect to discovery metastore when metastore session is done
	doneCh <- struct{}{}
	require.Eventually(t, func() bool {
		return discoveryConnectTime.Load() == int64(3)
	}, time.Second, time.Millisecond*20)

	// check the watch channel can be reset after error happens
	watchRespReset <- srvdiscovery.WatchResp{
		AddSet: map[srvdiscovery.UUID]srvdiscovery.ServiceResource{
			"uuid-2": {Addr: "127.0.0.1:10002"},
		},
	}
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
