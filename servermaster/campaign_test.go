package servermaster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/servermaster/cluster"
	"github.com/hanfei1991/microcosm/test"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.uber.org/atomic"
)

func init() {
	// initialized the logger to make genEmbedEtcdConfig working.
	err := log.InitLogger(&log.Config{})
	if err != nil {
		panic(err)
	}
}

// TestLeaderLoopSuccess tests a node starts LeaderLoop and campaigns to be
// leader successfully, and starts leaderServiceFn
func TestLeaderLoopSuccess(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	name := "server-master-leader-loop-test1"
	addr, etcd, client, cleanFn := test.PrepareEtcd(t, name)
	defer cleanFn()

	runLeaderCounter := atomic.NewInt32(0)
	mockLeaderService := func(ctx context.Context) error {
		runLeaderCounter.Add(1)
		<-ctx.Done()
		return ctx.Err()
	}

	// prepare server master
	cfg := NewConfig()
	cfg.Etcd.Name = name
	cfg.AdvertiseAddr = addr
	s := &Server{
		cfg:             cfg,
		etcd:            etcd,
		etcdClient:      client,
		leaderServiceFn: mockLeaderService,
		info:            &model.NodeInfo{ID: model.DeployNodeID(name)},
	}
	err := s.reset(ctx)
	require.Nil(t, err)

	// start to run leader loop
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := s.leaderLoop(ctx)
		require.EqualError(t, err, context.Canceled.Error())
	}()
	require.Eventually(t, func() bool {
		return runLeaderCounter.Load() == int32(1)
	}, time.Second*2, time.Millisecond*20)
	cancel()
	wg.Wait()
}

// TestLeaderLoopMeetStaleData tests a node meets stale owner data when
// campaining, it will cleanup stale data and retry to campaign again.
func TestLeaderLoopMeetStaleData(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	name := "server-master-leader-loop-stale-data-test1"
	addr, etcd, client, cleanFn := test.PrepareEtcd(t, name)
	defer cleanFn()

	runLeaderCounter := atomic.NewInt32(0)
	mockLeaderService := func(ctx context.Context) error {
		runLeaderCounter.Add(1)
		<-ctx.Done()
		return ctx.Err()
	}

	cfg := NewConfig()
	cfg.Etcd.Name = name
	cfg.AdvertiseAddr = addr
	s := &Server{
		cfg:             cfg,
		etcd:            etcd,
		etcdClient:      client,
		leaderServiceFn: mockLeaderService,
		info:            &model.NodeInfo{ID: model.DeployNodeID(name)},
	}

	// simulate stale campaign data
	sess, err := concurrency.NewSession(client, concurrency.WithTTL(10))
	require.Nil(t, err)
	election, err := cluster.NewEtcdElection(ctx, client, sess, cluster.EtcdElectionConfig{
		CreateSessionTimeout: time.Second * 3,
		TTL:                  time.Second * 10,
		Prefix:               adapter.MasterCampaignKey.Path(),
	})
	require.Nil(t, err)
	_, _, err = election.Campaign(ctx, s.member(), time.Second*3)
	require.Nil(t, err)

	err = s.reset(ctx)
	require.Nil(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := s.leaderLoop(ctx)
		require.EqualError(t, err, context.Canceled.Error())
	}()
	require.Eventually(t, func() bool {
		return runLeaderCounter.Load() == int32(1)
	}, time.Second*2, time.Millisecond*20)
	cancel()
	wg.Wait()
}

// TestLeaderLoopWatchLeader tests a non-leader node enters LeaderLoop, finds an
// existing leader can starts to watch leader.
func TestLeaderLoopWatchLeader(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	name := "server-master-leader-loop-watch-leader-test1"
	addr, etcd, client, cleanFn := test.PrepareEtcd(t, name)
	defer cleanFn()

	cfg := NewConfig()
	cfg.Etcd.Name = name
	cfg.AdvertiseAddr = addr
	s := &Server{
		cfg:        cfg,
		etcd:       etcd,
		etcdClient: client,
		info:       &model.NodeInfo{ID: model.DeployNodeID(name)},
	}

	// Simulate another node campaigns to be leader, note we use another node
	// name but hack to use the same gRPC address(etcd endpoint) to support
	// creating leader client connection from non-leader node.
	cfg2 := NewConfig()
	cfg2.Etcd.Name = name + "-node-copy"
	cfg2.AdvertiseAddr = addr
	s2 := &Server{
		cfg: cfg2,
	}
	sess, err := concurrency.NewSession(client, concurrency.WithTTL(10))
	require.Nil(t, err)
	election, err := cluster.NewEtcdElection(ctx, client, sess, cluster.EtcdElectionConfig{
		CreateSessionTimeout: time.Second * 3,
		TTL:                  time.Second * 10,
		Prefix:               adapter.MasterCampaignKey.Path(),
	})
	require.Nil(t, err)
	_, _, err = election.Campaign(ctx, s2.member(), time.Second*3)
	require.Nil(t, err)

	err = s.reset(ctx)
	require.Nil(t, err)
	s.leaderClient.RLock()
	leaderCli := s.leaderClient.cli
	s.leaderClient.RUnlock()
	require.Nil(t, leaderCli)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := s.leaderLoop(ctx)
		require.EqualError(t, err, context.Canceled.Error())
	}()

	// check s.watchLeader is called
	require.Eventually(t, func() bool {
		member, exists := s.checkLeader()
		if !exists {
			return false
		}
		if member.Name != s2.name() {
			return false
		}
		s.leaderClient.RLock()
		leaderCli := s.leaderClient.cli
		s.leaderClient.RUnlock()
		return leaderCli != nil
	}, time.Second*2, time.Millisecond*20)

	cancel()
	wg.Wait()
}
