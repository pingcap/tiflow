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

package servermaster

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/atomic"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/adapter"
	"github.com/pingcap/tiflow/engine/pkg/rpcutil"
	"github.com/pingcap/tiflow/engine/servermaster/cluster"
	"github.com/pingcap/tiflow/engine/test"
	"github.com/pingcap/tiflow/engine/test/mock"
	"github.com/pingcap/tiflow/pkg/logutil"
)

func init() {
	// initialized the logger to make genEmbedEtcdConfig working.
	err := logutil.InitLogger(&logutil.Config{})
	if err != nil {
		panic(err)
	}
}

// TestLeaderLoopSuccess tests a node starts LeaderLoop and campaigns to be
// leader successfully, and starts leaderServiceFn
func TestLeaderLoopSuccess(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	_, _, client, cleanFn := test.PrepareEtcd(t, "etcd0")
	defer cleanFn()

	runLeaderCounter := atomic.NewInt32(0)
	mockLeaderService := func(ctx context.Context) error {
		runLeaderCounter.Add(1)
		<-ctx.Done()
		return ctx.Err()
	}

	id := "servermaster0"
	// prepare server master
	cfg := GetDefaultMasterConfig()
	s := &Server{
		id:              id,
		cfg:             cfg,
		etcdClient:      client,
		leaderServiceFn: mockLeaderService,
		info:            &model.NodeInfo{ID: model.DeployNodeID(id)},
	}
	preRPCHook := rpcutil.NewPreRPCHook[pb.MasterClient](
		s.id,
		&s.leader,
		s.masterCli,
		&s.leaderInitialized,
		s.rpcLogRL,
		nil,
	)
	s.masterRPCHook = preRPCHook
	sessionCfg, err := s.generateSessionConfig()
	require.Nil(t, err)
	session, err := cluster.NewEtcdSession(ctx, client, sessionCfg)
	require.Nil(t, err)
	err = session.Reset(ctx)
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
	_, _, client, cleanFn := test.PrepareEtcd(t, "etcd0")
	defer cleanFn()

	runLeaderCounter := atomic.NewInt32(0)
	mockLeaderService := func(ctx context.Context) error {
		runLeaderCounter.Add(1)
		<-ctx.Done()
		return ctx.Err()
	}

	cfg := GetDefaultMasterConfig()
	id := "servermaster0"
	s := &Server{
		id:              id,
		cfg:             cfg,
		etcdClient:      client,
		leaderServiceFn: mockLeaderService,
		info:            &model.NodeInfo{ID: model.DeployNodeID(id)},
	}
	preRPCHook := rpcutil.NewPreRPCHook[pb.MasterClient](
		s.id,
		&s.leader,
		s.masterCli,
		&s.leaderInitialized,
		s.rpcLogRL,
		nil,
	)
	s.masterRPCHook = preRPCHook

	// simulate stale campaign data
	sess, err := concurrency.NewSession(client, concurrency.WithTTL(10))
	require.Nil(t, err)
	election, err := cluster.NewEtcdElection(ctx, client, sess, cluster.EtcdElectionConfig{
		TTL:    time.Second * 10,
		Prefix: adapter.MasterCampaignKey.Path(),
	})
	require.Nil(t, err)
	_, _, err = election.Campaign(ctx, s.member(), time.Second*3)
	require.Nil(t, err)

	sessionCfg, err := s.generateSessionConfig()
	require.Nil(t, err)
	session, err := cluster.NewEtcdSession(ctx, client, sessionCfg)
	require.Nil(t, err)
	err = session.Reset(ctx)
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
// existing leader can start to watch leader.
func TestLeaderLoopWatchLeader(t *testing.T) {
	test.SetGlobalTestFlag(true)
	defer test.SetGlobalTestFlag(false)

	ctx, cancel := context.WithCancel(context.Background())

	_, _, client, cleanFn := test.PrepareEtcd(t, "etcd0")
	defer cleanFn()

	const serverCount = 3
	servers := make([]*Server, 0, serverCount)
	sessions := make([]cluster.Session, 0, serverCount)
	for i := 0; i < serverCount; i++ {
		id := "servermaster" + strconv.Itoa(i)
		cfg := GetDefaultMasterConfig()
		cfg.AdvertiseAddr = "servermaster" + strconv.Itoa(i)

		s := &Server{
			id:          id,
			cfg:         cfg,
			etcdClient:  client,
			info:        &model.NodeInfo{ID: model.DeployNodeID(id)},
			masterCli:   &rpcutil.LeaderClientWithLock[pb.MasterClient]{},
			resourceCli: &rpcutil.LeaderClientWithLock[pb.ResourceManagerClient]{},
		}
		preRPCHook := rpcutil.NewPreRPCHook[pb.MasterClient](
			s.id,
			&s.leader,
			s.masterCli,
			&s.leaderInitialized,
			s.rpcLogRL,
			nil,
		)
		s.masterRPCHook = preRPCHook
		s.leaderServiceFn = func(ctx context.Context) error {
			s.leader.Store(&rpcutil.Member{
				Name:          s.name(),
				AdvertiseAddr: s.cfg.AdvertiseAddr,
				IsLeader:      true,
			})
			<-ctx.Done()
			return nil
		}

		_, err := mock.NewMasterServer(cfg.AdvertiseAddr, s)
		require.NoError(t, err)

		servers = append(servers, s)

		sessionCfg, err := s.generateSessionConfig()
		require.Nil(t, err)
		session, err := cluster.NewEtcdSession(ctx, client, sessionCfg)
		require.Nil(t, err)
		sessions = append(sessions, session)
	}

	var wg sync.WaitGroup
	wg.Add(serverCount)
	for i := 0; i < serverCount; i++ {
		s := servers[i]
		session := sessions[i]
		go func() {
			defer wg.Done()
			err := session.Reset(ctx)
			require.Nil(t, err)

			err = s.leaderLoop(ctx)
			require.EqualError(t, err, context.Canceled.Error())
		}()
	}

	leaderIndex := -1

	require.Eventually(t, func() bool {
		leaderIndex = -1
		for i, server := range servers {
			if server.IsLeader() {
				leaderIndex = i
				return true
			}
		}
		return false
	}, time.Second*10, time.Millisecond*100, "failed to elect a leader")

	// check s.watchLeader is called in non-leader node
	for i := 0; i < serverCount; i++ {
		if i == leaderIndex {
			continue
		}
		s := servers[i]
		require.Eventually(t, func() bool {
			member, exists := s.masterRPCHook.CheckLeader()
			if !exists {
				return false
			}
			if member.Name != servers[leaderIndex].name() {
				return false
			}
			return s.masterCli.Get() != nil
		}, defaultCampaignTimeout*2, time.Millisecond*100)

	}

	cancel()
	wg.Wait()
}
