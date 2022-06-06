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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/adapter"
	"github.com/pingcap/tiflow/engine/pkg/rpcutil"
	"github.com/pingcap/tiflow/engine/servermaster/cluster"
	"github.com/pingcap/tiflow/engine/test"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"
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
	preRPCHook := rpcutil.NewPreRPCHook[pb.MasterClient](
		s.id,
		&s.leader,
		s.masterCli,
		&s.leaderInitialized,
		s.rpcLogRL,
	)
	s.masterRPCHook = preRPCHook
	session, err := cluster.NewEtcdSession(ctx, client, s.member(), s.info, s.cfg.RPCTimeout, s.cfg.KeepAliveTTL)
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
	id := genServerMasterUUID(name)
	s := &Server{
		id:              id,
		cfg:             cfg,
		etcd:            etcd,
		etcdClient:      client,
		leaderServiceFn: mockLeaderService,
		info:            &model.NodeInfo{ID: model.DeployNodeID(name)},
	}
	preRPCHook := rpcutil.NewPreRPCHook[pb.MasterClient](
		s.id,
		&s.leader,
		s.masterCli,
		&s.leaderInitialized,
		s.rpcLogRL,
	)
	s.masterRPCHook = preRPCHook

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

	session, err := cluster.NewEtcdSession(ctx, client, s.member(), s.info, s.cfg.RPCTimeout, s.cfg.KeepAliveTTL)
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
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	serverCount := 3
	names := make([]string, 0, serverCount)
	for i := 0; i < serverCount; i++ {
		names = append(names, fmt.Sprintf("server-master-leader-loop-watch-leader-test-%d", i))
	}
	addrs, etcds, client, cleanFn := test.PrepareEtcdCluster(t, names)
	defer cleanFn()

	mockLeaderServiceFn := func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	}

	servers := make([]*Server, 0, serverCount)
	sessions := make([]cluster.Session, 0, serverCount)
	for i := range names {
		cfg := NewConfig()
		cfg.Etcd.Name = names[i]
		cfg.AdvertiseAddr = addrs[i]
		s := &Server{
			id:          genServerMasterUUID(names[i]),
			cfg:         cfg,
			etcd:        etcds[i],
			etcdClient:  client,
			info:        &model.NodeInfo{ID: model.DeployNodeID(names[i])},
			masterCli:   &rpcutil.LeaderClientWithLock[pb.MasterClient]{},
			resourceCli: &rpcutil.LeaderClientWithLock[pb.ResourceManagerClient]{},
		}
		preRPCHook := rpcutil.NewPreRPCHook[pb.MasterClient](
			s.id,
			&s.leader,
			s.masterCli,
			&s.leaderInitialized,
			s.rpcLogRL,
		)
		s.masterRPCHook = preRPCHook
		s.leaderServiceFn = mockLeaderServiceFn
		servers = append(servers, s)

		session, err := cluster.NewEtcdSession(ctx, client, s.member(), s.info,
			s.cfg.RPCTimeout, s.cfg.KeepAliveTTL)
		require.Nil(t, err)
		sessions = append(sessions, session)
	}

	leaderIndex := -1
	for i, server := range servers {
		if server.etcd.Server.Lead() == uint64(server.etcd.Server.ID()) {
			leaderIndex = i
			break
		}
	}
	require.LessOrEqual(t, 0, leaderIndex)
	require.LessOrEqual(t, leaderIndex, serverCount)

	var wg sync.WaitGroup
	wg.Add(serverCount)
	go func() {
		defer wg.Done()
		session := sessions[leaderIndex]
		err := session.Reset(ctx)
		require.Nil(t, err)
		leaderServer := servers[leaderIndex]
		err = leaderServer.leaderLoop(ctx)
		require.EqualError(t, err, context.Canceled.Error())
	}()
	for i := 0; i < serverCount; i++ {
		if i == leaderIndex {
			continue
		}
		s := servers[i]
		session := sessions[i]
		go func() {
			defer wg.Done()
			err := session.Reset(ctx)
			require.Nil(t, err)

			// check masterCli is not set
			leaderCli := s.masterCli.Get()
			require.Nil(t, leaderCli)

			err = s.leaderLoop(ctx)
			require.EqualError(t, err, context.Canceled.Error())
		}()
	}

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
		}, time.Second*2, time.Millisecond*20)

	}

	cancel()
	wg.Wait()
}

func TestCampaignMeetLeaseExpire(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	name := "server-master-campaign-meet-lease-expire"
	addr, etcd, client, cleanFn := test.PrepareEtcd(t, name)
	defer cleanFn()

	cfg := NewConfig()
	cfg.Etcd.Name = name
	cfg.AdvertiseAddr = addr
	id := genServerMasterUUID(name)
	s := &Server{
		id:         id,
		cfg:        cfg,
		etcd:       etcd,
		etcdClient: client,
		info:       &model.NodeInfo{ID: model.DeployNodeID(name)},
	}
	preRPCHook := rpcutil.NewPreRPCHook[pb.MasterClient](
		s.id,
		&s.leader,
		s.masterCli,
		&s.leaderInitialized,
		s.rpcLogRL,
	)
	s.masterRPCHook = preRPCHook

	session, err := cluster.NewEtcdSession(ctx, client, s.member(), s.info, s.cfg.RPCTimeout, s.cfg.KeepAliveTTL)
	require.Nil(t, err)
	err = session.Reset(ctx)
	require.Nil(t, err)
	_, _, err = session.Campaign(ctx, time.Second)
	require.Nil(t, err)

	// simulate server lease expire, campaign will fail
	// _, err = client.Revoke(ctx, session.(*cluster.EtcdSession).session.Lease())
	// require.Nil(t, err)
	// err = s.campaign(ctx, time.Second)
	// require.Regexp(t, ".*etcdserver: requested lease not found", err)

	// after handle campaign error, another try of campaign will success
	// err = s.handleCampaignError(ctx, err)
	// require.Nil(t, err)
	// err = s.campaign(ctx, time.Second)
	// require.Nil(t, err)
}
