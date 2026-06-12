// Copyright 2019 PingCAP, Inc.
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

package election

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/utils/tempurl"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func TestSuite(t *testing.T) {
	suite.Run(t, new(testElectionSuite))
}

type testElectionSuite struct {
	suite.Suite

	etcd     *embed.Etcd
	endPoint string

	notifyBlockTime time.Duration
}

func (t *testElectionSuite) SetupTest() {
	t.Require().NoError(log.InitLogger(&log.Config{}))

	cfg := embed.NewConfig()
	cfg.Name = "election-test"
	cfg.Dir = t.T().TempDir()
	cfg.ZapLoggerBuilder = embed.NewZapCoreLoggerBuilder(log.L().Logger, log.L().Core(), log.Props().Syncer)
	cfg.Logger = "zap"
	err := cfg.Validate() // verify & trigger the builder
	t.Require().NoError(err)

	t.endPoint = tempurl.Alloc()
	url2, err := url.Parse(t.endPoint)
	t.Require().NoError(err)
	cfg.ListenClientUrls = []url.URL{*url2}
	cfg.AdvertiseClientUrls = cfg.ListenClientUrls

	url2, err = url.Parse(tempurl.Alloc())
	t.Require().NoError(err)
	cfg.ListenPeerUrls = []url.URL{*url2}
	cfg.AdvertisePeerUrls = cfg.ListenPeerUrls

	cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, url2)
	cfg.ClusterState = embed.ClusterStateFlagNew

	t.etcd, err = embed.StartEtcd(cfg)
	t.Require().NoError(err)
	select {
	case <-t.etcd.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		t.T().Fatal("start embed etcd timeout")
	}

	// some notify leader information is not handled, just reduce the block time and ignore them
	t.notifyBlockTime = 100 * time.Millisecond
}

func (t *testElectionSuite) TearDownTest() {
	t.etcd.Close()
}

func (t *testElectionSuite) testElection2After1(normalExit bool) {
	var (
		timeout    = 3 * time.Second
		sessionTTL = 60
		key        = "unit-test/election-2-after-1"
		ID1        = "member1"
		ID2        = "member2"
		ID3        = "member3"
		addr1      = "127.0.0.1:1"
		addr2      = "127.0.0.1:2"
		addr3      = "127.0.0.1:3"
	)
	cli, err := etcdutil.CreateClient([]string{t.endPoint}, nil)
	t.Require().NoError(err)
	defer cli.Close()
	ctx0, cancel0 := context.WithCancel(context.Background())
	defer cancel0()
	_, err = cli.Delete(ctx0, key, clientv3.WithPrefix())
	t.Require().NoError(err)

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	if !normalExit {
		t.Require().NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/pkg/election/mockCampaignLoopExitedAbnormally", `return()`))
		//nolint:errcheck
		defer failpoint.Disable("github.com/pingcap/tiflow/dm/pkg/election/mockCampaignLoopExitedAbnormally")
	}
	e1, err := NewElection(ctx1, cli, sessionTTL, key, ID1, addr1, t.notifyBlockTime)
	t.Require().NoError(err)
	defer e1.Close()

	// e1 should become the leader
	select {
	case leader := <-e1.LeaderNotify():
		t.Require().Equal(ID1, leader.ID)
	case <-time.After(timeout):
		t.T().Fatal("leader campaign timeout")
	}
	t.Require().True(e1.IsLeader())
	_, leaderID, leaderAddr, err := e1.LeaderInfo(ctx1)
	t.Require().NoError(err)
	t.Require().Equal(e1.ID(), leaderID)
	t.Require().Equal(addr1, leaderAddr)
	if !normalExit {
		t.Require().NoError(failpoint.Disable("github.com/pingcap/tiflow/dm/pkg/election/mockCampaignLoopExitedAbnormally"))
	}

	// start e2
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	e2, err := NewElection(ctx2, cli, sessionTTL, key, ID2, addr2, t.notifyBlockTime)
	t.Require().NoError(err)
	defer e2.Close()
	select {
	case leader := <-e2.leaderCh:
		t.Require().Equal(ID1, leader.ID)
	case <-time.After(timeout):
		t.T().Fatal("leader campaign timeout")
	}
	// but the leader should still be e1
	_, leaderID, leaderAddr, err = e2.LeaderInfo(ctx2)
	t.Require().NoError(err)
	t.Require().Equal(e1.ID(), leaderID)
	t.Require().Equal(addr1, leaderAddr)
	t.Require().False(e2.IsLeader())

	var wg sync.WaitGroup
	e1.Close() // stop the campaign for e1
	t.Require().False(e1.IsLeader())

	ctx3, cancel3 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel3()
	deleted, err := e2.ClearSessionIfNeeded(ctx3, ID1)
	t.Require().NoError(err)
	if normalExit {
		// for normally exited election, session has already been closed before
		t.Require().False(deleted)
	} else {
		// for abnormally exited election, session will be cleared here
		t.Require().True(deleted)
	}

	// e2 should become the leader
	select {
	case leader := <-e2.LeaderNotify():
		t.Require().Equal(ID2, leader.ID)
	case <-time.After(timeout):
		t.T().Fatal("leader campaign timeout")
	}
	t.Require().True(e2.IsLeader())
	_, leaderID, leaderAddr, err = e2.LeaderInfo(ctx2)
	t.Require().NoError(err)
	t.Require().Equal(e2.ID(), leaderID)
	t.Require().Equal(addr2, leaderAddr)

	// only e2's election info is left in etcd
	ctx4, cancel4 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel4()
	resp, err := cli.Get(ctx4, key, clientv3.WithPrefix())
	t.Require().NoError(err)
	t.Require().Len(resp.Kvs, 1)

	// if closing the client when campaigning, we should get an error
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case err2 := <-e2.ErrorNotify():
			t.Require().True(terror.ErrElectionCampaignFail.Equal(err2))
			// the old session is done, but we can't create a new one.
			t.Require().Error(err2)
			t.Require().Regexp(".*fail to campaign leader: create a new session.*", err2.Error())
		case <-time.After(timeout):
			t.T().Fatal("do not receive error for e2")
		}
	}()
	cli.Close() // close the client
	wg.Wait()

	// can not elect with closed client.
	ctx5, cancel5 := context.WithCancel(context.Background())
	defer cancel5()
	_, err = NewElection(ctx5, cli, sessionTTL, key, ID3, addr3, t.notifyBlockTime)
	t.Require().True(terror.ErrElectionCampaignFail.Equal(err))
	t.Require().Error(err)
	t.Require().Regexp(".*Message: fail to campaign leader: create the initial session, RawCause: context canceled.*", err.Error())
	cancel0()
}

func (t *testElectionSuite) TestElection2After1() {
	t.testElection2After1(true)
	t.testElection2After1(false)
}

func (t *testElectionSuite) TestElectionAlways1() {
	var (
		timeout    = 3 * time.Second
		sessionTTL = 60
		key        = "unit-test/election-always-1"
		ID1        = "member1"
		ID2        = "member2"
		addr1      = "127.0.0.1:1234"
		addr2      = "127.0.0.1:2345"
	)
	cli, err := etcdutil.CreateClient([]string{t.endPoint}, nil)
	t.Require().NoError(err)
	defer cli.Close()

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	e1, err := NewElection(ctx1, cli, sessionTTL, key, ID1, addr1, t.notifyBlockTime)
	t.Require().NoError(err)
	defer e1.Close()

	// e1 should become the leader
	select {
	case leader := <-e1.LeaderNotify():
		t.Require().Equal(ID1, leader.ID)
	case <-time.After(timeout):
		t.T().Fatal("leader campaign timeout")
	}
	t.Require().True(e1.IsLeader())
	_, leaderID, leaderAddr, err := e1.LeaderInfo(ctx1)
	t.Require().NoError(err)
	t.Require().Equal(e1.ID(), leaderID)
	t.Require().Equal(addr1, leaderAddr)

	// start e2
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	e2, err := NewElection(ctx2, cli, sessionTTL, key, ID2, addr2, t.notifyBlockTime)
	t.Require().NoError(err)
	defer e2.Close()
	time.Sleep(100 * time.Millisecond) // wait 100ms to start the campaign
	// but the leader should still be e1
	_, leaderID, leaderAddr, err = e2.LeaderInfo(ctx2)
	t.Require().NoError(err)
	t.Require().Equal(e1.ID(), leaderID)
	t.Require().Equal(addr1, leaderAddr)
	t.Require().False(e2.IsLeader())

	// cancel the campaign for e2, should get no errors
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case err2 := <-e2.ErrorNotify():
			t.T().Fatalf("cancel the campaign should not get an error, %v", err2)
		case <-time.After(timeout): // wait 3s
		}
	}()
	cancel2()
	wg.Wait()

	// e1 is still the leader
	t.Require().True(e1.IsLeader())
	_, leaderID, leaderAddr, err = e1.LeaderInfo(ctx1)
	t.Require().NoError(err)
	t.Require().Equal(e1.ID(), leaderID)
	t.Require().Equal(addr1, leaderAddr)
	t.Require().False(e2.IsLeader())
}

func (t *testElectionSuite) TestElectionEvictLeader() {
	var (
		timeout    = 3 * time.Second
		sessionTTL = 60
		key        = "unit-test/election-evict-leader"
		ID1        = "member1"
		ID2        = "member2"
		addr1      = "127.0.0.1:1234"
		addr2      = "127.0.0.1:2345"
	)
	cli, err := etcdutil.CreateClient([]string{t.endPoint}, nil)
	t.Require().NoError(err)
	defer cli.Close()

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	e1, err := NewElection(ctx1, cli, sessionTTL, key, ID1, addr1, t.notifyBlockTime)
	t.Require().NoError(err)
	defer e1.Close()

	// e1 should become the leader
	select {
	case leader := <-e1.LeaderNotify():
		t.Require().Equal(ID1, leader.ID)
	case <-time.After(timeout):
		t.T().Fatal("leader campaign timeout")
	}
	t.Require().True(e1.IsLeader())
	_, leaderID, leaderAddr, err := e1.LeaderInfo(ctx1)
	t.Require().NoError(err)
	t.Require().Equal(e1.ID(), leaderID)
	t.Require().Equal(addr1, leaderAddr)

	// start e2
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	e2, err := NewElection(ctx2, cli, sessionTTL, key, ID2, addr2, t.notifyBlockTime)
	t.Require().NoError(err)
	defer e2.Close()
	time.Sleep(100 * time.Millisecond) // wait 100ms to start the campaign
	// but the leader should still be e1
	_, leaderID, leaderAddr, err = e2.LeaderInfo(ctx2)
	t.Require().NoError(err)
	t.Require().Equal(e1.ID(), leaderID)
	t.Require().Equal(addr1, leaderAddr)
	t.Require().False(e2.IsLeader())

	// e1 evict leader, and e2 will be the leader
	e1.EvictLeader()
	utils.WaitSomething(8, 250*time.Millisecond, func() bool {
		_, leaderID, _, _ = e2.LeaderInfo(ctx2)
		return leaderID == e2.ID()
	})
	_, leaderID, leaderAddr, err = e2.LeaderInfo(ctx2)
	t.Require().NoError(err)
	t.Require().Equal(e2.ID(), leaderID)
	t.Require().Equal(addr2, leaderAddr)
	utils.WaitSomething(10, 10*time.Millisecond, func() bool {
		return e2.IsLeader()
	})

	// cancel evict of e1, and then evict e2, e1 will be the leader
	e1.CancelEvictLeader()
	e2.EvictLeader()
	utils.WaitSomething(8, 250*time.Millisecond, func() bool {
		_, leaderID, _, _ = e1.LeaderInfo(ctx1)
		return leaderID == e1.ID()
	})
	_, leaderID, leaderAddr, err = e1.LeaderInfo(ctx1)
	t.Require().NoError(err)
	t.Require().Equal(e1.ID(), leaderID)
	t.Require().Equal(addr1, leaderAddr)
	utils.WaitSomething(10, 10*time.Millisecond, func() bool {
		return e1.IsLeader()
	})
}

func (t *testElectionSuite) TestElectionDeleteKey() {
	var (
		timeout    = 3 * time.Second
		sessionTTL = 60
		key        = "unit-test/election-delete-key"
		ID         = "member"
		addr       = "127.0.0.1:1234"
	)
	cli, err := etcdutil.CreateClient([]string{t.endPoint}, nil)
	t.Require().NoError(err)
	defer cli.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e, err := NewElection(ctx, cli, sessionTTL, key, ID, addr, t.notifyBlockTime)
	t.Require().NoError(err)
	defer e.Close()

	// should become the leader
	select {
	case leader := <-e.LeaderNotify():
		t.Require().Equal(ID, leader.ID)
	case <-time.After(timeout):
		t.T().Fatal("leader campaign timeout")
	}
	t.Require().True(e.IsLeader())
	leaderKey, leaderID, leaderAddr, err := e.LeaderInfo(ctx)
	t.Require().NoError(err)
	t.Require().Equal(e.ID(), leaderID)
	t.Require().Equal(addr, leaderAddr)

	// the leader retired after deleted the key
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		select {
		case err2 := <-e.ErrorNotify():
			t.T().Fatalf("delete the leader key should not get an error, %v", err2)
		case leader := <-e.LeaderNotify():
			t.Require().Nil(leader)
		}
	}()
	_, err = cli.Delete(ctx, leaderKey)
	t.Require().NoError(err)
	wg.Wait()
}

func (t *testElectionSuite) TestElectionSucceedButReturnError() {
	var (
		timeout    = 5 * time.Second
		sessionTTL = 60
		key        = "unit-test/election-succeed-but-return-error"
		ID1        = "member1"
		ID2        = "member2"
		addr1      = "127.0.0.1:1"
		addr2      = "127.0.0.1:2"
	)
	cli, err := etcdutil.CreateClient([]string{t.endPoint}, nil)
	t.Require().NoError(err)
	defer cli.Close()

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	e1, err := NewElection(ctx1, cli, sessionTTL, key, ID1, addr1, t.notifyBlockTime)
	t.Require().NoError(err)
	defer e1.Close()

	// e1 should become the leader
	select {
	case leader := <-e1.LeaderNotify():
		t.Require().Equal(ID1, leader.ID)
	case <-time.After(timeout):
		t.T().Fatal("leader campaign timeout")
	}
	t.Require().True(e1.IsLeader())
	_, leaderID, leaderAddr, err := e1.LeaderInfo(ctx1)
	t.Require().NoError(err)
	t.Require().Equal(e1.ID(), leaderID)
	t.Require().Equal(addr1, leaderAddr)

	// start e2
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	e2, err := NewElection(ctx2, cli, sessionTTL, key, ID2, addr2, t.notifyBlockTime)
	t.Require().NoError(err)
	defer e2.Close()
	select {
	case leader := <-e2.leaderCh:
		t.Require().Equal(ID1, leader.ID)
	case <-time.After(timeout):
		t.T().Fatal("leader campaign timeout")
	}
	// but the leader should still be e1
	_, leaderID, leaderAddr, err = e2.LeaderInfo(ctx2)
	t.Require().NoError(err)
	t.Require().Equal(e1.ID(), leaderID)
	t.Require().Equal(addr1, leaderAddr)
	t.Require().False(e2.IsLeader())

	t.Require().NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/pkg/election/mockCapaignSucceedButReturnErr", `return()`))
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/pkg/election/mockCapaignSucceedButReturnErr")

	e1.Close() // stop the campaign for e1
	t.Require().False(e1.IsLeader())

	// e2 should become the leader
	select {
	case leader := <-e2.LeaderNotify():
		t.Require().Equal(ID2, leader.ID)
	case <-time.After(timeout):
		t.T().Fatal("leader campaign timeout")
	}

	// the leader retired after deleted the key
	select {
	case err2 := <-e2.ErrorNotify():
		t.T().Fatalf("delete the leader key should not get an error, %v", err2)
	case leader := <-e2.LeaderNotify():
		t.Require().Nil(leader)
	case <-time.After(timeout):
		t.T().Fatal("leader retire timeout")
	}
}
