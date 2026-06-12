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

package etcdutil

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/clientv3util"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/tests/v3/integration"
)

type testEtcdUtilSuite struct {
	suite.Suite
}

func (t *testEtcdUtilSuite) SetupSuite() {
	// initialized the logger to make genEmbedEtcdConfig working.
	t.Require().NoError(log.InitLogger(&log.Config{}))
	// this is to trigger  `etcd.io/etcd/embed.(*Config).setupLogging()`
	// otherwise `newConfig` will datarace with `TestDoOpsInOneTxnWithRetry.NewClusterV3.Launch.m.grpcServer.Serve(m.grpcListener)`
	t.newConfig("not used", 1)
}

func TestSuite(t *testing.T) {
	integration.BeforeTestExternal(t)
	suite.Run(t, new(testEtcdUtilSuite))
}

func (t *testEtcdUtilSuite) newConfig(name string, portCount int) *embed.Config {
	cfg := embed.NewConfig()
	cfg.Name = name
	cfg.Dir = t.T().TempDir()
	cfg.ZapLoggerBuilder = embed.NewZapCoreLoggerBuilder(log.L().Logger, log.L().Core(), log.Props().Syncer)
	cfg.Logger = "zap"
	t.Require().NoError(cfg.Validate())

	cfg.ListenClientUrls = []url.URL{}
	for i := 0; i < portCount; i++ {
		endPoint := tempurl.Alloc()
		cu, err2 := url.Parse(endPoint)
		t.Require().NoError(err2)
		cfg.ListenClientUrls = append(cfg.ListenClientUrls, *cu)
	}

	cfg.AdvertiseClientUrls = cfg.ListenClientUrls
	cfg.ListenPeerUrls = []url.URL{}
	ic := make([]string, 0, portCount)
	for i := 0; i < portCount; i++ {
		endPoint := tempurl.Alloc()
		pu, err2 := url.Parse(endPoint)
		t.Require().NoError(err2)
		cfg.ListenPeerUrls = append(cfg.ListenPeerUrls, *pu)
		ic = append(ic, fmt.Sprintf("%s=%s", cfg.Name, pu))
	}
	cfg.AdvertisePeerUrls = cfg.ListenPeerUrls
	cfg.InitialCluster = strings.Join(ic, ",")
	cfg.ClusterState = embed.ClusterStateFlagNew
	return cfg
}

func (t *testEtcdUtilSuite) urlsToStrings(urls []url.URL) []string {
	ret := make([]string, 0, len(urls))
	for _, u := range urls {
		ret = append(ret, u.String())
	}
	return ret
}

func (t *testEtcdUtilSuite) startEtcd(cfg *embed.Config) *embed.Etcd {
	e, err := embed.StartEtcd(cfg)
	t.Require().NoError(err)

	timeout := 10 * time.Second
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(timeout):
		e.Server.Stop()
		t.T().Fatalf("start embed etcd timeout %v", timeout)
	}
	return e
}

func (t *testEtcdUtilSuite) createEtcdClient(endpoints []string) *clientv3.Client {
	cli, err := CreateClient(endpoints, nil)
	t.Require().NoError(err)
	return cli
}

func (t *testEtcdUtilSuite) checkMember(mid uint64, m *etcdserverpb.Member, cfg *embed.Config) {
	if m.Name != "" { // no name exists after `member add`
		t.Require().Equal(cfg.Name, m.Name)
	}
	t.Require().Equal(mid, m.ID)
	require.ElementsMatch(t.T(), m.ClientURLs, t.urlsToStrings(cfg.AdvertiseClientUrls))
	require.ElementsMatch(t.T(), m.PeerURLs, t.urlsToStrings(cfg.AdvertisePeerUrls))
}

func (t *testEtcdUtilSuite) TestMemberUtil() {
	for i := 1; i <= 3; i++ {
		t.testMemberUtilInternal(i)
	}
}

func (t *testEtcdUtilSuite) testMemberUtilInternal(portCount int) {
	// start a etcd
	cfg1 := t.newConfig("etcd1", portCount)
	endpoints1 := t.urlsToStrings(cfg1.ListenClientUrls)
	etcd1 := t.startEtcd(cfg1)
	defer etcd1.Close()

	// list member
	cli := t.createEtcdClient(endpoints1)
	listResp1, err := ListMembers(cli)
	t.Require().NoError(err)
	t.Require().Len(listResp1.Members, 1)
	t.checkMember(uint64(etcd1.Server.ID()), listResp1.Members[0], cfg1)

	// add member
	cfg2 := t.newConfig("etcd2", portCount)
	cfg2.InitialCluster = cfg1.InitialCluster + "," + cfg2.InitialCluster
	cfg2.ClusterState = embed.ClusterStateFlagExisting
	addResp, err := AddMember(cli, t.urlsToStrings(cfg2.AdvertisePeerUrls))
	t.Require().NoError(err)
	t.Require().Len(addResp.Members, 2)

	// start the added member
	etcd2 := t.startEtcd(cfg2)
	defer etcd2.Close()
	t.Require().Equal(uint64(etcd2.Server.ID()), addResp.Member.ID)

	// list member again
	listResp2, err := ListMembers(cli)
	t.Require().NoError(err)
	t.Require().Len(listResp2.Members, 2)
	for _, m := range listResp2.Members {
		switch m.ID {
		case uint64(etcd1.Server.ID()):
			t.checkMember(uint64(etcd1.Server.ID()), m, cfg1)
		case uint64(etcd2.Server.ID()):
			t.checkMember(uint64(etcd2.Server.ID()), m, cfg2)
		default:
			t.T().Fatalf("unknown member %v", m)
		}
	}
}

func (t *testEtcdUtilSuite) TestRemoveMember() {
	// test remove one member that is not the one we connected to.
	// if we remove the one we connected to, the test might fail, see more in https://github.com/etcd-io/etcd/pull/7242
	cluster := integration.NewClusterV3(t.T(), &integration.ClusterConfig{Size: 3})
	defer cluster.Terminate(t.T())
	leaderIdx := cluster.WaitLeader(t.T())
	t.Require().NotEqual(-1, leaderIdx)
	cli := cluster.Client(leaderIdx)
	respList, err := ListMembers(cli)
	t.Require().NoError(err)
	t.Require().Len(respList.Members, 3)
	for _, m := range respList.Members {
		if m.ID != respList.Header.MemberId {
			respRemove, removeErr := RemoveMember(cli, m.ID)
			t.Require().NoError(removeErr)
			t.Require().Len(respRemove.Members, 2)
			break
		}
	}
	respList, err = ListMembers(cli)
	t.Require().NoError(err)
	t.Require().Len(respList.Members, 2)
}

func (t *testEtcdUtilSuite) TestDoOpsInOneTxnWithRetry() {
	var (
		key1 = "/test/etcdutil/do-ops-in-one-txn-with-retry-1"
		key2 = "/test/etcdutil/do-ops-in-one-txn-with-retry-2"
		val1 = "foo"
		val2 = "bar"
		val  = "foo-bar"
	)
	cluster := integration.NewClusterV3(t.T(), &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t.T())

	cli := cluster.RandClient()

	resp, rev1, err := DoTxnWithRepeatable(cli, ThenOpFunc(clientv3.OpPut(key1, val1), clientv3.OpPut(key2, val2)))
	t.Require().NoError(err)
	t.Require().Greater(rev1, int64(0))
	t.Require().Len(resp.Responses, 2)

	// both cmps are true
	cmp1 := clientv3.Compare(clientv3.Value(key1), "=", val1)
	cmp2 := clientv3.Compare(clientv3.Value(key2), "=", val2)
	resp, rev2, err := DoTxnWithRepeatable(cli, FullOpFunc([]clientv3.Cmp{cmp1, cmp2}, []clientv3.Op{
		clientv3.OpPut(key1, val), clientv3.OpPut(key2, val),
	}, []clientv3.Op{}))
	t.Require().NoError(err)
	t.Require().Greater(rev2, rev1)
	t.Require().Len(resp.Responses, 2)

	// one of cmps are false
	cmp1 = clientv3.Compare(clientv3.Value(key1), "=", val)
	cmp2 = clientv3.Compare(clientv3.Value(key2), "=", val2)
	resp, rev3, err := DoTxnWithRepeatable(cli, FullOpFunc([]clientv3.Cmp{cmp1, cmp2}, []clientv3.Op{}, []clientv3.Op{
		clientv3.OpDelete(key1), clientv3.OpDelete(key2),
	}))
	t.Require().NoError(err)
	t.Require().Greater(rev3, rev2)
	t.Require().Len(resp.Responses, 2)

	// enable failpoint
	t.Require().NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/pkg/etcdutil/ErrNoSpace", `3*return()`))
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/pkg/etcdutil/ErrNoSpace")

	// put again
	resp, rev2, err = DoTxnWithRepeatable(cli, FullOpFunc([]clientv3.Cmp{clientv3util.KeyMissing(key1), clientv3util.KeyMissing(key2)}, []clientv3.Op{
		clientv3.OpPut(key1, val), clientv3.OpPut(key2, val),
	}, []clientv3.Op{}))
	t.Require().NoError(err)
	t.Require().Greater(rev2, rev1)
	t.Require().Len(resp.Responses, 2)
}

func (t *testEtcdUtilSuite) TestIsRetryableError() {
	t.Require().True(IsRetryableError(v3rpc.ErrCompacted))
	t.Require().True(IsRetryableError(v3rpc.ErrNoLeader))
	t.Require().True(IsRetryableError(v3rpc.ErrNoSpace))

	t.Require().False(IsRetryableError(v3rpc.ErrCorrupt))
	t.Require().False(IsRetryableError(terror.ErrDecodeEtcdKeyFail))
	t.Require().False(IsRetryableError(nil))
}
