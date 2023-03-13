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

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/clientv3util"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/tests/v3/integration"
)

var etcdTestSuite = Suite(&testEtcdUtilSuite{})

type testEtcdUtilSuite struct {
	testT *testing.T
}

func (t *testEtcdUtilSuite) SetUpSuite(c *C) {
	// initialized the logger to make genEmbedEtcdConfig working.
	c.Assert(log.InitLogger(&log.Config{}), IsNil)
	// this is to trigger  `etcd.io/etcd/embed.(*Config).setupLogging()`
	// otherwise `newConfig` will datarace with `TestDoOpsInOneTxnWithRetry.NewClusterV3.Launch.m.grpcServer.Serve(m.grpcListener)`
	t.newConfig(c, "not used", 1)
}

func TestSuite(t *testing.T) {
	integration.BeforeTestExternal(t)
	// inject *testing.T to suite
	s := etcdTestSuite.(*testEtcdUtilSuite)
	s.testT = t
	TestingT(t)
}

func (t *testEtcdUtilSuite) newConfig(c *C, name string, portCount int) *embed.Config {
	cfg := embed.NewConfig()
	cfg.Name = name
	cfg.Dir = c.MkDir()
	cfg.ZapLoggerBuilder = embed.NewZapCoreLoggerBuilder(log.L().Logger, log.L().Core(), log.Props().Syncer)
	cfg.Logger = "zap"
	c.Assert(cfg.Validate(), IsNil)

	cfg.LCUrls = []url.URL{}
	for i := 0; i < portCount; i++ {
		endPoint := tempurl.Alloc()
		cu, err2 := url.Parse(endPoint)
		c.Assert(err2, IsNil)
		cfg.LCUrls = append(cfg.LCUrls, *cu)
	}

	cfg.ACUrls = cfg.LCUrls
	cfg.LPUrls = []url.URL{}
	ic := make([]string, 0, portCount)
	for i := 0; i < portCount; i++ {
		endPoint := tempurl.Alloc()
		pu, err2 := url.Parse(endPoint)
		c.Assert(err2, IsNil)
		cfg.LPUrls = append(cfg.LPUrls, *pu)
		ic = append(ic, fmt.Sprintf("%s=%s", cfg.Name, pu))
	}
	cfg.APUrls = cfg.LPUrls
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

func (t *testEtcdUtilSuite) startEtcd(c *C, cfg *embed.Config) *embed.Etcd {
	e, err := embed.StartEtcd(cfg)
	c.Assert(err, IsNil)

	timeout := 10 * time.Second
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(timeout):
		e.Server.Stop()
		c.Fatalf("start embed etcd timeout %v", timeout)
	}
	return e
}

func (t *testEtcdUtilSuite) createEtcdClient(c *C, endpoints []string) *clientv3.Client {
	cli, err := CreateClient(endpoints, nil)
	c.Assert(err, IsNil)
	return cli
}

func (t *testEtcdUtilSuite) checkMember(c *C, mid uint64, m *etcdserverpb.Member, cfg *embed.Config) {
	if m.Name != "" { // no name exists after `member add`
		c.Assert(m.Name, Equals, cfg.Name)
	}
	c.Assert(m.ID, Equals, mid)
	require.ElementsMatch(t.testT, m.ClientURLs, t.urlsToStrings(cfg.ACUrls))
	require.ElementsMatch(t.testT, m.PeerURLs, t.urlsToStrings(cfg.APUrls))
}

func (t *testEtcdUtilSuite) TestMemberUtil(c *C) {
	for i := 1; i <= 3; i++ {
		t.testMemberUtilInternal(c, i)
	}
}

func (t *testEtcdUtilSuite) testMemberUtilInternal(c *C, portCount int) {
	// start a etcd
	cfg1 := t.newConfig(c, "etcd1", portCount)
	endpoints1 := t.urlsToStrings(cfg1.LCUrls)
	etcd1 := t.startEtcd(c, cfg1)
	defer etcd1.Close()

	// list member
	cli := t.createEtcdClient(c, endpoints1)
	listResp1, err := ListMembers(cli)
	c.Assert(err, IsNil)
	c.Assert(listResp1.Members, HasLen, 1)
	t.checkMember(c, uint64(etcd1.Server.ID()), listResp1.Members[0], cfg1)

	// add member
	cfg2 := t.newConfig(c, "etcd2", portCount)
	cfg2.InitialCluster = cfg1.InitialCluster + "," + cfg2.InitialCluster
	cfg2.ClusterState = embed.ClusterStateFlagExisting
	addResp, err := AddMember(cli, t.urlsToStrings(cfg2.APUrls))
	c.Assert(err, IsNil)
	c.Assert(addResp.Members, HasLen, 2)

	// start the added member
	etcd2 := t.startEtcd(c, cfg2)
	defer etcd2.Close()
	c.Assert(addResp.Member.ID, Equals, uint64(etcd2.Server.ID()))

	// list member again
	listResp2, err := ListMembers(cli)
	c.Assert(err, IsNil)
	c.Assert(listResp2.Members, HasLen, 2)
	for _, m := range listResp2.Members {
		switch m.ID {
		case uint64(etcd1.Server.ID()):
			t.checkMember(c, uint64(etcd1.Server.ID()), m, cfg1)
		case uint64(etcd2.Server.ID()):
			t.checkMember(c, uint64(etcd2.Server.ID()), m, cfg2)
		default:
			c.Fatalf("unknown member %v", m)
		}
	}
}

func (t *testEtcdUtilSuite) TestRemoveMember(c *C) {
	// test remove one member that is not the one we connected to.
	// if we remove the one we connected to, the test might fail, see more in https://github.com/etcd-io/etcd/pull/7242
	cluster := integration.NewClusterV3(t.testT, &integration.ClusterConfig{Size: 3})
	defer cluster.Terminate(t.testT)
	leaderIdx := cluster.WaitLeader(t.testT)
	c.Assert(leaderIdx, Not(Equals), -1)
	cli := cluster.Client(leaderIdx)
	respList, err := ListMembers(cli)
	c.Assert(err, IsNil)
	c.Assert(respList.Members, HasLen, 3)
	for _, m := range respList.Members {
		if m.ID != respList.Header.MemberId {
			respRemove, removeErr := RemoveMember(cli, m.ID)
			c.Assert(removeErr, IsNil)
			c.Assert(respRemove.Members, HasLen, 2)
			break
		}
	}
	respList, err = ListMembers(cli)
	c.Assert(err, IsNil)
	c.Assert(respList.Members, HasLen, 2)
}

func (t *testEtcdUtilSuite) TestDoOpsInOneTxnWithRetry(c *C) {
	var (
		key1 = "/test/etcdutil/do-ops-in-one-txn-with-retry-1"
		key2 = "/test/etcdutil/do-ops-in-one-txn-with-retry-2"
		val1 = "foo"
		val2 = "bar"
		val  = "foo-bar"
	)
	cluster := integration.NewClusterV3(t.testT, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t.testT)

	cli := cluster.RandClient()

	resp, rev1, err := DoTxnWithRepeatable(cli, ThenOpFunc(clientv3.OpPut(key1, val1), clientv3.OpPut(key2, val2)))
	c.Assert(err, IsNil)
	c.Assert(rev1, Greater, int64(0))
	c.Assert(resp.Responses, HasLen, 2)

	// both cmps are true
	cmp1 := clientv3.Compare(clientv3.Value(key1), "=", val1)
	cmp2 := clientv3.Compare(clientv3.Value(key2), "=", val2)
	resp, rev2, err := DoTxnWithRepeatable(cli, FullOpFunc([]clientv3.Cmp{cmp1, cmp2}, []clientv3.Op{
		clientv3.OpPut(key1, val), clientv3.OpPut(key2, val),
	}, []clientv3.Op{}))
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)
	c.Assert(resp.Responses, HasLen, 2)

	// one of cmps are false
	cmp1 = clientv3.Compare(clientv3.Value(key1), "=", val)
	cmp2 = clientv3.Compare(clientv3.Value(key2), "=", val2)
	resp, rev3, err := DoTxnWithRepeatable(cli, FullOpFunc([]clientv3.Cmp{cmp1, cmp2}, []clientv3.Op{}, []clientv3.Op{
		clientv3.OpDelete(key1), clientv3.OpDelete(key2),
	}))
	c.Assert(err, IsNil)
	c.Assert(rev3, Greater, rev2)
	c.Assert(resp.Responses, HasLen, 2)

	// enable failpoint
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/pkg/etcdutil/ErrNoSpace", `3*return()`), IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/pkg/etcdutil/ErrNoSpace")

	// put again
	resp, rev2, err = DoTxnWithRepeatable(cli, FullOpFunc([]clientv3.Cmp{clientv3util.KeyMissing(key1), clientv3util.KeyMissing(key2)}, []clientv3.Op{
		clientv3.OpPut(key1, val), clientv3.OpPut(key2, val),
	}, []clientv3.Op{}))
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)
	c.Assert(resp.Responses, HasLen, 2)
}

func (t *testEtcdUtilSuite) TestIsRetryableError(c *C) {
	c.Assert(IsRetryableError(v3rpc.ErrCompacted), IsTrue)
	c.Assert(IsRetryableError(v3rpc.ErrNoLeader), IsTrue)
	c.Assert(IsRetryableError(v3rpc.ErrNoSpace), IsTrue)

	c.Assert(IsRetryableError(v3rpc.ErrCorrupt), IsFalse)
	c.Assert(IsRetryableError(terror.ErrDecodeEtcdKeyFail), IsFalse)
	c.Assert(IsRetryableError(nil), IsFalse)
}
