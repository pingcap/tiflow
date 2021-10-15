// Copyright 2020 PingCAP, Inc.
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

package upgrade

import (
	"context"
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/integration"

	"github.com/pingcap/dm/dm/common"
)

var (
	etcdTestCli   *clientv3.Client
	bigTxnTestCli *clientv3.Client
)

func TestUpgrade(t *testing.T) {
	mockCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer mockCluster.Terminate(t)

	etcdTestCli = mockCluster.RandClient()

	bigCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1, MaxTxnOps: 2048})
	defer bigCluster.Terminate(t)

	bigTxnTestCli = bigCluster.RandClient()

	TestingT(t)
}

func clearTestData(c *C) {
	clearVersion := clientv3.OpDelete(common.ClusterVersionKey)
	_, err := etcdTestCli.Txn(context.Background()).Then(clearVersion).Commit()
	c.Assert(err, IsNil)
}

type testForEtcd struct{}

var _ = SerialSuites(&testForEtcd{})

func (t *testForEtcd) TestTryUpgrade(c *C) {
	defer clearTestData(c)

	// mock upgrade functions.
	oldUpgrades := upgrades
	defer func() {
		upgrades = oldUpgrades
	}()
	mockVerNo := uint64(0)
	upgrades = []func(cli *clientv3.Client, uctx Context) error{
		func(cli *clientv3.Client, uctx Context) error {
			mockVerNo = currentInternalNo + 1
			return nil
		},
	}

	// no previous version exist, new cluster.
	ver, rev1, err := GetVersion(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev1, Greater, int64(0))
	c.Assert(ver.NotSet(), IsTrue)

	// try to upgrade, run actual upgrade functions
	c.Assert(TryUpgrade(etcdTestCli, newUpgradeContext()), IsNil)
	ver, rev2, err := GetVersion(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)
	c.Assert(ver, DeepEquals, CurrentVersion)
	c.Assert(mockVerNo, Equals, uint64(5))

	// try to upgrade again, do nothing because the version is the same.
	mockVerNo = 0
	c.Assert(TryUpgrade(etcdTestCli, newUpgradeContext()), IsNil)
	ver, rev3, err := GetVersion(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev3, Equals, rev2)
	c.Assert(ver, DeepEquals, CurrentVersion)
	c.Assert(mockVerNo, Equals, uint64(0))

	// mock a newer current version.
	oldCurrentVer := CurrentVersion
	defer func() {
		CurrentVersion = oldCurrentVer
	}()
	newerVer := NewVersion(currentInternalNo+1, "mock-current-ver")
	CurrentVersion = newerVer

	// try to upgrade, to a newer version, upgrade operations applied.
	c.Assert(TryUpgrade(etcdTestCli, newUpgradeContext()), IsNil)
	ver, rev4, err := GetVersion(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev4, Greater, rev3)
	c.Assert(ver, DeepEquals, newerVer)
	c.Assert(mockVerNo, Equals, currentInternalNo+1)

	// try to upgrade, to an older version, do nothing.
	mockVerNo = 0
	CurrentVersion = oldCurrentVer
	c.Assert(TryUpgrade(etcdTestCli, newUpgradeContext()), IsNil)
	ver, rev5, err := GetVersion(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev5, Equals, rev4)
	c.Assert(ver, DeepEquals, newerVer) // not changed.
	c.Assert(mockVerNo, Equals, uint64(0))
}

func (t *testForEtcd) TestUpgradeToVer3(c *C) {
	ctx := context.Background()
	source := "source-1"
	oldKey := common.UpstreamConfigKeyAdapterV1.Encode(source)
	oldVal := "test"

	_, err := etcdTestCli.Put(ctx, oldKey, oldVal)
	c.Assert(err, IsNil)
	c.Assert(upgradeToVer3(ctx, etcdTestCli), IsNil)

	newKey := common.UpstreamConfigKeyAdapter.Encode(source)
	resp, err := etcdTestCli.Get(ctx, newKey)
	c.Assert(err, IsNil)
	c.Assert(resp.Kvs, HasLen, 1)
	c.Assert(string(resp.Kvs[0].Value), Equals, oldVal)

	// test won't overwrite new value
	newVal := "test2"
	_, err = etcdTestCli.Put(ctx, newKey, newVal)
	c.Assert(err, IsNil)
	c.Assert(upgradeToVer3(ctx, etcdTestCli), IsNil)
	resp, err = etcdTestCli.Get(ctx, newKey)
	c.Assert(err, IsNil)
	c.Assert(resp.Kvs, HasLen, 1)
	c.Assert(string(resp.Kvs[0].Value), Equals, newVal)

	for i := 0; i < 500; i++ {
		key := common.UpstreamConfigKeyAdapterV1.Encode(fmt.Sprintf("%s-%d", source, i))
		val := fmt.Sprintf("%s-%d", oldVal, i)
		_, err := etcdTestCli.Put(ctx, key, val)
		c.Assert(err, IsNil)
	}
	c.Assert(upgradeToVer3(ctx, etcdTestCli), ErrorMatches, ".*too many operations in txn request.*")

	for i := 0; i < 1000; i++ {
		key := common.UpstreamConfigKeyAdapterV1.Encode(fmt.Sprintf("%s-%d", source, i))
		val := fmt.Sprintf("%s-%d", oldVal, i)
		_, err := bigTxnTestCli.Put(ctx, key, val)
		c.Assert(err, IsNil)
	}
	c.Assert(upgradeToVer3(ctx, bigTxnTestCli), IsNil)
}
