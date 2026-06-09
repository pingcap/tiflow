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

	"github.com/pingcap/tiflow/dm/common"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

var (
	mockCluster   *integration.ClusterV3
	bigTxnCluster *integration.ClusterV3
	etcdTestCli   *clientv3.Client
	bigTxnTestCli *clientv3.Client
)

func TestUpgrade(t *testing.T) {
	integration.BeforeTestExternal(t)

	suite.Run(t, new(testForEtcd))
	suite.Run(t, new(testForBigTxn))
}

func clearTestData(t require.TestingT) {
	clearVersion := clientv3.OpDelete(common.ClusterVersionKey)
	_, err := etcdTestCli.Txn(context.Background()).Then(clearVersion).Commit()
	require.NoError(t, err)
}

type testForEtcd struct {
	suite.Suite
}

type testForBigTxn struct {
	suite.Suite
}

func (t *testForEtcd) SetupSuite() {
	mockCluster = integration.NewClusterV3(t.T(), &integration.ClusterConfig{Size: 1})
	etcdTestCli = mockCluster.RandClient()
}

func (t *testForEtcd) TearDownSuite() {
	mockCluster.Terminate(t.T())
}

func (t *testForBigTxn) SetupSuite() {
	bigTxnCluster = integration.NewClusterV3(t.T(), &integration.ClusterConfig{Size: 1, MaxTxnOps: 2048})
	bigTxnTestCli = bigTxnCluster.RandClient()
}

func (t *testForBigTxn) TearDownSuite() {
	bigTxnCluster.Terminate(t.T())
}

func (t *testForEtcd) TestTryUpgrade() {
	defer clearTestData(t.T())

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
	t.Require().NoError(err)
	t.Require().Greater(rev1, int64(0))
	t.Require().True(ver.NotSet())

	// try to upgrade, run actual upgrade functions
	t.Require().NoError(TryUpgrade(etcdTestCli, newUpgradeContext()))
	ver, rev2, err := GetVersion(etcdTestCli)
	t.Require().NoError(err)
	t.Require().Greater(rev2, rev1)
	t.Require().Equal(CurrentVersion, ver)
	t.Require().Equal(uint64(5), mockVerNo)

	// try to upgrade again, do nothing because the version is the same.
	mockVerNo = 0
	t.Require().NoError(TryUpgrade(etcdTestCli, newUpgradeContext()))
	ver, rev3, err := GetVersion(etcdTestCli)
	t.Require().NoError(err)
	t.Require().Equal(rev2, rev3)
	t.Require().Equal(CurrentVersion, ver)
	t.Require().Equal(uint64(0), mockVerNo)

	// mock a newer current version.
	oldCurrentVer := CurrentVersion
	defer func() {
		CurrentVersion = oldCurrentVer
	}()
	newerVer := NewVersion(currentInternalNo+1, "mock-current-ver")
	CurrentVersion = newerVer

	// try to upgrade, to a newer version, upgrade operations applied.
	t.Require().NoError(TryUpgrade(etcdTestCli, newUpgradeContext()))
	ver, rev4, err := GetVersion(etcdTestCli)
	t.Require().NoError(err)
	t.Require().Greater(rev4, rev3)
	t.Require().Equal(newerVer, ver)
	t.Require().Equal(currentInternalNo+1, mockVerNo)

	// try to upgrade, to an older version, do nothing.
	mockVerNo = 0
	CurrentVersion = oldCurrentVer
	t.Require().NoError(TryUpgrade(etcdTestCli, newUpgradeContext()))
	ver, rev5, err := GetVersion(etcdTestCli)
	t.Require().NoError(err)
	t.Require().Equal(rev4, rev5)
	t.Require().Equal(newerVer, ver) // not changed.
	t.Require().Equal(uint64(0), mockVerNo)
}

func (t *testForEtcd) TestUpgradeToVer3() {
	ctx := context.Background()
	source := "source-1"
	oldKey := common.UpstreamConfigKeyAdapterV1.Encode(source)
	oldVal := "test"

	_, err := etcdTestCli.Put(ctx, oldKey, oldVal)
	t.Require().NoError(err)
	t.Require().NoError(upgradeToVer3(ctx, etcdTestCli))

	newKey := common.UpstreamConfigKeyAdapter.Encode(source)
	resp, err := etcdTestCli.Get(ctx, newKey)
	t.Require().NoError(err)
	t.Require().Len(resp.Kvs, 1)
	t.Require().Equal(oldVal, string(resp.Kvs[0].Value))

	// test won't overwrite new value
	newVal := "test2"
	_, err = etcdTestCli.Put(ctx, newKey, newVal)
	t.Require().NoError(err)
	t.Require().NoError(upgradeToVer3(ctx, etcdTestCli))
	resp, err = etcdTestCli.Get(ctx, newKey)
	t.Require().NoError(err)
	t.Require().Len(resp.Kvs, 1)
	t.Require().Equal(newVal, string(resp.Kvs[0].Value))

	for i := 0; i < 500; i++ {
		key := common.UpstreamConfigKeyAdapterV1.Encode(fmt.Sprintf("%s-%d", source, i))
		val := fmt.Sprintf("%s-%d", oldVal, i)
		_, err := etcdTestCli.Put(ctx, key, val)
		t.Require().NoError(err)
	}
	err = upgradeToVer3(ctx, etcdTestCli)
	t.Require().Error(err)
	t.Require().Regexp(".*too many operations in txn request.*", err.Error())
}

func (t *testForBigTxn) TestUpgradeToVer3() {
	source := "source-1"
	oldVal := "test"
	ctx := context.Background()

	for i := 0; i < 1000; i++ {
		key := common.UpstreamConfigKeyAdapterV1.Encode(fmt.Sprintf("%s-%d", source, i))
		val := fmt.Sprintf("%s-%d", oldVal, i)
		_, err := bigTxnTestCli.Put(ctx, key, val)
		t.Require().NoError(err)
	}
	t.Require().NoError(upgradeToVer3(ctx, bigTxnTestCli))
}
