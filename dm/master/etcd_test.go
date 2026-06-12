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

package master

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/utils/tempurl"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

type testEtcdSuite struct {
	suite.Suite
}

func (t *testEtcdSuite) SetupSuite() {
	// initialized the logger to make genEmbedEtcdConfig working.
	t.Require().NoError(log.InitLogger(&log.Config{}))
}

func (t *testEtcdSuite) TestStartEtcdFail() {
	cfgCluster := NewConfig()
	cfgCluster.Name = "dm-master-1"
	cfgCluster.DataDir = t.T().TempDir()
	cfgCluster.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfgCluster.PeerUrls = tempurl.Alloc()
	t.Require().NoError(cfgCluster.adjust())

	// add another non-existing member for bootstrapping.
	cfgCluster.InitialCluster = fmt.Sprintf("%s=%s,%s=%s",
		cfgCluster.Name, cfgCluster.AdvertisePeerUrls,
		"dm-master-2", tempurl.Alloc())
	t.Require().NoError(cfgCluster.adjust())

	// start an etcd cluster
	cfgClusterEtcd := genEmbedEtcdConfigWithLogger("info")
	cfgClusterEtcd, err := cfgCluster.genEmbedEtcdConfig(cfgClusterEtcd)
	t.Require().NoError(err)
	e, err := startEtcd(cfgClusterEtcd, nil, nil, 3*time.Second)
	t.Require().True(terror.ErrMasterStartEmbedEtcdFail.Equal(err))
	t.Require().Nil(e)
}

func (t *testEtcdSuite) TestPrepareJoinEtcd() {
	cfgCluster := NewConfig() // used to start an etcd cluster
	cfgCluster.Name = "dm-master-1"
	cfgCluster.DataDir = t.T().TempDir()
	cfgCluster.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfgCluster.AdvertiseAddr = cfgCluster.MasterAddr
	cfgCluster.PeerUrls = tempurl.Alloc()
	t.Require().NoError(cfgCluster.adjust())
	cfgClusterEtcd := genEmbedEtcdConfigWithLogger("info")
	cfgClusterEtcd, err := cfgCluster.genEmbedEtcdConfig(cfgClusterEtcd)
	t.Require().NoError(err)

	cfgBefore := t.cloneConfig(cfgCluster) // before `prepareJoinEtcd` applied
	cfgBefore.DataDir = t.T().TempDir()    // overwrite some config items
	cfgBefore.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfgBefore.AdvertiseAddr = cfgBefore.MasterAddr
	cfgBefore.PeerUrls = tempurl.Alloc()
	cfgBefore.AdvertisePeerUrls = cfgBefore.PeerUrls
	t.Require().NoError(cfgBefore.adjust())

	cfgAfter := t.cloneConfig(cfgBefore) // after `prepareJoinEtcd applied

	joinCluster := cfgCluster.MasterAddr
	joinFP := filepath.Join(cfgBefore.DataDir, "join")
	memberDP := filepath.Join(cfgBefore.DataDir, "member")

	// not set `join`, do nothing
	t.Require().NoError(prepareJoinEtcd(cfgAfter))
	t.Require().Equal(cfgBefore, cfgAfter)

	// try to join self
	cfgAfter.Join = cfgAfter.MasterAddr
	err = prepareJoinEtcd(cfgAfter)
	t.Require().True(terror.ErrMasterJoinEmbedEtcdFail.Equal(err))
	t.Require().Error(err)
	t.Require().Regexp(".*fail to join embed etcd: join self.*is forbidden.*", err.Error())

	// update `join` to a valid item
	cfgBefore.Join = joinCluster

	// join with persistent data
	t.Require().NoError(os.WriteFile(joinFP, []byte(joinCluster), privateDirMode))
	cfgAfter = t.cloneConfig(cfgBefore)
	t.Require().NoError(prepareJoinEtcd(cfgAfter))
	t.Require().Equal(joinCluster, cfgAfter.InitialCluster)
	t.Require().Equal(embed.ClusterStateFlagExisting, cfgAfter.InitialClusterState)
	t.Require().NoError(os.Remove(joinFP)) // remove the persistent data

	// join with invalid persistent data
	t.Require().NoError(os.Mkdir(joinFP, privateDirMode)) // use directory as invalid persistent data (file)
	cfgAfter = t.cloneConfig(cfgBefore)
	err = prepareJoinEtcd(cfgAfter)
	t.Require().True(terror.ErrMasterJoinEmbedEtcdFail.Equal(err))
	t.Require().Error(err)
	t.Require().Regexp(".*fail to join embed etcd: read persistent join data.*", err.Error())
	t.Require().NoError(os.Remove(joinFP)) // remove the persistent data
	t.Require().Equal(cfgBefore, cfgAfter) // not changed

	// restart with previous data
	t.Require().NoError(os.Mkdir(memberDP, privateDirMode))
	t.Require().NoError(os.Mkdir(filepath.Join(memberDP, "wal"), privateDirMode))
	t.Require().NoError(prepareJoinEtcd(cfgAfter))
	t.Require().Equal("", cfgAfter.InitialCluster)
	t.Require().Equal(embed.ClusterStateFlagExisting, cfgAfter.InitialClusterState)
	t.Require().NoError(os.RemoveAll(memberDP)) // remove previous data

	// start an etcd cluster
	e1, err := startEtcd(cfgClusterEtcd, nil, nil, etcdStartTimeout)
	t.Require().NoError(err)
	defer e1.Close()

	// same `name`, duplicate
	cfgAfter = t.cloneConfig(cfgBefore)
	err = prepareJoinEtcd(cfgAfter)
	t.Require().True(terror.ErrMasterJoinEmbedEtcdFail.Equal(err))
	t.Require().Error(err)
	t.Require().Regexp(".*fail to join embed etcd: missing data or joining a duplicate member.*", err.Error())
	t.Require().Equal(cfgBefore, cfgAfter) // not changed

	// set a different name
	cfgBefore.Name = "dm-master-2"

	// add member with invalid `advertise-peer-urls`
	cfgAfter = t.cloneConfig(cfgBefore)
	cfgAfter.AdvertisePeerUrls = "invalid-advertise-peer-urls"
	err = prepareJoinEtcd(cfgAfter)
	t.Require().True(terror.ErrMasterJoinEmbedEtcdFail.Equal(err))
	t.Require().Error(err)
	t.Require().Regexp(".*fail to join embed etcd: add member.*", err.Error())

	// join with existing cluster
	cfgAfter = t.cloneConfig(cfgBefore)
	t.Require().NoError(prepareJoinEtcd(cfgAfter))
	t.Require().Equal(embed.ClusterStateFlagExisting, cfgAfter.InitialClusterState)
	obtainClusters := strings.Split(cfgAfter.InitialCluster, ",")
	sort.Strings(obtainClusters)
	expectedClusters := []string{
		cfgCluster.InitialCluster,
		fmt.Sprintf("%s=%s", cfgAfter.Name, cfgAfter.PeerUrls),
	}
	sort.Strings(expectedClusters)
	t.Require().Equal(expectedClusters, obtainClusters)

	// join data should exist now
	joinData, err := os.ReadFile(joinFP)
	t.Require().NoError(err)
	t.Require().Equal(cfgAfter.InitialCluster, string(joinData))

	// prepare join done, but has not start the etcd to complete the join, can not join anymore.
	cfgAfter2 := t.cloneConfig(cfgBefore)
	cfgAfter2.Name = "dm-master-3" // overwrite some items
	cfgAfter2.DataDir = t.T().TempDir()
	cfgAfter2.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfgAfter2.AdvertiseAddr = cfgAfter2.MasterAddr
	cfgAfter2.PeerUrls = tempurl.Alloc()
	cfgAfter2.AdvertisePeerUrls = cfgAfter2.PeerUrls
	err = prepareJoinEtcd(cfgAfter2)
	t.Require().True(terror.ErrMasterJoinEmbedEtcdFail.Equal(err))
	t.Require().Error(err)
	t.Require().Regexp(".*context deadline exceeded.*", err.Error())

	// start the joining etcd
	cfgAfterEtcd := genEmbedEtcdConfigWithLogger("info")
	cfgAfterEtcd, err = cfgAfter.genEmbedEtcdConfig(cfgAfterEtcd)
	t.Require().NoError(err)
	e2, err := startEtcd(cfgAfterEtcd, nil, nil, etcdStartTimeout)
	t.Require().NoError(err)
	defer e2.Close()

	// try join again
	for i := 0; i < 20; i++ {
		err = prepareJoinEtcd(cfgAfter2)
		if err == nil {
			break
		}
		// for `etcdserver: unhealthy cluster`, try again later
		t.Require().True(terror.ErrMasterJoinEmbedEtcdFail.Equal(err))
		t.Require().Error(err)
		t.Require().Regexp(".*fail to join embed etcd: add member.*: etcdserver: unhealthy cluster.*", err.Error())
		time.Sleep(500 * time.Millisecond)
	}
	t.Require().NoError(err)
}

func (t *testEtcdSuite) cloneConfig(cfg *Config) *Config {
	clone := NewConfig()
	*clone = *cfg
	return clone
}

func (t *testEtcdSuite) TestIsDirExist() {
	d := "./directory-not-exists"
	t.Require().False(isDirExist(d))

	// empty directory
	d = t.T().TempDir()
	t.Require().True(isDirExist(d))

	// data exists in the directory
	for i := 1; i <= 3; i++ {
		fp := filepath.Join(d, fmt.Sprintf("file.%d", i))
		t.Require().NoError(os.WriteFile(fp, nil, privateDirMode))
		t.Require().True(isDirExist(d))
		t.Require().False(isDirExist(fp)) // not a directory
	}
}

func (t *testEtcdSuite) TestEtcdAutoCompaction() {
	cfg := NewConfig()
	t.Require().NoError(cfg.FromContent(SampleConfig))

	cfg.DataDir = t.T().TempDir()
	cfg.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg.AdvertiseAddr = cfg.MasterAddr
	cfg.AutoCompactionRetention = "1s"

	ctx, cancel := context.WithCancel(context.Background())
	s := NewServer(cfg)
	t.Require().NoError(s.Start(ctx))

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{cfg.MasterAddr},
	})
	t.Require().NoError(err)

	for i := 0; i < 100; i++ {
		_, err = etcdCli.Put(ctx, "key", fmt.Sprintf("%03d", i))
		t.Require().NoError(err)
	}
	time.Sleep(3 * time.Second)
	resp, err := etcdCli.Get(ctx, "key")
	t.Require().NoError(err)

	utils.WaitSomething(10, time.Second, func() bool {
		_, err = etcdCli.Get(ctx, "key", clientv3.WithRev(resp.Header.Revision-1))
		return err != nil
	})
	t.Require().Error(err)
	t.Require().Regexp(".*required revision has been compacted.*", err.Error())

	cancel()
	s.Close()
}
