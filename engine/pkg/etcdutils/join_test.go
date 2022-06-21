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

package etcdutils

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/phayes/freeport"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
)

func allocTempURL(t *testing.T) string {
	port, err := freeport.GetFreePort()
	require.Nil(t, err)
	return fmt.Sprintf("127.0.0.1:%d", port)
}

// TestPrepareJoinEtcd tests join etcd one by one into a cluster
// - start etcd-1 first
// - join etcd-1 with etcd-2
// - start etcd-2
// - join etcd-3 with etcd-2
// TODO: refactor to better table driven cases
func TestPrepareJoinEtcd(t *testing.T) {
	// initialized the logger to make genEmbedEtcdConfig working.
	require.Nil(t, log.InitLogger(&log.Config{}))

	masterAddr1 := allocTempURL(t)
	advertiseAddr1 := allocTempURL(t)

	cfgCluster1 := &ConfigParams{
		Name:     "dataflow-master-1",
		DataDir:  t.TempDir(),
		PeerUrls: "http://" + allocTempURL(t),
	}
	cfgCluster1.Adjust("", embed.ClusterStateFlagNew)

	cfgClusterEtcd1 := GenEmbedEtcdConfigWithLogger("info")
	cfgClusterEtcd1, err := GenEmbedEtcdConfig(cfgClusterEtcd1, masterAddr1, advertiseAddr1, cfgCluster1)
	require.Nil(t, err)

	// cfgBefore is a configuration template
	cfgBefore := &ConfigParams{
		Name:     "dataflow-master-1",
		DataDir:  t.TempDir(),
		PeerUrls: "http://" + allocTempURL(t),
	}
	cfgBefore.Adjust("", embed.ClusterStateFlagNew)
	cfgCluster2 := cloneConfig(cfgBefore) // after `prepareJoinEtcd applied
	masterAddr2 := allocTempURL(t)

	// test some corner cases
	{
		// not set `join`, do nothing
		require.Nil(t, PrepareJoinEtcd(cfgCluster2, masterAddr2))
		require.Equal(t, cfgCluster2, cfgBefore)

		// try to join self
		cfgCluster2.Join = masterAddr2
		err = PrepareJoinEtcd(cfgCluster2, masterAddr2)
		require.True(t, errors.ErrMasterJoinEmbedEtcdFail.Equal(err))
		require.Regexp(t, ".*join self.*is forbidden.*", err)

		// update `join` to a valid item
		joinCluster := masterAddr1
		joinFP := filepath.Join(cfgBefore.DataDir, "join")
		cfgBefore.Join = joinCluster

		// join with persistent data
		require.Nil(t, os.WriteFile(joinFP, []byte(joinCluster), privateDirMode))
		cfgCluster2 = cloneConfig(cfgBefore)
		require.Nil(t, PrepareJoinEtcd(cfgCluster2, masterAddr2))
		require.Equal(t, joinCluster, cfgCluster2.InitialCluster)
		require.Equal(t, embed.ClusterStateFlagExisting, cfgCluster2.InitialClusterState)
		require.Nil(t, os.Remove(joinFP)) // remove the persistent data

		require.Nil(t, os.Mkdir(joinFP, 0o700))
		cfgCluster2 = cloneConfig(cfgBefore)
		err = PrepareJoinEtcd(cfgCluster2, masterAddr2)
		require.NotNil(t, err)
		require.Regexp(t, ".*failed to join embed etcd: read persistent join data.*", err)
		require.Nil(t, os.Remove(joinFP))        // remove the persistent data
		require.Equal(t, cfgBefore, cfgCluster2) // not changed

		// restart with previous data
		memberDP := filepath.Join(cfgBefore.DataDir, "member")
		require.Nil(t, os.Mkdir(memberDP, 0o700))
		require.Nil(t, os.Mkdir(filepath.Join(memberDP, "wal"), 0o700))
		err = PrepareJoinEtcd(cfgCluster2, masterAddr2)
		require.Nil(t, err)
		require.Equal(t, "", cfgCluster2.InitialCluster)
		require.Equal(t, embed.ClusterStateFlagExisting, cfgCluster2.InitialClusterState)
		require.Nil(t, os.RemoveAll(memberDP)) // remove previous data
	}

	// start etcd cluster-1
	e1, err := StartEtcd(cfgClusterEtcd1, nil, nil, time.Minute)
	require.Nil(t, err)
	defer e1.Close()

	// test some failure cases
	{
		// same `name`, duplicate
		cfgCluster2 = cloneConfig(cfgBefore)
		err = PrepareJoinEtcd(cfgCluster2, masterAddr2)
		require.NotNil(t, err)
		require.Regexp(t, ".*failed to join embed etcd: missing data or joining a duplicate member.*", err)
		require.Equal(t, cfgBefore, cfgCluster2) // not changed

		// set a different name
		cfgBefore.Name = "dataflow-master-2"

		// add member with invalid `advertise-peer-urls`
		cfgCluster2 = cloneConfig(cfgBefore)
		cfgCluster2.AdvertisePeerUrls = "invalid-advertise-peer-urls"
		err = PrepareJoinEtcd(cfgCluster2, masterAddr2)
		require.NotNil(t, err)
		require.Regexp(t, ".*failed to join embed etcd: add member.*", err)
	}

	// join with existing cluster
	cfgCluster2 = cloneConfig(cfgBefore)
	require.Nil(t, PrepareJoinEtcd(cfgCluster2, masterAddr2))
	require.Equal(t, embed.ClusterStateFlagExisting, cfgCluster2.InitialClusterState)

	obtainClusters := strings.Split(cfgCluster2.InitialCluster, ",")
	sort.Strings(obtainClusters)
	expectedClusters := []string{
		cfgCluster1.InitialCluster,
		fmt.Sprintf("%s=%s", cfgCluster2.Name, cfgCluster2.PeerUrls),
	}
	sort.Strings(expectedClusters)
	require.Equal(t, expectedClusters, obtainClusters)

	// join data should exist now
	joinData, err := os.ReadFile(filepath.Join(cfgBefore.DataDir, "join"))
	require.Nil(t, err)
	require.Equal(t, cfgCluster2.InitialCluster, string(joinData))

	// prepare join done, but has not start the etcd to complete the join, can not join anymore.
	cfgCluster3 := cloneConfig(cfgBefore)
	cfgCluster3.Name = "dataflow-master-3" // overwrite some items
	cfgCluster3.DataDir = t.TempDir()
	masterAddr3 := allocTempURL(t)
	cfgCluster3.PeerUrls = "http://" + allocTempURL(t)
	cfgCluster3.AdvertisePeerUrls = cfgCluster3.PeerUrls
	err = PrepareJoinEtcd(cfgCluster3, masterAddr3)
	require.NotNil(t, err)
	require.Regexp(t, ".*context deadline exceeded.*", err)

	// start the joining etcd-2
	cfgClusterEtcd2 := GenEmbedEtcdConfigWithLogger("info")
	cfgClusterEtcd2, err = GenEmbedEtcdConfig(cfgClusterEtcd2, masterAddr2, masterAddr2, cfgCluster2)
	require.Nil(t, err)
	e2, err := StartEtcd(cfgClusterEtcd2, nil, nil, time.Minute)
	require.Nil(t, err)
	defer e2.Close()

	// try join etcd-3 again
	for i := 0; i < 20; i++ {
		err = PrepareJoinEtcd(cfgCluster3, masterAddr3)
		if err == nil {
			break
		}
		// for `etcdserver: unhealthy cluster`, try again later
		require.NotNil(t, err)
		require.Regexp(t, ".*failed to join embed etcd: add member.*: etcdserver: unhealthy cluster.*", err)
		time.Sleep(500 * time.Millisecond)
	}
	require.Nil(t, err)
}

func cloneConfig(cfg *ConfigParams) *ConfigParams {
	clone := &ConfigParams{}
	*clone = *cfg
	return clone
}

func TestIsDirExist(t *testing.T) {
	d := "./directory-not-exists"
	require.False(t, isDirExist(d))

	d = t.TempDir()

	// empty directory
	require.True(t, isDirExist(d))

	// data exists in the directory
	for i := 1; i <= 3; i++ {
		fp := filepath.Join(d, fmt.Sprintf("file.%d", i))
		require.Nil(t, os.WriteFile(fp, nil, privateDirMode))
		require.True(t, isDirExist(d))
		require.False(t, isDirExist(fp)) // not a directory
	}
}
