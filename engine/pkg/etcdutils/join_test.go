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

func TestPrepareJoinEtcd(t *testing.T) {
	// initialized the logger to make genEmbedEtcdConfig working.
	require.Nil(t, log.InitLogger(&log.Config{}))

	masterAddr := allocTempURL(t)
	advertiseAddr := allocTempURL(t)

	cfgCluster := &ConfigParams{} // used to start an etcd cluster
	cfgCluster.Name = "dataflow-master-1"
	cfgCluster.DataDir = t.TempDir()
	cfgCluster.PeerUrls = "http://" + allocTempURL(t)
	cfgCluster.Adjust("", embed.ClusterStateFlagNew)

	cfgClusterEtcd := GenEmbedEtcdConfigWithLogger("info")
	cfgClusterEtcd, err := GenEmbedEtcdConfig(cfgClusterEtcd, masterAddr, advertiseAddr, cfgCluster)
	require.Nil(t, err)

	cfgBefore := cloneConfig(cfgCluster) // before `prepareJoinEtcd` applied
	cfgBefore.DataDir = t.TempDir()      // overwrite some config items
	beforeMasterAddr := allocTempURL(t)
	cfgBefore.PeerUrls = "http://" + allocTempURL(t)
	cfgBefore.AdvertisePeerUrls = cfgBefore.PeerUrls
	cfgBefore.Adjust("", embed.ClusterStateFlagNew)

	cfgAfter := cloneConfig(cfgBefore) // after `prepareJoinEtcd applied

	// not set `join`, do nothing
	require.Nil(t, PrepareJoinEtcd(cfgAfter, beforeMasterAddr))
	require.Equal(t, cfgAfter, cfgBefore)

	// try to join self
	cfgAfter.Join = beforeMasterAddr
	err = PrepareJoinEtcd(cfgAfter, beforeMasterAddr)
	require.True(t, errors.ErrMasterJoinEmbedEtcdFail.Equal(err))
	require.Regexp(t, ".*join self.*is forbidden.*", err)

	// update `join` to a valid item
	joinCluster := masterAddr
	joinFP := filepath.Join(cfgBefore.DataDir, "join")
	cfgBefore.Join = joinCluster

	// join with persistent data
	require.Nil(t, os.WriteFile(joinFP, []byte(joinCluster), privateDirMode))
	cfgAfter = cloneConfig(cfgBefore)
	require.Nil(t, PrepareJoinEtcd(cfgAfter, beforeMasterAddr))
	require.Equal(t, joinCluster, cfgAfter.InitialCluster)
	require.Equal(t, embed.ClusterStateFlagExisting, cfgAfter.InitialClusterState)
	require.Nil(t, os.Remove(joinFP)) // remove the persistent data

	require.Nil(t, os.Mkdir(joinFP, 0o700))
	cfgAfter = cloneConfig(cfgBefore)
	err = PrepareJoinEtcd(cfgAfter, beforeMasterAddr)
	require.NotNil(t, err)
	require.Regexp(t, ".*failed to join embed etcd: read persistent join data.*", err)
	require.Nil(t, os.Remove(joinFP))     // remove the persistent data
	require.Equal(t, cfgBefore, cfgAfter) // not changed

	// restart with previous data
	memberDP := filepath.Join(cfgBefore.DataDir, "member")
	require.Nil(t, os.Mkdir(memberDP, 0o700))
	require.Nil(t, os.Mkdir(filepath.Join(memberDP, "wal"), 0o700))
	err = PrepareJoinEtcd(cfgAfter, beforeMasterAddr)
	require.Nil(t, err)
	require.Equal(t, "", cfgAfter.InitialCluster)
	require.Equal(t, embed.ClusterStateFlagExisting, cfgAfter.InitialClusterState)
	require.Nil(t, os.RemoveAll(memberDP)) // remove previous data

	// start an etcd cluster
	e1, err := StartEtcd(cfgClusterEtcd, nil, nil, time.Minute)
	require.Nil(t, err)
	defer e1.Close()

	// same `name`, duplicate
	cfgAfter = cloneConfig(cfgBefore)
	err = PrepareJoinEtcd(cfgAfter, beforeMasterAddr)
	require.NotNil(t, err)
	require.Regexp(t, ".*failed to join embed etcd: missing data or joining a duplicate member.*", err)
	require.Equal(t, cfgBefore, cfgAfter) // not changed

	// set a different name
	cfgBefore.Name = "dataflow-master-2"

	// add member with invalid `advertise-peer-urls`
	cfgAfter = cloneConfig(cfgBefore)
	cfgAfter.AdvertisePeerUrls = "invalid-advertise-peer-urls"
	err = PrepareJoinEtcd(cfgAfter, beforeMasterAddr)
	require.NotNil(t, err)
	require.Regexp(t, ".*failed to join embed etcd: add member.*", err)

	// join with existing cluster
	cfgAfter = cloneConfig(cfgBefore)
	require.Nil(t, PrepareJoinEtcd(cfgAfter, beforeMasterAddr))
	require.Equal(t, embed.ClusterStateFlagExisting, cfgAfter.InitialClusterState)

	obtainClusters := strings.Split(cfgAfter.InitialCluster, ",")
	sort.Strings(obtainClusters)
	expectedClusters := []string{
		cfgCluster.InitialCluster,
		fmt.Sprintf("%s=%s", cfgAfter.Name, cfgAfter.PeerUrls),
	}
	sort.Strings(expectedClusters)
	require.Equal(t, expectedClusters, obtainClusters)

	// join data should exist now
	joinData, err := os.ReadFile(joinFP)
	require.Nil(t, err)
	require.Equal(t, cfgAfter.InitialCluster, string(joinData))

	// prepare join done, but has not start the etcd to complete the join, can not join anymore.
	cfgAfter2 := cloneConfig(cfgBefore)
	cfgAfter2.Name = "dataflow-master-3" // overwrite some items
	cfgAfter2.DataDir = t.TempDir()
	after2MasterAddr := allocTempURL(t)
	cfgAfter2.PeerUrls = "http://" + allocTempURL(t)
	cfgAfter2.AdvertisePeerUrls = cfgAfter2.PeerUrls
	err = PrepareJoinEtcd(cfgAfter2, after2MasterAddr)
	require.NotNil(t, err)
	require.Regexp(t, ".*context deadline exceeded.*", err)

	// start the joining etcd
	cfgAfterEtcd := GenEmbedEtcdConfigWithLogger("info")
	cfgAfterEtcd, err = GenEmbedEtcdConfig(cfgAfterEtcd, after2MasterAddr, after2MasterAddr, cfgAfter)
	require.Nil(t, err)
	e2, err := StartEtcd(cfgAfterEtcd, nil, nil, time.Minute)
	require.Nil(t, err)
	defer e2.Close()

	// try join again
	for i := 0; i < 20; i++ {
		cfgAfterEtcd, err = GenEmbedEtcdConfig(cfgAfterEtcd, after2MasterAddr, after2MasterAddr, cfgAfter)
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
