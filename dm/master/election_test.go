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

package master

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/tempurl"
)

func TestFailToStartLeader(t *testing.T) {
	err := log.InitLogger(&log.Config{Level: "info"})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())

	var s1, s2 *Server
	defer func() {
		cancel()
		if s1 != nil {
			s1.Close()
		}
		if s2 != nil {
			s2.Close()
		}
	}()

	// create a new cluster
	cfg1 := NewConfig()
	require.NoError(t, cfg1.FromContent(SampleConfig))
	cfg1.Name = "dm-master-1"
	cfg1.DataDir = t.TempDir()
	cfg1.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg1.AdvertiseAddr = cfg1.MasterAddr
	cfg1.PeerUrls = tempurl.Alloc()
	cfg1.AdvertisePeerUrls = cfg1.PeerUrls
	cfg1.InitialCluster = fmt.Sprintf("%s=%s", cfg1.Name, cfg1.AdvertisePeerUrls)

	s1 = NewServer(cfg1)
	require.NoError(t, s1.Start(ctx))
	// wait the first one become the leader
	require.Eventually(t, func() bool {
		return s1.election.IsLeader() && s1.scheduler.Started()
	}, 3*time.Second, 100*time.Millisecond)

	// join to an existing cluster
	cfg2 := NewConfig()
	require.NoError(t, cfg2.FromContent(SampleConfig))
	cfg2.Name = "dm-master-2"
	cfg2.DataDir = t.TempDir()
	cfg2.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg2.AdvertiseAddr = cfg2.MasterAddr
	cfg2.PeerUrls = tempurl.Alloc()
	cfg2.AdvertisePeerUrls = cfg2.PeerUrls
	cfg2.Join = cfg1.MasterAddr // join to an existing cluster

	// imitate fail to start scheduler/pessimism/optimism
	require.NoError(t, failpoint.Enable("github.com/pingcap/tiflow/dm/master/FailToStartLeader", `return("dm-master-2")`))
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/master/FailToStartLeader")

	s2 = NewServer(cfg2)
	require.NoError(t, s2.Start(ctx))
	// wait the second master ready
	time.Sleep(time.Second)
	require.False(t, s2.election.IsLeader())

	client, err := etcdutil.CreateClient(strings.Split(cfg1.AdvertisePeerUrls, ","), nil)
	require.NoError(t, err)
	defer client.Close()

	// s1 is still the leader
	_, leaderID, _, err := s2.election.LeaderInfo(ctx)
	require.NoError(t, err)
	require.Equal(t, cfg1.Name, leaderID)
	require.Greater(t, s1.ClusterID(), uint64(0))
	require.Equal(t, uint64(0), s2.ClusterID())

	s1.election.Resign()
	time.Sleep(1 * time.Second)

	// s1 is still the leader
	require.Eventually(t, func() bool {
		_, leaderID, _, err = s2.election.LeaderInfo(ctx)
		require.NoError(t, err)
		return leaderID == cfg1.Name
	}, 3*time.Second, 100*time.Millisecond)
	clusterID := s1.ClusterID()

	require.NoError(t, failpoint.Disable("github.com/pingcap/tiflow/dm/master/FailToStartLeader"))
	s1.election.Resign()
	time.Sleep(1 * time.Second)

	// s2 now become leader
	_, leaderID, _, err = s2.election.LeaderInfo(ctx)
	require.NoError(t, err)
	require.Equal(t, cfg2.Name, leaderID)
	require.Equal(t, clusterID, s2.ClusterID())
}
