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
	"fmt"
	"testing"
	"time"

	"github.com/phayes/freeport"
	"github.com/pingcap/tiflow/engine/pkg/etcdutil"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
)

func init() {
	err := logutil.InitLogger(&logutil.Config{Level: "warn"})
	if err != nil {
		panic(err)
	}
}

func allocTempURL(t *testing.T) string {
	port, err := freeport.GetFreePort()
	require.Nil(t, err)
	return fmt.Sprintf("127.0.0.1:%d", port)
}

func TestStartEtcdTimeout(t *testing.T) {
	t.Parallel()

	name1 := "test-start-etcd-timeout-1"
	name2 := "test-start-etcd-timeout-2"

	masterAddr := allocTempURL(t)
	advertiseAddr := masterAddr
	cfgCluster := &etcdutil.ConfigParams{}
	cfgCluster.Name = name1
	cfgCluster.DataDir = t.TempDir()
	peer1 := allocTempURL(t)
	peer2 := peer1
	require.Eventually(t, func() bool {
		peer2 = allocTempURL(t)
		return peer2 != peer1
	}, time.Second, time.Millisecond*10)
	cfgCluster.PeerUrls = "http://" + peer1
	cfgCluster.InitialCluster = fmt.Sprintf("%s=http://%s,%s=http://%s", name1, peer1, name2, peer2)
	cfgCluster.Adjust("", embed.ClusterStateFlagNew)

	cfgClusterEtcd := etcdutil.GenEmbedEtcdConfigWithLogger("info")
	cfgClusterEtcd, err := etcdutil.GenEmbedEtcdConfig(cfgClusterEtcd, masterAddr, advertiseAddr, cfgCluster)
	require.Nil(t, err)

	_, err = etcdutil.StartEtcd(cfgClusterEtcd, nil, nil, time.Millisecond*100)
	require.EqualError(t, err, "[DFLOW:ErrMasterStartEmbedEtcdFail]start embed etcd timeout 100ms")
}
