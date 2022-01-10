package servermaster

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/etcdutils"
	"github.com/phayes/freeport"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

func init() {
	// initialized the logger to make genEmbedEtcdConfig working.
	err := log.InitLogger(&log.Config{})
	if err != nil {
		panic(err)
	}
}

func allocTempURL(t *testing.T) string {
	port, err := freeport.GetFreePort()
	require.Nil(t, err)
	return fmt.Sprintf("127.0.0.1:%d", port)
}

func TestMembershipIface(t *testing.T) {
	t.Parallel()

	dir, err := ioutil.TempDir("", "test1")
	require.Nil(t, err)
	defer os.RemoveAll(dir)

	masterAddr := allocTempURL(t)
	advertiseAddr := masterAddr
	cfgCluster := &etcdutils.ConfigParams{}
	cfgCluster.Name = "dataflow-master-1"
	cfgCluster.DataDir = dir
	cfgCluster.PeerUrls = "http://" + allocTempURL(t)
	cfgCluster.Adjust("", embed.ClusterStateFlagNew)

	cfgClusterEtcd := etcdutils.GenEmbedEtcdConfigWithLogger("info")
	cfgClusterEtcd, err = etcdutils.GenEmbedEtcdConfig(cfgClusterEtcd, masterAddr, advertiseAddr, cfgCluster)
	require.Nil(t, err)

	etcd, err := etcdutils.StartEtcd(cfgClusterEtcd, nil, nil, time.Minute)
	require.Nil(t, err)
	defer etcd.Close()

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{advertiseAddr},
		DialTimeout: 3 * time.Second,
	})
	require.Nil(t, err)

	// prepare master info config
	ctx := context.Background()
	cfg := NewConfig()
	cfg.Etcd.Name = cfgCluster.Name
	cfg.AdvertiseAddr = advertiseAddr
	cfgBytes, err := json.Marshal(cfg)
	require.Nil(t, err)
	_, err = client.Put(ctx, adapter.MasterInfoKey.Encode(cfgCluster.Name), string(cfgBytes))
	require.Nil(t, err)

	// test Membership.GetConfigs
	membership := &EtcdMembership{etcdCli: client}
	cfgs, err := membership.GetConfigs(ctx)
	require.Nil(t, err)
	require.Equal(t, 1, len(cfgs))
	require.Contains(t, cfgs, cfgCluster.Name)

	// test Membership.GetMembers
	etcdLeaderID := etcd.Server.Lead()
	leader := &Member{Name: cfgCluster.Name}
	members, err := membership.GetMembers(ctx, leader, etcdLeaderID)
	require.Nil(t, err)
	require.Len(t, members, 1)
}
