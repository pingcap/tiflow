package test

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/pkg/etcdutils"
	"github.com/phayes/freeport"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

func allocTempURL(t *testing.T) string {
	port, err := freeport.GetFreePort()
	require.Nil(t, err)
	return fmt.Sprintf("127.0.0.1:%d", port)
}

// PrepareEtcd creates a single embed etcd server used fo test, and returns
// - advertiseAddr of the embed etcd
// - embed etcd server
// - etcd client
// - etcd cleanup function
func PrepareEtcd(t *testing.T, name string) (string, *embed.Etcd, *clientv3.Client, func()) {
	dir, err := ioutil.TempDir("", name)
	require.Nil(t, err)

	masterAddr := allocTempURL(t)
	advertiseAddr := masterAddr
	cfgCluster := &etcdutils.ConfigParams{}
	cfgCluster.Name = name
	cfgCluster.DataDir = dir
	cfgCluster.PeerUrls = "http://" + allocTempURL(t)
	cfgCluster.Adjust("", embed.ClusterStateFlagNew)

	cfgClusterEtcd := etcdutils.GenEmbedEtcdConfigWithLogger("info")
	cfgClusterEtcd, err = etcdutils.GenEmbedEtcdConfig(cfgClusterEtcd, masterAddr, advertiseAddr, cfgCluster)
	require.Nil(t, err)

	etcd, err := etcdutils.StartEtcd(cfgClusterEtcd, nil, nil, time.Minute)
	require.Nil(t, err)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{advertiseAddr},
		DialTimeout: 3 * time.Second,
	})
	require.Nil(t, err)

	cleanFn := func() {
		os.RemoveAll(dir)
		etcd.Close()
	}

	return advertiseAddr, etcd, client, cleanFn
}
