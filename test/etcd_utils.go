package test

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/pkg/etcdutils"
	"github.com/phayes/freeport"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
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

func PrepareEtcdCluster(t *testing.T, names []string) (
	advertiseAddrs []string,
	embedEtcds []*embed.Etcd,
	etcdCli *clientv3.Client,
	cleanFn func(),
) {
	dirs := make([]string, 0, len(names))
	cfgs := make([]*etcdutils.ConfigParams, 0, len(names))
	initialCluster := make([]string, 0, len(names))
	for _, name := range names {
		dir, err := ioutil.TempDir("", name)
		require.Nil(t, err)
		dirs = append(dirs, dir)

		advertiseAddr := allocTempURL(t)
		advertiseAddrs = append(advertiseAddrs, advertiseAddr)

		cfgCluster := &etcdutils.ConfigParams{}
		cfgCluster.Name = name
		cfgCluster.DataDir = dir
		cfgCluster.PeerUrls = "http://" + allocTempURL(t)
		cfgs = append(cfgs, cfgCluster)

		initialCluster = append(initialCluster, fmt.Sprintf("%s=%s", name, cfgCluster.PeerUrls))
	}
	etcdCh := make(chan *embed.Etcd)
	for idx, cfg := range cfgs {
		cfg.InitialCluster = strings.Join(initialCluster, ",")
		fmt.Printf("cfg: %+v\n", cfg)
		cfg.Adjust("", embed.ClusterStateFlagNew)
		cfgClusterEtcd := etcdutils.GenEmbedEtcdConfigWithLogger("info")
		addr := advertiseAddrs[idx]
		cfgClusterEtcd, err := etcdutils.GenEmbedEtcdConfig(cfgClusterEtcd, addr, addr, cfg)
		require.Nil(t, err)
		go func() {
			etcd, err := etcdutils.StartEtcd(cfgClusterEtcd, nil, nil, time.Minute)
			require.Nil(t, err)
			etcdCh <- etcd
		}()
	}
fetchLoop:
	for {
		select {
		case etcd := <-etcdCh:
			embedEtcds = append(embedEtcds, etcd)
			if len(embedEtcds) == len(names) {
				break fetchLoop
			}
		case <-time.After(time.Minute):
			t.Error("etcd doesn't start in time")
		}
	}

	var err error
	etcdCli, err = clientv3.New(clientv3.Config{
		Endpoints:   advertiseAddrs,
		DialTimeout: 3 * time.Second,
	})
	require.Nil(t, err)

	cleanFn = func() {
		for _, dir := range dirs {
			os.RemoveAll(dir)
		}
		for _, etcd := range embedEtcds {
			etcd.Close()
		}
	}

	return
}
