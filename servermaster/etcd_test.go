package servermaster

import (
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/pkg/etcdutils"
	"github.com/phayes/freeport"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
)

func init() {
	err := log.InitLogger(&log.Config{Level: "warn"})
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
	dir, err := ioutil.TempDir("", name1)
	require.Nil(t, err)

	masterAddr := allocTempURL(t)
	advertiseAddr := masterAddr
	cfgCluster := &etcdutils.ConfigParams{}
	cfgCluster.Name = name1
	cfgCluster.DataDir = dir
	peer1 := allocTempURL(t)
	peer2 := allocTempURL(t)
	cfgCluster.PeerUrls = "http://" + peer1
	cfgCluster.InitialCluster = fmt.Sprintf("%s=http://%s,%s=http://%s", name1, peer1, name2, peer2)
	cfgCluster.Adjust("", embed.ClusterStateFlagNew)

	cfgClusterEtcd := etcdutils.GenEmbedEtcdConfigWithLogger("info")
	cfgClusterEtcd, err = etcdutils.GenEmbedEtcdConfig(cfgClusterEtcd, masterAddr, advertiseAddr, cfgCluster)
	require.Nil(t, err)

	_, err = etcdutils.StartEtcd(cfgClusterEtcd, nil, nil, time.Millisecond*100)
	require.EqualError(t, err, "[DFLOW:ErrMasterStartEmbedEtcdFail]start embed etcd timeout 100ms")
}
