package replication

import (
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"
)

type ownerTestHarness struct {
	server *embed.Etcd
	newClient func() *etcd.Client
}

func newOwnerTestHarness(etcdServerDir string) *ownerTestHarness {
	url, server, err := etcd.SetupEmbedEtcd(etcdServerDir)
	if err != nil {
		log.Panic("Could not start embedded Etcd server", zap.Error(err))
	}

	endpoints := []string{url.String()}
	newClientFunc := func() *etcd.Client {
		rawCli, err := clientv3.NewFromURLs(endpoints)
		if err != nil {
			log.Panic("Could not create Etcd client", zap.Error(err))
		}
		return etcd.Wrap(rawCli, map[string]prometheus.Counter{})
	}

	return &ownerTestHarness{
		server:    server,
		newClient: newClientFunc,
	}
}
