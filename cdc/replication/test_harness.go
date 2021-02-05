package replication

import (
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/prometheus/client_golang/prometheus"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"
	"time"
)

type ownerTestHarness struct {
	server    *embed.Etcd
	newClient func() *etcd.Client

	pdClient     pd.Client
	bootstrapper changeFeedBootstrapper
	cfManager    changeFeedManager
	gcManager    *gcManager
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

func (h *ownerTestHarness) CreateOwner() *Owner {
	state := newCDCReactorState()

	pdClient := h.pdClient
	if pdClient == nil {
		pdClient =
	}

	bootstrapper := h.bootstrapper
	if bootstrapper == nil {
		bootstrapper = newBootstrapper(h.pdClient, nil)
	}

	cfManager := h.cfManager
	if cfManager == nil {
		cfManager = newChangeFeedManager(state, bootstrapper)
	}

	gcManager := h.gcManager
	if gcManager == nil {
		gcManager = newGCManager(h.pdClient, 600)
	}

	reactor := newOwnerReactor(state, cfManager, gcManager)
	etcdWorker, err := orchestrator.NewEtcdWorker(h.newClient(), kv.EtcdKeyBase, reactor, state)
	if err != nil {
		log.Panic("ownerTestHarness: could not create EtcdWorker", zap.Error(err))
	}

	return &Owner{
		etcdWorker:   etcdWorker,
		reactor:      reactor,
		tickInterval: 100 * time.Millisecond,
	}
}
