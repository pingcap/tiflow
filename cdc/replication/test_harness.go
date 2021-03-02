// Copyright 2021 PingCAP, Inc.
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

package replication

import (
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/replication/mock"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	kv2 "github.com/pingcap/tidb/kv"
	"github.com/prometheus/client_golang/prometheus"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"
)

type ownerTestHarness struct {
	server    *embed.Etcd
	newClient func() *etcd.Client

	kvStore kv2.Storage

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

func (h *ownerTestHarness) CreateOwner(startTs uint64) *Owner {
	state := newCDCReactorState()

	pdClient := h.pdClient
	if pdClient == nil {
		pdClient = mock.NewMockPDClient(startTs - 300)
	}

	bootstrapper := h.bootstrapper
	if bootstrapper == nil {
		bootstrapper = newBootstrapper(pdClient, nil)
	}

	cfManager := h.cfManager
	if cfManager == nil {
		cfManager = newChangeFeedManager(state, bootstrapper)
	}

	gcManager := h.gcManager
	if gcManager == nil {
		gcManager = newGCManager(pdClient, 600)
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
