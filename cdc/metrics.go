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

package cdc

import (
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/owner"
	"github.com/pingcap/ticdc/cdc/processor"
	tablepipeline "github.com/pingcap/ticdc/cdc/processor/pipeline"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/cdc/puller/sorter"
	"github.com/pingcap/ticdc/cdc/sink"
<<<<<<< HEAD
	"github.com/pingcap/ticdc/pkg/config"
=======
	"github.com/pingcap/ticdc/cdc/sorter"
	"github.com/pingcap/ticdc/cdc/sorter/leveldb"
	"github.com/pingcap/ticdc/cdc/sorter/memory"
	"github.com/pingcap/ticdc/cdc/sorter/unified"
	"github.com/pingcap/ticdc/pkg/actor"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/orchestrator"
>>>>>>> 2569abaa3 (etcd_worker: batch etcd patch (#3277))
	"github.com/prometheus/client_golang/prometheus"
)

var registry = prometheus.NewRegistry()

func init() {
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	registry.MustRegister(prometheus.NewGoCollector())

	kv.InitMetrics(registry)
	puller.InitMetrics(registry)
	sink.InitMetrics(registry)
	entry.InitMetrics(registry)
<<<<<<< HEAD
=======
	processor.InitMetrics(registry)
	tablepipeline.InitMetrics(registry)
	owner.InitMetrics(registry)
	etcd.InitMetrics(registry)
	initServerMetrics(registry)
	actor.InitMetrics(registry)
	orchestrator.InitMetrics(registry)
	// Sorter metrics
>>>>>>> 2569abaa3 (etcd_worker: batch etcd patch (#3277))
	sorter.InitMetrics(registry)
	if config.NewReplicaImpl {
		processor.InitMetrics(registry)
		tablepipeline.InitMetrics(registry)
		owner.InitMetrics(registry)
	} else {
		initProcessorMetrics(registry)
		initOwnerMetrics(registry)
	}
	initServerMetrics(registry)
}
