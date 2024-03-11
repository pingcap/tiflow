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

package server

import (
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/cdc/processor"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/cdc/puller"
	"github.com/pingcap/tiflow/cdc/puller/memorysorter"
	redo "github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/sink/observer"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	tikvmetrics "github.com/tikv/client-go/v2/metrics"
)

var registry = prometheus.NewRegistry()

func init() {
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	registry.MustRegister(prometheus.NewGoCollector(
		collectors.WithGoCollections(collectors.GoRuntimeMemStatsCollection | collectors.GoRuntimeMetricsCollection)))

	initServerMetrics(registry)
	kv.InitMetrics(registry)
	puller.InitMetrics(registry)
	metrics.InitMetrics(registry)
	entry.InitMetrics(registry)
	processor.InitMetrics(registry)
	owner.InitMetrics(registry)
	etcd.InitMetrics(registry)
	orchestrator.InitMetrics(registry)
	p2p.InitMetrics(registry)
	engine.InitMetrics(registry)
	memorysorter.InitMetrics(registry)
	redo.InitMetrics(registry)
	scheduler.InitMetrics(registry)
	observer.InitMetrics(registry)
	gc.InitMetrics(registry)
	// TiKV client metrics, including metrics about resolved and region cache.
	originalRegistry := prometheus.DefaultRegisterer
	prometheus.DefaultRegisterer = registry
	tikvmetrics.InitMetrics("ticdc", "tikvclient")
	tikvmetrics.RegisterMetrics()
	prometheus.DefaultRegisterer = originalRegistry
}
