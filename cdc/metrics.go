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
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/cdc/processor"
	tablepipeline "github.com/pingcap/tiflow/cdc/processor/pipeline"
	"github.com/pingcap/tiflow/cdc/puller"
	redowriter "github.com/pingcap/tiflow/cdc/redo/writer"
	"github.com/pingcap/tiflow/cdc/sink"
	"github.com/pingcap/tiflow/cdc/sink/producer/kafka"
	"github.com/pingcap/tiflow/cdc/sorter/memory"
	"github.com/pingcap/tiflow/cdc/sorter/unified"
	"github.com/pingcap/tiflow/pkg/actor"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/prometheus/client_golang/prometheus"
	tikvmetrics "github.com/tikv/client-go/v2/metrics"
)

var registry = prometheus.NewRegistry()

func init() {
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	registry.MustRegister(prometheus.NewGoCollector())

	kv.InitMetrics(registry)
	puller.InitMetrics(registry)
	sink.InitMetrics(registry)
	entry.InitMetrics(registry)
	processor.InitMetrics(registry)
	tablepipeline.InitMetrics(registry)
	owner.InitMetrics(registry)
	etcd.InitMetrics(registry)
	initServerMetrics(registry)
	actor.InitMetrics(registry)
	orchestrator.InitMetrics(registry)
	// Sorter metrics
	memory.InitMetrics(registry)
	unified.InitMetrics(registry)
	redowriter.InitMetrics(registry)
	kafka.InitMetrics(registry)
	// TiKV client metrics, including metrics about resolved and region cache.
	originalRegistry := prometheus.DefaultRegisterer
	prometheus.DefaultRegisterer = registry
	tikvmetrics.InitMetrics("ticdc", "tikvclient")
	tikvmetrics.RegisterMetrics()
	prometheus.DefaultRegisterer = originalRegistry
}
