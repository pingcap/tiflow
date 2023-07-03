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

package puller

import (
	"github.com/prometheus/client_golang/prometheus"
)

// PullerEventCounter is the counter of puller's received events
// There are two types of events: kv (row changed event), resolved (resolved ts event).
var PullerEventCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "puller",
		Name:      "txn_collect_event_count", // keep the old name for compatibility
		Help:      "The number of events received by a puller",
	}, []string{"namespace", "changefeed", "type"})

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(PullerEventCounter)
}
