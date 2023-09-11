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

package etcd

import "github.com/prometheus/client_golang/prometheus"

var etcdRequestCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "etcd",
		Name:      "request_count",
		Help:      "request counter of etcd operation",
	}, []string{"type"})

var etcdStateGauge = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "etcd",
		Name:      "etcd_client",
		Help:      "Etcd client states.",
	}, []string{"type"})

// InitMetrics registers the etcd request counter.
func InitMetrics(registry *prometheus.Registry) {
	prometheus.MustRegister(etcdStateGauge)
	registry.MustRegister(etcdRequestCounter)
}
