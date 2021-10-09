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
	"os"
	"runtime"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	etcdHealthCheckDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "etcd_health_check_duration",
			Help:      "Bucketed histogram of processing time (s) of flushing events in processor",
			Buckets:   prometheus.ExponentialBuckets(0.0001 /* 0.1ms */, 2, 18),
		}, []string{"capture", "pd"})

	goGC = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "go_gc",
			Help:      "The value of GOGC",
		})

	goMaxProcs = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "go_max_procs",
			Help:      "The value of GOMAXPROCS",
		})
)

// RecordGoRuntimeSettings records GOGC settings.
func RecordGoRuntimeSettings() {
	// The default GOGC value is 100. See debug.SetGCPercent.
	gogcValue := 100
	if val, err := strconv.Atoi(os.Getenv("GOGC")); err == nil {
		gogcValue = val
	}
	goGC.Set(float64(gogcValue))

	maxProcs := runtime.GOMAXPROCS(0)
	goMaxProcs.Set(float64(maxProcs))
}

// initServerMetrics registers all metrics used in processor
func initServerMetrics(registry *prometheus.Registry) {
	registry.MustRegister(etcdHealthCheckDuration)
	registry.MustRegister(goGC)
	registry.MustRegister(goMaxProcs)
}
