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

package memory

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	defaultMetricInterval = time.Second * 15
)

var (
	entrySorterResolvedChanSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "puller",
			Name:      "entry_sorter_resolved_chan_size",
			Help:      "Puller entry sorter resolved channel size",
		}, []string{"namespace", "changefeed", "table"})
	entrySorterOutputChanSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "puller",
			Name:      "entry_sorter_output_chan_size",
			Help:      "Puller entry sorter output channel size",
		}, []string{"namespace", "changefeed", "table"})
	entrySorterUnsortedSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "puller",
			Name:      "entry_sorter_unsorted_size",
			Help:      "Puller entry sorter unsorted items size",
		}, []string{"namespace", "changefeed", "table"})
	entrySorterSortDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "puller",
			Name:      "entry_sorter_sort",
			Help:      "Bucketed histogram of processing time (s) of sort in entry sorter.",
			Buckets:   prometheus.ExponentialBuckets(0.000001, 10, 10),
		}, []string{"namespace", "changefeed", "table"})
	entrySorterMergeDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "puller",
			Name:      "entry_sorter_merge",
			Help:      "Bucketed histogram of processing time (s) of merge in entry sorter.",
			Buckets:   prometheus.ExponentialBuckets(0.000001, 10, 10),
		}, []string{"namespace", "changefeed", "table"})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(entrySorterResolvedChanSizeGauge)
	registry.MustRegister(entrySorterOutputChanSizeGauge)
	registry.MustRegister(entrySorterUnsortedSizeGauge)
	registry.MustRegister(entrySorterSortDuration)
	registry.MustRegister(entrySorterMergeDuration)
}
