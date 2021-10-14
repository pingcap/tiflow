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

package kv

import (
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	grpcMetrics = grpc_prometheus.NewClientMetrics()

	eventFeedErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "event_feed_error_count",
			Help:      "The number of error return by tikv",
		}, []string{"type"})
	eventFeedGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "event_feed_count",
			Help:      "The number of event feed running",
		})
	scanRegionsDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "scan_regions_duration_seconds",
			Help:      "The time it took to finish a scanRegions call.",
			Buckets:   prometheus.ExponentialBuckets(0.001 /* 1 ms */, 2, 18),
		}, []string{"capture"})
	eventSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "event_size_bytes",
			Help:      "Size of KV events.",
			Buckets:   prometheus.ExponentialBuckets(16, 2, 25),
		}, []string{"capture", "type"})
	pullEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "pull_event_count",
			Help:      "event count received by this puller",
		}, []string{"type", "capture", "changefeed"})
	sendEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "send_event_count",
			Help:      "event count sent to event channel by this puller",
		}, []string{"type", "capture", "changefeed"})
	clientChannelSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "channel_size",
			Help:      "size of each channel in kv client",
		}, []string{"channel"})
	clientRegionTokenSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "region_token",
			Help:      "size of region token in kv client",
		}, []string{"store", "changefeed", "capture"})
	cachedRegionSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "cached_region",
			Help:      "cached region that has not requested to TiKV in kv client",
		}, []string{"store", "changefeed", "capture"})
	batchResolvedEventSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "batch_resolved_event_size",
			Help:      "The number of region in one batch resolved ts event",
			Buckets:   prometheus.ExponentialBuckets(2, 2, 16),
		}, []string{"capture", "changefeed"})
	etcdRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "etcd",
			Name:      "request_count",
			Help:      "request counter of etcd operation",
		}, []string{"type", "capture"})
	grpcPoolStreamGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "grpc_stream_count",
			Help:      "active stream count of each gRPC connection",
		}, []string{"store"})
)

// InitMetrics registers all metrics in the kv package
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(eventFeedErrorCounter)
	registry.MustRegister(scanRegionsDuration)
	registry.MustRegister(eventSize)
	registry.MustRegister(eventFeedGauge)
	registry.MustRegister(pullEventCounter)
	registry.MustRegister(sendEventCounter)
	registry.MustRegister(clientChannelSize)
	registry.MustRegister(clientRegionTokenSize)
	registry.MustRegister(cachedRegionSize)
	registry.MustRegister(batchResolvedEventSize)
	registry.MustRegister(etcdRequestCounter)
	registry.MustRegister(grpcPoolStreamGauge)

	// Register client metrics to registry.
	registry.MustRegister(grpcMetrics)
}
