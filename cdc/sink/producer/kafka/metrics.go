// Copyright 2022 PingCAP, Inc.
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

package kafka

import (
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rcrowley/go-metrics"
	"go.uber.org/zap"
)

// saramaMetrics wrap prometheus metrics, update value from sarama metrics.
type saramaMetrics struct {
	// metricsName is used to fetch sarama metrics
	metricsName string
	collector   prometheus.Collector
}

var (
	// `Histogram`
	batchSize = saramaMetrics{
		metricsName: "batch-size",
		collector: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "ticdc",
				Subsystem: "sink",
				Name:      "kafka_producer_batch_size",
				Help:      "the number of bytes sent per partition per request for all topics",
			}, []string{"capture", "changefeed"}),
	}

	// `Meter`
	recordSendRate = saramaMetrics{
		metricsName: "record-send-rate",
		collector: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "ticdc",
				Subsystem: "sink",
				Name:      "kafka_producer_record_send_rate",
				Help:      "Records/second sent to all topics",
			}, []string{"capture", "changefeed"}),
	}

	// `Histogram`
	recordsPerRequest = saramaMetrics{
		metricsName: "records-per-request",
		collector: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "ticdc",
				Subsystem: "sink",
				Name:      "kafka_producer_records_per_request",
				Help:      "the number of records sent per request for all topics",
			}, []string{"capture", "changefeed"}),
	}
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(batchSize.collector)
	registry.MustRegister(recordSendRate.collector)
	registry.MustRegister(recordsPerRequest.collector)
}

type saramaMetricsMonitor struct {
	captureAddr  string
	changefeedID string

	registry metrics.Registry
	metrics  []saramaMetrics
}

// Refresh all monitored metrics
func (sm *saramaMetricsMonitor) Refresh() {
	for _, m := range sm.metrics {
		m.refresh(sm.registry)
	}
}

func NewSaramaMetricsMonitor(registry metrics.Registry, captureAddr, changefeedID string) *saramaMetricsMonitor {
	metrics := make([]saramaMetrics, 0)
	metrics = append(metrics, batchSize.withLabelValues(captureAddr, changefeedID))
	metrics = append(metrics, recordSendRate.withLabelValues(captureAddr, changefeedID))
	metrics = append(metrics, recordsPerRequest.withLabelValues(captureAddr, changefeedID))
	return &saramaMetricsMonitor{
		captureAddr:  captureAddr,
		changefeedID: changefeedID,
		registry:     registry,
		metrics:      metrics,
	}
}

func (sm *saramaMetricsMonitor) Cleanup() {
	for _, item := range sm.metrics {
		item.deleteLabelValues(sm.captureAddr, sm.changefeedID)
	}
}

func (m saramaMetrics) withLabelValues(labels ...string) saramaMetrics {
	var collector prometheus.Collector
	switch tp := m.collector.(type) {
	case *prometheus.HistogramVec:
		collector = tp.WithLabelValues(labels...).(prometheus.Collector)
	case *prometheus.GaugeVec:
		collector = tp.WithLabelValues(labels...).(prometheus.Collector)
	case *prometheus.CounterVec:
		collector = tp.WithLabelValues(labels...).(prometheus.Collector)
	default:
		log.Panic("unsupported prometheus collector type", zap.Any("tp", tp))
	}

	return saramaMetrics{
		m.metricsName,
		collector,
	}
}

func (m saramaMetrics) deleteLabelValues(labels ...string) {
	switch tp := m.collector.(type) {
	case *prometheus.HistogramVec:
		tp.DeleteLabelValues(labels...)
	case *prometheus.GaugeVec:
		tp.DeleteLabelValues(labels...)
	case *prometheus.CounterVec:
		tp.DeleteLabelValues(labels...)
	}
}

func (m saramaMetrics) refresh(registry metrics.Registry) {
	// fetch sarama metrics
	rawMetric := registry.Get(m.metricsName)
	if rawMetric == nil {
		return
	}

	var value float64
	switch tp := rawMetric.(type) {
	case metrics.Meter:
		// `Meter` record the moving average of all recorded values.
		// use `Rate1` the `one-minute moving average rate of events per second`
		value = tp.Snapshot().Rate1()
	case metrics.Histogram:
		// `Histogram` record the distribution of recorded values.
		// use `Mean` at the moment.
		value = tp.Snapshot().Mean()
		//case metrics.Counter:
		//	value = float64(tp.Snapshot().Count())
		//case metrics.Gauge:
		//	value = float64(tp.Snapshot().Value())
	}

	// update prometheus metrics
	switch tp := m.collector.(type) {
	case prometheus.Histogram:
		tp.Observe(value)
	case prometheus.Gauge:
		tp.Set(value)
	default:
		log.Panic("unsupported prometheus collector type", zap.Any("tp", tp))
	}
}

//func printMetrics(w io.Writer, r metrics.Registry) {
//	recordSendRateMetric := r.Get("record-send-rate")
//	requestLatencyMetric := r.Get("request-latency-in-ms")
//	outgoingByteRateMetric := r.Get("outgoing-byte-rate")
//	requestsInFlightMetric := r.Get("requests-in-flight")
//
//	if recordSendRateMetric == nil || requestLatencyMetric == nil || outgoingByteRateMetric == nil ||
//		requestsInFlightMetric == nil {
//		return
//	}
//	recordSendRate := recordSendRateMetric.(metrics.Meter).Snapshot()
//	requestLatency := requestLatencyMetric.(metrics.Histogram).Snapshot()
//	requestLatencyPercentiles := requestLatency.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
//	outgoingByteRate := outgoingByteRateMetric.(metrics.Meter).Snapshot()
//	requestsInFlight := requestsInFlightMetric.(metrics.Counter).Count()
//	fmt.Fprintf(w, "%d records sent, %.1f records/sec (%.2f MiB/sec ingress, %.2f MiB/sec egress), "+
//		"%.1f ms avg latency, %.1f ms stddev, %.1f ms 50th, %.1f ms 75th, "+
//		"%.1f ms 95th, %.1f ms 99th, %.1f ms 99.9th, %d total req. in flight\n",
//		recordSendRate.Count(),
//		recordSendRate.RateMean(),
//		recordSendRate.RateMean()*float64(1000)/1024/1024,
//		outgoingByteRate.RateMean()/1024/1024,
//		requestLatency.Mean(),
//		requestLatency.StdDev(),
//		requestLatencyPercentiles[0],
//		requestLatencyPercentiles[1],
//		requestLatencyPercentiles[2],
//		requestLatencyPercentiles[3],
//		requestLatencyPercentiles[4],
//		requestsInFlight,
//	)
//}
