// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package cloudstorage

import "github.com/prometheus/client_golang/prometheus"

const (
	namespace = "ticdc"
	subsystem = "sink"
)

// Metrics for cloud storage sink
var (
	// CloudStorageWriteBytesGauge records the total number of bytes written to cloud storage.
	CloudStorageWriteBytesGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "cloud_storage_write_bytes_total",
		Help:      "Total number of bytes written to cloud storage",
	}, []string{"namespace", "changefeed"})

	// CloudStorageFileCountGauge records the number of files generated by cloud storage sink.
	CloudStorageFileCountGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "cloud_storage_file_count",
		Help:      "Total number of files managed by a cloud storage sink",
	}, []string{"namespace", "changefeed"})
)

// InitMetrics registers all metrics in this file.
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(CloudStorageWriteBytesGauge)
	registry.MustRegister(CloudStorageFileCountGauge)
}
