// Copyright 2023 PingCAP, Inc.
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

package common

import (
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/prometheus/client_golang/prometheus"
)

var compressionRatio = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "kafka_sink",
		Name:      "codec_compression_ratio",
		Help:      "The TiCDC kafka sink codec compression ratio",
		Buckets:   prometheus.LinearBuckets(0, 1, 20),
	}, []string{"namespace", "changefeed"})

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(compressionRatio)
}

// CleanMetrics remove metrics belong to the given changefeed.
func CleanMetrics(changefeedID model.ChangeFeedID) {
	compressionRatio.DeleteLabelValues(changefeedID.Namespace, changefeedID.ID)
}
