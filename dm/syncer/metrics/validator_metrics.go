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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/pingcap/tiflow/dm/pkg/metricsproxy"
	"github.com/pingcap/tiflow/engine/pkg/promutil"
)

var defaultFactory = &promutil.PromFactory{}

var (
	validatorErrorCount = metricsproxy.NewGaugeVec(defaultFactory,
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "validator",
			Name:      "validator_error_count",
			Help:      "total number of validator errors",
		}, []string{"task", "source_id"})

	validatorLogPosLatency = metricsproxy.NewGaugeVec(defaultFactory,
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "validator",
			Name:      "validator_logpos_latency",
			Help:      "the log pos latency between validator and syncer",
		}, []string{"task", "source_id"})
	validatorLogFileLatency = metricsproxy.NewGaugeVec(defaultFactory,
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "validator",
			Name:      "validator_logfile_latency",
			Help:      "the log file latency between validator and syncer",
		}, []string{"task", "source_id"})

	validatorBinlogPos = metricsproxy.NewGaugeVec(defaultFactory,
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "validator",
			Name:      "validator_binlog_pos",
			Help:      "binlog position of the validator",
		}, []string{"task", "source_id"})

	validatorBinlogFile = metricsproxy.NewGaugeVec(defaultFactory,
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "validator",
			Name:      "validator_binlog_file",
			Help:      "current binlog file of the validator",
		}, []string{"task", "source_id"})
)

func RegisterValidatorMetrics(registry *prometheus.Registry) {
	registry.MustRegister(validatorErrorCount)
	registry.MustRegister(validatorLogPosLatency)
	registry.MustRegister(validatorLogFileLatency)
	registry.MustRegister(validatorBinlogPos)
	registry.MustRegister(validatorBinlogFile)
}

func RemoveValidatorLabelValuesWithTask(task string) {
	validatorErrorCount.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	validatorLogPosLatency.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	validatorLogFileLatency.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	validatorBinlogPos.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	validatorBinlogFile.DeleteAllAboutLabels(prometheus.Labels{"task": task})
}

type ValidatorMetrics struct {
	ErrorCount     prometheus.Gauge
	LogPosLatency  prometheus.Gauge
	LogFileLatency prometheus.Gauge
	BinlogFile     prometheus.Gauge
	BinlogPos      prometheus.Gauge
}

func NewValidatorMetrics(taskName, sourceID string) *ValidatorMetrics {
	return &ValidatorMetrics{
		BinlogPos:      validatorBinlogPos.WithLabelValues(taskName, sourceID),
		BinlogFile:     validatorBinlogFile.WithLabelValues(taskName, sourceID),
		LogPosLatency:  validatorLogPosLatency.WithLabelValues(taskName, sourceID),
		LogFileLatency: validatorLogFileLatency.WithLabelValues(taskName, sourceID),
		ErrorCount:     validatorErrorCount.WithLabelValues(taskName, sourceID),
	}
}
