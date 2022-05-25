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
)

var (
	ValidatorErrorCount = metricsproxy.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "validator",
			Name:      "validator_error_count",
			Help:      "total number of validator errors",
		}, []string{"task", "source_id", "worker"})

	ValidatorLogPosLatency = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "validator",
			Name:      "validator_logpos_latency",
			Help:      "the log pos latency between validator and syncer",
		}, []string{"task", "source_id", "worker"})
	ValidatorLogFileLatency = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "validator",
			Name:      "validator_logfile_latency",
			Help:      "the log file latency between validator and syncer",
		}, []string{"task", "source_id", "worker"})

	ValidatorBinlogPos = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "validator",
			Name:      "validator_binlog_pos",
			Help:      "binlog position of the validator",
		}, []string{"task", "source_id", "worker"})

	ValidatorBinlogFile = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "validator",
			Name:      "validator_binlog_file",
			Help:      "current binlog file of the validator",
		}, []string{"task", "source_id", "worker"})
)

func RegisterValidatorMetrics(registry *prometheus.Registry) {
	registry.MustRegister(ValidatorErrorCount)
	registry.MustRegister(ValidatorLogPosLatency)
	registry.MustRegister(ValidatorLogFileLatency)
	registry.MustRegister(ValidatorBinlogPos)
	registry.MustRegister(ValidatorBinlogFile)
}

func RemoveValidatorLabelValuesWithTask(task string) {
	ValidatorErrorCount.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	ValidatorLogPosLatency.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	ValidatorLogFileLatency.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	ValidatorBinlogPos.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	ValidatorBinlogFile.DeleteAllAboutLabels(prometheus.Labels{"task": task})
}
