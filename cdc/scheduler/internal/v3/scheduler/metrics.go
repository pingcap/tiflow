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

package scheduler

import (
	"github.com/prometheus/client_golang/prometheus"
)

var scheduleTaskCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "scheduler",
		Name:      "task",
		Help:      "The total number of scheduler tasks",
	}, []string{"namespace", "changefeed", "scheduler", "task"})

// InitMetrics registers all metrics used in scheduler
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(scheduleTaskCounter)
}
