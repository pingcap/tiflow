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

package servermaster

import (
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	serverFactory          = promutil.NewFactory4Framework()
	serverExecutorNumGauge = serverFactory.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tiflow",
			Subsystem: "server_master",
			Name:      "executor_num",
			Help:      "number of executor servers in this cluster",
		}, []string{"status"})
	serverJobNumGauge = serverFactory.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tiflow",
			Subsystem: "server_master",
			Name:      "job_num",
			Help:      "number of jobs in this cluster",
		}, []string{"status"})
)
