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

package metricsproxy // nolint:dupl

import (
	"math/rand"
	"time"

	. "github.com/pingcap/check"

	"github.com/prometheus/client_golang/prometheus"
)

func (t *testMetricsProxySuite) TestGaugeVecProxy(c *C) {
	rand.Seed(time.Now().UnixNano())
	for _, oneCase := range testCases {
		gauge := NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   "dm",
			Subsystem:   "metricsProxy",
			Name:        "Test_Gauge",
			Help:        "dm gauge metrics proxy test",
			ConstLabels: nil,
		}, oneCase.LabelsNames)
		for _, aArgs := range oneCase.AddArgs {
			if rand.Intn(199)%2 == 0 {
				gauge.WithLabelValues(aArgs...).Add(float64(rand.Intn(199)))
			} else {
				labels := make(prometheus.Labels)
				for k, labelName := range oneCase.LabelsNames {
					labels[labelName] = aArgs[k]
				}
				gauge.With(labels)
			}
		}
		for _, dArgs := range oneCase.DeleteArgs {
			gauge.DeleteAllAboutLabels(dArgs)
		}

		cOutput := make(chan prometheus.Metric, len(oneCase.AddArgs)*3)

		gauge.Collect(cOutput)

		c.Assert(len(cOutput), Equals, oneCase.WantResLength)
	}
}
