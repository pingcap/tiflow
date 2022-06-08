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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package promutil

import (
	"github.com/prometheus/client_golang/prometheus"
)

// PromFactory implements Factory by calling prometheus.NewXXX.
type PromFactory struct{}

// NewPromFactory creates PromFactory.
func NewPromFactory() Factory {
	return &PromFactory{}
}

// NewCounter implements Factory.NewCounter.
func (f *PromFactory) NewCounter(opts prometheus.CounterOpts) prometheus.Counter {
	return prometheus.NewCounter(opts)
}

// NewCounterVec implements Factory.NewCounterVec.
func (f *PromFactory) NewCounterVec(opts prometheus.CounterOpts, labelNames []string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(opts, labelNames)
}

// NewGauge implements Factory.NewGauge.
func (f *PromFactory) NewGauge(opts prometheus.GaugeOpts) prometheus.Gauge {
	return prometheus.NewGauge(opts)
}

// NewGaugeVec implements Factory.NewGaugeVec.
func (f *PromFactory) NewGaugeVec(opts prometheus.GaugeOpts, labelNames []string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(opts, labelNames)
}

// NewHistogram implements Factory.NewHistogram.
func (f *PromFactory) NewHistogram(opts prometheus.HistogramOpts) prometheus.Histogram {
	return prometheus.NewHistogram(opts)
}

// NewHistogramVec implements Factory.NewHistogramVec.
func (f *PromFactory) NewHistogramVec(opts prometheus.HistogramOpts, labelNames []string) *prometheus.HistogramVec {
	return prometheus.NewHistogramVec(opts, labelNames)
}
