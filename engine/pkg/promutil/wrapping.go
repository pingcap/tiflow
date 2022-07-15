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

package promutil

import (
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// WrappingFactory uses inner Factory to create metrics and attaches some information
// to the metrics it created.
// The usage is dataflow engine can wrap prefix and labels for DM, and DM can wrap
// labels for dumpling/lightning.
type WrappingFactory struct {
	inner       Factory
	prefix      string
	constLabels prometheus.Labels
}

// NewWrappingFactory creates a WrappingFactory.
// if `prefix` is not empty, it is added to the metric name to avoid cross app metric
// conflict, e.g. $prefix_$namespace_$subsystem_$name.
// `labels` is added to user metric when create metrics.
func NewWrappingFactory(f Factory, prefix string, labels prometheus.Labels) Factory {
	return &WrappingFactory{
		inner:       f,
		prefix:      prefix,
		constLabels: labels,
	}
}

// NewCounter works like the function of the same name in the prometheus package
// except for it will wrap prefix and constLabels. Thread-safe.
func (f *WrappingFactory) NewCounter(opts prometheus.CounterOpts) prometheus.Counter {
	return f.inner.NewCounter(*wrapCounterOpts(f.prefix, f.constLabels, &opts))
}

// NewCounterVec works like the function of the same name in the prometheus package
// except for it will wrap prefix and constLabels. Thread-safe.
func (f *WrappingFactory) NewCounterVec(opts prometheus.CounterOpts, labelNames []string) *prometheus.CounterVec {
	return f.inner.NewCounterVec(*wrapCounterOpts(f.prefix, f.constLabels, &opts), labelNames)
}

// NewGauge works like the function of the same name in the prometheus package
// except for it will wrap prefix and constLabels. Thread-safe.
func (f *WrappingFactory) NewGauge(opts prometheus.GaugeOpts) prometheus.Gauge {
	return f.inner.NewGauge(*wrapGaugeOpts(f.prefix, f.constLabels, &opts))
}

// NewGaugeVec works like the function of the same name in the prometheus package
// except for it will wrap prefix and constLabels. Thread-safe.
func (f *WrappingFactory) NewGaugeVec(opts prometheus.GaugeOpts, labelNames []string) *prometheus.GaugeVec {
	return f.inner.NewGaugeVec(*wrapGaugeOpts(f.prefix, f.constLabels, &opts), labelNames)
}

// NewHistogram works like the function of the same name in the prometheus package
// except for it will wrap prefix and constLabels. Thread-safe.
func (f *WrappingFactory) NewHistogram(opts prometheus.HistogramOpts) prometheus.Histogram {
	return f.inner.NewHistogram(*wrapHistogramOpts(f.prefix, f.constLabels, &opts))
}

// NewHistogramVec works like the function of the same name in the prometheus package
// except for it will wrap prefix and constLabels. Thread-safe.
func (f *WrappingFactory) NewHistogramVec(opts prometheus.HistogramOpts, labelNames []string) *prometheus.HistogramVec {
	return f.inner.NewHistogramVec(*wrapHistogramOpts(f.prefix, f.constLabels, &opts), labelNames)
}

func wrapCounterOpts(prefix string, constLabels prometheus.Labels, opts *prometheus.CounterOpts) *prometheus.CounterOpts {
	if opts.ConstLabels == nil && constLabels != nil {
		opts.ConstLabels = make(prometheus.Labels)
	}
	wrapOptsCommon(prefix, constLabels, &opts.Namespace, opts.ConstLabels)
	return opts
}

func wrapGaugeOpts(prefix string, constLabels prometheus.Labels, opts *prometheus.GaugeOpts) *prometheus.GaugeOpts {
	if opts.ConstLabels == nil && constLabels != nil {
		opts.ConstLabels = make(prometheus.Labels)
	}
	wrapOptsCommon(prefix, constLabels, &opts.Namespace, opts.ConstLabels)
	return opts
}

func wrapHistogramOpts(prefix string, constLabels prometheus.Labels, opts *prometheus.HistogramOpts) *prometheus.HistogramOpts {
	if opts.ConstLabels == nil && constLabels != nil {
		opts.ConstLabels = make(prometheus.Labels)
	}
	wrapOptsCommon(prefix, constLabels, &opts.Namespace, opts.ConstLabels)
	return opts
}

func wrapOptsCommon(prefix string, constLabels prometheus.Labels, namespace *string, cls prometheus.Labels) {
	// namespace SHOULD NOT be nil
	if prefix != "" {
		if *namespace != "" {
			*namespace = prefix + "_" + *namespace
		} else {
			*namespace = prefix
		}
	}
	for name, value := range constLabels {
		if _, exists := cls[name]; exists {
			log.Panic("duplicate label name", zap.String("label", name))
		}
		cls[name] = value
	}
}
