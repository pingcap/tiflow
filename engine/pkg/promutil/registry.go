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
	"sync"

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	dto "github.com/prometheus/client_model/go"
)

var _ prometheus.Gatherer = globalMetricGatherer

// NOTICE: we don't use prometheus.DefaultRegistry in case of incorrect usage of a
// non-wrapped metric by app(user)
var (
	globalMetricRegistry                     = NewRegistry()
	globalMetricGatherer prometheus.Gatherer = globalMetricRegistry
)

func init() {
	globalMetricRegistry.MustRegister(systemID, collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	globalMetricRegistry.MustRegister(systemID, collectors.NewGoCollector(
		collectors.WithGoCollections(collectors.GoRuntimeMemStatsCollection|collectors.GoRuntimeMetricsCollection)))
}

// Registry is used for registering metric
type Registry struct {
	mu       sync.Mutex
	registry *prometheus.Registry

	// collectorByWorker is for cleaning all collectors for specific worker(jobmaster/worker)
	collectorByWorker map[frameModel.WorkerID][]prometheus.Collector
}

// NewRegistry return a new Registry
func NewRegistry() *Registry {
	return &Registry{
		registry:          prometheus.NewRegistry(),
		collectorByWorker: make(map[frameModel.WorkerID][]prometheus.Collector),
	}
}

// MustRegister registers the provided Collector of the specified worker
func (r *Registry) MustRegister(workerID frameModel.WorkerID, c prometheus.Collector) {
	if c == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	r.registry.MustRegister(c)

	var (
		cls    []prometheus.Collector
		exists bool
	)
	cls, exists = r.collectorByWorker[workerID]
	if !exists {
		cls = make([]prometheus.Collector, 0)
	}
	cls = append(cls, c)
	r.collectorByWorker[workerID] = cls
}

// Unregister unregisters all Collectors of the specified worker
func (r *Registry) Unregister(workerID frameModel.WorkerID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	cls, exists := r.collectorByWorker[workerID]
	if exists {
		for _, collector := range cls {
			r.registry.Unregister(collector)
		}
		delete(r.collectorByWorker, workerID)
	}
}

// Gather implements Gatherer interface
func (r *Registry) Gather() ([]*dto.MetricFamily, error) {
	// NOT NEED lock here. prometheus.Registry has thread-safe methods
	return r.registry.Gather()
}

// AutoRegisterFactory uses inner Factory to create metrics and register metrics
// to Registry with id. Panic if it can't register successfully.
type AutoRegisterFactory struct {
	inner Factory
	r     *Registry
	// ID identify the worker(jobmaster/worker) the factory owns
	// It's used to unregister all collectors when worker exits normally or commits suicide
	id frameModel.WorkerID
}

// NewAutoRegisterFactory creates an AutoRegisterFactory.
func NewAutoRegisterFactory(f Factory, r *Registry, id frameModel.WorkerID) Factory {
	return &AutoRegisterFactory{
		inner: f,
		r:     r,
		id:    id,
	}
}

// NewCounter implements Factory.NewCounter.
func (f *AutoRegisterFactory) NewCounter(opts prometheus.CounterOpts) prometheus.Counter {
	c := f.inner.NewCounter(opts)
	f.r.MustRegister(f.id, c)
	return c
}

// NewCounterVec implements Factory.NewCounterVec.
func (f *AutoRegisterFactory) NewCounterVec(opts prometheus.CounterOpts, labelNames []string) *prometheus.CounterVec {
	c := f.inner.NewCounterVec(opts, labelNames)
	f.r.MustRegister(f.id, c)
	return c
}

// NewGauge implements Factory.NewGauge.
func (f *AutoRegisterFactory) NewGauge(opts prometheus.GaugeOpts) prometheus.Gauge {
	c := f.inner.NewGauge(opts)
	f.r.MustRegister(f.id, c)
	return c
}

// NewGaugeVec implements Factory.NewGaugeVec.
func (f *AutoRegisterFactory) NewGaugeVec(opts prometheus.GaugeOpts, labelNames []string) *prometheus.GaugeVec {
	c := f.inner.NewGaugeVec(opts, labelNames)
	f.r.MustRegister(f.id, c)
	return c
}

// NewHistogram implements Factory.NewHistogram.
func (f *AutoRegisterFactory) NewHistogram(opts prometheus.HistogramOpts) prometheus.Histogram {
	c := f.inner.NewHistogram(opts)
	f.r.MustRegister(f.id, c)
	return c
}

// NewHistogramVec implements Factory.NewHistogramVec.
func (f *AutoRegisterFactory) NewHistogramVec(opts prometheus.HistogramOpts, labelNames []string) *prometheus.HistogramVec {
	c := f.inner.NewHistogramVec(opts, labelNames)
	f.r.MustRegister(f.id, c)
	return c
}

// RegOnlyRegister can Register collectors but can't Unregister. It's only used in tests.
type RegOnlyRegister struct {
	r prometheus.Registerer
}

// NewOnlyRegRegister creates an RegOnlyRegister.
func NewOnlyRegRegister(r prometheus.Registerer) prometheus.Registerer {
	return &RegOnlyRegister{r}
}

// Register implements prometheus.Registerer.
func (o RegOnlyRegister) Register(collector prometheus.Collector) error {
	return o.r.Register(collector)
}

// MustRegister implements prometheus.Registerer.
func (o RegOnlyRegister) MustRegister(collector ...prometheus.Collector) {
	o.r.MustRegister(collector...)
}

// Unregister implements prometheus.Registerer.
func (o RegOnlyRegister) Unregister(collector prometheus.Collector) bool {
	return false
}

// UnregOnlyRegister can Unregister collectors but can't Register. It's used to
// protect the global registry.
type UnregOnlyRegister struct {
	r prometheus.Registerer
}

// NewOnlyUnregRegister creates an UnregOnlyRegister.
func NewOnlyUnregRegister(r prometheus.Registerer) prometheus.Registerer {
	return &UnregOnlyRegister{r}
}

// Register implements prometheus.Registerer.
func (o UnregOnlyRegister) Register(collector prometheus.Collector) error {
	return nil
}

// MustRegister implements prometheus.Registerer.
func (o UnregOnlyRegister) MustRegister(collector ...prometheus.Collector) {}

// Unregister implements prometheus.Registerer.
func (o UnregOnlyRegister) Unregister(collector prometheus.Collector) bool {
	return o.r.Unregister(collector)
}
