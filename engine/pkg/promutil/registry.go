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

	libModel "github.com/pingcap/tiflow/engine/lib/model"
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
	globalMetricRegistry.MustRegister(systemID, collectors.NewGoCollector())
}

// Registry is used for registering metric
type Registry struct {
	sync.Mutex
	*prometheus.Registry

	// collectorByWorker is for cleaning all collectors for specific worker(jobmaster/worker)
	collectorByWorker map[libModel.WorkerID][]prometheus.Collector
}

// NewRegistry return a new Registry
func NewRegistry() *Registry {
	return &Registry{
		Registry:          prometheus.NewRegistry(),
		collectorByWorker: make(map[libModel.WorkerID][]prometheus.Collector),
	}
}

// MustRegister registers the provided Collector of the specified worker
func (r *Registry) MustRegister(workerID libModel.WorkerID, c prometheus.Collector) {
	if c == nil {
		return
	}
	r.Lock()
	defer r.Unlock()

	r.Registry.MustRegister(c)

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
func (r *Registry) Unregister(workerID libModel.WorkerID) {
	r.Lock()
	defer r.Unlock()

	cls, exists := r.collectorByWorker[workerID]
	if exists {
		for _, collector := range cls {
			r.Registry.Unregister(collector)
		}
		delete(r.collectorByWorker, workerID)
	}
}

// Gather implements Gatherer interface
func (r *Registry) Gather() ([]*dto.MetricFamily, error) {
	// NOT NEED lock here. prometheus.Registry has thread-safe methods
	return r.Registry.Gather()
}
