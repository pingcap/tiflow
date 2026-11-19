// Copyright 2019 PingCAP, Inc.
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

package worker

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"slices"
	"sort"
	"sync"
	"time"

	cpu "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tiflow/dm/common"
	"github.com/pingcap/tiflow/dm/dumpling"
	"github.com/pingcap/tiflow/dm/loader"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/relay"
	"github.com/pingcap/tiflow/dm/syncer/metrics"
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
)

const (
	opErrTypeBeforeOp    = "BeforeAnyOp"
	opErrTypeSourceBound = "SourceBound"
	opErrTypeRelaySource = "RelaySource"
)

var (
	f         = &promutil.PromFactory{}
	taskState = f.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "worker",
			Name:      "task_state",
			Help:      "state of task, 0 - invalidStage, 1 - New, 2 - Running, 3 - Paused, 4 - Stopped, 5 - Finished",
		}, []string{"task", "source_id", "worker"})

	// opErrCounter cleans on worker close, which is the same time dm-worker exits, so no explicit clean.
	opErrCounter = f.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "worker",
			Name:      "operate_error",
			Help:      "number of different operate error",
		}, []string{"worker", "type"})

	cpuUsageGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "worker",
			Name:      "cpu_usage",
			Help:      "the cpu usage of worker",
		})
	metricLabelsMu sync.RWMutex
	metricLabels   = make(map[string][]*dto.LabelPair)
	metricRefs     = make(map[string]int)
)

// taskMetricGatherer exposes task-associated worker CPU usage and injects labels
// at gather time because task metrics are created across several DM and Lightning
// packages. Registrations are reference-counted because multiple subtasks of the
// same task share labels.
type taskMetricGatherer struct {
	prometheus.Registerer
	gatherer prometheus.Gatherer
}

func (g taskMetricGatherer) Gather() ([]*dto.MetricFamily, error) {
	families, err := g.gatherer.Gather()
	if err != nil {
		return families, err
	}
	families = appendTaskWorkerCPUUsage(families)
	metricLabelsMu.RLock()
	defer metricLabelsMu.RUnlock()
	if len(metricLabels) == 0 {
		return families, nil
	}
	for _, family := range families {
		if len(family.Metric) == 0 {
			continue
		}
		taskLabelIndex := -1
		for i, label := range family.Metric[0].Label {
			if label.GetName() == "task" {
				taskLabelIndex = i
				break
			}
		}
		if taskLabelIndex < 0 {
			continue
		}
		existing := make(map[string]struct{}, len(family.Metric[0].Label))
		for _, label := range family.Metric[0].Label {
			existing[label.GetName()] = struct{}{}
		}
		for _, metric := range family.Metric {
			var task string
			metricLabelsByName := existing
			if taskLabelIndex < len(metric.Label) && metric.Label[taskLabelIndex].GetName() == "task" {
				task = metric.Label[taskLabelIndex].GetValue()
			} else {
				// Metric families normally have one label layout. Keep a defensive
				// fallback for unchecked collectors with inconsistent metrics.
				metricLabelsByName = make(map[string]struct{}, len(metric.Label))
				for _, label := range metric.Label {
					metricLabelsByName[label.GetName()] = struct{}{}
					if label.GetName() == "task" {
						task = label.GetValue()
					}
				}
			}
			labels, ok := metricLabels[task]
			if !ok || task == "" {
				continue
			}
			for _, label := range labels {
				if _, exists := metricLabelsByName[label.GetName()]; exists {
					return families, fmt.Errorf(
						"task metric label %q conflicts with metric family %q",
						label.GetName(), family.GetName())
				}
			}
			// LabelPair values are immutable after registration and may be shared
			// safely by all series for the same task.
			metric.Label = append(metric.Label, labels...)
		}
	}
	return families, nil
}

// appendTaskWorkerCPUUsage associates the existing worker CPU sample with each
// task-state series. Deriving the series at gather time keeps their lifecycle in
// sync with taskState, including paused tasks and stopped/finished task cleanup.
// This is the whole worker's CPU usage, not CPU attributed to an individual task.
// This derived metric is intended for TiDB Cloud only, allowing Cloud to select
// worker CPU usage through task metric labels without worker-level configuration.
func appendTaskWorkerCPUUsage(families []*dto.MetricFamily) []*dto.MetricFamily {
	var cpuUsage *dto.Gauge
	var taskMetrics []*dto.Metric
	for _, family := range families {
		switch family.GetName() {
		case "dm_worker_cpu_usage":
			if len(family.Metric) == 1 {
				cpuUsage = family.Metric[0].Gauge
			}
		case "dm_worker_task_state":
			taskMetrics = family.Metric
		}
	}
	if cpuUsage == nil || len(taskMetrics) == 0 {
		return families
	}

	name := "dm_task_worker_cpu_usage"
	help := "CPU usage of the worker hosting the task, in percent (100 means one CPU core). " +
		"Shared worker CPU is repeated for each task, not attributed to the task."
	family := &dto.MetricFamily{
		Name:   &name,
		Help:   &help,
		Type:   dto.MetricType_GAUGE.Enum(),
		Metric: make([]*dto.Metric, 0, len(taskMetrics)),
	}
	for _, metric := range taskMetrics {
		family.Metric = append(family.Metric, &dto.Metric{
			// Label values and the CPU sample are immutable. Clone the label slice
			// so custom labels can be appended independently to both families.
			Label: slices.Clone(metric.Label),
			Gauge: cpuUsage,
		})
	}
	// Preserve the ordering required by prometheus.Gatherer.
	i := sort.Search(len(families), func(i int) bool { return families[i].GetName() >= name })
	return slices.Insert(families, i, family)
}

func registerTaskMetricLabels(task string, labels map[string]string) {
	if len(labels) == 0 {
		return
	}
	metricLabelsMu.Lock()
	defer metricLabelsMu.Unlock()
	if metricRefs[task] == 0 {
		metricLabels[task] = makeTaskMetricLabelPairs(labels)
	}
	metricRefs[task]++
}

func replaceTaskMetricLabels(task string, labels map[string]string) {
	metricLabelsMu.Lock()
	defer metricLabelsMu.Unlock()
	if metricRefs[task] > 0 {
		metricLabels[task] = makeTaskMetricLabelPairs(labels)
	}
}

func makeTaskMetricLabelPairs(labels map[string]string) []*dto.LabelPair {
	names := make([]string, 0, len(labels))
	for name := range labels {
		names = append(names, name)
	}
	sort.Strings(names)
	pairs := make([]*dto.LabelPair, 0, len(names))
	for _, name := range names {
		value := labels[name]
		pairs = append(pairs, &dto.LabelPair{Name: &name, Value: &value})
	}
	return pairs
}

func unregisterTaskMetricLabels(task string) {
	metricLabelsMu.Lock()
	defer metricLabelsMu.Unlock()
	if metricRefs[task] <= 1 {
		delete(metricRefs, task)
		delete(metricLabels, task)
		return
	}
	metricRefs[task]--
}

type statusHandler struct{}

func (h *statusHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	text := version.GetRawInfo()
	_, err := w.Write([]byte(text))
	if err != nil && !common.IsErrNetClosing(err) {
		log.L().Error("fail to write status response", log.ShortError(err))
	}
}

// Note: handle error inside the function with returning it.
func (s *Server) collectMetrics() {
	// CPU usage metric
	cpuUsage := cpu.GetCPUPercentage()
	cpuUsageGauge.Set(cpuUsage)
}

func (s *Server) runBackgroundJob(ctx context.Context) {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.collectMetrics()

		case <-ctx.Done():
			return
		}
	}
}

// RegistryMetrics registries metrics for worker.
func RegistryMetrics() {
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	registry.MustRegister(prometheus.NewGoCollector(
		collectors.WithGoCollections(collectors.GoRuntimeMemStatsCollection | collectors.GoRuntimeMetricsCollection)))

	registry.MustRegister(cpuUsageGauge)

	registry.MustRegister(taskState)
	registry.MustRegister(opErrCounter)

	relay.RegisterMetrics(registry)
	dumpling.RegisterMetrics(registry)
	loader.RegisterMetrics(registry)
	metrics.RegisterValidatorMetrics(registry)
	metrics.DefaultMetricsProxies.RegisterMetrics(registry)
	prometheus.DefaultGatherer = taskMetricGatherer{Registerer: registry, gatherer: registry}
}

// InitStatus initializes the HTTP status server.
func InitStatus(lis net.Listener) {
	mux := http.NewServeMux()
	mux.Handle("/status", &statusHandler{})
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	httpS := &http.Server{
		Handler: mux,
	}
	err := httpS.Serve(lis)
	if err != nil && !common.IsErrNetClosing(err) && err != http.ErrServerClosed {
		log.L().Error("status server returned", log.ShortError(err))
	}
}
