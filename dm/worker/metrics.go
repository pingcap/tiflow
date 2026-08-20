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
	"net"
	"net/http"
	"net/http/pprof"
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
	metricLabels   = make(map[string]map[string]string)
	metricRefs     = make(map[string]int)
)

type taskMetricGatherer struct{ gatherer prometheus.Gatherer }

func (g taskMetricGatherer) Gather() ([]*dto.MetricFamily, error) {
	families, err := g.gatherer.Gather()
	if err != nil {
		return families, err
	}
	metricLabelsMu.RLock()
	defer metricLabelsMu.RUnlock()
	for _, family := range families {
		for _, metric := range family.Metric {
			var task string
			for _, label := range metric.Label {
				if label.GetName() == "task" {
					task = label.GetValue()
					break
				}
			}
			labels, ok := metricLabels[task]
			if !ok || task == "" {
				continue
			}
			existing := make(map[string]struct{}, len(metric.Label))
			for _, label := range metric.Label {
				existing[label.GetName()] = struct{}{}
			}
			for name, value := range labels {
				if _, exists := existing[name]; exists {
					continue
				}
				metric.Label = append(metric.Label, &dto.LabelPair{
					Name: stringPtr(name), Value: stringPtr(value),
				})
			}
		}
	}
	return families, nil
}

func registerTaskMetricLabels(task string, labels map[string]string) {
	if len(labels) == 0 {
		return
	}
	metricLabelsMu.Lock()
	defer metricLabelsMu.Unlock()
	if metricRefs[task] == 0 {
		metricLabels[task] = copyMetricLabels(labels)
	}
	metricRefs[task]++
}

func stringPtr(value string) *string { return &value }

func copyMetricLabels(labels map[string]string) map[string]string {
	result := make(map[string]string, len(labels))
	for key, value := range labels {
		result[key] = value
	}
	return result
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
	prometheus.DefaultGatherer = taskMetricGatherer{gatherer: registry}
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
