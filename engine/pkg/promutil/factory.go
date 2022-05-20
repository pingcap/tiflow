package promutil

import "github.com/prometheus/client_golang/prometheus"

// Metric produced by Factory has some inner const labels attached to it.
// 1. tenant const-labels: {tenant="xxx", project_id="xxx"}
// to distinguish different tenant/project metric
// 2. task const-labels: {job_id="xxx"} {worker_id="xxx"}
// app job master metric only has `job_id` label, app worker has all.
// (a) `job_id` can distinguish different tasks of the same job type
// (b) `worker_id` can distinguish different worker of the same job
// e.g.
// For JobMaster:
//  {tenant="user0", project_id="debug", job_id="job0", xxx="xxx"(user defined const labels)}
// For Worker:
//  {tenant="user0", project_id="debug", job_id="job0", worker_id="worker0"，
//     xxx="xxx"(user defined labels)}
// For Framework:
//  {framework="true"}

// Besides, some specific prefix will be added to metric name to avoid
// cross app metric conflict.
// Currently, we will add `job_type` to the metric name.
// e.g. $Namespace_$Subsystem_$Name(original) --->
//		$JobType_$Namespace_$Subsystem_$Name(actual)

// Factory is the interface to create some native prometheus metric
type Factory interface {
	// NewCounter works like the function of the same name in the prometheus
	// package, but it automatically registers the Counter with the Factory's
	// Registerer. Panic if it can't register successfully.
	NewCounter(opts prometheus.CounterOpts) prometheus.Counter

	// NewCounterVec works like the function of the same name in the
	// prometheus, package but it automatically registers the CounterVec with
	// the Factory's Registerer. Panic if it can't register successfully.
	NewCounterVec(opts prometheus.CounterOpts, labelNames []string) *prometheus.CounterVec

	// NewGauge works like the function of the same name in the prometheus
	// package, but it automatically registers the Gauge with the Factory's
	// Registerer. Panic if it can't register successfully.
	NewGauge(opts prometheus.GaugeOpts) prometheus.Gauge

	// NewGaugeVec works like the function of the same name in the prometheus
	// package but it automatically registers the GaugeVec with the Factory's
	// Registerer. Panic if it can't register successfully.
	NewGaugeVec(opts prometheus.GaugeOpts, labelNames []string) *prometheus.GaugeVec

	// NewHistogram works like the function of the same name in the prometheus
	// package but it automatically registers the Histogram with the Factory's
	// Registerer. Panic if it can't register successfully.
	NewHistogram(opts prometheus.HistogramOpts) prometheus.Histogram

	// NewHistogramVec works like the function of the same name in the
	// prometheus package but it automatically registers the HistogramVec
	// with the Factory's Registerer. Panic if it can't register successfully.
	NewHistogramVec(opts prometheus.HistogramOpts, labelNames []string) *prometheus.HistogramVec
}
