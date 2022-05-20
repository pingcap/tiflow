package promutil

import (
	"net/http"

	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/pkg/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// [NOTICE]: SHOULD NOT use following functions. USE functions in 'util.go' INSTEAD.
// They are just for easy scenarios testing.

// HTTPHandlerForMetricImpl return http.Handler for prometheus metric
func HTTPHandlerForMetricImpl(gather prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(
		gather,
		promhttp.HandlerOpts{},
	)
}

// NewFactory4JobMasterImpl return a Factory for jobmaster
func NewFactory4JobMasterImpl(reg *Registry, info tenant.ProjectInfo, jobType libModel.JobType, jobID libModel.MasterID) Factory {
	return &wrappingFactory{
		r:      reg,
		prefix: jobType,
		id:     jobID,
		constLabels: prometheus.Labels{
			constLabelTenantKey:  info.TenantID,
			constLabelProjectKey: info.ProjectID,
			constLabelJobKey:     jobID,
		},
	}
}

// NewFactory4WorkerImpl return a Factory for worker
func NewFactory4WorkerImpl(reg *Registry, info tenant.ProjectInfo, jobType libModel.JobType, jobID libModel.MasterID,
	workerID libModel.WorkerID,
) Factory {
	return &wrappingFactory{
		r:      reg,
		prefix: jobType,
		id:     workerID,
		constLabels: prometheus.Labels{
			constLabelTenantKey:  info.TenantID,
			constLabelProjectKey: info.ProjectID,
			constLabelJobKey:     jobID,
			constLabelWorkerKey:  workerID,
		},
	}
}

// NewFactory4FrameworkImpl return a Factory for dataflow framework
func NewFactory4FrameworkImpl(reg *Registry) Factory {
	return &wrappingFactory{
		r:      reg,
		prefix: frameworkMetricPrefix,
		id:     frameworkID,
		constLabels: prometheus.Labels{
			constLabelFrameworkKey: "true",
		},
	}
}
