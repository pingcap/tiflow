package promutil

import (
	"net/http"

	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/pkg/tenant"
)

// Routine to get a Factory:
// 1. Servermaster/Executor maintains a process-level prometheus.Registerer singleton.
// 2. 'BaseMaster/BaseWorker' interface offers a method 'func PromFactory() Factory'.
// 3. When app implements 'MasterImpl/WorkerImpl', it can get a Factory object by BaseWorker.PromFactory().
// Actually, the return Factory object would be the wrappingFactory which can produce prometheus metric object
// with tenant and task information of dataflow engine.
// 4. App uses Factory.NewCounter(xxx) to produce the native prometheus object without any concern about the
// registration and http handler. Similar to usage of promauto.

const (
	systemID              = "dataflow-system"
	frameworkID           = "dateflow-framework"
	frameworkMetricPrefix = "dataflow" // avoid metric conflict with app
)

const (
	/// framework const lable
	constLabelFrameworkKey = "framework"

	/// app const label
	// constLabelTenantKey and constLabelProjectKey is used to recognize metric for tenant/project
	constLabelTenantKey  = "tenant"
	constLabelProjectKey = "project_id"
	// constLabelJobKey is used to recognize jobs of the same job type
	constLabelJobKey = "job_id"
	// constLabelWorkerKey is used to recognize workers of the same job
	constLabelWorkerKey = "worker_id"
)

// HTTPHandlerForMetric return http.Handler for prometheus metric
func HTTPHandlerForMetric() http.Handler {
	return HTTPHandlerForMetricImpl(globalMetricGatherer)
}

// NewFactory4JobMaster return a Factory for jobmaster
func NewFactory4JobMaster(info tenant.ProjectInfo, jobType libModel.JobType, jobID libModel.MasterID) Factory {
	return NewFactory4JobMasterImpl(globalMetricRegistry, info, jobType, jobID)
}

// NewFactory4Worker return a Factory for worker
func NewFactory4Worker(reg *Registry, info tenant.ProjectInfo, jobType libModel.JobType, jobID libModel.MasterID,
	workerID libModel.WorkerID,
) Factory {
	return NewFactory4WorkerImpl(globalMetricRegistry, info, jobType, jobID, workerID)
}

// NewFactory4Framework return a Factory for dataflow framework
// NOTICE: we use auto service label tagged by cloud service to distinguish
// different dataflow engine or different executor
func NewFactory4Framework() Factory {
	return NewFactory4FrameworkImpl(globalMetricRegistry)
}
