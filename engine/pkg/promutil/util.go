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
	"net/http"

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	engineModel "github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/prometheus/client_golang/prometheus"
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

	// test const lable
	constLableTestKey = "test_id"
)

// HTTPHandlerForMetric return http.Handler for prometheus metric
func HTTPHandlerForMetric() http.Handler {
	return HTTPHandlerForMetricImpl(globalMetricGatherer)
}

// Metric produced by WrappingFactory has some inner const labels attached to it.
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
//
// Besides, some specific prefix will be added to metric name to avoid
// cross app metric conflict.
// Currently, we will add `job_type` to the metric name.
// e.g. $Namespace_$Subsystem_$Name(original) --->
//		$JobType_$Namespace_$Subsystem_$Name(actual)

// NewFactory4Master return a Factory for jobmaster
func NewFactory4Master(info tenant.ProjectInfo, jobType engineModel.JobType, jobID engineModel.JobID) Factory {
	// Only for the jobmanager
	if jobType == engineModel.JobTypeJobManager {
		return NewFactory4Framework()
	}

	// TODO: If the job type prefix is not required in metrics, change implementation
	// of NewFactory4MasterImpl.
	return NewFactory4MasterImpl(globalMetricRegistry, info, "", jobID)
}

// NewFactory4Worker return a Factory for worker
func NewFactory4Worker(info tenant.ProjectInfo, jobType engineModel.JobType, jobID engineModel.JobID,
	workerID frameModel.WorkerID,
) Factory {
	// TODO: If the job type prefix is not required in metrics, change implementation
	// of NewFactory4WorkerImpl.
	return NewFactory4WorkerImpl(globalMetricRegistry, info, "", jobID, workerID)
}

// NewFactory4Test return a Factory for test
// NOTICE: we use testing.T.TempDir() to distinguish different test
func NewFactory4Test(testKey string) Factory {
	return NewFactory4TestImpl(globalMetricRegistry, testKey)
}

// NewFactory4Framework return a Factory for dataflow framework
// NOTICE: we use auto service label tagged by cloud service to distinguish
// different dataflow engine or different executor
func NewFactory4Framework() Factory {
	return NewFactory4FrameworkImpl(globalMetricRegistry)
}

// UnregisterWorkerMetrics unregisters all metrics of workerID
// IF 'worker' is a job master, use job id as workerID
// IF 'worker' is a worker, use worker id as workerID
func UnregisterWorkerMetrics(workerID frameModel.WorkerID) {
	globalMetricRegistry.Unregister(workerID)
}

// GetGlobalMetricRegistry returns the global metric registry where auto-registering metrics are registered.
// The register to it should be only be done by its derived factories from NewFactory4Master, ...
// And the returned registry can be only used for unregister.
func GetGlobalMetricRegistry() prometheus.Registerer {
	return NewOnlyUnregRegister(globalMetricRegistry.registry)
}
