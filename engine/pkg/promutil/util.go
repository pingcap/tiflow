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

	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
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

// NewFactory4Master return a Factory for jobmaster
func NewFactory4Master(info tenant.ProjectInfo, jobType libModel.JobType, jobID libModel.MasterID) Factory {
	return NewFactory4MasterImpl(globalMetricRegistry, info, jobType, jobID)
}

// NewFactory4Worker return a Factory for worker
func NewFactory4Worker(info tenant.ProjectInfo, jobType libModel.JobType, jobID libModel.MasterID,
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
