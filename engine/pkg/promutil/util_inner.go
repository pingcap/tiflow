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

// NewFactory4MasterImpl return a Factory for jobmaster
func NewFactory4MasterImpl(reg *Registry, info tenant.ProjectInfo, jobType libModel.JobType, jobID libModel.MasterID) Factory {
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
