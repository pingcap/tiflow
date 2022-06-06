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

package logutil

import (
	"github.com/pingcap/log"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"go.uber.org/zap"
)

const (
	/// framework const lable
	constFieldFrameworkKey   = "framework"
	constFieldFrameworkValue = true

	/// app const label
	// constFieldTenantKey and constFieldProjectKey is used to recognize metric for tenant/project
	constFieldTenantKey  = "tenant"
	constFieldProjectKey = "project_id"
	// constFieldJobKey is used to recognize jobs of the same job type
	constFieldJobKey = "job_id"
	// FieldWorkerKey is used to recognize workers of the same job
	constFieldWorkerKey = "worker_id"
)

// NewLogger4Framework return a new logger for framework
func NewLogger4Framework() *zap.Logger {
	return log.L().With(
		zap.Bool(constFieldFrameworkKey, constFieldFrameworkValue),
	)
}

// NewLogger4Master return a new logger for master
func NewLogger4Master(project tenant.ProjectInfo, masterID libModel.MasterID) *zap.Logger {
	return log.L().With(
		zap.String(constFieldTenantKey, project.TenantID()),
		zap.String(constFieldProjectKey, project.ProjectID()),
		zap.String(constFieldJobKey, masterID),
	)
}

// NewLogger4Worker return a new logger for worker
func NewLogger4Worker(project tenant.ProjectInfo, masterID libModel.MasterID, workerID libModel.WorkerID) *zap.Logger {
	return log.L().With(
		zap.String(constFieldTenantKey, project.TenantID()),
		zap.String(constFieldProjectKey, project.ProjectID()),
		zap.String(constFieldJobKey, masterID),
		zap.String(constFieldWorkerKey, workerID),
	)
}
