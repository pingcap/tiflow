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
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"go.uber.org/zap"
)

const (
	/// app const label
	// constFieldTenantKey and constFieldProjectKey is used to recognize metric for tenant/project
	constFieldTenantKey  = "tenant"
	constFieldProjectKey = "project_id"
	// ConstFieldJobKey is used to recognize jobs of the same job type
	ConstFieldJobKey = "job_id"
	// ConstFieldWorkerKey is used to recognize workers of the same job
	ConstFieldWorkerKey = "worker_id"
)

// WithProjectInfo attaches project info to logger
func WithProjectInfo(logger *zap.Logger, project tenant.ProjectInfo) *zap.Logger {
	return logger.With(
		zap.String(constFieldTenantKey, project.TenantID()),
		zap.String(constFieldProjectKey, project.ProjectID()),
	)
}

// WithMasterID attaches master id to logger
func WithMasterID(logger *zap.Logger, masterID frameModel.MasterID) *zap.Logger {
	return logger.With(
		zap.String(ConstFieldJobKey, masterID),
	)
}

// WithWorkerID attaches worker id to logger
func WithWorkerID(logger *zap.Logger, workerID frameModel.WorkerID) *zap.Logger {
	return logger.With(
		zap.String(ConstFieldWorkerKey, workerID),
	)
}
