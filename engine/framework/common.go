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

package framework

import (
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/framework/internal/master"
	"github.com/pingcap/tiflow/engine/framework/model"
	engineModel "github.com/pingcap/tiflow/engine/model"
)

type (
	// WorkerType alias to model.WorkerType
	WorkerType = model.WorkerType

	// WorkerConfig stores worker config in any type
	WorkerConfig = interface{}

	// WorkerHandle alias to master.WorkerHandle
	WorkerHandle = master.WorkerHandle

	// MockHandle is a mock for WorkerHandle.
	// Re-exported for testing.
	MockHandle = master.MockHandle

	// MasterFailoverReasonCode is used as reason code
	MasterFailoverReasonCode int32
)

// Defines all task type
// TODO: Refine me.Currently, when adding a new worker type or job type, we need to modify many code places,
// NOTICE: DO NOT CHANGE the previous worker type
// Modify the comment in model IF you add some new worker type
const (
	JobManager = WorkerType(iota + 1)
	// job master
	CvsJobMaster
	FakeJobMaster
	DMJobMaster
	CdcJobMaster
	// task
	CvsTask
	FakeTask
	DmTask
	CdcTask
	// worker
	WorkerDMDump
	WorkerDMLoad
	WorkerDMSync
	// extend the worker type here
)

// Defines all reason codes
const (
	MasterTimedOut = MasterFailoverReasonCode(iota + 1)
	MasterReportedError
)

// MasterFailoverReason contains failover reason code and error message
type MasterFailoverReason struct {
	Code         MasterFailoverReasonCode
	ErrorMessage string
}

// MustConvertWorkerType2JobType return the job type of worker type. Panic if it fail.
// TODO: let user register a unique identifier for the metric prefix
func MustConvertWorkerType2JobType(tp WorkerType) engineModel.JobType {
	switch tp {
	case JobManager:
		// jobmanager is the framework level job
		return engineModel.JobTypeJobManager
	case CvsJobMaster, CvsTask:
		return engineModel.JobTypeCVSDemo
	case FakeJobMaster, FakeTask:
		return engineModel.JobTypeFakeJob
	case DMJobMaster, DmTask, WorkerDMDump, WorkerDMLoad, WorkerDMSync:
		return engineModel.JobTypeDM
	case CdcJobMaster, CdcTask:
		return engineModel.JobTypeCDC
	}

	log.L().Panic("unexpected fail when convert worker type to job type", zap.Int32("worker_type", int32(tp)))
	return engineModel.JobTypeInvalid
}
