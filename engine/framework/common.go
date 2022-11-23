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
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/framework/internal/master"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	engineModel "github.com/pingcap/tiflow/engine/model"
	"go.uber.org/zap"
)

type (
	// WorkerType alias to model.WorkerType
	WorkerType = frameModel.WorkerType

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

// Defines all reason codes
const (
	MasterTimedOut = MasterFailoverReasonCode(iota + 1)
	MasterReportedError
)

// MasterFailoverReason contains failover reason code and error message
type MasterFailoverReason struct {
	Code     MasterFailoverReasonCode
	ErrorMsg string
}

// MustConvertWorkerType2JobType return the job type of worker type. Panic if it fail.
// TODO: let user register a unique identifier for the metric prefix
func MustConvertWorkerType2JobType(tp WorkerType) engineModel.JobType {
	switch tp {
	case frameModel.JobManager:
		// jobmanager is the framework level job
		return engineModel.JobTypeJobManager
	case frameModel.CvsJobMaster, frameModel.CvsTask:
		return engineModel.JobTypeCVSDemo
	case frameModel.FakeJobMaster, frameModel.FakeTask:
		return engineModel.JobTypeFakeJob
	case frameModel.DMJobMaster, frameModel.DmTask, frameModel.WorkerDMDump, frameModel.WorkerDMLoad, frameModel.WorkerDMSync:
		return engineModel.JobTypeDM
	case frameModel.CdcJobMaster, frameModel.CdcTask:
		return engineModel.JobTypeCDC
	}

	log.Panic("unexpected fail when convert worker type to job type", zap.Stringer("worker_type", tp))
	return engineModel.JobTypeInvalid
}

// ExitReason is the type for exit reason
type ExitReason int

// define some ExitReason
const (
	ExitReasonUnknown = ExitReason(iota)
	ExitReasonFinished
	ExitReasonCanceled
	ExitReasonFailed
)

// WorkerStateToExitReason translates WorkerState to ExitReason
// TODO: business logic should not sense 'WorkerState'
func WorkerStateToExitReason(code frameModel.WorkerState) ExitReason {
	switch code {
	case frameModel.WorkerStateFinished:
		return ExitReasonFinished
	case frameModel.WorkerStateStopped:
		return ExitReasonCanceled
	case frameModel.WorkerStateError:
		return ExitReasonFailed
	}

	return ExitReasonUnknown
}
