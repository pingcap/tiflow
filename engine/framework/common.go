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
	"github.com/pingcap/errors"

	"github.com/pingcap/tiflow/engine/framework/master"
	"github.com/pingcap/tiflow/engine/framework/model"
)

type (
	// WorkerType alias to model.WorkerType
	WorkerType = model.WorkerType
	// WorkerConfig stores worker config in any type
	WorkerConfig = interface{}
)

// Defines all task type
const (
	JobManager = model.WorkerType(iota + 1)
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
)

// MasterFailoverReasonCode is used as reason code
type MasterFailoverReasonCode int32

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

// WorkerHandle alias to master.WorkerHandle
type WorkerHandle = master.WorkerHandle

// nolint:revive
var StopAfterTick = errors.New("stop after tick")

// WorkerTypeForMetric return a prefix for metric
// TODO: let user register a unique identifier for the metric prefix
func WorkerTypeForMetric(t WorkerType) string {
	switch t {
	case JobManager:
		// jobmanager is the framework level job
		return ""
	case CvsJobMaster, CvsTask:
		return "cvs"
	case FakeJobMaster, FakeTask:
		return "fake"
	case DMJobMaster, DmTask, WorkerDMDump, WorkerDMLoad, WorkerDMSync:
		return "dm"
	case CdcJobMaster, CdcTask:
		return "cdc"
	}

	return "unknown_job"
}
