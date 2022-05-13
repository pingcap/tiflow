package lib

import (
	"github.com/pingcap/errors"

	"github.com/hanfei1991/microcosm/lib/master"
	"github.com/hanfei1991/microcosm/lib/model"
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
