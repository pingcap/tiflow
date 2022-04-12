package lib

import (
	"encoding/json"
	"time"

	"github.com/pingcap/errors"

	"github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type (
	WorkerType int64

	WorkerConfig = interface{}
)

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
)

type TimeoutConfig struct {
	workerTimeoutDuration            time.Duration
	workerTimeoutGracefulDuration    time.Duration
	workerHeartbeatInterval          time.Duration
	workerReportStatusInterval       time.Duration
	masterHeartbeatCheckLoopInterval time.Duration
}

var defaultTimeoutConfig TimeoutConfig = TimeoutConfig{
	workerTimeoutDuration:            time.Second * 15,
	workerTimeoutGracefulDuration:    time.Second * 5,
	workerHeartbeatInterval:          time.Second * 3,
	workerReportStatusInterval:       time.Second * 3,
	masterHeartbeatCheckLoopInterval: time.Second * 1,
}.Adjust()

// Adjust validates the TimeoutConfig and adjusts it
func (config TimeoutConfig) Adjust() TimeoutConfig {
	var tc TimeoutConfig = config
	// worker timeout duration must be 2 times larger than worker heartbeat interval
	if tc.workerTimeoutDuration < 2*tc.workerHeartbeatInterval+time.Second*3 {
		tc.workerTimeoutDuration = 2*tc.workerHeartbeatInterval + time.Second*3
	}
	return tc
}

type (
	MasterMetaKVData struct {
		ID         model.MasterID         `json:"id"`
		Addr       string                 `json:"addr"`
		NodeID     p2p.NodeID             `json:"node-id"`
		Epoch      model.Epoch            `json:"epoch"`
		StatusCode model.MasterStatusCode `json:"status"`
		Tp         WorkerType             `json:"type"`

		// Config holds business-specific data
		Config []byte `json:"config"`
		// TODO: add master status and checkpoint data
	}
)

func (m *MasterMetaKVData) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

func (m *MasterMetaKVData) Unmarshal(data []byte) error {
	return json.Unmarshal(data, m)
}

type WorkerMetaKVData struct {
	MasterID   Master                 `json:"id"`
	NodeID     p2p.NodeID             `json:"node-id"`
	StatusCode model.WorkerStatusCode `json:"status-code"`
	Message    string                 `json:"message"`
}

type MasterFailoverReasonCode int32

const (
	MasterTimedOut = MasterFailoverReasonCode(iota + 1)
	MasterReportedError
)

type MasterFailoverReason struct {
	Code         MasterFailoverReasonCode
	ErrorMessage string
}

// nolint:revive
var StopAfterTick = errors.New("stop after tick")
