package lib

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/pingcap/errors"

	"github.com/hanfei1991/microcosm/pkg/clock"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type (
	WorkerStatusCode int32
	WorkerType       int64

	MasterStatusCode int32

	Epoch        = int64
	WorkerConfig = interface{}
	MasterID     = string
	WorkerID     = string
)

// Among these statuses, only WorkerStatusCreated is used by the framework
// for now. The rest are for the business logic to use.
// TODO think about whether to manage the transition of the statuses.
// TODO: need a FSM graph
const (
	WorkerStatusNormal = WorkerStatusCode(iota + 1)
	WorkerStatusCreated
	WorkerStatusInit
	WorkerStatusError
	WorkerStatusFinished
)

// Job master statuses
const (
	MasterStatusUninit = MasterStatusCode(iota + 1)
	MasterStatusInit
	MasterStatusFinished
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

type WorkerStatus struct {
	Code         WorkerStatusCode `json:"code"`
	ErrorMessage string           `json:"error-message"`

	// ExtBytes carries the serialized form of the Ext field, which is used in
	// business logic only.
	// Business logic can parse the raw bytes and decode into business Go object
	ExtBytes []byte `json:"ext-bytes"`
}

func (s *WorkerStatus) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

func HeartbeatPingTopic(masterID MasterID) p2p.Topic {
	return fmt.Sprintf("heartbeat-ping-%s", masterID)
}

func HeartbeatPongTopic(masterID MasterID, workerID WorkerID) p2p.Topic {
	// TODO do we need hex-encoding here?
	return fmt.Sprintf("heartbeat-pong-%s-%s", masterID, workerID)
}

func WorkerStatusUpdatedTopic(masterID MasterID) string {
	return fmt.Sprintf("worker-status-updated-%s", masterID)
}

type WorkerStatusUpdatedMessage struct {
	FromWorkerID WorkerID `json:"from-worker-id"`
	Epoch        Epoch    `json:"epoch"`
}

type HeartbeatPingMessage struct {
	SendTime     clock.MonotonicTime `json:"send-time"`
	FromWorkerID WorkerID            `json:"from-worker-id"`
	Epoch        Epoch               `json:"epoch"`
}

type HeartbeatPongMessage struct {
	SendTime   clock.MonotonicTime `json:"send-time"`
	ReplyTime  time.Time           `json:"reply-time"`
	ToWorkerID WorkerID            `json:"to-worker-id"`
	Epoch      Epoch               `json:"epoch"`
}

type (
	MasterMetaKVData struct {
		ID         MasterID         `json:"id"`
		Addr       string           `json:"addr"`
		NodeID     p2p.NodeID       `json:"node-id"`
		Epoch      Epoch            `json:"epoch"`
		StatusCode MasterStatusCode `json:"status"`
		Tp         WorkerType       `json:"type"`

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
	MasterID   Master           `json:"id"`
	NodeID     p2p.NodeID       `json:"node-id"`
	StatusCode WorkerStatusCode `json:"status-code"`
	Message    string           `json:"message"`
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
