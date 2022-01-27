package lib

import (
	"context"
	"fmt"
	"time"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/clock"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type (
	MasterID         string
	WorkerID         string
	WorkerStatusCode int32
	WorkerType       int64

	Epoch        = int64
	WorkerConfig = interface{}
)

// Among these statuses, only WorkerStatusCreated is used by the framework
// for now. The rest are for the business logic to use.
// TODO think about whether to manage the transition of the statuses.
const (
	WorkerStatusNormal = WorkerStatusCode(iota + 1)
	WorkerStatusCreated
	WorkerStatusInit
	WorkerStatusError
	WorkerStatusFinished
)

const (
	JobManager = WorkerType(iota + 1)
	CvsJobMaster
	DmJobMaster
	CdcJobMaster
	CvsTask
	DmTask
	CdcTask
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
}

type WorkerStatus struct {
	Code         WorkerStatusCode `json:"code"`
	ErrorMessage string           `json:"error-message"`
	Ext          interface{}      `json:"ext"`
}

type Closer interface {
	Close(ctx context.Context) error
}

func HeartbeatPingTopic(masterID MasterID, workerID WorkerID) p2p.Topic {
	return fmt.Sprintf("heartbeat-ping-%s-%s", string(masterID), string(workerID))
}

func HeartbeatPongTopic(masterID MasterID, workerID WorkerID) p2p.Topic {
	// TODO do we need hex-encoding here?
	return fmt.Sprintf("heartbeat-pong-%s-%s", string(masterID), string(workerID))
}

func WorkloadReportTopic(masterID MasterID) p2p.Topic {
	return fmt.Sprintf("workload-report-%s", masterID)
}

func StatusUpdateTopic(masterID MasterID, workerID WorkerID) p2p.Topic {
	return fmt.Sprintf("status-update-%s-%s", masterID, workerID)
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

type StatusUpdateMessage struct {
	WorkerID WorkerID     `json:"worker-id"`
	Status   WorkerStatus `json:"status"`
}

type WorkloadReportMessage struct {
	WorkerID WorkerID       `json:"worker-id"`
	Workload model.RescUnit `json:"workload"`
}

type (
	MasterMetaExt    = interface{}
	MasterMetaKVData struct {
		ID          MasterID   `json:"id"`
		Addr        string     `json:"addr"`
		NodeID      p2p.NodeID `json:"node-id"`
		Epoch       Epoch      `json:"epoch"`
		Initialized bool       `json:"initialized"`

		// Ext holds business-specific data
		Ext MasterMetaExt `json:"ext"`
	}
)

type MasterFailoverReasonCode int32

const (
	MasterTimedOut = MasterFailoverReasonCode(iota + 1)
	MasterReportedError
)

type MasterFailoverReason struct {
	Code         MasterFailoverReasonCode
	ErrorMessage string
}
