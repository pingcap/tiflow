package lib

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/clock"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/pingcap/errors"
)

type (
	WorkerStatusCode int32
	WorkerType       int64

	Epoch        = int64
	WorkerConfig = interface{}
	MasterID     = string
	WorkerID     = string
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
	// job master
	CvsJobMaster
	FakeJobMaster
	DmJobMaster
	CdcJobMaster
	// task
	CvsTask
	FakeTask
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

	// ExtBytes carries the serialized form of the Ext field.
	// See below for more information.
	// DO NOT access ExtBytes from business logic.
	ExtBytes []byte `json:"ext-bytes"`

	// Ext should be an object of a type specified by the
	// business logic. But since Go does not support generics yet,
	// we have to put `interface{}` as the type, which fails to tell
	// the json library the actual type the data needs to be deserialized into.
	// So we use ExtBytes to carry raw bytes of the Ext object, and deserialize
	// the object by ourselves, rather than rely on any library.
	Ext interface{} `json:"-"`
}

func (s *WorkerStatus) fillExt(tpi interface{}) (err error) {
	defer func() {
		// ExtBytes is no longer useful after this function returns.
		s.ExtBytes = nil
		if r := recover(); r != nil {
			err = errors.Errorf("Fill ext field of worker status failed: %v", r)
		}
	}()
	obj := reflect.New(reflect.TypeOf(tpi).Elem()).Interface()
	err = json.Unmarshal(s.ExtBytes, obj)
	if err != nil {
		return errors.Trace(err)
	}
	s.Ext = obj
	return nil
}

func (s *WorkerStatus) marshalExt() error {
	bytes, err := json.Marshal(s.Ext)
	if err != nil {
		return errors.Trace(err)
	}

	s.ExtBytes = bytes
	return nil
}

func HeartbeatPingTopic(masterID MasterID, workerID WorkerID) p2p.Topic {
	return fmt.Sprintf("heartbeat-ping-%s-%s", masterID, workerID)
}

func HeartbeatPongTopic(masterID MasterID, workerID WorkerID) p2p.Topic {
	// TODO do we need hex-encoding here?
	return fmt.Sprintf("heartbeat-pong-%s-%s", masterID, workerID)
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
	MasterMetaKVData struct {
		ID          MasterID   `json:"id"`
		Addr        string     `json:"addr"`
		NodeID      p2p.NodeID `json:"node-id"`
		Epoch       Epoch      `json:"epoch"`
		Initialized bool       `json:"initialized"`

		// Ext holds business-specific data
		MasterMetaExt *MasterMetaExt `json:"meta-ext"`
	}
)

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
