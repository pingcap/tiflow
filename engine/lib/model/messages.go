package model

import (
	"fmt"
	"time"

	"github.com/hanfei1991/microcosm/pkg/clock"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

// HeartbeatPingTopic is heartbeat ping message topic, each master has a unique one.
func HeartbeatPingTopic(masterID MasterID) p2p.Topic {
	return fmt.Sprintf("heartbeat-ping-%s", masterID)
}

// HeartbeatPongTopic is heartbeat pong message topic, each worker has a unique one.
func HeartbeatPongTopic(masterID MasterID, workerID WorkerID) p2p.Topic {
	// TODO do we need hex-encoding here?
	return fmt.Sprintf("heartbeat-pong-%s-%s", masterID, workerID)
}

// WorkerStatusChangeRequestTopic message topic used when updating worker status
func WorkerStatusChangeRequestTopic(masterID MasterID, workerID WorkerID) p2p.Topic {
	return fmt.Sprintf("worker-status-change-req-%s-%s", masterID, workerID)
}

// HeartbeatPingMessage ships information in heartbeat ping
type HeartbeatPingMessage struct {
	SendTime     clock.MonotonicTime `json:"send-time"`
	FromWorkerID WorkerID            `json:"from-worker-id"`
	Epoch        Epoch               `json:"epoch"`
	IsFinished   bool                `json:"is-finished"`
}

// HeartbeatPongMessage ships information in heartbeat pong
type HeartbeatPongMessage struct {
	SendTime   clock.MonotonicTime `json:"send-time"`
	ReplyTime  time.Time           `json:"reply-time"`
	ToWorkerID WorkerID            `json:"to-worker-id"`
	Epoch      Epoch               `json:"epoch"`
	IsFinished bool                `json:"is-finished"`
}

// StatusChangeRequest ships information when updating worker status
type StatusChangeRequest struct {
	SendTime     clock.MonotonicTime `json:"send-time"`
	FromMasterID MasterID            `json:"from-master-id"`
	Epoch        Epoch               `json:"epoch"`
	ExpectState  WorkerStatusCode    `json:"expect-state"`
}
