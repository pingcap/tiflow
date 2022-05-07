package model

import (
	"fmt"
	"time"

	"github.com/hanfei1991/microcosm/pkg/clock"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

func HeartbeatPingTopic(masterID MasterID) p2p.Topic {
	return fmt.Sprintf("heartbeat-ping-%s", masterID)
}

func HeartbeatPongTopic(masterID MasterID, workerID WorkerID) p2p.Topic {
	// TODO do we need hex-encoding here?
	return fmt.Sprintf("heartbeat-pong-%s-%s", masterID, workerID)
}

func WorkerStatusChangeRequestTopic(masterID MasterID, workerID WorkerID) p2p.Topic {
	return fmt.Sprintf("worker-status-change-req-%s-%s", masterID, workerID)
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

type StatusChangeRequest struct {
	SendTime     clock.MonotonicTime `json:"send-time"`
	FromMasterID MasterID            `json:"from-master-id"`
	Epoch        Epoch               `json:"epoch"`
	ExpectState  WorkerStatusCode    `json:"expect-state"`
}
