package dm

import (
	"fmt"

	"github.com/hanfei1991/microcosm/jobmaster/dm/metadata"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

// Message use for asynchronous message.
// It use json format to transfer, all the fields should be public.
type Message interface{}

// MessageWithID use for synchronous request/response message.
type MessageWithID struct {
	ID      uint64
	Message Message
}

type Request Message

type Response Message

func OperateTaskMessageTopic(masterID libModel.MasterID, taskID string) p2p.Topic {
	return fmt.Sprintf("operate-task-message-%s-%s", masterID, taskID)
}

type OperateTaskMessage struct {
	TaskID string
	Stage  metadata.TaskStage
}
