package dm

import (
	"context"
	"time"

	"github.com/hanfei1991/microcosm/jobmaster/dm/runtime"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	dmpkg "github.com/hanfei1991/microcosm/pkg/dm"
)

var defaultResponseTimeOut = time.Second * 2

type SendHandle interface {
	ID() libModel.WorkerID
	SendMessage(ctx context.Context, topic string, message interface{}, nonblocking bool) error
}

// MessageAgent hold by TaskWorker, it manage all interactions with jobmaster
type MessageAgent struct {
	messagePair *dmpkg.MessagePair
	sendHandle  SendHandle
	id          libModel.WorkerID
	taskID      string
}

func NewMessageAgent(sender SendHandle) *MessageAgent {
	messageAgent := &MessageAgent{
		messagePair: dmpkg.NewMessagePair(),
		sendHandle:  sender,
	}
	return messageAgent
}

func (agent *MessageAgent) OperateTaskResponse(ctx context.Context, messageID uint64, taskStatus runtime.TaskStatus, errMsg string) error {
	topic := dmpkg.QueryStatusRequestTopic(agent.id, agent.taskID)
	response := &dmpkg.QueryStatusResponse{
		ErrorMsg:   errMsg,
		TaskStatus: taskStatus,
	}

	ctx, cancel := context.WithTimeout(ctx, defaultResponseTimeOut)
	defer cancel()
	return agent.messagePair.SendResponse(ctx, topic, messageID, response, agent.sendHandle)
}
