package dm

import (
	"context"
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/jobmaster/dm/config"
	"github.com/hanfei1991/microcosm/jobmaster/dm/metadata"
	"github.com/hanfei1991/microcosm/lib"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/clock"
	dmpkg "github.com/hanfei1991/microcosm/pkg/dm"
	"github.com/pingcap/errors"
)

var (
	DefaultMessageTimeOut = time.Second * 2
	DefaultRequestTimeOut = time.Second * 30
)

type Master interface {
	// for create worker
	CreateWorker(workerType lib.WorkerType, config lib.WorkerConfig, cost model.RescUnit) (lib.WorkerID, error)
	// for operate-task
	CurrentEpoch() lib.Epoch
}

type SendHandle interface {
	ID() lib.WorkerID
	SendMessage(ctx context.Context, topic string, message interface{}, nonblocking bool) error
}

// MessageAgent hold by Jobmaster, it manage all interactions with workers
type MessageAgent struct {
	master      Master
	clocker     clock.Clock
	messagePair *dmpkg.MessagePair
	// for stop a task
	id lib.WorkerID
	// taskID -> Sender(WorkerHandle)
	sendHandles sync.Map
}

func NewMessageAgent(initSenders map[string]SendHandle, id lib.WorkerID, master Master) *MessageAgent {
	messageAgent := &MessageAgent{
		master:      master,
		clocker:     clock.New(),
		id:          id,
		messagePair: dmpkg.NewMessagePair(),
	}
	for task, sender := range initSenders {
		messageAgent.UpdateWorkerHandle(task, sender)
	}
	return messageAgent
}

func (agent *MessageAgent) UpdateWorkerHandle(taskID string, sendHandle SendHandle) {
	if sendHandle == nil {
		agent.sendHandles.Delete(taskID)
	} else {
		agent.sendHandles.Store(taskID, sendHandle)
	}
}

// Manage all interactions with workers in the message agent
// Though we can create worker in jobmaster directly
func (agent *MessageAgent) CreateWorker(ctx context.Context, taskID string, workerType lib.WorkerType, taskCfg *config.TaskCfg) (lib.WorkerID, error) {
	if _, ok := agent.sendHandles.Load(taskID); ok {
		return "", errors.Errorf("worker for task %s already exist", taskID)
	}
	return agent.master.CreateWorker(workerType, taskCfg, 1)
}

func (agent *MessageAgent) StopWorker(ctx context.Context, taskID lib.WorkerID, workerID lib.WorkerID) error {
	v, ok := agent.sendHandles.Load(taskID)
	if !ok {
		return errors.Errorf("worker for task %s not exist", taskID)
	}

	sender := v.(SendHandle)
	if sender.ID() != workerID {
		return errors.Errorf("worker for task %s mismatch: want %s, get %s", taskID, workerID, sender.ID())
	}

	topic := lib.WorkerStatusChangeRequestTopic(agent.id, workerID)
	message := &lib.StatusChangeRequest{
		SendTime:     agent.clocker.Mono(),
		FromMasterID: agent.id,
		Epoch:        agent.master.CurrentEpoch(),
		ExpectState:  libModel.WorkerStatusStopped,
	}

	ctx, cancel := context.WithTimeout(ctx, DefaultMessageTimeOut)
	defer cancel()
	return sender.SendMessage(ctx, topic, message, true)
}

func (agent *MessageAgent) OperateTask(ctx context.Context, taskID string, stage metadata.TaskStage) error {
	if stage != metadata.StageRunning && stage != metadata.StagePaused {
		return errors.Errorf("invalid expected stage %d for task %s", stage, taskID)
	}
	v, ok := agent.sendHandles.Load(taskID)
	if !ok {
		return errors.Errorf("worker for task %s not exist", taskID)
	}

	topic := dmpkg.OperateTaskMessageTopic(agent.id, taskID)
	message := &dmpkg.OperateTaskMessage{
		TaskID: taskID,
		Stage:  stage,
	}

	ctx, cancel := context.WithTimeout(ctx, DefaultMessageTimeOut)
	defer cancel()
	return v.(SendHandle).SendMessage(ctx, topic, message, true)
}

func (agent *MessageAgent) OnWorkerMessage(response dmpkg.MessageWithID) error {
	return agent.messagePair.OnResponse(response)
}
