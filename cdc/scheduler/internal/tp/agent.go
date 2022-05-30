// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tp

import (
	"context"
	"time"

	"github.com/edwingeng/deque"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/pipeline"
	"github.com/pingcap/tiflow/cdc/scheduler/internal"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/tp/schedulepb"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/version"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

var _ internal.Agent = (*agent)(nil)

type agent struct {
	trans     transport
	tableExec internal.TableExecutor

	// runningTasks track all in progress dispatch table task
	runningTasks map[model.TableID]*dispatchTableTask

	// pendingTasks is a queue of dispatch table task yet to be processed.
	// the Deque stores *dispatchTableTask.
	pendingTasks deque.Deque

	// owner's information
	ownerInfo ownerInfo

	// maintain the capture's information
	version      string
	captureID    model.CaptureID
	changeFeedID model.ChangeFeedID
	epoch        schedulepb.ProcessorEpoch

	// the capture is stopping, should reject all add table request
	stopping bool
}

type ownerInfo struct {
	revision  schedulepb.OwnerRevision
	version   string
	captureID string
}

func NewAgent(ctx context.Context,
	captureID model.CaptureID,
	changeFeedID model.ChangeFeedID,
	messageServer *p2p.MessageServer,
	messageRouter p2p.MessageRouter,
	etcdClient *etcd.CDCEtcdClient,
	tableExecutor internal.TableExecutor) (internal.Agent, error) {
	result := &agent{
		version:      version.ReleaseSemver(),
		captureID:    captureID,
		changeFeedID: changeFeedID,
		tableExec:    tableExecutor,
		pendingTasks: deque.NewDeque(),
		runningTasks: make(map[model.TableID]*dispatchTableTask),
	}
	trans, err := newTransport(ctx, changeFeedID, messageServer, messageRouter)
	if err != nil {
		return nil, errors.Trace(err)
	}
	result.trans = trans

	conf := config.GetGlobalServerConfig()
	flushInterval := time.Duration(conf.ProcessorFlushInterval)

	log.Debug("creating processor agent",
		zap.String("capture", captureID),
		zap.String("namespace", changeFeedID.Namespace),
		zap.String("changefeed", changeFeedID.ID),
		zap.Duration("sendCheckpointTsInterval", flushInterval))

	etcdCliCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	ownerCaptureID, err := etcdClient.GetOwnerID(etcdCliCtx, etcd.CaptureOwnerKey)
	if err != nil {
		if err != concurrency.ErrElectionNoLeader {
			return nil, errors.Trace(err)
		}
		// We tolerate the situation where there is no owner.
		// If we are registered in Etcd, an elected Owner will have to
		// contact us before it can schedule any table.
		log.Info("no owner found. We will wait for an owner to contact us.",
			zap.String("capture", captureID),
			zap.String("namespace", changeFeedID.Namespace),
			zap.String("changefeed", changeFeedID.ID),
			zap.Error(err))
		return nil, nil
	}

	log.Debug("agent: owner found",
		zap.String("capture", captureID),
		zap.String("namespace", changeFeedID.Namespace),
		zap.String("changefeed", changeFeedID.ID),
		zap.String("ownerID", captureID))

	revision, err := etcdClient.GetOwnerRevision(etcdCliCtx, captureID)
	if err != nil {
		if cerror.ErrOwnerNotFound.Equal(err) || cerror.ErrNotOwner.Equal(err) {
			// These are expected errors when no owner has been elected
			log.Info("no owner found when querying for the owner revision",
				zap.String("capture", captureID),
				zap.String("namespace", changeFeedID.Namespace),
				zap.String("changefeed", changeFeedID.ID),
				zap.Error(err))
			return nil, nil
		}
		return nil, err
	}

	result.resetEpoch()
	result.ownerInfo = ownerInfo{
		// owner's version can only be got by receiving heartbeat
		version:   "",
		captureID: ownerCaptureID,
		revision:  schedulepb.OwnerRevision{Revision: revision},
	}
	return result, nil
}

// Tick implement agent interface
func (a *agent) Tick(ctx context.Context) error {
	inboundMessages, err := a.trans.Recv(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	outboundMessages, err := a.handleMessage(inboundMessages)
	if err != nil {
		return errors.Trace(err)
	}

	responses, err := a.handleDispatchTableTasks(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	outboundMessages = append(outboundMessages, responses...)

	if err := a.trans.Send(ctx, outboundMessages); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (a *agent) handleMessage(msg []*schedulepb.Message) ([]*schedulepb.Message, error) {
	result := make([]*schedulepb.Message, 0)
	for _, message := range msg {
		ownerCaptureID := message.GetFrom()
		header := message.GetHeader()
		ownerVersion := header.GetVersion()
		ownerRevision := header.GetOwnerRevision().Revision
		processorEpoch := header.GetProcessorEpoch()

		if !a.handleOwnerInfo(ownerCaptureID, ownerRevision, ownerVersion) {
			continue
		}

		switch message.GetMsgType() {
		case schedulepb.MsgDispatchTableRequest:
			a.handleMessageDispatchTableRequest(message.DispatchTableRequest, processorEpoch)
		case schedulepb.MsgHeartbeat:
			response := a.handleMessageHeartbeat()
			result = append(result, response)
		case schedulepb.MsgUnknown:
		default:
			log.Warn("unknown message received", zap.Any("message", message))
		}
	}

	return result, nil
}

func tableStatus2PB(status pipeline.TableStatus) schedulepb.TableState {
	switch status {
	case pipeline.TableStatusPreparing:
		return schedulepb.TableStatePreparing
	case pipeline.TableStatusPrepared:
		return schedulepb.TableStatePrepared
	case pipeline.TableStatusReplicating:
		return schedulepb.TableStateReplicating
	case pipeline.TableStatusStopping:
		return schedulepb.TableStateStopping
	case pipeline.TableStatusStopped:
		return schedulepb.TableStateStopped
	}
	return schedulepb.TableStateAbsent
}

func (a *agent) newTableStatus(tableID model.TableID) schedulepb.TableStatus {
	meta := a.tableExec.GetTableMeta(tableID)
	return schedulepb.TableStatus{
		TableID: meta.TableID,
		State:   tableStatus2PB(meta.Status),
		Checkpoint: schedulepb.Checkpoint{
			CheckpointTs: meta.CheckpointTs,
			ResolvedTs:   meta.ResolvedTs,
		},
	}
}

func (a *agent) collectTableStatus() []schedulepb.TableStatus {
	allTables := a.tableExec.GetAllCurrentTables()
	tables := make([]schedulepb.TableStatus, 0, len(allTables))
	for _, tableID := range allTables {
		status := a.newTableStatus(tableID)
		tables = append(tables, status)
	}
	return tables
}

func (a *agent) handleMessageHeartbeat() *schedulepb.Message {
	tables := a.collectTableStatus()
	response := &schedulepb.HeartbeatResponse{
		Tables:     tables,
		IsStopping: a.stopping,
	}
	return &schedulepb.Message{
		Header:            a.newMessageHeader(),
		MsgType:           schedulepb.MsgHeartbeatResponse,
		From:              a.captureID,
		To:                a.ownerInfo.captureID,
		HeartbeatResponse: response,
	}
}

type dispatchTableTaskStatus int32

const (
	dispatchTableTaskReceived = dispatchTableTaskStatus(iota + 1)
	dispatchTableTaskProcessed
)

type dispatchTableTask struct {
	TableID   model.TableID
	StartTs   model.Ts
	IsRemove  bool
	IsPrepare bool
	Epoch     schedulepb.ProcessorEpoch
	status    dispatchTableTaskStatus
}

func (a *agent) handleMessageDispatchTableRequest(
	request *schedulepb.DispatchTableRequest,
	epoch schedulepb.ProcessorEpoch,
) {
	if a.epoch != epoch {
		log.Info("agent: dispatch table request epoch does not match, ignore it",
			zap.String("capture", a.captureID),
			zap.String("namespace", a.changeFeedID.Namespace),
			zap.String("changefeed", a.changeFeedID.ID),
			zap.Any("epoch", epoch),
			zap.Any("expected", a.epoch))
		return
	}
	var task *dispatchTableTask
	switch req := request.Request.(type) {
	case *schedulepb.DispatchTableRequest_AddTable:
		if a.stopping {
			log.Info("agent: decline handle add table request",
				zap.String("capture", a.captureID),
				zap.String("namespace", a.changeFeedID.Namespace),
				zap.String("changefeed", a.changeFeedID.ID))
			return
		}
		task = &dispatchTableTask{
			TableID:   req.AddTable.GetTableID(),
			StartTs:   req.AddTable.GetCheckpoint().GetCheckpointTs(),
			IsRemove:  false,
			IsPrepare: req.AddTable.GetIsSecondary(),
			Epoch:     epoch,
			status:    dispatchTableTaskReceived,
		}
	case *schedulepb.DispatchTableRequest_RemoveTable:
		task = &dispatchTableTask{
			TableID:  req.RemoveTable.GetTableID(),
			IsRemove: true,
			Epoch:    epoch,
			status:   dispatchTableTaskReceived,
		}
	default:
		log.Warn("agent: ignore unknown dispatch table request",
			zap.String("capture", a.captureID),
			zap.String("namespace", a.changeFeedID.Namespace),
			zap.String("changefeed", a.changeFeedID.ID),
			zap.Any("request", request))
		return
	}

	if task == nil {
		log.Panic("invalid dispatch table request",
			zap.String("capture", a.captureID),
			zap.String("namespace", a.changeFeedID.Namespace),
			zap.String("changefeed", a.changeFeedID.ID),
			zap.Any("request", request))
	}

	if _, ok := a.runningTasks[task.TableID]; ok {
		log.Panic("duplicate dispatch table request",
			zap.String("capture", a.captureID),
			zap.String("namespace", a.changeFeedID.Namespace),
			zap.String("changefeed", a.changeFeedID.ID),
			zap.Any("request", request))
	}

	a.pendingTasks.PushBack(task)
}

func (a *agent) handleRemoveTableTask(
	ctx context.Context,
	task *dispatchTableTask,
) (response *schedulepb.Message) {
	if task.status == dispatchTableTaskReceived {
		done := a.tableExec.RemoveTable(ctx, task.TableID)
		if !done {
			status := a.newTableStatus(task.TableID)
			return a.newRemoveTableResponseMessage(status)
		}
		task.status = dispatchTableTaskProcessed
	}

	checkpointTs, done := a.tableExec.IsRemoveTableFinished(ctx, task.TableID)
	if !done {
		return nil
	}
	status := schedulepb.TableStatus{
		TableID: task.TableID,
		State:   schedulepb.TableStateStopped, // todo: if the table is `absent`, also return a stopped here, `stopped` is identical to `absent`.
		Checkpoint: schedulepb.Checkpoint{
			CheckpointTs: checkpointTs,
		},
	}
	message := a.newRemoveTableResponseMessage(status)
	delete(a.runningTasks, task.TableID)
	return message
}

func (a *agent) newRemoveTableResponseMessage(
	status schedulepb.TableStatus,
) *schedulepb.Message {
	message := &schedulepb.Message{
		Header:  a.newMessageHeader(),
		MsgType: schedulepb.MsgDispatchTableResponse,
		From:    a.captureID,
		To:      a.ownerInfo.captureID,
		DispatchTableResponse: &schedulepb.DispatchTableResponse{
			Response: &schedulepb.DispatchTableResponse_RemoveTable{
				RemoveTable: &schedulepb.RemoveTableResponse{
					Status:     &status,
					Checkpoint: &status.Checkpoint,
				},
			},
		},
	}

	return message
}

func (a *agent) handleAddTableTask(
	ctx context.Context,
	task *dispatchTableTask,
) (*schedulepb.Message, error) {
	if task.status == dispatchTableTaskReceived {
		if a.stopping {
			status := a.newTableStatus(task.TableID)
			message := a.newAddTableResponseMessage(status, true)
			delete(a.runningTasks, task.TableID)
			return message, nil
		}
		done, err := a.tableExec.AddTable(ctx, task.TableID, task.StartTs, task.IsPrepare)
		if err != nil || !done {
			log.Info("add table failed",
				zap.String("capture", a.captureID),
				zap.String("namespace", a.changeFeedID.Namespace),
				zap.String("changefeed", a.changeFeedID.ID),
				zap.Any("task", task),
				zap.Error(err))
			status := a.newTableStatus(task.TableID)
			message := a.newAddTableResponseMessage(status, false)
			return message, errors.Trace(err)
		}
		task.status = dispatchTableTaskProcessed
	}

	done := a.tableExec.IsAddTableFinished(ctx, task.TableID, task.IsPrepare)
	if !done {
		return nil, nil
	}
	// must be finished here
	log.Info("finish processing add table task",
		zap.String("capture", a.captureID),
		zap.String("namespace", a.changeFeedID.Namespace),
		zap.String("changefeed", a.changeFeedID.ID),
		zap.Any("task", task))

	status := a.newTableStatus(task.TableID)
	message := a.newAddTableResponseMessage(status, false)
	delete(a.runningTasks, task.TableID)

	return message, nil
}

func (a *agent) newAddTableResponseMessage(
	status schedulepb.TableStatus,
	reject bool,
) *schedulepb.Message {
	return &schedulepb.Message{
		Header:  a.newMessageHeader(),
		MsgType: schedulepb.MsgDispatchTableResponse,
		From:    a.captureID,
		To:      a.ownerInfo.captureID,
		DispatchTableResponse: &schedulepb.DispatchTableResponse{
			Response: &schedulepb.DispatchTableResponse_AddTable{
				AddTable: &schedulepb.AddTableResponse{
					Status:     &status,
					Checkpoint: &status.Checkpoint,
					Reject:     reject,
				},
			},
		},
	}
}

func (a *agent) fetchPendingTasks() {
	for !a.pendingTasks.Empty() {
		batch := a.pendingTasks.PopManyFront(128 /* batch size */)
		for _, item := range batch {
			task := item.(*dispatchTableTask)
			if task.Epoch != a.epoch {
				log.Info("dispatch request epoch does not match",
					zap.String("capture", a.captureID),
					zap.String("namespace", a.changeFeedID.Namespace),
					zap.String("changefeed", a.changeFeedID.ID),
					zap.Any("epoch", task.Epoch),
					zap.Any("expected", a.epoch))
				continue
			}
			if _, ok := a.runningTasks[task.TableID]; ok {
				log.Panic("duplicate dispatch table request",
					zap.String("capture", a.captureID),
					zap.String("namespace", a.changeFeedID.Namespace),
					zap.String("changefeed", a.changeFeedID.ID),
					zap.Any("task", task))
			}
			a.runningTasks[task.TableID] = task
		}
	}
}

func (a *agent) handleDispatchTableTasks(
	ctx context.Context,
) (result []*schedulepb.Message, err error) {
	a.fetchPendingTasks()
	result = make([]*schedulepb.Message, 0)
	for _, task := range a.runningTasks {
		var response *schedulepb.Message
		if task.IsRemove {
			response = a.handleRemoveTableTask(ctx, task)
		} else {
			response, err = a.handleAddTableTask(ctx, task)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		if response != nil {
			result = append(result, response)
		}
	}
	return result, nil
}

// GetLastSentCheckpointTs implement agent interface
func (a *agent) GetLastSentCheckpointTs() (checkpointTs model.Ts) {
	// no need to implement this.
	return internal.CheckpointCannotProceed
}

// Close implement agent interface
func (a *agent) Close() error {
	log.Debug("agent: closing")
	return a.trans.Close()
}

func (a *agent) newMessageHeader() *schedulepb.Message_Header {
	return &schedulepb.Message_Header{
		Version:        a.version,
		OwnerRevision:  a.ownerInfo.revision,
		ProcessorEpoch: a.epoch,
	}
}

// handleOwnerInfo return false, if the given owner's info is staled.
// update owner's info to the latest otherwise.
// id: the incoming owner's capture ID
// revision: the incoming owner's revision as generated by Etcd election.
// version: the incoming owner's semantic version string
func (a *agent) handleOwnerInfo(id model.CaptureID, revision int64, version string) bool {
	if a.ownerInfo.revision.Revision == revision {
		if a.ownerInfo.captureID != id {
			// This panic will happen only if two messages have been received
			// with the same ownerRev but with different ownerIDs.
			// This should never happen unless the election via Etcd is buggy.
			log.Panic("owner IDs do not match",
				zap.String("capture", a.captureID),
				zap.String("namespace", a.changeFeedID.Namespace),
				zap.String("changefeed", a.changeFeedID.ID),
				zap.String("expected", a.ownerInfo.captureID),
				zap.String("actual", id))
		}
		return true
	}

	// the current owner is staled
	if a.ownerInfo.revision.Revision < revision {
		a.ownerInfo.captureID = id
		a.ownerInfo.revision.Revision = revision
		a.ownerInfo.version = version

		a.resetEpoch()

		log.Info("new owner in power, drop pending dispatch table tasks",
			zap.String("capture", a.captureID),
			zap.String("namespace", a.changeFeedID.Namespace),
			zap.String("changefeed", a.changeFeedID.ID),
			zap.Any("owner", a.ownerInfo),
			zap.Int("droppedTaskCount", a.pendingTasks.Len()))
		// Resets the deque so that pending operations from the previous owner
		// will not be processed.
		// Note: these pending operations have not yet been processed by the agent,
		// so it is okay to lose them.
		a.pendingTasks = deque.NewDeque()
		return true
	}

	// staled owner heartbeat, just ignore it.
	log.Info("agent: message from staled owner",
		zap.String("capture", a.captureID),
		zap.String("namespace", a.changeFeedID.Namespace),
		zap.String("changefeed", a.changeFeedID.ID),
		zap.Any("staledOwner", ownerInfo{
			captureID: id,
			revision:  schedulepb.OwnerRevision{Revision: revision},
			version:   version,
		}),
		zap.Any("owner", a.ownerInfo))
	return false
}

func (a *agent) resetEpoch() {
	a.epoch = schedulepb.ProcessorEpoch{Epoch: uuid.New().String()}
}
