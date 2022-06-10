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

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/pipeline"
	"github.com/pingcap/tiflow/cdc/scheduler/internal"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/tp/schedulepb"
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

	tables map[model.TableID]*table

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

// NewAgent returns a new agent.
func NewAgent(ctx context.Context,
	captureID model.CaptureID,
	changeFeedID model.ChangeFeedID,
	messageServer *p2p.MessageServer,
	messageRouter p2p.MessageRouter,
	etcdClient *etcd.CDCEtcdClient,
	tableExecutor internal.TableExecutor,
) (internal.Agent, error) {
	result := &agent{
		version:      version.ReleaseSemver(),
		captureID:    captureID,
		changeFeedID: changeFeedID,
		tableExec:    tableExecutor,
	}
	trans, err := newTransport(ctx, changeFeedID, agentRole, messageServer, messageRouter)
	if err != nil {
		return nil, errors.Trace(err)
	}
	result.trans = trans

	etcdCliCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	ownerCaptureID, err := etcdClient.GetOwnerID(etcdCliCtx)
	if err != nil {
		if err != concurrency.ErrElectionNoLeader {
			return nil, errors.Trace(err)
		}
		// We tolerate the situation where there is no owner.
		// If we are registered in Etcd, an elected Owner will have to
		// contact us before it can schedule any table.
		log.Info("tpscheduler: no owner found. We will wait for an owner to contact us.",
			zap.String("ownerCaptureID", ownerCaptureID),
			zap.String("namespace", changeFeedID.Namespace),
			zap.String("changefeed", changeFeedID.ID),
			zap.Error(err))
		return result, nil
	}

	log.Info("tpscheduler: agent owner found",
		zap.String("ownerCaptureID", ownerCaptureID),
		zap.String("captureID", captureID),
		zap.String("namespace", changeFeedID.Namespace),
		zap.String("changefeed", changeFeedID.ID))

	revision, err := etcdClient.GetOwnerRevision(etcdCliCtx, ownerCaptureID)
	if err != nil {
		if cerror.ErrOwnerNotFound.Equal(err) || cerror.ErrNotOwner.Equal(err) {
			// These are expected errors when no owner has been elected
			log.Info("tpscheduler: no owner found when querying for the owner revision",
				zap.String("ownerCaptureID", ownerCaptureID),
				zap.String("captureID", captureID),
				zap.String("namespace", changeFeedID.Namespace),
				zap.String("changefeed", changeFeedID.ID),
				zap.Error(err))
			return result, nil
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
	inboundMessages, err := a.recvMsgs(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	outboundMessages, err := a.handleMessage(ctx, inboundMessages)
	if err != nil {
		return errors.Trace(err)
	}

	responses, err := a.poll(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	outboundMessages = append(outboundMessages, responses...)

	if err := a.sendMsgs(ctx, outboundMessages); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (a *agent) handleMessage(ctx context.Context, msg []*schedulepb.Message) ([]*schedulepb.Message, error) {
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
		case schedulepb.MsgHeartbeat:
			response := a.handleMessageHeartbeat(message.Heartbeat.GetTableIDs())
			result = append(result, response)
		case schedulepb.MsgDispatchTableRequest:
			response, err := a.handleMessageDispatchTableRequest(
				ctx, message.DispatchTableRequest, processorEpoch)
			if err != nil {
				return result, errors.Trace(err)
			}
			if response != nil {
				result = append(result, response)
			}
		default:
			log.Warn("tpscheduler: unknown message received",
				zap.String("capture", a.captureID),
				zap.String("namespace", a.changeFeedID.Namespace),
				zap.String("changefeed", a.changeFeedID.ID),
				zap.Any("message", message))
		}
	}
	return result, nil
}

func (a *agent) poll(ctx context.Context) ([]*schedulepb.Message, error) {
	result := make([]*schedulepb.Message, 0)
	for tableID, table := range a.tables {
		message, err := table.poll(ctx)
		if err != nil {
			return result, errors.Trace(err)
		}
		result = append(result, message)
		if table.status.State == schedulepb.TableStateStopped {
			delete(a.tables, tableID)
		}
	}
	return result, nil
}

func tableStatus2PB(state pipeline.TableState) schedulepb.TableState {
	switch state {
	case pipeline.TableStatePreparing:
		return schedulepb.TableStatePreparing
	case pipeline.TableStatePrepared:
		return schedulepb.TableStatePrepared
	case pipeline.TableStateReplicating:
		return schedulepb.TableStateReplicating
	case pipeline.TableStateStopping:
		return schedulepb.TableStateStopping
	case pipeline.TableStateStopped:
		return schedulepb.TableStateStopped
	case pipeline.TableStateAbsent:
		return schedulepb.TableStateAbsent
	default:
	}
	return schedulepb.TableStateUnknown
}

func (a *agent) newTableStatus(tableID model.TableID) schedulepb.TableStatus {
	meta := a.tableExec.GetTableMeta(tableID)
	state := tableStatus2PB(meta.State)
	return schedulepb.TableStatus{
		TableID: meta.TableID,
		State:   state,
		Checkpoint: schedulepb.Checkpoint{
			CheckpointTs: meta.CheckpointTs,
			ResolvedTs:   meta.ResolvedTs,
		},
	}
}

// refresh the table' status, should be called before any table operations.
func (t *table) refresh() {
	meta := t.executor.GetTableMeta(t.id)
	state := tableStatus2PB(meta.State)
	t.status = schedulepb.TableStatus{
		TableID: meta.TableID,
		State:   state,
		Checkpoint: schedulepb.Checkpoint{
			CheckpointTs: meta.CheckpointTs,
			ResolvedTs:   meta.ResolvedTs,
		},
	}
}

// refreshAllTables make the agent track all table's latest status
func (a *agent) refreshAllTables() {
	currentTables := a.tableExec.GetAllCurrentTables()
	for _, tableID := range currentTables {
		table, ok := a.tables[tableID]
		if !ok {
			table := newTable(tableID, a.tableExec)
			a.tables[tableID] = table
		}
		table.refresh()
	}
}

func (a *agent) collectAllTables(expected []model.TableID) []schedulepb.TableStatus {
	result := make([]schedulepb.TableStatus, 0, len(expected))
	for _, tableID := range expected {
		table, ok := a.tables[tableID]
		if ok {
			result = append(result, table.status)
		} else {
			stats := a.newTableStatus(tableID)
			result = append(result, stats)
		}
	}
	return result
}

func (a *agent) handleMessageHeartbeat(expected []model.TableID) *schedulepb.Message {
	// `heartbeat` would always be the first message received by the agent,
	// it's a good chance for the agent to warm up by collect all tables status.
	a.refreshAllTables()
	tables := a.collectAllTables(expected)
	response := &schedulepb.HeartbeatResponse{
		Tables:     tables,
		IsStopping: a.stopping,
	}

	message := &schedulepb.Message{
		Header:            a.newMessageHeader(),
		MsgType:           schedulepb.MsgHeartbeatResponse,
		From:              a.captureID,
		To:                a.ownerInfo.captureID,
		HeartbeatResponse: response,
	}

	log.Debug("tpscheduler: agent generate heartbeat response",
		zap.String("capture", a.captureID),
		zap.String("namespace", a.changeFeedID.Namespace),
		zap.String("changefeed", a.changeFeedID.ID),
		zap.Any("message", message))

	return message
}

type dispatchTableTask struct {
	TableID   model.TableID
	StartTs   model.Ts
	IsRemove  bool
	IsPrepare bool
	Epoch     schedulepb.ProcessorEpoch
}

func (a *agent) handleMessageDispatchTableRequest(
	ctx context.Context,
	request *schedulepb.DispatchTableRequest,
	epoch schedulepb.ProcessorEpoch,
) (*schedulepb.Message, error) {
	if a.epoch != epoch {
		log.Info("tpscheduler: agent receive dispatch table request "+
			"epoch does not match, ignore it",
			zap.String("capture", a.captureID),
			zap.String("namespace", a.changeFeedID.Namespace),
			zap.String("changefeed", a.changeFeedID.ID),
			zap.String("epoch", epoch.Epoch),
			zap.String("expected", a.epoch.Epoch))
		return nil, nil
	}
	var (
		table *table
		task  *dispatchTableTask
		ok    bool
	)
	switch req := request.Request.(type) {
	case *schedulepb.DispatchTableRequest_AddTable:
		if a.stopping {
			log.Info("tpscheduler: agent is stopping, and decline handle add table request",
				zap.String("capture", a.captureID),
				zap.String("namespace", a.changeFeedID.Namespace),
				zap.String("changefeed", a.changeFeedID.ID),
				zap.Any("request", request))
			return nil, nil
		}
		tableID := req.AddTable.GetTableID()
		task = &dispatchTableTask{
			TableID:   tableID,
			StartTs:   req.AddTable.GetCheckpoint().CheckpointTs,
			IsRemove:  false,
			IsPrepare: req.AddTable.GetIsSecondary(),
			Epoch:     epoch,
		}
		table, ok := a.tables[tableID]
		if !ok {
			table = newTable(tableID, a.tableExec)
			a.tables[tableID] = table
		}
	case *schedulepb.DispatchTableRequest_RemoveTable:
		tableID := req.RemoveTable.GetTableID()
		table, ok = a.tables[tableID]
		if !ok {
			log.Warn("tpscheduler: agent ignore remove table request,"+
				"since the table not found",
				zap.Any("tableID", tableID),
				zap.String("capture", a.captureID),
				zap.String("namespace", a.changeFeedID.Namespace),
				zap.String("changefeed", a.changeFeedID.ID),
				zap.Any("request", request))
			return nil, nil
		}
		task = &dispatchTableTask{
			TableID:  tableID,
			IsRemove: true,
			Epoch:    epoch,
		}
	default:
		log.Warn("tpscheduler: agent ignore unknown dispatch table request",
			zap.String("capture", a.captureID),
			zap.String("namespace", a.changeFeedID.Namespace),
			zap.String("changefeed", a.changeFeedID.ID),
			zap.Any("request", request))
		return nil, nil
	}
	table.injectDispatchTableTask(task)
	message, err := table.poll(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if table.status.State == schedulepb.TableStateStopped {
		delete(a.tables, table.id)
	}
	return message, nil
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
					Checkpoint: status.Checkpoint,
				},
			},
		},
	}

	return message
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
					Checkpoint: status.Checkpoint,
					Reject:     reject,
				},
			},
		},
	}
}

// GetLastSentCheckpointTs implement agent interface
func (a *agent) GetLastSentCheckpointTs() (checkpointTs model.Ts) {
	// no need to implement this.
	return internal.CheckpointCannotProceed
}

// Close implement agent interface
func (a *agent) Close() error {
	log.Debug("tpscheduler: agent closed",
		zap.String("capture", a.captureID),
		zap.String("namespace", a.changeFeedID.Namespace),
		zap.String("changefeed", a.changeFeedID.ID))
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
			log.Panic("tpscheduler: owner IDs do not match",
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

		log.Info("tpscheduler: new owner in power, drop pending dispatch table tasks",
			zap.String("capture", a.captureID),
			zap.String("namespace", a.changeFeedID.Namespace),
			zap.String("changefeed", a.changeFeedID.ID),
			zap.Any("owner", a.ownerInfo))
		return true
	}

	// staled owner heartbeat, just ignore it.
	log.Info("tpscheduler: message from staled owner",
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

func (a *agent) recvMsgs(ctx context.Context) ([]*schedulepb.Message, error) {
	messages, err := a.trans.Recv(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	n := 0
	for _, val := range messages {
		// only receive not staled messages
		if !a.handleOwnerInfo(val.From, val.Header.OwnerRevision.Revision, val.Header.Version) {
			continue
		}
		messages[n] = val
		n++
	}
	return messages[:n], nil
}

func (a *agent) sendMsgs(ctx context.Context, msgs []*schedulepb.Message) error {
	for i := range msgs {
		m := msgs[i]
		// Correctness check.
		if len(m.To) == 0 || m.MsgType == schedulepb.MsgUnknown {
			log.Panic("tpscheduler: invalid message no destination or unknown message type",
				zap.String("capture", a.captureID),
				zap.String("namespace", a.changeFeedID.Namespace),
				zap.String("changefeed", a.changeFeedID.ID),
				zap.Any("message", m))
		}
		m.Header = a.newMessageHeader()
		m.From = a.captureID
		m.To = a.ownerInfo.captureID
	}
	return a.trans.Send(ctx, msgs)
}

// table is a state machine that manage the table's state,
// also tracking its progress by utilize the `TableExecutor`
type table struct {
	id model.TableID

	status   schedulepb.TableStatus
	executor internal.TableExecutor

	task *dispatchTableTask
}

func newTable(tableID model.TableID, executor internal.TableExecutor) *table {
	return &table{
		id:       tableID,
		executor: executor,
		task:     nil,
	}
}

func (t *table) TableStatus() schedulepb.TableStatus {
	t.refresh()
	return t.status
}

func newAddTableResponseMessage(status schedulepb.TableStatus, reject bool) *schedulepb.Message {
	return &schedulepb.Message{
		MsgType: schedulepb.MsgDispatchTableResponse,
		DispatchTableResponse: &schedulepb.DispatchTableResponse{
			Response: &schedulepb.DispatchTableResponse_AddTable{
				AddTable: &schedulepb.AddTableResponse{
					Status:     &status,
					Checkpoint: status.Checkpoint,
					Reject:     reject,
				},
			},
		},
	}
}

func newRemoveTableResponseMessage(status schedulepb.TableStatus) *schedulepb.Message {
	message := &schedulepb.Message{
		MsgType: schedulepb.MsgDispatchTableResponse,
		DispatchTableResponse: &schedulepb.DispatchTableResponse{
			Response: &schedulepb.DispatchTableResponse_RemoveTable{
				RemoveTable: &schedulepb.RemoveTableResponse{
					Status:     &status,
					Checkpoint: status.Checkpoint,
				},
			},
		},
	}

	return message
}

func (t *table) handleRemoveTableTask(ctx context.Context) *schedulepb.Message {
	switch t.status.State {
	case schedulepb.TableStateAbsent:
		log.Warn("tpscheduler: remove table, but table is absent", zap.Any("table", t))
		t.task = nil
		message := newRemoveTableResponseMessage(t.status) // no checkpoint
		return message
	case schedulepb.TableStateStopping, // stopping now is useless
		schedulepb.TableStateStopped:
		// release table resource, and get the latest checkpoint
		checkpointTs, done := t.executor.IsRemoveTableFinished(ctx, t.id)
		if !done {
			// actually, this should never be triggered, since we know that table is stopped.
			return nil
		}
		t.task = nil
		t.status.Checkpoint.CheckpointTs = checkpointTs
		t.status.State = schedulepb.TableStateStopped
		message := newRemoveTableResponseMessage(t.status)
		return message
	case schedulepb.TableStatePreparing,
		schedulepb.TableStatePrepared,
		schedulepb.TableStateReplicating:
		done := t.executor.RemoveTable(ctx, t.task.TableID)
		status := t.TableStatus()
		if !done {
			// set table state be `stopping` to let the
			// coordinator know that the request was received.
			status.State = schedulepb.TableStateStopping
		}
		message := newRemoveTableResponseMessage(status)
		return message
	default:
	}
	return nil
}

func (t *table) handleAddTableTask(ctx context.Context) (*schedulepb.Message, error) {
	switch t.status.State {
	case schedulepb.TableStateAbsent:
		done, err := t.executor.AddTable(ctx, t.task.TableID, t.task.StartTs, t.task.IsPrepare)
		if err != nil || !done {
			log.Info("tpscheduler: agent add table failed",
				zap.Any("task", t.task),
				zap.Error(err))
			status := t.status
			message := newAddTableResponseMessage(status, false)
			return message, errors.Trace(err)
		}
	case schedulepb.TableStateReplicating:
		// `replicating` is a stable state, so we return a response message here
		// this also indicate that the `add table` task finished.
		t.task = nil
		log.Info("tpscheduler: table is replicating", zap.Any("table", t))
		message := newAddTableResponseMessage(t.status, false)
		return message, nil
	case schedulepb.TableStatePrepared:
		if t.task.IsPrepare {
			// `prepared` is a stable state, if the task was to prepare the table.
			t.task = nil
			log.Info("tpscheduler: table is prepared", zap.Any("table", t))
			message := newAddTableResponseMessage(t.status, false)
			return message, nil
		}
		// if this hit, the `task` must be `isPrepare` false
		done, err := t.executor.AddTable(ctx, t.task.TableID, t.task.StartTs, false)
		if err != nil || !done {
			log.Info("tpscheduler: agent add table failed",
				zap.Any("task", t.task),
				zap.Error(err))
			status := t.status
			message := newAddTableResponseMessage(status, false)
			return message, errors.Trace(err)
		}
	case schedulepb.TableStatePreparing,
		schedulepb.TableStateStopping,
		schedulepb.TableStateStopped:
		log.Warn("tpscheduler: agent ignore add table", zap.Any("table", t))
		t.task = nil
	default:
	}
	return nil, nil
}

func (t *table) injectDispatchTableTask(task *dispatchTableTask) {
	if t.id != task.TableID {
		log.Panic("tpscheduler: tableID not match",
			zap.Int64("tableID", t.id),
			zap.Int64("task.TableID", task.TableID))
	}
	if t.task == nil {
		log.Info("tpscheduler: table found new task",
			zap.Any("table", t), zap.Any("task", task))
		t.task = task
		return
	}
	log.Warn("tpscheduler: table inject dispatch table task ignored,"+
		"since there is one not finished yet",
		zap.Any("table", t),
		zap.Any("nowTask", t.task),
		zap.Any("ignoredTask", task))
}

func (t *table) poll(ctx context.Context) (*schedulepb.Message, error) {
	if t.task == nil {
		return nil, nil
	}
	t.refresh()
	if t.task.IsRemove {
		return t.handleRemoveTableTask(ctx), nil
	}
	return t.handleAddTableTask(ctx)
}
