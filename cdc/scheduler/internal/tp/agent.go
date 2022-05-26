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
	"github.com/pingcap/tiflow/cdc/scheduler/internal"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/tp/schedulepb"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/p2p"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

const (
	getOwnerFromEtcdTimeout         = time.Second * 5
	messageHandlerOperationsTimeout = time.Second * 5
	barrierNotAdvancingWarnDuration = time.Second * 10
	printWarnLogMinInterval         = time.Second * 1
)

var _ internal.Agent = (*agent)(nil)

type agent struct {
	trans     transport
	tableExec internal.TableExecutor

	tables map[model.TableID]*schedulepb.TableStatus

	// runningTasks track all in progress dispatch table request
	runningTasks map[model.TableID]*dispatchTableTask

	// maintain owner information
	ownerInfo *ownerInfo

	// maintain capture information
	epoch string
	// todo: shall we maintain this?
	captureID    model.CaptureID
	changeFeedID model.ChangeFeedID

	// todo: these fields need to be revised
	barrierSeqs map[p2p.Topic]p2p.Seq
}

type ownerInfo struct {
	version   string
	captureID string
	revision  int64
}

func NewAgent(ctx context.Context,
	changeFeedID model.ChangeFeedID,
	messageServer *p2p.MessageServer,
	messageRouter p2p.MessageRouter,
	etcdClient *etcd.CDCEtcdClient,
	tableExecutor internal.TableExecutor) (internal.Agent, error) {
	result := &agent{
		epoch:        uuid.New().String(),
		changeFeedID: changeFeedID,
		tableExec:    tableExecutor,
		tables:       make(map[model.TableID]*schedulepb.TableStatus),
		runningTasks: make(map[model.TableID]*dispatchTableTask),
	}
	trans, err := newTransport(ctx, changeFeedID, messageServer, messageRouter)
	if err != nil {
		return nil, err
	}
	result.trans = trans

	conf := config.GetGlobalServerConfig()
	flushInterval := time.Duration(conf.ProcessorFlushInterval)

	log.Debug("creating processor agent",
		zap.String("namespace", changeFeedID.Namespace),
		zap.String("changefeed", changeFeedID.ID),
		zap.Duration("sendCheckpointTsInterval", flushInterval))

	//result.Agent = base.NewBaseAgent(changeFeedID, tableExecutor, result, &base.AgentConfig{SendCheckpointTsInterval: flushInterval})
	etcdCliCtx, cancel := context.WithTimeout(ctx, getOwnerFromEtcdTimeout)
	defer cancel()

	captureID, err := etcdClient.GetOwnerID(etcdCliCtx, etcd.CaptureOwnerKey)
	if err != nil {
		if err != concurrency.ErrElectionNoLeader {
			return nil, err
		}
		// We tolerate the situation where there is no owner.
		// If we are registered in Etcd, an elected Owner will have to
		// contact us before it can schedule any table.
		log.Info("no owner found. We will wait for an owner to contact us.",
			zap.String("namespace", changeFeedID.Namespace),
			zap.String("changefeed", changeFeedID.ID),
			zap.Error(err))
		return nil, err
	}

	log.Debug("found owner",
		zap.String("namespace", changeFeedID.Namespace),
		zap.String("changefeed", changeFeedID.ID),
		zap.String("ownerID", captureID))

	revision, err := etcdClient.GetOwnerRevision(etcdCliCtx, captureID)
	if err != nil {
		if cerror.ErrOwnerNotFound.Equal(err) || cerror.ErrNotOwner.Equal(err) {
			// These are expected errors when no owner has been elected
			log.Info("no owner found when querying for the owner revision",
				zap.String("namespace", changeFeedID.Namespace),
				zap.String("changefeed", changeFeedID.ID),
				zap.Error(err))
			return nil, nil
		}
		return nil, err
	}

	result.ownerInfo = &ownerInfo{
		// todo: how to get owner's `version` ?
		version:   "",
		captureID: captureID,
		revision:  revision,
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
		header := message.GetHeader()
		if !a.updateOwnerInfo(message.GetFrom(), header.GetVersion(), header.GetOwnerRevision().Revision) {
			continue
		}

		switch message.GetMsgType() {
		case schedulepb.MsgDispatchTableRequest:
			a.handleMessageDispatchTableRequest(message.DispatchTableRequest, message.GetFrom(), header.GetProcessorEpoch())
		case schedulepb.MsgHeartbeat:
			response, err := a.handleMessageHeartbeat()
			if err != nil {
				log.Warn("agent: handle heartbeat failed", zap.Error(err))
			}
			if response != nil {
				result = append(result, response)
			}
		case schedulepb.MsgUnknown:
		default:
			log.Warn("unknown message received")
		}
	}

	return result, nil
}

func (a *agent) handleMessageHeartbeat() (*schedulepb.Message, error) {
	// TODO: build s.tables from Heartbeat message.
	tables := make([]schedulepb.TableStatus, 0, len(a.tables))
	for _, table := range a.tables {
		// `table` should always track the latest information ?
		tables = append(tables, *table)
	}
	response := &schedulepb.HeartbeatResponse{
		Tables: tables,
		// todo (Ling Jin): how to set `IsStopping`
		IsStopping: false,
	}
	return &schedulepb.Message{
		Header:            a.newMessageHeader(),
		MsgType:           schedulepb.MsgHeartbeatResponse,
		From:              a.captureID,
		To:                a.ownerInfo.captureID,
		HeartbeatResponse: response,
	}, nil
}

type dispatchTableRequestStatus int32

const (
	dispatchTableRequestReceived = dispatchTableRequestStatus(iota + 1)
	dispatchTableRequestProcessed
	dispatchTableRequestFinished
)

type dispatchTableTask struct {
	TableID   model.TableID
	StartTs   model.Ts
	IsRemove  bool
	IsPrepare bool
	Epoch     schedulepb.ProcessorEpoch

	// FromOwnerID is for debugging purposesFromOwnerID
	FromOwnerID model.CaptureID

	status dispatchTableRequestStatus
}

func (a *agent) handleMessageDispatchTableRequest(request *schedulepb.DispatchTableRequest, from model.CaptureID, epoch schedulepb.ProcessorEpoch) {
	if epoch.Epoch != a.epoch {
		log.Info("dispatch table request epoch does not match, ignore it",
			zap.String("epoch", epoch.Epoch),
			zap.String("expected", a.epoch))
		return
	}

	var task *dispatchTableTask
	if addTable := request.GetAddTable(); addTable != nil {
		task = &dispatchTableTask{
			TableID:     addTable.GetTableID(),
			StartTs:     addTable.GetCheckpoint().GetCheckpointTs(),
			IsRemove:    false,
			IsPrepare:   addTable.GetIsSecondary(),
			Epoch:       epoch,
			FromOwnerID: from,
			status:      dispatchTableRequestReceived,
		}
	} else if removeTable := request.GetRemoveTable(); removeTable != nil {
		task = &dispatchTableTask{
			TableID:     removeTable.GetTableID(),
			IsRemove:    true,
			Epoch:       epoch,
			FromOwnerID: from,
			status:      dispatchTableRequestReceived,
		}
	}

	if task == nil {
		log.Panic("invalid dispatch table request", zap.Any("request", request))
	}

	if _, ok := a.runningTasks[task.TableID]; ok {
		log.Panic("duplicate dispatch table request", zap.Any("request", request))
	}

	a.runningTasks[task.TableID] = task
}

func (a *agent) handleRemoveTableTask(ctx context.Context, task *dispatchTableTask) (response *schedulepb.Message, err error) {
	var (
		checkpointTs model.Ts
		done         bool
	)
	switch task.status {
	case dispatchTableRequestReceived:
		done, err = a.tableExec.RemoveTable(ctx, task.TableID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !done {
			return nil, nil
		}
		task.status = dispatchTableRequestProcessed
		fallthrough
	case dispatchTableRequestProcessed:
		checkpointTs, done = a.tableExec.IsRemoveTableFinished(ctx, task.TableID)
		if !done {
			return nil, nil
		}
		task.status = dispatchTableRequestFinished
		fallthrough
	case dispatchTableRequestFinished:
	}
	log.Info("finish processing add table task", zap.Any("task", task))
	response = &schedulepb.Message{
		Header:  a.newMessageHeader(),
		MsgType: schedulepb.MsgDispatchTableResponse,
		From:    a.captureID,
		To:      a.ownerInfo.captureID,
		DispatchTableResponse: &schedulepb.DispatchTableResponse{
			Response: &schedulepb.DispatchTableResponse_RemoveTable{
				RemoveTable: &schedulepb.RemoveTableResponse{
					Status: &schedulepb.TableStatus{
						TableID: task.TableID,
						State:   0,
						Checkpoint: schedulepb.Checkpoint{
							CheckpointTs: checkpointTs,
						},
					},
					Checkpoint: &schedulepb.Checkpoint{
						CheckpointTs: checkpointTs,
						ResolvedTs:   0,
					},
				}},
		},
	}

	// todo: delete the task here, is ok ?
	delete(a.runningTasks, task.TableID)

	return response, nil
}

func (a *agent) handleAddTableTask(ctx context.Context, task *dispatchTableTask) (*schedulepb.Message, error) {
	switch task.status {
	case dispatchTableRequestReceived:
		done, err := a.tableExec.AddTable(ctx, task.TableID, task.StartTs, task.IsPrepare)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !done {
			return nil, nil
		}
		task.status = dispatchTableRequestProcessed
		fallthrough
	case dispatchTableRequestProcessed:
		done := a.tableExec.IsAddTableFinished(ctx, task.TableID, task.IsPrepare)
		if !done {
			return nil, nil
		}
		task.status = dispatchTableRequestFinished
		fallthrough
	case dispatchTableRequestFinished:
	}
	log.Info("finish processing add table task", zap.Any("task", task))
	response := &schedulepb.Message{
		Header:  a.newMessageHeader(),
		MsgType: schedulepb.MsgDispatchTableResponse,
		From:    a.captureID,
		To:      a.ownerInfo.captureID,
		DispatchTableResponse: &schedulepb.DispatchTableResponse{
			Response: &schedulepb.DispatchTableResponse_AddTable{
				AddTable: &schedulepb.AddTableResponse{
					Status: &schedulepb.TableStatus{
						TableID: task.TableID,
						// todo: how to the set the state
						State: schedulepb.TableStatePreparing,
						Checkpoint: schedulepb.Checkpoint{
							CheckpointTs: 0,
							ResolvedTs:   0,
						},
					},
					Checkpoint: &schedulepb.Checkpoint{
						CheckpointTs: 0,
						ResolvedTs:   0,
					},
					Reject: false,
				},
			},
		},
	}

	// todo: delete the task here, is ok ?
	delete(a.runningTasks, task.TableID)

	return response, nil
}

func (a *agent) handleDispatchTableTasks(ctx context.Context) (result []*schedulepb.Message, err error) {
	result = make([]*schedulepb.Message, 0)
	for _, task := range a.runningTasks {
		var response *schedulepb.Message
		if task.IsRemove {
			response, err = a.handleRemoveTableTask(ctx, task)
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
	return internal.CheckpointCannotProceed
}

// Close implement agent interface
func (a *agent) Close() error {
	return nil
}

func (a *agent) newCheckpointMessage() *schedulepb.Message {
	tableIDs := make([]model.TableID, 0, len(a.tables))
	for tableID := range a.tables {
		tableIDs = append(tableIDs, tableID)
	}
	// checkpointTs, resolvedTs := a.tableExec.GetCheckpoint()
	return &schedulepb.Message{
		Header:  a.newMessageHeader(),
		MsgType: schedulepb.MsgCheckpoint,
		From:    a.captureID,
		To:      a.ownerInfo.captureID,
		// Checkpoints: a.newCheckpoint(),
		//Checkpoint: a.newCheckpoint(checkpointTs, resolvedTs, tableIDs...),
	}
}

func (a *agent) newMessageHeader() *schedulepb.Message_Header {
	return &schedulepb.Message_Header{
		Version:        a.ownerInfo.version,
		OwnerRevision:  schedulepb.OwnerRevision{Revision: a.ownerInfo.revision},
		ProcessorEpoch: schedulepb.ProcessorEpoch{Epoch: a.epoch},
	}
}

func (a *agent) newCheckpoint(checkpointTs, resolvedTs model.Ts) schedulepb.Checkpoint {
	return schedulepb.Checkpoint{
		CheckpointTs: checkpointTs,
		ResolvedTs:   resolvedTs,
		//TableIDs:     tables,
	}
}

// updateOwnerInfo tries to update the stored ownerInfo, and returns false if the
// owner is stale, in which case the incoming message should be ignored since
// it has come from an owner that for sure is dead.
//
// ownerCaptureID: the incoming owner's capture ID
// ownerRev: the incoming owner's revision as generated by Etcd election.
func (a *agent) updateOwnerInfo(id model.CaptureID, version string, revision int64) bool {
	// staled owner heartbeat, just ignore it.
	if a.ownerInfo.revision > revision {
		log.Info("heartbeat: from staled owner",
			zap.Any("staledOwner", ownerInfo{
				captureID: id,
				revision:  revision,
			}),
			zap.Any("owner", a.ownerInfo))
		return false
	}

	if a.ownerInfo.revision == revision && a.ownerInfo.captureID != id {
		// This panic will happen only if two messages have been received
		// with the same ownerRev but with different ownerIDs.
		// This should never happen unless the election via Etcd is buggy.
		log.Panic("owner IDs do not match",
			zap.String("expected", a.ownerInfo.captureID),
			zap.String("actual", id))
	}

	if a.ownerInfo.revision < revision {
		a.ownerInfo.captureID = id
		a.ownerInfo.revision = revision
		a.ownerInfo.version = version

		log.Info("new owner in power", zap.Any("owner", a.ownerInfo))

		// todo: shall drop all pending operations ?
		// Resets the deque so that pending operations from the previous owner
		// will not be processed.
		// Note: these pending operations have not yet been processed by the agent,
		// so it is okay to lose them.
		//a.pendingOpsMu.Lock()
		//a.pendingOps = deque.NewDeque()
		//a.pendingOpsMu.Unlock()
		//return true
	}
	return true
}
