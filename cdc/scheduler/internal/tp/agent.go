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
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/version"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

var _ internal.Agent = (*agent)(nil)

type agent struct {
	agentInfo
	trans transport

	tableM *tableManager

	ownerInfo ownerInfo

	// Liveness of the capture.
	//
	// LivenessCaptureStopping rejects all add table requests.
	liveness model.Liveness
}

type agentInfo struct {
	Version      string
	CaptureID    model.CaptureID
	ChangeFeedID model.ChangeFeedID
	Epoch        schedulepb.ProcessorEpoch
}

func (a agentInfo) resetEpoch() {
	a.Epoch = schedulepb.ProcessorEpoch{Epoch: uuid.New().String()}
}

func newAgentInfo(changefeedID model.ChangeFeedID, captureID model.CaptureID) agentInfo {
	result := agentInfo{
		Version:      version.ReleaseSemver(),
		CaptureID:    captureID,
		ChangeFeedID: changefeedID,
		Epoch:        schedulepb.ProcessorEpoch{},
	}
	result.resetEpoch()

	return result
}

type ownerInfo struct {
	Revision  schedulepb.OwnerRevision
	Version   string
	CaptureID string
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
	trans, err := newTransport(ctx, changeFeedID, agentRole, messageServer, messageRouter)
	if err != nil {
		return nil, errors.Trace(err)
	}

	result := &agent{
		agentInfo: newAgentInfo(changeFeedID, captureID),
		tableM:    newTableManager(tableExecutor),
		trans:     trans,
	}

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

	result.ownerInfo = ownerInfo{
		// owner's version can only be got by receiving heartbeat
		Version:   "",
		CaptureID: ownerCaptureID,
		Revision:  schedulepb.OwnerRevision{Revision: revision},
	}
	return result, nil
}

// Tick implement agent interface
func (a *agent) Tick(ctx context.Context, liveness model.Liveness) error {
	inboundMessages, err := a.recvMsgs(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	a.handleLivenessUpdate(liveness, livenessSourceTick)

	outboundMessages := a.handleMessage(inboundMessages)

	responses, err := a.tableM.poll(ctx, a.liveness)
	if err != nil {
		return errors.Trace(err)
	}

	outboundMessages = append(outboundMessages, responses...)

	if err := a.sendMsgs(ctx, outboundMessages); err != nil {
		return errors.Trace(err)
	}

	return nil
}

const (
	livenessSourceTick      = "tick"
	livenessSourceHeartbeat = "heartbeat"
)

func (a *agent) handleLivenessUpdate(liveness model.Liveness, source string) {
	if a.liveness != liveness {
		if a.liveness == model.LivenessCaptureStopping {
			// No way to go back, once it becomes shutting down,
			return
		}
		log.Info("tpscheduler: agent liveness changed",
			zap.String("old", a.liveness.String()),
			zap.String("new", liveness.String()),
			zap.String("source", source))
		a.liveness = liveness
	}
}

func (a *agent) handleMessage(msg []*schedulepb.Message) []*schedulepb.Message {
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
			response := a.handleMessageHeartbeat(message.GetHeartbeat())
			result = append(result, response)
		case schedulepb.MsgDispatchTableRequest:
			a.handleMessageDispatchTableRequest(message.DispatchTableRequest, processorEpoch)
		default:
			log.Warn("tpscheduler: unknown message received",
				zap.String("capture", a.CaptureID),
				zap.String("namespace", a.ChangeFeedID.Namespace),
				zap.String("changefeed", a.ChangeFeedID.ID),
				zap.Any("message", message))
		}
	}
	return result
}

func (a *agent) handleMessageHeartbeat(request *schedulepb.Heartbeat) *schedulepb.Message {
	allTables := a.tableM.getAllTables()
	result := make([]schedulepb.TableStatus, 0, len(allTables))
	for _, table := range allTables {
		status := table.getTableStatus()
		if table.task != nil && table.task.IsRemove {
			status.State = schedulepb.TableStateStopping
		}
		result = append(result, status)
	}
	for _, tableID := range request.GetTableIDs() {
		if _, ok := allTables[tableID]; !ok {
			status := a.tableM.getTableStatus(tableID)
			result = append(result, status)
		}
	}

	if request.IsStopping {
		a.handleLivenessUpdate(model.LivenessCaptureStopping, livenessSourceHeartbeat)
	}
	response := &schedulepb.HeartbeatResponse{
		Tables:   result,
		Liveness: a.liveness,
	}

	message := &schedulepb.Message{
		MsgType:           schedulepb.MsgHeartbeatResponse,
		HeartbeatResponse: response,
	}

	log.Debug("tpscheduler: agent generate heartbeat response",
		zap.String("capture", a.CaptureID),
		zap.String("namespace", a.ChangeFeedID.Namespace),
		zap.String("changefeed", a.ChangeFeedID.ID),
		zap.Any("message", message))

	return message
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
	if a.Epoch != epoch {
		log.Info("tpscheduler: agent receive dispatch table request "+
			"epoch does not match, ignore it",
			zap.String("capture", a.CaptureID),
			zap.String("namespace", a.ChangeFeedID.Namespace),
			zap.String("changefeed", a.ChangeFeedID.ID),
			zap.String("epoch", epoch.Epoch),
			zap.String("expected", a.Epoch.Epoch))
		return
	}
	var (
		table *table
		task  *dispatchTableTask
		ok    bool
	)
	// make the assumption that all tables are tracked by the agent now.
	// this should be guaranteed by the caller of this method.
	switch req := request.Request.(type) {
	case *schedulepb.DispatchTableRequest_AddTable:
		if a.liveness != model.LivenessCaptureAlive {
			log.Info("tpscheduler: agent is stopping, and reject handle add table request",
				zap.String("capture", a.CaptureID),
				zap.String("namespace", a.ChangeFeedID.Namespace),
				zap.String("changefeed", a.ChangeFeedID.ID),
				zap.Any("request", request))
			return
		}
		tableID := req.AddTable.GetTableID()
		task = &dispatchTableTask{
			TableID:   tableID,
			StartTs:   req.AddTable.GetCheckpoint().CheckpointTs,
			IsRemove:  false,
			IsPrepare: req.AddTable.GetIsSecondary(),
			Epoch:     epoch,
			status:    dispatchTableTaskReceived,
		}
		table = a.tableM.addTable(tableID)
	case *schedulepb.DispatchTableRequest_RemoveTable:
		tableID := req.RemoveTable.GetTableID()
		table, ok = a.tableM.getTable(tableID)
		if !ok {
			log.Warn("tpscheduler: agent ignore remove table request,"+
				"since the table not found",
				zap.Any("tableID", tableID),
				zap.String("capture", a.CaptureID),
				zap.String("namespace", a.ChangeFeedID.Namespace),
				zap.String("changefeed", a.ChangeFeedID.ID),
				zap.Any("request", request))
			return
		}
		task = &dispatchTableTask{
			TableID:  tableID,
			IsRemove: true,
			Epoch:    epoch,
			status:   dispatchTableTaskReceived,
		}
	default:
		log.Warn("tpscheduler: agent ignore unknown dispatch table request",
			zap.String("capture", a.CaptureID),
			zap.String("namespace", a.ChangeFeedID.Namespace),
			zap.String("changefeed", a.ChangeFeedID.ID),
			zap.Any("request", request))
		return
	}
	table.injectDispatchTableTask(task)
}

// GetLastSentCheckpointTs implement agent interface
func (a *agent) GetLastSentCheckpointTs() (checkpointTs model.Ts) {
	// no need to implement this.
	return internal.CheckpointCannotProceed
}

// Close implement agent interface
func (a *agent) Close() error {
	log.Debug("tpscheduler: agent closed",
		zap.String("capture", a.CaptureID),
		zap.String("namespace", a.ChangeFeedID.Namespace),
		zap.String("changefeed", a.ChangeFeedID.ID))
	return a.trans.Close()
}

// handleOwnerInfo return false, if the given owner's info is staled.
// update owner's info to the latest otherwise.
// id: the incoming owner's capture ID
// revision: the incoming owner's revision as generated by Etcd election.
// version: the incoming owner's semantic version string
func (a *agent) handleOwnerInfo(id model.CaptureID, revision int64, version string) bool {
	if a.ownerInfo.Revision.Revision == revision {
		if a.ownerInfo.CaptureID != id {
			// This panic will happen only if two messages have been received
			// with the same ownerRev but with different ownerIDs.
			// This should never happen unless the election via Etcd is buggy.
			log.Panic("tpscheduler: owner IDs do not match",
				zap.String("capture", a.CaptureID),
				zap.String("namespace", a.ChangeFeedID.Namespace),
				zap.String("changefeed", a.ChangeFeedID.ID),
				zap.String("expected", a.ownerInfo.CaptureID),
				zap.String("actual", id))
		}
		return true
	}

	// the current owner is staled
	if a.ownerInfo.Revision.Revision < revision {
		a.ownerInfo.CaptureID = id
		a.ownerInfo.Revision.Revision = revision
		a.ownerInfo.Version = version

		a.resetEpoch()

		log.Info("tpscheduler: new owner in power",
			zap.String("capture", a.CaptureID),
			zap.String("namespace", a.ChangeFeedID.Namespace),
			zap.String("changefeed", a.ChangeFeedID.ID),
			zap.Any("owner", a.ownerInfo), zap.Any("agent", a))
		return true
	}

	// staled owner heartbeat, just ignore it.
	log.Info("tpscheduler: message from staled owner",
		zap.String("capture", a.CaptureID),
		zap.String("namespace", a.ChangeFeedID.Namespace),
		zap.String("changefeed", a.ChangeFeedID.ID),
		zap.Any("staledOwner", ownerInfo{
			CaptureID: id,
			Revision:  schedulepb.OwnerRevision{Revision: revision},
			Version:   version,
		}),
		zap.Any("owner", a.ownerInfo),
		zap.Any("agent", a.agentInfo))
	return false
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
		if m.MsgType == schedulepb.MsgUnknown {
			log.Panic("tpscheduler: invalid message no destination or unknown message type",
				zap.String("capture", a.CaptureID),
				zap.String("namespace", a.ChangeFeedID.Namespace),
				zap.String("changefeed", a.ChangeFeedID.ID),
				zap.Any("message", m))
		}
		m.Header = &schedulepb.Message_Header{
			Version:        a.Version,
			OwnerRevision:  a.ownerInfo.Revision,
			ProcessorEpoch: a.Epoch,
		}
		m.From = a.CaptureID
		m.To = a.ownerInfo.CaptureID
	}
	return a.trans.Send(ctx, msgs)
}
