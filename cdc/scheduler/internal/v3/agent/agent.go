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

package agent

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/scheduler/internal"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/compat"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/transport"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/version"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

var _ internal.Agent = (*agent)(nil)

type agent struct {
	agentInfo
	trans  transport.Transport
	compat *compat.Compat

	tableM *tableManager

	ownerInfo ownerInfo

	// Liveness of the capture.
	// It changes to LivenessCaptureStopping in following cases:
	// 1. The capture receives a SIGTERM signal.
	// 2. The agent receives a stopping heartbeat.
	liveness *model.Liveness
}

type agentInfo struct {
	Version         string
	CaptureID       model.CaptureID
	ChangeFeedID    model.ChangeFeedID
	Epoch           schedulepb.ProcessorEpoch
	changefeedEpoch uint64
}

func (a agentInfo) resetEpoch() {
	a.Epoch = schedulepb.ProcessorEpoch{Epoch: uuid.New().String()}
}

func newAgentInfo(
	changefeedID model.ChangeFeedID, captureID model.CaptureID, changefeedEpoch uint64,
) agentInfo {
	result := agentInfo{
		Version:         version.ReleaseSemver(),
		CaptureID:       captureID,
		ChangeFeedID:    changefeedID,
		Epoch:           schedulepb.ProcessorEpoch{},
		changefeedEpoch: changefeedEpoch,
	}
	result.resetEpoch()

	return result
}

type ownerInfo struct {
	Revision  schedulepb.OwnerRevision
	Version   string
	CaptureID string
}

func newAgent(
	ctx context.Context,
	captureID model.CaptureID,
	liveness *model.Liveness,
	changeFeedID model.ChangeFeedID,
	etcdClient etcd.CDCEtcdClient,
	tableExecutor internal.TableExecutor,
	changefeedEpoch uint64,
) (internal.Agent, error) {
	result := &agent{
		agentInfo: newAgentInfo(changeFeedID, captureID, changefeedEpoch),
		tableM:    newTableManager(changeFeedID, tableExecutor),
		liveness:  liveness,
		compat:    compat.New(map[model.CaptureID]*model.CaptureInfo{}),
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
		log.Info("schedulerv3: no owner found. We will wait for an owner to contact us.",
			zap.String("ownerCaptureID", ownerCaptureID),
			zap.String("namespace", changeFeedID.Namespace),
			zap.String("changefeed", changeFeedID.ID),
			zap.Error(err))
		return result, nil
	}

	log.Info("schedulerv3: agent owner found",
		zap.String("ownerCaptureID", ownerCaptureID),
		zap.String("captureID", captureID),
		zap.String("namespace", changeFeedID.Namespace),
		zap.String("changefeed", changeFeedID.ID))

	revision, err := etcdClient.GetOwnerRevision(etcdCliCtx, ownerCaptureID)
	if err != nil {
		if errors.ErrOwnerNotFound.Equal(err) || errors.ErrNotOwner.Equal(err) {
			// These are expected errors when no owner has been elected
			log.Info("schedulerv3: no owner found when querying for the owner revision",
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

// NewAgent returns a new agent.
func NewAgent(ctx context.Context,
	captureID model.CaptureID,
	liveness *model.Liveness,
	changeFeedID model.ChangeFeedID,
	messageServer *p2p.MessageServer,
	messageRouter p2p.MessageRouter,
	etcdClient etcd.CDCEtcdClient,
	tableExecutor internal.TableExecutor,
	changefeedEpoch uint64,
) (internal.Agent, error) {
	result, err := newAgent(
		ctx, captureID, liveness, changeFeedID, etcdClient, tableExecutor, changefeedEpoch)
	if err != nil {
		return nil, errors.Trace(err)
	}

	trans, err := transport.NewTransport(
		ctx, changeFeedID, transport.AgentRole, messageServer, messageRouter)
	if err != nil {
		return nil, errors.Trace(err)
	}

	result.(*agent).trans = trans
	return result, nil
}

// Tick implement agent interface
func (a *agent) Tick(ctx context.Context) (*schedulepb.Barrier, error) {
	inboundMessages, err := a.recvMsgs(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	outboundMessages, barrier := a.handleMessage(inboundMessages)

	responses, err := a.tableM.poll(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	outboundMessages = append(outboundMessages, responses...)

	if err := a.sendMsgs(ctx, outboundMessages); err != nil {
		return nil, errors.Trace(err)
	}

	return barrier, nil
}

func (a *agent) handleLivenessUpdate(liveness model.Liveness) {
	currentLiveness := a.liveness.Load()
	if currentLiveness != liveness {
		ok := a.liveness.Store(liveness)
		if ok {
			log.Info("schedulerv3: agent updates liveness",
				zap.String("old", currentLiveness.String()),
				zap.String("new", liveness.String()))
		}
	}
}

func (a *agent) handleMessage(msg []*schedulepb.Message) (result []*schedulepb.Message, barrier *schedulepb.Barrier) {
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
			var reMsg *schedulepb.Message
			reMsg, barrier = a.handleMessageHeartbeat(message.GetHeartbeat())
			result = append(result, reMsg)
		case schedulepb.MsgDispatchTableRequest:
			a.handleMessageDispatchTableRequest(message.DispatchTableRequest, processorEpoch)
		default:
			log.Warn("schedulerv3: unknown message received",
				zap.String("capture", a.CaptureID),
				zap.String("namespace", a.ChangeFeedID.Namespace),
				zap.String("changefeed", a.ChangeFeedID.ID),
				zap.Any("message", message))
		}
	}
	return
}

func (a *agent) handleMessageHeartbeat(request *schedulepb.Heartbeat) (*schedulepb.Message, *schedulepb.Barrier) {
	allTables := a.tableM.getAllTables()
	result := make([]tablepb.TableStatus, 0, len(allTables))

	for tableID, table := range allTables {
		status := table.getTableStatus(request.CollectStats)
		if status.Checkpoint.CheckpointTs > status.Checkpoint.ResolvedTs {
			log.Warn("schedulerv3: CheckpointTs is greater than ResolvedTs",
				zap.String("namespace", a.ChangeFeedID.Namespace),
				zap.String("changefeed", a.ChangeFeedID.ID),
				zap.Int64("tableID", tableID))
		}
		if table.task != nil && table.task.IsRemove {
			status.State = tablepb.TableStateStopping
		}
		result = append(result, status)
	}
	for _, tableID := range request.GetTableIDs() {
		if _, ok := allTables[tableID]; !ok {
			status := a.tableM.getTableStatus(tableID, request.CollectStats)
			result = append(result, status)
		}
	}

	if request.IsStopping {
		a.handleLivenessUpdate(model.LivenessCaptureStopping)
	}
	response := &schedulepb.HeartbeatResponse{
		Tables:   result,
		Liveness: a.liveness.Load(),
	}

	message := &schedulepb.Message{
		MsgType:           schedulepb.MsgHeartbeatResponse,
		HeartbeatResponse: response,
	}

	log.Debug("schedulerv3: agent generate heartbeat response",
		zap.String("capture", a.CaptureID),
		zap.String("namespace", a.ChangeFeedID.Namespace),
		zap.String("changefeed", a.ChangeFeedID.ID),
		zap.Any("message", message))

	return message, request.GetBarrier()
}

type dispatchTableTaskStatus int32

const (
	dispatchTableTaskReceived = dispatchTableTaskStatus(iota + 1)
	dispatchTableTaskProcessed
)

type dispatchTableTask struct {
	TableID    model.TableID
	Checkpoint tablepb.Checkpoint
	IsRemove   bool
	IsPrepare  bool
	Epoch      schedulepb.ProcessorEpoch
	status     dispatchTableTaskStatus
}

func (a *agent) handleMessageDispatchTableRequest(
	request *schedulepb.DispatchTableRequest,
	epoch schedulepb.ProcessorEpoch,
) {
	if a.Epoch != epoch {
		log.Info("schedulerv3: agent receive dispatch table request "+
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
	// this should be guaranteed by the caller of the method.
	switch req := request.Request.(type) {
	case *schedulepb.DispatchTableRequest_AddTable:
		tableID := req.AddTable.GetTableID()
		task = &dispatchTableTask{
			TableID:    tableID,
			Checkpoint: req.AddTable.GetCheckpoint(),
			IsRemove:   false,
			IsPrepare:  req.AddTable.GetIsSecondary(),
			Epoch:      epoch,
			status:     dispatchTableTaskReceived,
		}
		table = a.tableM.addTable(tableID)
	case *schedulepb.DispatchTableRequest_RemoveTable:
		tableID := req.RemoveTable.GetTableID()
		table, ok = a.tableM.getTable(tableID)
		if !ok {
			log.Warn("schedulerv3: agent ignore remove table request, "+
				"since the table not found",
				zap.String("capture", a.CaptureID),
				zap.String("namespace", a.ChangeFeedID.Namespace),
				zap.String("changefeed", a.ChangeFeedID.ID),
				zap.Any("tableID", tableID),
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
		log.Warn("schedulerv3: agent ignore unknown dispatch table request",
			zap.String("capture", a.CaptureID),
			zap.String("namespace", a.ChangeFeedID.Namespace),
			zap.String("changefeed", a.ChangeFeedID.ID),
			zap.Any("request", request))
		return
	}
	table.injectDispatchTableTask(task)
}

// Close implement agent interface
func (a *agent) Close() error {
	log.Debug("schedulerv3: agent closed",
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
			log.Panic("schedulerv3: owner IDs do not match",
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

		a.compat.UpdateCaptureInfo(map[model.CaptureID]*model.CaptureInfo{
			id: {
				Version: a.ownerInfo.Version,
				ID:      a.ownerInfo.CaptureID,
			},
		})
		log.Info("schedulerv3: new owner in power",
			zap.String("capture", a.CaptureID),
			zap.String("namespace", a.ChangeFeedID.Namespace),
			zap.String("changefeed", a.ChangeFeedID.ID),
			zap.Any("owner", a.ownerInfo), zap.Any("agent", a))
		return true
	}

	// staled owner heartbeat, just ignore it.
	log.Info("schedulerv3: message from staled owner",
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
	for _, msg := range messages {
		// only receive not staled messages
		if !a.handleOwnerInfo(msg.From, msg.Header.OwnerRevision.Revision, msg.Header.Version) {
			continue
		}
		// Check changefeed epoch, drop message if mismatch.
		if a.compat.CheckChangefeedEpochEnabled(msg.From) &&
			msg.Header.ChangefeedEpoch.Epoch != a.changefeedEpoch {
			continue
		}
		messages[n] = msg
		n++
	}
	return messages[:n], nil
}

func (a *agent) sendMsgs(ctx context.Context, msgs []*schedulepb.Message) error {
	for i := range msgs {
		m := msgs[i]
		if m.MsgType == schedulepb.MsgUnknown {
			log.Panic("schedulerv3: invalid message no destination or unknown message type",
				zap.String("capture", a.CaptureID),
				zap.String("namespace", a.ChangeFeedID.Namespace),
				zap.String("changefeed", a.ChangeFeedID.ID),
				zap.Any("message", m))
		}
		m.Header = &schedulepb.Message_Header{
			Version:        a.Version,
			OwnerRevision:  a.ownerInfo.Revision,
			ProcessorEpoch: a.Epoch,
			ChangefeedEpoch: schedulepb.ChangefeedEpoch{
				Epoch: a.changefeedEpoch,
			},
		}
		m.From = a.CaptureID
		m.To = a.ownerInfo.CaptureID
	}
	return a.trans.Send(ctx, msgs)
}
