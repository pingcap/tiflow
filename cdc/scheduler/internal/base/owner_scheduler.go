// Copyright 2021 PingCAP, Inc.
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

package base

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	sched "github.com/pingcap/tiflow/cdc/scheduler/internal"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/base/protocol"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/version"
	"go.uber.org/zap"
)

// SchedulerV2 schedules tables through P2P.
type SchedulerV2 struct {
	*ScheduleDispatcher
	ownerRevision int64

	messageServer *p2p.MessageServer
	messageRouter p2p.MessageRouter

	changeFeedID  model.ChangeFeedID
	handlerErrChs []<-chan error

	stats *schedulerStats
}

// NewSchedulerV2 creates a new SchedulerV2
func NewSchedulerV2(
	ctx context.Context,
	changeFeedID model.ChangeFeedID,
	checkpointTs model.Ts,
	messageServer *p2p.MessageServer,
	messageRouter p2p.MessageRouter,
	ownerRevision int64,
) (*SchedulerV2, error) {
	ret := &SchedulerV2{
		changeFeedID:  changeFeedID,
		messageServer: messageServer,
		messageRouter: messageRouter,
		stats:         &schedulerStats{},
		ownerRevision: ownerRevision,
	}
	ret.ScheduleDispatcher = NewBaseScheduleDispatcher(changeFeedID, ret, checkpointTs)
	if err := ret.registerPeerMessageHandlers(ctx); err != nil {
		return nil, err
	}
	log.Debug("scheduler created", zap.Uint64("checkpointTs", checkpointTs))
	return ret, nil
}

// Tick implements the interface ScheduleDispatcher.
func (s *SchedulerV2) Tick(
	ctx context.Context,
	checkpointTs model.Ts,
	currentTables []model.TableID,
	captures map[model.CaptureID]*model.CaptureInfo,
) (checkpoint, resolvedTs model.Ts, err error) {
	if err := s.checkForHandlerErrors(ctx); err != nil {
		return sched.CheckpointCannotProceed, sched.CheckpointCannotProceed, errors.Trace(err)
	}
	return s.ScheduleDispatcher.Tick(ctx, checkpointTs, currentTables, captures)
}

// DispatchTable implements the interface ScheduleDispatcherCommunicator.
func (s *SchedulerV2) DispatchTable(
	ctx context.Context,
	changeFeedID model.ChangeFeedID,
	tableID model.TableID,
	startTs model.Ts,
	captureID model.CaptureID,
	isDelete bool,
	epoch protocol.ProcessorEpoch,
) (done bool, err error) {
	topic := protocol.DispatchTableTopic(changeFeedID)
	message := &protocol.DispatchTableMessage{
		OwnerRev: s.ownerRevision,
		ID:       tableID,
		StartTs:  startTs,
		IsDelete: isDelete,
		Epoch:    epoch,
	}

	defer func() {
		if err != nil {
			return
		}
		log.Info("schedulerV2: DispatchTable",
			zap.Any("message", message),
			zap.Any("successful", done),
			zap.String("namespace", changeFeedID.Namespace),
			zap.String("changefeedID", changeFeedID.ID),
			zap.String("captureID", captureID))
	}()

	ok, err := s.trySendMessage(ctx, captureID, topic, message)
	if err != nil {
		return false, errors.Trace(err)
	}
	if !ok {
		return false, nil
	}

	s.stats.RecordDispatch()
	log.Debug("send message successfully",
		zap.String("topic", topic),
		zap.Any("message", message))

	return true, nil
}

// Announce implements the interface ScheduleDispatcherCommunicator.
func (s *SchedulerV2) Announce(
	ctx context.Context,
	changeFeedID model.ChangeFeedID,
	captureID model.CaptureID,
) (done bool, err error) {
	topic := protocol.AnnounceTopic(changeFeedID)
	message := &protocol.AnnounceMessage{
		OwnerRev:     s.ownerRevision,
		OwnerVersion: version.ReleaseSemver(),
	}

	defer func() {
		if err != nil {
			return
		}
		log.Info("schedulerV2: Announce",
			zap.Any("message", message),
			zap.Any("successful", done),
			zap.String("namespace", changeFeedID.Namespace),
			zap.String("changefeedID", changeFeedID.ID),
			zap.String("captureID", captureID))
	}()

	ok, err := s.trySendMessage(ctx, captureID, topic, message)
	if err != nil {
		return false, errors.Trace(err)
	}
	if !ok {
		return false, nil
	}

	s.stats.RecordAnnounce()
	log.Debug("send message successfully",
		zap.String("topic", topic),
		zap.Any("message", message))

	return true, nil
}

func (s *SchedulerV2) getClient(target model.CaptureID) (*p2p.MessageClient, bool) {
	client := s.messageRouter.GetClient(target)
	if client == nil {
		log.Warn("scheduler: no message client found, retry later",
			zap.String("target", target))
		return nil, false
	}
	return client, true
}

func (s *SchedulerV2) trySendMessage(
	ctx context.Context,
	target model.CaptureID,
	topic p2p.Topic,
	value interface{},
) (bool, error) {
	// TODO (zixiong): abstract this function out together with the similar method in
	//                 cdc/scheduler/processor_agent.go
	// We probably need more advanced logic to handle and mitigate complex failure situations.

	client, ok := s.getClient(target)
	if !ok {
		return false, nil
	}

	_, err := client.TrySendMessage(ctx, topic, value)
	if err != nil {
		if cerror.ErrPeerMessageSendTryAgain.Equal(err) {
			return false, nil
		}
		if cerror.ErrPeerMessageClientClosed.Equal(err) {
			log.Warn("peer messaging client is closed while trying to send a message through it. "+
				"Report a bug if this warning repeats",
				zap.String("namespace", s.changeFeedID.Namespace),
				zap.String("changefeedID", s.changeFeedID.ID),
				zap.String("target", target))
			return false, nil
		}
		return false, errors.Trace(err)
	}

	return true, nil
}

// Close implements the interface ScheduleDispatcher.
func (s *SchedulerV2) Close(ctx context.Context) {
	log.Debug("scheduler closed",
		zap.String("namespace", s.changeFeedID.Namespace),
		zap.String("changefeedID", s.changeFeedID.ID))
	s.deregisterPeerMessageHandlers(ctx)
}

func (s *SchedulerV2) registerPeerMessageHandlers(ctx context.Context) (ret error) {
	defer func() {
		if ret != nil {
			s.deregisterPeerMessageHandlers(ctx)
		}
	}()

	errCh, err := s.messageServer.SyncAddHandler(
		ctx,
		protocol.DispatchTableResponseTopic(s.changeFeedID),
		&protocol.DispatchTableResponseMessage{},
		func(sender string, messageI interface{}) error {
			message := messageI.(*protocol.DispatchTableResponseMessage)
			s.stats.RecordDispatchResponse()
			s.OnAgentFinishedTableOperation(sender, message.ID, message.Epoch)
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	s.handlerErrChs = append(s.handlerErrChs, errCh)

	errCh, err = s.messageServer.SyncAddHandler(
		ctx,
		protocol.SyncTopic(s.changeFeedID),
		&protocol.SyncMessage{},
		func(sender string, messageI interface{}) error {
			message := messageI.(*protocol.SyncMessage)
			s.stats.RecordSync()
			s.OnAgentSyncTaskStatuses(
				sender,
				message.Epoch,
				message.Running,
				message.Adding,
				message.Removing)
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	s.handlerErrChs = append(s.handlerErrChs, errCh)

	errCh, err = s.messageServer.SyncAddHandler(
		ctx,
		protocol.CheckpointTopic(s.changeFeedID),
		&protocol.CheckpointMessage{},
		func(sender string, messageI interface{}) error {
			message := messageI.(*protocol.CheckpointMessage)
			s.stats.RecordCheckpoint()
			s.OnAgentCheckpoint(sender, message.CheckpointTs, message.ResolvedTs)
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	s.handlerErrChs = append(s.handlerErrChs, errCh)

	return nil
}

func (s *SchedulerV2) deregisterPeerMessageHandlers(ctx context.Context) {
	err := s.messageServer.SyncRemoveHandler(
		ctx,
		protocol.DispatchTableResponseTopic(s.changeFeedID))
	if err != nil {
		log.Error("failed to remove peer message handler", zap.Error(err))
	}

	err = s.messageServer.SyncRemoveHandler(
		ctx,
		protocol.SyncTopic(s.changeFeedID))
	if err != nil {
		log.Error("failed to remove peer message handler", zap.Error(err))
	}

	err = s.messageServer.SyncRemoveHandler(
		ctx,
		protocol.CheckpointTopic(s.changeFeedID))
	if err != nil {
		log.Error("failed to remove peer message handler", zap.Error(err))
	}
}

func (s *SchedulerV2) checkForHandlerErrors(ctx context.Context) error {
	for _, errCh := range s.handlerErrChs {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case err := <-errCh:
			return errors.Trace(err)
		default:
		}
	}
	return nil
}

type schedulerStats struct {
	ChangefeedID model.ChangeFeedID

	AnnounceSentCount            int64
	SyncReceiveCount             int64
	DispatchSentCount            int64
	DispatchResponseReceiveCount int64
	CheckpointReceiveCount       int64

	// TODO add prometheus metrics
}

func (s *schedulerStats) RecordAnnounce() {
	atomic.AddInt64(&s.AnnounceSentCount, 1)
}

func (s *schedulerStats) RecordSync() {
	atomic.AddInt64(&s.SyncReceiveCount, 1)
}

func (s *schedulerStats) RecordDispatch() {
	atomic.AddInt64(&s.DispatchSentCount, 1)
}

func (s *schedulerStats) RecordDispatchResponse() {
	atomic.AddInt64(&s.DispatchResponseReceiveCount, 1)
}

func (s *schedulerStats) RecordCheckpoint() {
	atomic.AddInt64(&s.CheckpointReceiveCount, 1)
}
