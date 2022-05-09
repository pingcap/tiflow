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

package owner

import (
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	pscheduler "github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/version"
	"go.uber.org/zap"
)

// scheduler is an interface for scheduling tables.
// Since in our design, we do not record checkpoints per table,
// how we calculate the global watermarks (checkpoint-ts and resolved-ts)
// is heavily coupled with how tables are scheduled.
// That is why we have a scheduler interface that also reports the global watermarks.
type scheduler interface {
	// Tick is called periodically from the owner, and returns
	// updated global watermarks.
	Tick(
		ctx context.Context,
		state *orchestrator.ChangefeedReactorState,
		currentTables []model.TableID,
		captures map[model.CaptureID]*model.CaptureInfo,
	) (newCheckpointTs, newResolvedTs model.Ts, err error)

	// MoveTable is used to trigger manual table moves.
	MoveTable(tableID model.TableID, target model.CaptureID)

	// Rebalance is used to trigger manual workload rebalances.
	Rebalance()

	// Close closes the scheduler and releases resources.
	Close(ctx context.Context)
}

type schedulerV2 struct {
	*pscheduler.BaseScheduleDispatcher

	messageServer *p2p.MessageServer
	messageRouter p2p.MessageRouter

	changeFeedID  model.ChangeFeedID
	handlerErrChs []<-chan error

	stats *schedulerStats
}

// NewSchedulerV2 creates a new schedulerV2
func NewSchedulerV2(
	ctx context.Context,
	changeFeedID model.ChangeFeedID,
	checkpointTs model.Ts,
	messageServer *p2p.MessageServer,
	messageRouter p2p.MessageRouter,
) (*schedulerV2, error) {
	ret := &schedulerV2{
		changeFeedID:  changeFeedID,
		messageServer: messageServer,
		messageRouter: messageRouter,
		stats:         &schedulerStats{},
	}
	ret.BaseScheduleDispatcher = pscheduler.NewBaseScheduleDispatcher(changeFeedID, ret, checkpointTs)
	if err := ret.registerPeerMessageHandlers(ctx); err != nil {
		return nil, err
	}
	log.Debug("scheduler created", zap.Uint64("checkpointTs", checkpointTs))
	return ret, nil
}

// newSchedulerV2FromCtx creates a new schedulerV2 from context.
// This function is factored out to facilitate unit testing.
func newSchedulerV2FromCtx(ctx context.Context, startTs uint64) (scheduler, error) {
	changeFeedID := ctx.ChangefeedVars().ID
	messageServer := ctx.GlobalVars().MessageServer
	messageRouter := ctx.GlobalVars().MessageRouter
	ret, err := NewSchedulerV2(ctx, changeFeedID, startTs, messageServer, messageRouter)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return ret, nil
}

func newScheduler(ctx context.Context, startTs uint64) (scheduler, error) {
	conf := config.GetGlobalServerConfig()
	if conf.Debug.EnableNewScheduler {
		return newSchedulerV2FromCtx(ctx, startTs)
	}
	return newSchedulerV1(), nil
}

func (s *schedulerV2) Tick(
	ctx context.Context,
	state *orchestrator.ChangefeedReactorState,
	currentTables []model.TableID,
	captures map[model.CaptureID]*model.CaptureInfo,
) (checkpoint, resolvedTs model.Ts, err error) {
	if err := s.checkForHandlerErrors(ctx); err != nil {
		return pscheduler.CheckpointCannotProceed, pscheduler.CheckpointCannotProceed, errors.Trace(err)
	}
	return s.BaseScheduleDispatcher.Tick(ctx, state.Status.CheckpointTs, currentTables, captures)
}

func (s *schedulerV2) DispatchTable(
	ctx context.Context,
	changeFeedID model.ChangeFeedID,
	tableID model.TableID,
	captureID model.CaptureID,
	isDelete bool,
	epoch model.ProcessorEpoch,
) (done bool, err error) {
	topic := model.DispatchTableTopic(changeFeedID)
	message := &model.DispatchTableMessage{
		OwnerRev: ctx.GlobalVars().OwnerRevision,
		ID:       tableID,
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

func (s *schedulerV2) Announce(
	ctx context.Context,
	changeFeedID model.ChangeFeedID,
	captureID model.CaptureID,
) (done bool, err error) {
	topic := model.AnnounceTopic(changeFeedID)
	message := &model.AnnounceMessage{
		OwnerRev:     ctx.GlobalVars().OwnerRevision,
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

func (s *schedulerV2) getClient(target model.CaptureID) (*p2p.MessageClient, bool) {
	client := s.messageRouter.GetClient(target)
	if client == nil {
		log.Warn("scheduler: no message client found, retry later",
			zap.String("target", target))
		return nil, false
	}
	return client, true
}

func (s *schedulerV2) trySendMessage(
	ctx context.Context,
	target model.CaptureID,
	topic p2p.Topic,
	value interface{},
) (bool, error) {
	// TODO (zixiong): abstract this function out together with the similar method in cdc/processor/agent.go
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

func (s *schedulerV2) Close(ctx context.Context) {
	log.Debug("scheduler closed",
		zap.String("namespace", s.changeFeedID.Namespace),
		zap.String("changefeedID", s.changeFeedID.ID))
	s.deregisterPeerMessageHandlers(ctx)
}

func (s *schedulerV2) registerPeerMessageHandlers(ctx context.Context) (ret error) {
	defer func() {
		if ret != nil {
			s.deregisterPeerMessageHandlers(ctx)
		}
	}()

	errCh, err := s.messageServer.SyncAddHandler(
		ctx,
		model.DispatchTableResponseTopic(s.changeFeedID),
		&model.DispatchTableResponseMessage{},
		func(sender string, messageI interface{}) error {
			message := messageI.(*model.DispatchTableResponseMessage)
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
		model.SyncTopic(s.changeFeedID),
		&model.SyncMessage{},
		func(sender string, messageI interface{}) error {
			message := messageI.(*model.SyncMessage)
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
		model.CheckpointTopic(s.changeFeedID),
		&model.CheckpointMessage{},
		func(sender string, messageI interface{}) error {
			message := messageI.(*model.CheckpointMessage)
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

func (s *schedulerV2) deregisterPeerMessageHandlers(ctx context.Context) {
	err := s.messageServer.SyncRemoveHandler(
		ctx,
		model.DispatchTableResponseTopic(s.changeFeedID))
	if err != nil {
		log.Error("failed to remove peer message handler", zap.Error(err))
	}

	err = s.messageServer.SyncRemoveHandler(
		ctx,
		model.SyncTopic(s.changeFeedID))
	if err != nil {
		log.Error("failed to remove peer message handler", zap.Error(err))
	}

	err = s.messageServer.SyncRemoveHandler(
		ctx,
		model.CheckpointTopic(s.changeFeedID))
	if err != nil {
		log.Error("failed to remove peer message handler", zap.Error(err))
	}
}

func (s *schedulerV2) checkForHandlerErrors(ctx context.Context) error {
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
