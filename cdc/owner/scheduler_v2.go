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
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	schedulerv2 "github.com/pingcap/ticdc/cdc/scheduler"
	"github.com/pingcap/ticdc/pkg/context"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/p2p"
	"go.uber.org/zap"
)

type schedulerV2 struct {
	*schedulerv2.BaseScheduleDispatcher

	changeFeedID  model.ChangeFeedID
	handlerErrChs []<-chan error
}

// NewSchedulerV2 creates a new schedulerV2
func NewSchedulerV2(ctx context.Context, changeFeedID model.ChangeFeedID, checkpointTs model.Ts) (*schedulerV2, error) {
	ret := &schedulerV2{
		changeFeedID: changeFeedID,
	}
	ret.BaseScheduleDispatcher = schedulerv2.NewBaseScheduleDispatcher(changeFeedID, ret, checkpointTs)
	if err := ret.registerPeerMessageHandlers(ctx); err != nil {
		return nil, err
	}
	log.Debug("scheduler created", zap.Uint64("checkpoint-ts", checkpointTs))
	return ret, nil
}

func (s *schedulerV2) Tick(
	ctx context.Context,
	checkpointTs model.Ts,
	currentTables []model.TableID,
	captures map[model.CaptureID]*model.CaptureInfo,
) (checkpoint, resolvedTs model.Ts, err error) {
	if err := s.checkForHandlerErrors(ctx); err != nil {
		return 0, 0, errors.Trace(err)
	}
	return s.BaseScheduleDispatcher.Tick(ctx, checkpointTs, currentTables, captures)
}

func (s *schedulerV2) DispatchTable(
	ctx context.Context,
	changeFeedID model.ChangeFeedID,
	tableID model.TableID,
	captureID model.CaptureID,
	boundaryTs model.Ts,
	isDelete bool,
) (done bool, err error) {
	client, ok := s.GetClient(ctx, captureID)
	if !ok {
		return false, nil
	}

	topic := schedulerv2.DispatchTableTopic(changeFeedID)
	message := &schedulerv2.DispatchTableMessage{
		OwnerRev:   ctx.GlobalVars().OwnerRev,
		ID:         tableID,
		IsDelete:   isDelete,
		BoundaryTs: boundaryTs,
	}

	_, err = client.TrySendMessage(ctx, topic, message)
	if err != nil {
		if cerrors.ErrPeerMessageSendTryAgain.Equal(err) {
			log.Warn("scheduler: send message failed, retry later", zap.Error(err))
			return false, nil
		}
		return false, errors.Trace(err)
	}
	log.Debug("send message successfully",
		zap.String("topic", string(topic)),
		zap.Any("message", message))

	return true, nil
}

func (s *schedulerV2) Announce(
	ctx context.Context,
	changeFeedID model.ChangeFeedID,
	captureID model.CaptureID,
) (bool, error) {
	client, ok := s.GetClient(ctx, captureID)
	if !ok {
		return false, nil
	}

	topic := schedulerv2.AnnounceTopic(changeFeedID)
	message := &schedulerv2.AnnounceMessage{OwnerRev: ctx.GlobalVars().OwnerRev}

	_, err := client.TrySendMessage(ctx, topic, message)
	if err != nil {
		if cerrors.ErrPeerMessageSendTryAgain.Equal(err) {
			log.Warn("scheduler: send message failed, retry later", zap.Error(err))
			return false, nil
		}
		return false, errors.Trace(err)
	}
	log.Debug("send message successfully",
		zap.String("topic", string(topic)),
		zap.Any("message", message))

	return true, nil
}

func (s *schedulerV2) GetClient(ctx context.Context, target model.CaptureID) (*p2p.MessageClient, bool) {
	messageRouter := ctx.GlobalVars().MessageRouter
	client := messageRouter.GetClient(target)
	if client == nil {
		log.Warn("scheduler: no message client found, retry later",
			zap.String("target", target))
		return nil, false
	}
	return client, true
}

func (s *schedulerV2) Close(ctx context.Context) {
	log.Debug("scheduler closed", zap.String("changefeed-id", s.changeFeedID))
	s.deregisterPeerMessageHandlers(ctx)
}

func (s *schedulerV2) registerPeerMessageHandlers(ctx context.Context) (ret error) {
	defer func() {
		if ret != nil {
			s.deregisterPeerMessageHandlers(ctx)
		}
	}()

	errCh, err := ctx.GlobalVars().MessageServer.MustAddHandler(
		ctx,
		string(schedulerv2.DispatchTableResponseTopic(s.changeFeedID)),
		&schedulerv2.DispatchTableResponseMessage{},
		func(sender string, messageI interface{}) error {
			message := messageI.(*schedulerv2.DispatchTableResponseMessage)
			s.OnAgentFinishedTableOperation(sender, message.ID)
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	s.handlerErrChs = append(s.handlerErrChs, errCh)

	errCh, err = ctx.GlobalVars().MessageServer.MustAddHandler(
		ctx,
		string(schedulerv2.SyncTopic(s.changeFeedID)),
		&schedulerv2.SyncMessage{},
		func(sender string, messageI interface{}) error {
			message := messageI.(*schedulerv2.SyncMessage)
			s.OnAgentSyncTaskStatuses(
				sender,
				message.Running,
				message.Adding,
				message.Removing)
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	s.handlerErrChs = append(s.handlerErrChs, errCh)

	errCh, err = ctx.GlobalVars().MessageServer.MustAddHandler(
		ctx,
		schedulerv2.CheckpointTopic(s.changeFeedID),
		&schedulerv2.CheckpointMessage{},
		func(sender string, messageI interface{}) error {
			message := messageI.(*schedulerv2.CheckpointMessage)
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
	err := ctx.GlobalVars().MessageServer.MustRemoveHandler(
		ctx,
		schedulerv2.DispatchTableResponseTopic(s.changeFeedID))
	if err != nil {
		log.Error("failed to remove peer message handler", zap.Error(err))
	}

	err = ctx.GlobalVars().MessageServer.MustRemoveHandler(
		ctx,
		schedulerv2.SyncTopic(s.changeFeedID))
	if err != nil {
		log.Error("failed to remove peer message handler", zap.Error(err))
	}

	err = ctx.GlobalVars().MessageServer.MustRemoveHandler(
		ctx,
		schedulerv2.CheckpointTopic(s.changeFeedID))
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
