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

package processor

import (
	stdContext "context"
	"time"

	"github.com/pingcap/ticdc/cdc/kv"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/scheduler"
	"github.com/pingcap/ticdc/pkg/context"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/p2p"
	"go.uber.org/zap"
)

type Messenger struct {
	scheduler.Agent

	messageServer *p2p.MessageServer
	messageRouter p2p.MessageRouter

	changeFeed     model.ChangeFeedID
	selfCaptureID  model.CaptureID
	ownerCaptureID model.CaptureID

	barrierSeqs map[p2p.Topic]p2p.Seq

	handlerErrChs []<-chan error
}

func newMessenger(
	ctx context.Context,
	executor scheduler.TableExecutor,
	changeFeedID model.ChangeFeedID,
	captureID model.CaptureID,
) (*Messenger, error) {
	ret := &Messenger{
		messageServer: ctx.GlobalVars().MessageServer,
		messageRouter: ctx.GlobalVars().MessageRouter,
		changeFeed:    changeFeedID,
		selfCaptureID: captureID,
		barrierSeqs:   map[p2p.Topic]p2p.Seq{},
	}
	ret.Agent = scheduler.NewBaseAgent(changeFeedID, executor, ret)
	if err := ret.registerPeerMessageHandlers(ctx); err != nil {
		return nil, errors.Trace(err)
	}

	// TODO refactor this! Might block critical path
	ownerCaptureID, err := ctx.GlobalVars().EtcdClient.GetOwnerID(ctx, kv.CaptureOwnerKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ret.ownerCaptureID = ownerCaptureID
	return ret, nil
}

func (m *Messenger) FinishTableOperation(
	ctx context.Context,
	tableID model.TableID,
) (bool, error) {
	done, err := m.trySendMessage(
		ctx, m.ownerCaptureID,
		scheduler.DispatchTableResponseTopic(m.changeFeed),
		&scheduler.DispatchTableResponseMessage{ID: tableID})
	if err != nil {
		return false, errors.Trace(err)
	}
	return done, nil
}

func (m *Messenger) SyncTaskStatuses(
	ctx context.Context,
	running, adding, removing []model.TableID,
) (bool, error) {
	done, err := m.trySendMessage(
		ctx,
		m.ownerCaptureID,
		scheduler.SyncTopic(m.changeFeed),
		&scheduler.SyncMessage{
			Running:  running,
			Adding:   adding,
			Removing: removing,
		})
	if err != nil {
		return false, errors.Trace(err)
	}
	return done, nil
}

func (m *Messenger) SendCheckpoint(
	ctx context.Context,
	checkpointTs model.Ts,
	resolvedTs model.Ts,
) (bool, error) {
	done, err := m.trySendMessage(
		ctx,
		m.ownerCaptureID,
		scheduler.CheckpointTopic(m.changeFeed),
		&scheduler.CheckpointMessage{
			CheckpointTs: checkpointTs,
			ResolvedTs:   resolvedTs,
		})
	if err != nil {
		return false, errors.Trace(err)
	}
	return done, nil
}

func (m *Messenger) Barrier(ctx context.Context) (done bool) {
	if m.ownerCaptureID == "" {
		// We should wait for the first owner to contact us.
		// We need to wait for the sync request anyways, and
		// there would not be any table to replicate for now.
		log.Debug("waiting for owner to request sync",
			zap.String("changefeed-id", m.changeFeed))
		return false
	}

	client := m.messageRouter.GetClient(m.ownerCaptureID)
	if client == nil {
		// client to found for owner.
		return false
	}
	for topic, waitSeq := range m.barrierSeqs {
		actualSeq, ok := client.CurrentAck(topic)
		if !ok {
			return false
		}
		if actualSeq >= waitSeq {
			delete(m.barrierSeqs, topic)
		} else {
			return false
		}
	}
	return true
}

func (m *Messenger) OnOwnerChanged(ctx context.Context, newOwnerCaptureID model.CaptureID) {
	m.ownerCaptureID = newOwnerCaptureID
	m.barrierSeqs = map[p2p.Topic]p2p.Seq{}
}

func (m *Messenger) Close() error {
	log.Debug("processor messenger: closing", zap.Stack("stack"))
	ctx, cancel := stdContext.WithTimeout(stdContext.Background(), time.Second*1)
	defer cancel()

	cdcCtx := context.NewContext(ctx, nil)
	if err := m.deregisterPeerMessageHandlers(cdcCtx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (m *Messenger) trySendMessage(
	ctx context.Context,
	target model.CaptureID,
	topic p2p.Topic,
	value interface{},
) (bool, error) {
	client := m.messageRouter.GetClient(target)
	if client == nil {
		log.Warn("processor: no message client found for owner, retry later",
			zap.String("owner", target))
		return false, nil
	}

	seq, err := client.TrySendMessage(ctx, topic, value)
	if err != nil {
		if cerrors.ErrPeerMessageSendTryAgain.Equal(err) {
			return false, nil
		}
		return false, errors.Trace(err)
	}

	m.barrierSeqs[topic] = seq
	return true, nil
}

func (m *Messenger) registerPeerMessageHandlers(ctx context.Context) (ret error) {
	defer func() {
		if ret != nil {
			if err := m.deregisterPeerMessageHandlers(ctx); err != nil {
				log.Error("failed to deregister handlers", zap.Error(err))
			}
		}
	}()

	errCh, err := m.messageServer.MustAddHandler(
		ctx,
		scheduler.DispatchTableTopic(m.changeFeed),
		&scheduler.DispatchTableMessage{},
		func(sender string, value interface{}) error {
			ownerCapture := sender
			message := value.(*scheduler.DispatchTableMessage)
			m.OnOwnerDispatchedTask(
				ownerCapture,
				message.OwnerRev,
				message.ID,
				message.BoundaryTs,
				message.IsDelete)
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	m.handlerErrChs = append(m.handlerErrChs, errCh)

	errCh, err = m.messageServer.MustAddHandler(
		ctx,
		scheduler.AnnounceTopic(m.changeFeed),
		&scheduler.AnnounceMessage{},
		func(sender string, value interface{}) error {
			ownerCapture := sender
			message := value.(*scheduler.AnnounceMessage)
			m.OnOwnerAnnounce(
				ownerCapture,
				message.OwnerRev)
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	m.handlerErrChs = append(m.handlerErrChs, errCh)
	return nil
}

func (m *Messenger) deregisterPeerMessageHandlers(ctx context.Context) error {
	err := m.messageServer.MustRemoveHandler(ctx, scheduler.DispatchTableTopic(m.changeFeed))
	if err != nil {
		return errors.Trace(err)
	}

	err = m.messageServer.MustRemoveHandler(ctx, scheduler.AnnounceTopic(m.changeFeed))
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}
