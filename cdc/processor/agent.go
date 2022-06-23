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

	"go.uber.org/zap/zapcore"

	"github.com/benbjohnson/clock"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/version"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	getOwnerFromEtcdTimeout         = time.Second * 5
	messageHandlerOperationsTimeout = time.Second * 5
	barrierNotAdvancingWarnDuration = time.Second * 10
	printWarnLogMinInterval         = time.Second * 1
)

// processorAgent is a data structure in the Processor that serves as a bridge with
// the Owner.
//
// processorAgent has a BaseAgent embedded in it, which handles the high-level logic of receiving
// commands from the Owner. It also implements ProcessorMessenger interface, which
// provides the BaseAgent with the necessary methods to send messages to the Owner.
//
// The reason for this design is to decouple the scheduling algorithm with the underlying
// RPC server/client.
//
// Note that Agent is not thread-safe, and it is not necessary for it to be thread-safe.
type processorAgent interface {
	scheduler.Agent
	scheduler.ProcessorMessenger
}

type agentImpl struct {
	*scheduler.BaseAgent

	messageServer *p2p.MessageServer
	messageRouter p2p.MessageRouter

	changeFeed     model.ChangeFeedID
	ownerCaptureID model.CaptureID
	ownerRevision  int64

	clock              clock.Clock
	barrierSeqs        map[p2p.Topic]p2p.Seq
	barrierLastCleared time.Time

	// TODO (zixiong): remove these limiters after we have a better way to handle this
	barrierLogRateLimiter  *rate.Limiter
	noClientLogRateLimiter *rate.Limiter

	handlerErrChs []<-chan error
}

func newAgent(
	ctx context.Context,
	messageServer *p2p.MessageServer,
	messageRouter p2p.MessageRouter,
	executor scheduler.TableExecutor,
	changeFeedID model.ChangeFeedID,
) (retVal processorAgent, err error) {
	ret := &agentImpl{
		messageServer: messageServer,
		messageRouter: messageRouter,

		changeFeed: changeFeedID,

		clock:                 clock.New(),
		barrierSeqs:           map[p2p.Topic]p2p.Seq{},
		barrierLogRateLimiter: rate.NewLimiter(rate.Every(printWarnLogMinInterval), 1),

		noClientLogRateLimiter: rate.NewLimiter(rate.Every(printWarnLogMinInterval), 1),
	}

	conf := config.GetGlobalServerConfig()
	flushInterval := time.Duration(conf.ProcessorFlushInterval)

	log.Debug("creating processor agent",
		zap.String("changefeed", changeFeedID),
		zap.Duration("sendCheckpointTsInterval", flushInterval))

	ret.BaseAgent = scheduler.NewBaseAgent(
		changeFeedID,
		executor,
		ret,
		&scheduler.BaseAgentConfig{SendCheckpointTsInterval: flushInterval})

	// Note that registerPeerMessageHandlers sets handlerErrChs.
	if err := ret.registerPeerMessageHandlers(); err != nil {
		log.Warn("failed to register processor message handlers",
			zap.String("changefeed", changeFeedID),
			zap.Error(err))
		return nil, errors.Trace(err)
	}
	defer func() {
		if err != nil {
			if err1 := ret.deregisterPeerMessageHandlers(); err1 != nil {
				log.Warn("failed to unregister processor message handlers",
					zap.String("changefeed", changeFeedID),
					zap.Error(err))
			}
		}
	}()

	etcdCliCtx, cancel := stdContext.WithTimeout(ctx, getOwnerFromEtcdTimeout)
	defer cancel()
	ownerCaptureID, err := ctx.GlobalVars().EtcdClient.
		GetOwnerID(etcdCliCtx, etcd.CaptureOwnerKey)
	if err != nil {
		if err != concurrency.ErrElectionNoLeader {
			return nil, errors.Trace(err)
		}
		// We tolerate the situation where there is no owner.
		// If we are registered in Etcd, an elected Owner will have to
		// contact us before it can schedule any table.
		log.Info("no owner found. We will wait for an owner to contact us.",
			zap.String("changefeed", changeFeedID),
			zap.Error(err))
		return ret, nil
	}

	ret.ownerCaptureID = ownerCaptureID
	log.Debug("found owner",
		zap.String("changefeed", changeFeedID),
		zap.String("ownerID", ownerCaptureID))

	ret.ownerRevision, err = ctx.GlobalVars().EtcdClient.
		GetOwnerRevision(etcdCliCtx, ownerCaptureID)
	if err != nil {
		if cerror.ErrOwnerNotFound.Equal(err) || cerror.ErrNotOwner.Equal(err) {
			// These are expected errors when no owner has been elected
			log.Info("no owner found when querying for the owner revision",
				zap.String("changefeed", changeFeedID),
				zap.Error(err))
			ret.ownerCaptureID = ""
			return ret, nil
		}
		return nil, errors.Trace(err)
	}
	return ret, nil
}

func (a *agentImpl) Tick(ctx context.Context) error {
	for _, errCh := range a.handlerErrChs {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case err := <-errCh:
			log.Warn("Processor Agent received error from message handler",
				zap.Error(err))
			return errors.Trace(err)
		default:
		}
	}

	if err := a.BaseAgent.Tick(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (a *agentImpl) FinishTableOperation(
	ctx context.Context,
	tableID model.TableID,
	epoch model.ProcessorEpoch,
) (done bool, err error) {
	topic := model.SyncTopic(a.changeFeed)
	if !a.Barrier(ctx) {
		if _, exists := a.barrierSeqs[topic]; exists {
			log.L().Info("Delay sending FinishTableOperation due to pending sync",
				zap.String("changefeedID", a.changeFeed),
				zap.String("ownerID", a.ownerCaptureID),
				zap.Int64("tableID", tableID),
				zap.String("epoch", epoch))
			return false, nil
		}
	}

	message := &model.DispatchTableResponseMessage{ID: tableID, Epoch: epoch}
	defer func() {
		if err != nil {
			return
		}
		log.Info("SchedulerAgent: FinishTableOperation", zap.Any("message", message),
			zap.Bool("successful", done),
			zap.String("changefeedID", a.changeFeed),
			zap.String("ownerID", a.ownerCaptureID))
	}()

	done, err = a.trySendMessage(
		ctx, a.ownerCaptureID,
		model.DispatchTableResponseTopic(a.changeFeed),
		message)
	if err != nil {
		return false, errors.Trace(err)
	}
	return done, nil
}

func (a *agentImpl) SyncTaskStatuses(
	ctx context.Context, epoch model.ProcessorEpoch, adding, removing, running []model.TableID,
) (done bool, err error) {
	if !a.Barrier(ctx) {
		// The Sync message needs to be strongly ordered w.r.t. other messages.
		return false, nil
	}

	message := &model.SyncMessage{
		ProcessorVersion: version.ReleaseSemver(),
		Epoch:            epoch,
		Running:          running,
		Adding:           adding,
		Removing:         removing,
	}

	defer func() {
		if err != nil {
			return
		}
		if log.GetLevel() == zapcore.DebugLevel {
			// The message can be REALLY large, so we do not print it
			// unless the log level is debug.
			log.Debug("SchedulerAgent: SyncTaskStatuses",
				zap.Any("message", message),
				zap.Bool("successful", done),
				zap.String("changefeedID", a.changeFeed),
				zap.String("ownerID", a.ownerCaptureID))
			return
		}
		log.Info("SchedulerAgent: SyncTaskStatuses",
			zap.Bool("successful", done),
			zap.String("changefeedID", a.changeFeed),
			zap.String("ownerID", a.ownerCaptureID))
	}()

	done, err = a.trySendMessage(
		ctx,
		a.ownerCaptureID,
		model.SyncTopic(a.changeFeed),
		message)
	if err != nil {
		return false, errors.Trace(err)
	}
	return done, nil
}

func (a *agentImpl) SendCheckpoint(
	ctx context.Context,
	checkpointTs model.Ts,
	resolvedTs model.Ts,
) (done bool, err error) {
	message := &model.CheckpointMessage{
		CheckpointTs: checkpointTs,
		ResolvedTs:   resolvedTs,
	}

	defer func() {
		if err != nil {
			return
		}
		// This log is very often, so we only print it if the
		// log level is debug.
		log.Debug("SchedulerAgent: SendCheckpoint",
			zap.Any("message", message),
			zap.Bool("successful", done),
			zap.String("changefeedID", a.changeFeed),
			zap.String("ownerID", a.ownerCaptureID))
	}()

	done, err = a.trySendMessage(
		ctx,
		a.ownerCaptureID,
		model.CheckpointTopic(a.changeFeed),
		message)
	if err != nil {
		return false, errors.Trace(err)
	}
	return done, nil
}

// Barrier returns whether there is a pending message not yet acknowledged by the owner.
// Please refer to the documentation on the ProcessorMessenger interface.
func (a *agentImpl) Barrier(_ context.Context) (done bool) {
	defer func() {
		if done {
			return
		}

		sinceLastAdvanced := a.clock.Since(a.barrierLastCleared)
		if sinceLastAdvanced > barrierNotAdvancingWarnDuration && a.barrierLogRateLimiter.Allow() {
			log.Warn("processor send barrier not advancing, report a bug if this log repeats",
				zap.String("changefeed", a.changeFeed),
				zap.String("ownerID", a.ownerCaptureID),
				zap.Duration("duration", sinceLastAdvanced))
		}
	}()

	if a.barrierLastCleared.IsZero() {
		a.barrierLastCleared = a.clock.Now()
	}

	if a.ownerCaptureID == "" {
		// We should wait for the first owner to contact us.
		// We need to wait for the sync request anyways, and
		// there would not be any table to replicate for now.
		log.Debug("waiting for owner to request sync",
			zap.String("changefeed", a.changeFeed))
		return false
	}

	client := a.messageRouter.GetClient(a.ownerCaptureID)
	if client == nil {
		// Client not found for owner.
		// Note that if the owner is eventually gone,
		// OnOwnerChanged will reset the barriers.
		a.printNoClientWarning(a.ownerCaptureID)
		return false
	}
	for topic, waitSeq := range a.barrierSeqs {
		actualSeq, ok := client.CurrentAck(topic)
		if !ok {
			return false
		}
		if actualSeq >= waitSeq {
			delete(a.barrierSeqs, topic)
		} else {
			return false
		}
	}

	a.barrierLastCleared = a.clock.Now()
	return true
}

func (a *agentImpl) OnOwnerChanged(
	ctx context.Context,
	newOwnerCaptureID model.CaptureID,
	newOwnerRev int64,
) {
	// The BaseAgent will notify us of an owner change if an AnnounceOwner is received.
	// However, we need to filter out the event if we already learned of this owner directly
	// from Etcd.
	if a.ownerCaptureID == newOwnerCaptureID && a.ownerRevision == newOwnerRev {
		return
	}
	a.ownerCaptureID = newOwnerCaptureID
	a.ownerRevision = newOwnerRev
	// Note that we clear the pending barriers.
	a.barrierSeqs = map[p2p.Topic]p2p.Seq{}
}

func (a *agentImpl) Close() error {
	log.Debug("processor messenger: closing", zap.Stack("stack"))
	if err := a.deregisterPeerMessageHandlers(); err != nil {
		log.Warn("failed to deregister processor message handlers",
			zap.String("changefeed", a.changeFeed),
			zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

func (a *agentImpl) trySendMessage(
	ctx context.Context,
	target model.CaptureID,
	topic p2p.Topic,
	value interface{},
) (bool, error) {
	// TODO (zixiong): abstract this function out together with the similar method in cdc/owner/scheduler.go
	// We probably need more advanced logic to handle and mitigate complex failure situations.

	client := a.messageRouter.GetClient(target)
	if client == nil {
		a.printNoClientWarning(target)
		return false, nil
	}

	seq, err := client.TrySendMessage(ctx, topic, value)
	if err != nil {
		if cerror.ErrPeerMessageSendTryAgain.Equal(err) {
			return false, nil
		}
		if cerror.ErrPeerMessageClientClosed.Equal(err) {
			log.Warn("peer messaging client is closed while trying to send a message through it. "+
				"Report a bug if this warning repeats",
				zap.String("changefeed", a.changeFeed),
				zap.String("target", target))
			return false, nil
		}
		return false, errors.Trace(err)
	}

	a.barrierSeqs[topic] = seq
	return true, nil
}

func (a *agentImpl) registerPeerMessageHandlers() (ret error) {
	defer func() {
		if ret != nil {
			if err := a.deregisterPeerMessageHandlers(); err != nil {
				log.Error("failed to deregister handlers", zap.Error(err))
			}
		}
	}()

	ctx, cancel := stdContext.WithTimeout(stdContext.Background(), messageHandlerOperationsTimeout)
	defer cancel()

	errCh, err := a.messageServer.SyncAddHandler(
		ctx,
		model.DispatchTableTopic(a.changeFeed),
		&model.DispatchTableMessage{},
		func(sender string, value interface{}) error {
			ownerCapture := sender
			message := value.(*model.DispatchTableMessage)
			a.OnOwnerDispatchedTask(
				ownerCapture,
				message.OwnerRev,
				message.ID,
				message.IsDelete,
				message.Epoch)
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	a.handlerErrChs = append(a.handlerErrChs, errCh)

	errCh, err = a.messageServer.SyncAddHandler(
		ctx,
		model.AnnounceTopic(a.changeFeed),
		&model.AnnounceMessage{},
		func(sender string, value interface{}) error {
			ownerCapture := sender
			message := value.(*model.AnnounceMessage)
			a.OnOwnerAnnounce(
				ownerCapture,
				message.OwnerRev)
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	a.handlerErrChs = append(a.handlerErrChs, errCh)
	return nil
}

func (a *agentImpl) deregisterPeerMessageHandlers() error {
	ctx, cancel := stdContext.WithTimeout(stdContext.Background(), messageHandlerOperationsTimeout)
	defer cancel()

	err := a.messageServer.SyncRemoveHandler(ctx, model.DispatchTableTopic(a.changeFeed))
	if err != nil {
		return errors.Trace(err)
	}

	err = a.messageServer.SyncRemoveHandler(ctx, model.AnnounceTopic(a.changeFeed))
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (a *agentImpl) printNoClientWarning(target model.CaptureID) {
	if !a.noClientLogRateLimiter.Allow() {
		return
	}
	log.Warn("processor: no message client found for owner, retry later",
		zap.String("ownerID", target))
}
