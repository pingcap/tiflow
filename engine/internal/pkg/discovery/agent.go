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

package discovery

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/notifier"
	"github.com/pingcap/tiflow/engine/pkg/srvdiscovery"
	"github.com/pingcap/tiflow/pkg/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// EventType describes the type of the event, i.e. Add or Del.
type EventType string

const (
	// EventTypeAdd indicates that a node has been added.
	EventTypeAdd = EventType("add")

	// EventTypeDel indicates that a node has been removed.
	EventTypeDel = EventType("del")
)

// Event is a node membership change event.
type Event struct {
	Tp   EventType
	Info model.NodeInfo
}

// Agent registers the current node and receives membership changes of all nodes.
type Agent interface {
	Run(ctx context.Context) error
	Subscribe(ctx context.Context) (srvdiscovery.Snapshot, *notifier.Receiver[Event], error)
}

type runnerFactory interface {
	Build(etcdCli *clientv3.Client,
		sessionTTL int,
		watchDur time.Duration,
		key string,
		value string,
	) srvdiscovery.DiscoveryRunner
}

type runnerFactoryImpl struct{}

func (f runnerFactoryImpl) Build(
	etcdCli *clientv3.Client,
	sessionTTL int,
	watchDur time.Duration,
	key string,
	value string,
) srvdiscovery.DiscoveryRunner {
	return srvdiscovery.NewDiscoveryRunnerImpl(
		etcdCli, sessionTTL, watchDur, key, value)
}

type agentImpl struct {
	selfInfo   *model.NodeInfo
	etcdCli    *clientv3.Client
	sessionTTL int
	watchDur   time.Duration

	discoveryRunnerFactory runnerFactory

	subscribeCh chan *subscribeReq
}

type subscribeReq struct {
	snap     srvdiscovery.Snapshot
	receiver *notifier.Receiver[Event]
	doneCh   chan struct{}
}

// NewAgent creates a new Agent that registers the current node to the
// discovery service and receives membership changes from the discovery service.
func NewAgent(
	selfInfo *model.NodeInfo,
	etcdCli *clientv3.Client,
	sessionTTL int,
	watchDur time.Duration,
) Agent {
	return &agentImpl{
		selfInfo:   selfInfo,
		etcdCli:    etcdCli,
		sessionTTL: sessionTTL,
		watchDur:   watchDur,

		discoveryRunnerFactory: runnerFactoryImpl{},
		subscribeCh:            make(chan *subscribeReq, 1),
	}
}

// Run runs the internal logic of an Agent. It blocks
// until an error has occurred, or it has been canceled.
func (a *agentImpl) Run(ctx context.Context) error {
	selfInfoStr, err := a.selfInfo.ToJSON()
	if err != nil {
		return errors.Annotate(err, "run discovery agent")
	}

	runner := a.discoveryRunnerFactory.Build(
		a.etcdCli, a.sessionTTL, a.watchDur, a.selfInfo.EtcdKey(), selfInfoStr,
	)

	discoveryCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// FIXME The semantics of ResetDiscovery is confusing. We will deal with this later.
	startTime := time.Now()
	session, err := runner.ResetDiscovery(discoveryCtx, true)
	duration := time.Since(startTime)
	if err != nil {
		log.Warn("establish discovery session failed",
			zap.Duration("duration", duration), logutil.ShortError(err))
		return errors.Trace(err)
	}
	log.Info("discovery session established", zap.Duration("duration", duration))
	defer func() {
		_ = session.Close()
	}()

	eventNotifier := notifier.NewNotifier[Event]()
	defer eventNotifier.Close()

	for {
		select {
		case <-discoveryCtx.Done():
			return errors.Trace(discoveryCtx.Err())
		case <-session.Done():
			log.Warn("metastore session is done", zap.String("executor-id", string(a.selfInfo.ID)))

			startTime := time.Now()
			session, err = runner.ResetDiscovery(ctx, true /* resetSession*/)
			duration := time.Since(startTime)

			if err != nil {
				log.Warn("reset discovery session failed",
					zap.Duration("duration", duration),
					logutil.ShortError(err))
				return errors.Annotate(err, "discovery agent session done")
			}
			log.Info("reset discovery session", zap.Duration("duration", duration))
		case req := <-a.subscribeCh:
			req.snap = runner.GetSnapshot()
			req.receiver = eventNotifier.NewReceiver()
			close(req.doneCh)
		case resp := <-runner.GetWatcher():
			if err := a.handleWatchResp(resp, runner, eventNotifier); err != nil {
				return err
			}
		}
	}
}

func (a *agentImpl) handleWatchResp(
	resp srvdiscovery.WatchResp,
	runner srvdiscovery.DiscoveryRunner,
	eventNotifier *notifier.Notifier[Event],
) error {
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		if duration > 1*time.Second {
			// Print a warning to indicate that there might be a
			// congestion problem.
			log.Warn("discovery agent broadcast took too long",
				zap.Duration("duration", duration))
		}
	}()

	if resp.Err != nil {
		return errors.Annotate(resp.Err, "discovery agent handleWatchResp")
	}

	for _, res := range resp.DelSet {
		eventNotifier.Notify(Event{
			Tp:   EventTypeDel,
			Info: res,
		})
	}

	for _, res := range resp.AddSet {
		eventNotifier.Notify(Event{
			Tp:   EventTypeAdd,
			Info: res,
		})
	}

	runner.ApplyWatchResult(resp)
	return nil
}

const (
	subscribeTimeout = 10 * time.Second
)

// Subscribe subscribes to the service membership changes.
func (a *agentImpl) Subscribe(ctx context.Context) (srvdiscovery.Snapshot, *notifier.Receiver[Event], error) {
	ctx, cancel := context.WithTimeout(ctx, subscribeTimeout)
	defer cancel()

	req := &subscribeReq{
		doneCh: make(chan struct{}),
	}
	select {
	case <-ctx.Done():
		return nil, nil, errors.Trace(ctx.Err())
	case a.subscribeCh <- req:
	}

	select {
	case <-ctx.Done():
		return nil, nil, errors.Trace(ctx.Err())
	case <-req.doneCh:
	}

	return req.snap, req.receiver, nil
}
