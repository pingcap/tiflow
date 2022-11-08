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

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/pkg/client"
	"github.com/pingcap/tiflow/engine/pkg/notifier"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
)

// EventType describes the type of the event, i.e. Add or Del.
type EventType string

const (
	// EventTypeAdd indicates that a node has been added.
	EventTypeAdd = EventType("add")

	// EventTypeDel indicates that a node has been removed.
	EventTypeDel = EventType("del")
)

// NodeType describes the type of the node.
type NodeType string

const (
	// NodeTypeMaster indicates that the node is a master.
	NodeTypeMaster = NodeType("master")
	// NodeTypeExecutor indicates that the node is an executor.
	NodeTypeExecutor = NodeType("executor")
)

// Event is a node membership change event.
type Event struct {
	Tp   EventType
	Node Node
}

// Node describes the information of a node.
type Node struct {
	Tp   NodeType
	ID   string
	Addr string
}

// Snapshot is the current service membership snapshot.
type Snapshot map[string]Node

// Agent registers receives membership changes of all nodes.
type Agent interface {
	Run(ctx context.Context) error
	Subscribe(ctx context.Context) (Snapshot, *notifier.Receiver[Event], error)
}

type agentImpl struct {
	discoveryClient  client.DiscoveryClient
	autoSyncInterval time.Duration
	eventCh          chan Event
	subscribeCh      chan *subscribeReq
}

type subscribeReq struct {
	snap     Snapshot
	receiver *notifier.Receiver[Event]
	doneCh   chan struct{}
}

// NewAgent creates a new Agent that receives membership changes from the discovery service.
// autoSyncInterval is the interval to update membership with the latest information and notify subscribers.
func NewAgent(
	discoveryClient client.DiscoveryClient,
	autoSyncInterval time.Duration,
) Agent {
	return &agentImpl{
		discoveryClient:  discoveryClient,
		eventCh:          make(chan Event, 16),
		subscribeCh:      make(chan *subscribeReq, 1),
		autoSyncInterval: autoSyncInterval,
	}
}

// Run runs the internal logic of an Agent. It blocks
// until an error has occurred, or it has been canceled.
func (a *agentImpl) Run(ctx context.Context) error {
	eventNotifier := notifier.NewNotifier[Event]()
	defer eventNotifier.Close()

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return a.autoSync(gCtx)
	})
	g.Go(func() error {
		snap := make(Snapshot)
		for {
			select {
			case <-gCtx.Done():
				return errors.Trace(gCtx.Err())
			case event := <-a.eventCh:
				eventNotifier.Notify(event)
				applyEvent(snap, event)
			case req := <-a.subscribeCh:
				req.snap = maps.Clone(snap)
				if err := eventNotifier.Flush(gCtx); err != nil {
					return errors.Trace(err)
				}
				req.receiver = eventNotifier.NewReceiver()
				close(req.doneCh)
			}
		}
	})
	return g.Wait()
}

func (a *agentImpl) autoSync(ctx context.Context) error {
	snap := make(Snapshot)
	for {
		newSnap, err := a.getSnapshot(ctx)
		if err != nil {
			if errors.Cause(err) == context.Canceled || errors.Cause(err) == context.DeadlineExceeded {
				return errors.Trace(err)
			}
			log.Warn("failed to get snapshot", zap.Error(err))
		} else {
			events := computeEvents(snap, newSnap)
			for _, event := range events {
				select {
				case a.eventCh <- event:
				case <-ctx.Done():
					return errors.Trace(ctx.Err())
				}
			}
			snap = newSnap
		}

		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-time.After(a.autoSyncInterval):
		}
	}
}

func (a *agentImpl) getSnapshot(ctx context.Context) (Snapshot, error) {
	masters, err := a.discoveryClient.ListMasters(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	executors, err := a.discoveryClient.ListExecutors(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	snap := make(Snapshot)
	for _, m := range masters {
		snap[m.Id] = Node{
			Tp:   NodeTypeMaster,
			ID:   m.Id,
			Addr: m.Address,
		}
	}
	for _, e := range executors {
		snap[e.Id] = Node{
			Tp:   NodeTypeExecutor,
			ID:   e.Id,
			Addr: e.Address,
		}
	}
	return snap, nil
}

const (
	subscribeTimeout = 10 * time.Second
)

// Subscribe subscribes to the service membership changes.
func (a *agentImpl) Subscribe(ctx context.Context) (Snapshot, *notifier.Receiver[Event], error) {
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

func applyEvent(snap Snapshot, event Event) {
	switch event.Tp {
	case EventTypeAdd:
		snap[event.Node.ID] = event.Node
	case EventTypeDel:
		delete(snap, event.Node.ID)
	}
}

func computeEvents(old, new Snapshot) []Event {
	var events []Event
	for id, node := range new {
		if _, ok := old[id]; !ok {
			events = append(events, Event{
				Tp:   EventTypeAdd,
				Node: node,
			})
		}
	}
	for id := range old {
		if _, ok := new[id]; !ok {
			events = append(events, Event{
				Tp:   EventTypeDel,
				Node: old[id],
			})
		}
	}
	return events
}
