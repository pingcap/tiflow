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

package pipeline

import (
	"container/list"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/cyclic/mark"
	"github.com/pingcap/tiflow/pkg/pipeline"
	pmessage "github.com/pingcap/tiflow/pkg/pipeline/message"
	"go.uber.org/zap"
)

// cyclicMarkNode match the mark row events and normal row events.
// Set the ReplicaID of normal row events and filter the mark row events
// and filter the normal row events by the FilterReplicaID config item.
type cyclicMarkNode struct {
	localReplicaID  uint64
	filterReplicaID map[uint64]struct{}
	markTableID     model.TableID

	// startTs -> events
	unknownReplicaIDEvents map[model.Ts][]*model.PolymorphicEvent
	// startTs -> replicaID
	currentReplicaIDs map[model.Ts]uint64
	currentCommitTs   uint64

	// todo : remove this flag after table actor is GA
	isTableActorMode bool
}

func newCyclicMarkNode(markTableID model.TableID) *cyclicMarkNode {
	return &cyclicMarkNode{
		markTableID:            markTableID,
		unknownReplicaIDEvents: make(map[model.Ts][]*model.PolymorphicEvent),
		currentReplicaIDs:      make(map[model.Ts]uint64),
	}
}

func (n *cyclicMarkNode) Init(ctx pipeline.NodeContext) error {
	return n.InitTableActor(ctx.ChangefeedVars().Info.Config.Cyclic.ReplicaID, ctx.ChangefeedVars().Info.Config.Cyclic.FilterReplicaID, false)
}

func (n *cyclicMarkNode) InitTableActor(localReplicaID uint64, filterReplicaID []uint64, isTableActorMode bool) error {
	n.localReplicaID = localReplicaID
	n.filterReplicaID = make(map[uint64]struct{})
	for _, rID := range filterReplicaID {
		n.filterReplicaID[rID] = struct{}{}
	}
	n.isTableActorMode = isTableActorMode
	// do nothing
	return nil
}

// Receive receives the message from the previous node.
// In the previous nodes(puller node and sorter node),
// the change logs of mark table and normal table are listen by one puller,
// and sorted by one sorter.
// So, this node will receive a commitTs-ordered stream
// which include the mark row events and normal row events.
// Under the above conditions, we need to cache at most one
// transaction's row events to matching row events.
// For every row event, Receive function flushes
// every the last transaction's row events,
// and adds the mark row event or normal row event into the cache.
func (n *cyclicMarkNode) Receive(ctx pipeline.NodeContext) error {
	msg := ctx.Message()
	_, err := n.TryHandleDataMessage(ctx, msg)
	return err
}

func (n *cyclicMarkNode) TryHandleDataMessage(
	ctx pipeline.NodeContext, msg pmessage.Message,
) (bool, error) {
	// limit the queue size when the table actor mode is enabled
	if n.isTableActorMode && ctx.(*cyclicNodeContext).queue.Len() >= defaultSyncResolvedBatch {
		return false, nil
	}
	switch msg.Tp {
	case pmessage.MessageTypePolymorphicEvent:
		event := msg.PolymorphicEvent
		n.flush(ctx, event.CRTs)
		if event.IsResolved() {
			ctx.SendToNextNode(msg)
			return true, nil
		}
		tableID, err := entry.DecodeTableID(event.RawKV.Key)
		if err != nil {
			return false, errors.Trace(err)
		}
		if tableID == n.markTableID {
			n.appendMarkRowEvent(ctx, event)
		} else {
			n.appendNormalRowEvent(ctx, event)
		}
		return true, nil
	}
	ctx.SendToNextNode(msg)
	return true, nil
}

// appendNormalRowEvent adds the normal row into the cache.
func (n *cyclicMarkNode) appendNormalRowEvent(ctx pipeline.NodeContext, event *model.PolymorphicEvent) {
	if event.CRTs != n.currentCommitTs {
		log.Panic("the CommitTs of the received event is not equal to the currentCommitTs, please report a bug", zap.Reflect("event", event), zap.Uint64("currentCommitTs", n.currentCommitTs))
	}
	if replicaID, exist := n.currentReplicaIDs[event.StartTs]; exist {
		// we already know the replicaID of this startTs, it means that the mark row of this startTs is already in cached.
		n.sendNormalRowEventToNextNode(ctx, replicaID, event)
		return
	}
	// for all normal row events which we don't know the replicaID for now. we cache them in unknownReplicaIDEvents.
	n.unknownReplicaIDEvents[event.StartTs] = append(n.unknownReplicaIDEvents[event.StartTs], event)
}

// appendMarkRowEvent adds the mark row event into the cache.
func (n *cyclicMarkNode) appendMarkRowEvent(ctx pipeline.NodeContext, event *model.PolymorphicEvent) {
	if event.CRTs != n.currentCommitTs {
		log.Panic("the CommitTs of the received event is not equal to the currentCommitTs, please report a bug", zap.Reflect("event", event), zap.Uint64("currentCommitTs", n.currentCommitTs))
	}
	markRow := event.Row
	if markRow == nil {
		return
	}
	replicaID := extractReplicaID(markRow)
	// Establishing the mapping from StartTs to ReplicaID
	n.currentReplicaIDs[markRow.StartTs] = replicaID
	if events, exist := n.unknownReplicaIDEvents[markRow.StartTs]; exist {
		// the replicaID of these events we did not know before, but now we know through received mark row now.
		delete(n.unknownReplicaIDEvents, markRow.StartTs)
		n.sendNormalRowEventToNextNode(ctx, replicaID, events...)
	}
}

func (n *cyclicMarkNode) flush(ctx pipeline.NodeContext, commitTs uint64) {
	if n.currentCommitTs == commitTs {
		return
	}
	// all mark events and normal events in current transaction is received now.
	// there are still unmatched normal events in the cache, their replicaID should be local replicaID.
	for _, events := range n.unknownReplicaIDEvents {
		n.sendNormalRowEventToNextNode(ctx, n.localReplicaID, events...)
	}
	if len(n.unknownReplicaIDEvents) != 0 {
		n.unknownReplicaIDEvents = make(map[model.Ts][]*model.PolymorphicEvent)
	}
	if len(n.currentReplicaIDs) != 0 {
		n.currentReplicaIDs = make(map[model.Ts]uint64)
	}
	n.currentCommitTs = commitTs
}

// sendNormalRowEventToNextNode filter the specified normal row events
// by the FilterReplicaID config item, and send events to the next node.
func (n *cyclicMarkNode) sendNormalRowEventToNextNode(ctx pipeline.NodeContext, replicaID uint64, events ...*model.PolymorphicEvent) {
	if _, shouldFilter := n.filterReplicaID[replicaID]; shouldFilter {
		return
	}
	for _, event := range events {
		event.Row.ReplicaID = replicaID
		ctx.SendToNextNode(pmessage.PolymorphicEventMessage(event))
	}
}

func (n *cyclicMarkNode) Destroy(ctx pipeline.NodeContext) error {
	// do nothing
	return nil
}

// extractReplicaID extracts replica ID from the given mark row.
func extractReplicaID(markRow *model.RowChangedEvent) uint64 {
	for _, c := range markRow.Columns {
		if c == nil {
			continue
		}
		if c.Name == mark.CyclicReplicaIDCol {
			return c.Value.(uint64)
		}
	}
	log.Panic("bad mark table, " + mark.CyclicReplicaIDCol + " not found")
	return 0
}

// cyclicNodeContext implements the NodeContext, cyclicMarkNode can be reused in table actor
// to buffer all messages with a queue, it will not block the actor system
type cyclicNodeContext struct {
	*actorNodeContext
	queue list.List
}

func newCyclicNodeContext(ctx *actorNodeContext) *cyclicNodeContext {
	return &cyclicNodeContext{
		actorNodeContext: ctx,
	}
}

// SendToNextNode implement the NodeContext interface, push the message to a queue
// the queue size is limited by TryHandleDataMessage，size is defaultSyncResolvedBatch
func (c *cyclicNodeContext) SendToNextNode(msg pmessage.Message) {
	c.queue.PushBack(msg)
}

// Message implements the NodeContext
func (c *cyclicNodeContext) Message() pmessage.Message {
	msg := c.tryGetProcessedMessage()
	if msg != nil {
		return *msg
	}
	return pmessage.Message{}
}

func (c *cyclicNodeContext) tryGetProcessedMessage() *pmessage.Message {
	el := c.queue.Front()
	if el == nil {
		return nil
	}
	msg := c.queue.Remove(el).(pmessage.Message)
	return &msg
}
