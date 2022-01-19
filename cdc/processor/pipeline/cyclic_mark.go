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
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/cyclic/mark"
	"github.com/pingcap/tiflow/pkg/pipeline"
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
}

func newCyclicMarkNode(markTableID model.TableID) pipeline.Node {
	return &cyclicMarkNode{
		markTableID:            markTableID,
		unknownReplicaIDEvents: make(map[model.Ts][]*model.PolymorphicEvent),
		currentReplicaIDs:      make(map[model.Ts]uint64),
	}
}

func (n *cyclicMarkNode) Init(ctx pipeline.NodeContext) error {
	n.localReplicaID = ctx.ChangefeedVars().Info.Config.Cyclic.ReplicaID
	filterReplicaID := ctx.ChangefeedVars().Info.Config.Cyclic.FilterReplicaID
	n.filterReplicaID = make(map[uint64]struct{})
	for _, rID := range filterReplicaID {
		n.filterReplicaID[rID] = struct{}{}
	}
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
	switch msg.Tp {
	case pipeline.MessageTypePolymorphicEvent:
		event := msg.PolymorphicEvent
		n.flush(ctx, event.CRTs)
		if event.RawKV.OpType == model.OpTypeResolved {
			ctx.SendToNextNode(msg)
			return nil
		}
		tableID, err := entry.DecodeTableID(event.RawKV.Key)
		if err != nil {
			return errors.Trace(err)
		}
		if tableID == n.markTableID {
			n.appendMarkRowEvent(ctx, event)
		} else {
			n.appendNormalRowEvent(ctx, event)
		}
		return nil
	}
	ctx.SendToNextNode(msg)
	return nil
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
		ctx.SendToNextNode(pipeline.PolymorphicEventMessage(event))
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
