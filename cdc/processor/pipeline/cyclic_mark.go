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
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/cyclic/mark"
	"github.com/pingcap/ticdc/pkg/pipeline"
	"go.uber.org/zap"
)

// cyclicMarkNode match the mark rows and normal rows, set the ReplicaID of normal rows and filter the mark rows
// and filter the normal rows by the FilterReplicaID config item.
type cyclicMarkNode struct {
	localReplicaID  uint64
	filterReplicaID map[uint64]struct{}
	markTableID     model.TableID

	// startTs -> rows
	rowsUnknownReplicaID map[model.Ts][]*model.PolymorphicEvent
	// startTs -> replicaID
	currentReplicaIDs map[model.Ts]uint64
	currentCommitTs   uint64
}

func newCyclicMarkNode(markTableID model.TableID) pipeline.Node {
	return &cyclicMarkNode{
		markTableID:          markTableID,
		rowsUnknownReplicaID: make(map[model.Ts][]*model.PolymorphicEvent),
		currentReplicaIDs:    make(map[model.Ts]uint64),
	}
}

func (n *cyclicMarkNode) Init(ctx pipeline.NodeContext) error {
	n.localReplicaID = ctx.Vars().Config.Cyclic.ReplicaID
	filterReplicaID := ctx.Vars().Config.Cyclic.FilterReplicaID
	n.filterReplicaID = make(map[uint64]struct{})
	for _, rID := range filterReplicaID {
		n.filterReplicaID[rID] = struct{}{}
	}
	// do nothing
	return nil
}

// Receive receives the message from the previous node
// In the previous nodes(puller node and sorter node),
// the change logs of mark table and normal table are listen by one puller,
// and sorted by one sorter. So, this node will receive a commitTs-ordered stream which include the mark rows and normal rows.
// Under the above conditions, we need to cache at most one transaction's rows to matching rows.
// For every row event, Receive function flushes every the last transaction's rows, and adds the mark row or normal row into the cache.
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
			err := n.appendMarkRow(ctx, event)
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			n.appendNormalRow(ctx, event)
		}
		return nil
	}
	ctx.SendToNextNode(msg)
	return nil
}

// appendNormalRow adds the normal row into the cache
func (n *cyclicMarkNode) appendNormalRow(ctx pipeline.NodeContext, event *model.PolymorphicEvent) {
	if event.CRTs != n.currentCommitTs {
		log.Panic("the CommitTs of the received event is not equal to the currentCommitTs, please report a bug", zap.Reflect("event", event), zap.Uint64("currentCommitTs", n.currentCommitTs))
	}
	if replicaID, exist := n.currentReplicaIDs[event.StartTs]; exist {
		// we already know the replicaID of this startTs, it means that the mark row of this startTs is already in cached.
		event.ReplicaID = replicaID
		n.sendNormalRowToNextNode(ctx, event.ReplicaID, event)
		return
	}
	// for all normal rows which we don't know the replicaID for now. we cache them in rowsUnknownReplicaID.
	n.rowsUnknownReplicaID[event.StartTs] = append(n.rowsUnknownReplicaID[event.StartTs], event)
}

// appendMarkRow adds the mark row into the cache
func (n *cyclicMarkNode) appendMarkRow(ctx pipeline.NodeContext, event *model.PolymorphicEvent) error {
	if event.CRTs != n.currentCommitTs {
		log.Panic("the CommitTs of the received event is not equal to the currentCommitTs, please report a bug", zap.Reflect("event", event), zap.Uint64("currentCommitTs", n.currentCommitTs))
	}
	err := event.WaitPrepare(ctx.StdContext())
	if err != nil {
		return errors.Trace(err)
	}
	markRow := event.Row
	if markRow == nil {
		return nil
	}
	replicaID := extractReplicaID(markRow)
	// Establishing the mapping from StartTs to ReplicaID
	n.currentReplicaIDs[markRow.StartTs] = replicaID
	if rows, exist := n.rowsUnknownReplicaID[markRow.StartTs]; exist {
		// the replicaID of these rows we did not know before, but now we know through received mark row now.
		delete(n.rowsUnknownReplicaID, markRow.StartTs)
		n.sendNormalRowToNextNode(ctx, replicaID, rows...)
	}
	return nil
}

func (n *cyclicMarkNode) flush(ctx pipeline.NodeContext, commitTs uint64) {
	if n.currentCommitTs == commitTs {
		return
	}
	// all mark rows and normal rows in current transaction is received now.
	// there are still unmatched normal rows in the cache, their replicaID should be local replicaID.
	for _, rows := range n.rowsUnknownReplicaID {
		for _, row := range rows {
			row.ReplicaID = n.localReplicaID
		}
		n.sendNormalRowToNextNode(ctx, n.localReplicaID, rows...)
	}
	if len(n.rowsUnknownReplicaID) != 0 {
		n.rowsUnknownReplicaID = make(map[model.Ts][]*model.PolymorphicEvent)
	}
	if len(n.currentReplicaIDs) != 0 {
		n.currentReplicaIDs = make(map[model.Ts]uint64)
	}
	n.currentCommitTs = commitTs
}

// sendNormalRowToNextNode filter the specified normal rows by the FilterReplicaID config item, and send rows to the next node.
func (n *cyclicMarkNode) sendNormalRowToNextNode(ctx pipeline.NodeContext, replicaID uint64, rows ...*model.PolymorphicEvent) {
	if _, shouldFilter := n.filterReplicaID[replicaID]; shouldFilter {
		return
	}
	for _, row := range rows {
		row.ReplicaID = replicaID
		ctx.SendToNextNode(pipeline.PolymorphicEventMessage(row))
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
