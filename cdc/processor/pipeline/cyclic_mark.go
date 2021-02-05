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
			n.appendMarkRow(ctx, event)
		} else {
			n.appendRow(ctx, event)
		}
		return nil
	}
	ctx.SendToNextNode(msg)
	return nil
}

func (n *cyclicMarkNode) appendRow(ctx pipeline.NodeContext, event *model.PolymorphicEvent) {
	if event.CRTs != n.currentCommitTs {
		log.Panic("the CommitTs of the received event is not equal to the currentCommitTs, please report a bug", zap.Reflect("event", event), zap.Uint64("currentCommitTs", n.currentCommitTs))
	}
	if replicaID, exist := n.currentReplicaIDs[event.StartTs]; exist {
		if _, shouldFilter := n.filterReplicaID[replicaID]; shouldFilter {
			return
		}
		event.ReplicaID = replicaID
		ctx.SendToNextNode(pipeline.PolymorphicEventMessage(event))
	} else {
		n.rowsUnknownReplicaID[event.StartTs] = append(n.rowsUnknownReplicaID[event.StartTs], event)
	}
	return
}

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
	n.currentReplicaIDs[markRow.StartTs] = replicaID
	if rows, exist := n.rowsUnknownReplicaID[markRow.StartTs]; exist {
		delete(n.rowsUnknownReplicaID, markRow.StartTs)
		if _, shouldFilter := n.filterReplicaID[replicaID]; shouldFilter {
			return nil
		}
		for _, row := range rows {
			row.ReplicaID = replicaID
			ctx.SendToNextNode(pipeline.PolymorphicEventMessage(row))
		}
	}
	return nil
}

func (n *cyclicMarkNode) flush(ctx pipeline.NodeContext, commitTs uint64) {
	if n.currentCommitTs == commitTs {
		return
	}
	for _, rows := range n.rowsUnknownReplicaID {
		// the localReplicaID can't include by filter replicaID, so we don't need to filter the rows
		for _, row := range rows {
			row.ReplicaID = n.localReplicaID
			ctx.SendToNextNode(pipeline.PolymorphicEventMessage(row))
		}
	}
	if len(n.rowsUnknownReplicaID) != 0 {
		n.rowsUnknownReplicaID = make(map[model.Ts][]*model.PolymorphicEvent)
	}
	if len(n.currentReplicaIDs) != 0 {
		n.currentReplicaIDs = make(map[model.Ts]uint64)
	}
	n.currentCommitTs = commitTs
}

func (n *cyclicMarkNode) Destroy(ctx pipeline.NodeContext) error {
	// do nothing
	return nil
}

// ExtractReplicaID extracts replica ID from the given mark row.
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
