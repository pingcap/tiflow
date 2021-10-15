// Copyright 2020 PingCAP, Inc.
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

package operator

import (
	"bytes"
	"fmt"
	"strings"
	"sync"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

// Operator contains an operation for specified binlog pos
// used by `handle-error`.
type Operator struct {
	uuid   string // add a UUID, make it more friendly to be traced in log
	op     pb.ErrorOp
	events []*replication.BinlogEvent // startLocation -> events
}

// newOperator creates a new operator with a random UUID.
func newOperator(op pb.ErrorOp, events []*replication.BinlogEvent) *Operator {
	return &Operator{
		uuid:   uuid.New().String(),
		op:     op,
		events: events,
	}
}

func (o *Operator) String() string {
	events := make([]string, 0)
	for _, e := range o.events {
		buf := new(bytes.Buffer)
		e.Dump(buf)
		events = append(events, buf.String())
	}
	return fmt.Sprintf("uuid: %s, op: %s, events: %s", o.uuid, o.op, strings.Join(events, "\n"))
}

// Holder holds error operator.
type Holder struct {
	mu        sync.Mutex
	operators map[string]*Operator
	logger    log.Logger
}

// NewHolder creates a new Holder.
func NewHolder(pLogger *log.Logger) *Holder {
	return &Holder{
		operators: make(map[string]*Operator),
		logger:    pLogger.WithFields(zap.String("component", "error operator holder")),
	}
}

// Set sets an Operator.
func (h *Holder) Set(pos string, op pb.ErrorOp, events []*replication.BinlogEvent) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if op == pb.ErrorOp_Revert {
		if _, ok := h.operators[pos]; !ok {
			return terror.ErrSyncerOperatorNotExist.Generate(pos)
		}
		delete(h.operators, pos)
		return nil
	}

	oper := newOperator(op, events)
	pre, ok := h.operators[pos]
	if ok {
		h.logger.Warn("overwrite operator", zap.String("position", pos), zap.Stringer("old operator", pre))
	}
	h.operators[pos] = oper
	h.logger.Info("set a new operator", zap.String("position", pos), zap.Stringer("new operator", oper))
	return nil
}

// GetEvent return a replace binlog event
// for example:
//			startLocation		endLocation
// event 1		1000, 0			1010, 0
// event 2		1010, 0			1020, 0	<--replace it with event a,b,c
// replace event a	1010, 0			1010, 1
// replace event b	1010, 1			1010, 2
// replace event c	1010, 2			1020, 0
// event 3		1020, 0			1030, 0.
func (h *Holder) GetEvent(startLocation binlog.Location) (*replication.BinlogEvent, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	key := startLocation.Position.String()
	operator, ok := h.operators[key]
	if !ok {
		return nil, terror.ErrSyncerReplaceEventNotExist.Generate(startLocation)
	}

	if len(operator.events) <= startLocation.Suffix {
		return nil, terror.ErrSyncerReplaceEvent.Generatef("replace events out of range, index: %d, total: %d", startLocation.Suffix, len(operator.events))
	}

	e := operator.events[startLocation.Suffix]
	buf := new(bytes.Buffer)
	e.Dump(buf)
	h.logger.Info("get replace event", zap.Stringer("event", buf))

	return e, nil
}

// MatchAndApply tries to match operation for event by location and apply it on replace events.
func (h *Holder) MatchAndApply(startLocation, endLocation binlog.Location, realEventHeaderTS uint32) (bool, pb.ErrorOp) {
	h.mu.Lock()
	defer h.mu.Unlock()

	key := startLocation.Position.String()
	operator, ok := h.operators[key]
	if !ok {
		return false, pb.ErrorOp_InvalidErrorOp
	}

	if operator.op == pb.ErrorOp_Replace {
		if len(operator.events) == 0 {
			// this should not happen
			return false, pb.ErrorOp_InvalidErrorOp
		}

		// set LogPos as start position
		for _, ev := range operator.events {
			ev.Header.LogPos = startLocation.Position.Pos
			ev.Header.Timestamp = realEventHeaderTS
			if e, ok := ev.Event.(*replication.QueryEvent); ok {
				if startLocation.GetGTID() != nil {
					e.GSet = startLocation.GetGTID().Origin()
				}
			}
		}

		// set the last replace event as end position
		e := operator.events[len(operator.events)-1]
		e.Header.EventSize = endLocation.Position.Pos - startLocation.Position.Pos
		e.Header.LogPos = endLocation.Position.Pos
		if e, ok := e.Event.(*replication.QueryEvent); ok {
			if endLocation.GetGTID() != nil {
				e.GSet = endLocation.GetGTID().Origin()
			}
		}
	}

	h.logger.Info("match and apply a operator", zap.Stringer("startlocation", startLocation), zap.Stringer("endlocation", endLocation), zap.Stringer("operator", operator))

	return true, operator.op
}

// RemoveOutdated remove outdated operator.
func (h *Holder) RemoveOutdated(flushLocation binlog.Location) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for pos := range h.operators {
		position, err := binlog.PositionFromPosStr(pos)
		if err != nil {
			// should not happen
			return err
		}
		if binlog.ComparePosition(position, flushLocation.Position) == -1 {
			h.logger.Info("remove a outdated operator", zap.Stringer("position", position), zap.Stringer("flush position", flushLocation.Position), zap.Stringer("operator", h.operators[pos]))
			delete(h.operators, pos)
		}
	}
	return nil
}
