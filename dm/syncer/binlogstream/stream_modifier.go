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

package binlogstream

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"go.uber.org/zap"
)

// operator contains an operation for specified binlog pos
// used by `handle-error`.
type operator struct {
	uuid      string // add a UUID, make it more friendly to be traced in log
	op        pb.ErrorOp
	pos       mysql.Position
	events    []*replication.BinlogEvent // ddls -> events
	originReq *pb.HandleWorkerErrorRequest
}

// newOperator creates a new operator with a random UUID.
func newOperator(
	op pb.ErrorOp,
	pos mysql.Position,
	events []*replication.BinlogEvent,
	originReq *pb.HandleWorkerErrorRequest,
) *operator {
	reqClone := *originReq
	return &operator{
		uuid:      uuid.New().String(),
		op:        op,
		pos:       pos,
		events:    events,
		originReq: &reqClone,
	}
}

func (o *operator) String() string {
	events := make([]string, 0)
	for _, e := range o.events {
		buf := new(bytes.Buffer)
		e.Dump(buf)
		events = append(events, buf.String())
	}
	return fmt.Sprintf("uuid: %s, op: %s, events: %s, originReq: %v", o.uuid, o.op, strings.Join(events, "\n"), o.originReq)
}

// streamModifier is not thread-safe.
type streamModifier struct {
	ops    []*operator // sorted on operator.Position
	nextOp int         // next operator whose location is waiting to be matched

	// next event in current operator. This field can be
	// modified by StreamerController.
	nextEventInOp int

	logger log.Logger
}

func newStreamModifier(logger log.Logger) *streamModifier {
	return &streamModifier{
		ops:    []*operator{},
		logger: logger,
	}
}

// Set handles HandleWorkerErrorRequest with ErrorOp_Skip, ErrorOp_Replace, ErrorOp_Inject.
// - ErrorOp_Skip: events will be ignored.
// - ErrorOp_Replace, ErrorOp_Inject: events should be query events generated by caller.
func (m *streamModifier) Set(req *pb.HandleWorkerErrorRequest, events []*replication.BinlogEvent) error {
	// precheck
	switch req.Op {
	case pb.ErrorOp_Skip:
		if len(events) != 0 {
			m.logger.Warn("skip op should not have events", zap.Int("eventLen", len(events)))
		}
	case pb.ErrorOp_Replace, pb.ErrorOp_Inject:
		if len(events) == 0 {
			return terror.ErrSyncerEvent.Generatef("%s op should have non-empty events", req.Op.String())
		}
	default:
		m.logger.DPanic("invalid error op", zap.String("op", req.Op.String()))
		return terror.ErrSyncerEvent.Generatef("invalid error op: %s", req.Op.String())
	}

	pos, err := binlog.PositionFromStr(req.BinlogPos)
	if err != nil {
		return err
	}

	toInject := newOperator(req.Op, pos, events, req)
	toInsertIndex := sort.Search(len(m.ops), func(i int) bool {
		return pos.Compare(m.ops[i].pos) <= 0
	})

	if toInsertIndex == len(m.ops) {
		m.ops = append(m.ops, toInject)
		return nil
	}

	pre := m.ops[toInsertIndex]
	if pre.pos.Compare(pos) == 0 {
		m.ops[toInsertIndex] = toInject
		m.logger.Warn("overwrite operator",
			zap.Stringer("position", pos),
			zap.Stringer("old operator", pre))
		return nil
	}

	m.ops = append(m.ops, nil)
	copy(m.ops[toInsertIndex+1:], m.ops[toInsertIndex:])
	m.ops[toInsertIndex] = toInject
	if toInsertIndex < m.nextOp {
		m.nextOp++
	}
	m.logger.Info("set a new operator",
		zap.Stringer("position", pos),
		zap.Stringer("new operator", toInject))
	return nil
}

// Delete will delete an operator. `posStr` should be in the format of "binlog-file:pos".
func (m *streamModifier) Delete(posStr string) error {
	pos, err := binlog.PositionFromStr(posStr)
	if err != nil {
		return err
	}

	toDeleteIndex := sort.Search(len(m.ops), func(i int) bool {
		return pos.Compare(m.ops[i].pos) <= 0
	})

	if toDeleteIndex < m.nextOp || toDeleteIndex == len(m.ops) {
		return terror.ErrSyncerOperatorNotExist.Generate(posStr)
	}
	pre := m.ops[toDeleteIndex]
	if pre.pos.Compare(pos) != 0 {
		return terror.ErrSyncerOperatorNotExist.Generate(posStr)
	}
	copy(m.ops[toDeleteIndex:], m.ops[toDeleteIndex+1:])
	m.ops = m.ops[:len(m.ops)-1]
	return nil
}

// ListEqualAndAfter returns a JSON string of operators equals and after the given
// position.
// - if argument is "", it returns all operators.
// - Otherwise caller should make sure the argument in format of "binlog-file:pos"
//   and it returns all operators >= this position.
func (m *streamModifier) ListEqualAndAfter(posStr string) string {
	toPrint := []*operator{}
	if posStr == "" {
		toPrint = m.ops
	} else {
		pos, err := binlog.PositionFromStr(posStr)
		if err != nil {
			m.logger.DPanic("invalid position, should be verified in caller",
				zap.String("position", posStr))
			return ""
		}

		i := 0
		for i < len(m.ops) && m.ops[i].pos.Compare(pos) < 0 {
			i++
		}

		if i < len(m.ops) {
			toPrint = m.ops[i:]
		}
	}

	reqs := make([]*pb.HandleWorkerErrorRequest, 0, len(toPrint))
	for _, op := range toPrint {
		reqs = append(reqs, op.originReq)
	}

	b, err2 := json.Marshal(reqs)
	if err2 != nil {
		m.logger.DPanic("failed to marshal operators", zap.Error(err2))
	}
	return string(b)
}

// RemoveOutdated removes outdated operators which will not be triggered again after
// upstream binlog streamer reset. A common usage is to use global checkpoint as
// the argument.
// RemoveOutdated will not remove the operator equals or after the `front`.
func (m *streamModifier) RemoveOutdated(pos mysql.Position) {
	lastRemoveIndex := 0
	for i, op := range m.ops {
		if op.pos.Compare(pos) < 0 {
			lastRemoveIndex = i
		} else {
			break
		}
	}
	if lastRemoveIndex > m.nextOp-1 {
		lastRemoveIndex = m.nextOp - 1
	}

	m.ops = m.ops[lastRemoveIndex+1:]
	m.nextOp -= lastRemoveIndex + 1
}

func (m *streamModifier) front() *operator {
	if m.nextOp == len(m.ops) {
		return nil
	}
	return m.ops[m.nextOp]
}

func (m *streamModifier) next() {
	m.nextOp++
	m.nextEventInOp = 0
}

// reset will also reset nextEventInOp to a correct value.
func (m *streamModifier) reset(loc binlog.Location) {
	m.nextOp = 0
	m.nextEventInOp = 0

	for i, op := range m.ops {
		if op.pos.Compare(loc.Position) < 0 {
			m.nextOp = i + 1
		} else {
			break
		}
	}

	if m.nextOp == len(m.ops) {
		return
	}
	op := m.ops[m.nextOp]
	if op.pos.Compare(loc.Position) > 0 {
		return
	}

	m.nextEventInOp = loc.Suffix
}
