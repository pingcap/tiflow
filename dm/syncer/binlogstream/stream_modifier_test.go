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
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
)

func TestListEqualAndAfter(t *testing.T) {
	t.Parallel()
	m := newStreamModifier(log.L())

	reqs := m.ListEqualAndAfter("")
	require.Len(t, reqs, 0)

	reqs = m.ListEqualAndAfter("(mysql.000001, 1234)")
	require.Len(t, reqs, 0)

	m.ops = []*operator{
		{
			pos:       mysql.Position{Name: "mysql.000001", Pos: 1234},
			originReq: &pb.HandleWorkerErrorRequest{Op: pb.ErrorOp_List},
		},
		{
			pos:       mysql.Position{Name: "mysql.000001", Pos: 2345},
			originReq: &pb.HandleWorkerErrorRequest{Op: pb.ErrorOp_Skip},
		},
	}

	reqs = m.ListEqualAndAfter("")
	require.Equal(t, pb.ErrorOp_List, reqs[0].Op)
	require.Equal(t, pb.ErrorOp_Skip, reqs[1].Op)

	reqs = m.ListEqualAndAfter("(mysql.000001, 1234)")
	require.Equal(t, pb.ErrorOp_List, reqs[0].Op)
	require.Equal(t, pb.ErrorOp_Skip, reqs[1].Op)

	reqs = m.ListEqualAndAfter("(mysql.000001, 1235)")
	require.Equal(t, pb.ErrorOp_Skip, reqs[0].Op)

	reqs = m.ListEqualAndAfter("(mysql.000001, 9999)")
	require.Len(t, reqs, 0)
}

func TestInteraction(t *testing.T) {
	t.Parallel()
	m := newStreamModifier(log.L())

	opInject := &operator{
		op:  pb.ErrorOp_Inject,
		pos: mysql.Position{Name: "mysql.000001", Pos: 1234},
		events: []*replication.BinlogEvent{
			{RawData: []byte("event1")},
			{RawData: []byte("event2")},
			{RawData: []byte("event3")},
		},
	}
	opSkip := &operator{
		op:  pb.ErrorOp_Skip,
		pos: mysql.Position{Name: "mysql.000001", Pos: 2345},
	}
	m.ops = []*operator{opInject, opSkip}

	op := m.front()
	require.Equal(t, opInject, op)
	// front is idempotent
	op = m.front()
	require.Equal(t, opInject, op)

	// now caller finish processing the first event, caller should call next
	m.next()

	op = m.front()
	require.Equal(t, opSkip, op)
	op = m.front()
	require.Equal(t, opSkip, op)

	m.next()
	op = m.front()
	require.Nil(t, op)

	// test reset to an early location
	m.reset(binlog.Location{Position: mysql.Position{Name: "mysql.000001", Pos: 100}})
	require.Equal(t, 0, m.nextEventInOp)
	op = m.front()
	require.Equal(t, opInject, op)
	m.next()
	op = m.front()
	require.Equal(t, opSkip, op)
	m.next()
	op = m.front()
	require.Nil(t, op)

	// test reset to a later location
	m.reset(binlog.Location{Position: mysql.Position{Name: "mysql.000001", Pos: 9999}})
	require.Equal(t, 0, m.nextEventInOp)
	op = m.front()
	require.Nil(t, op)

	// test reset to a location with Suffix
	m.reset(
		binlog.Location{
			Position: mysql.Position{Name: "mysql.000001", Pos: 1234},
			Suffix:   1,
		})
	require.Equal(t, 1, m.nextEventInOp)
	op = m.front()
	require.Equal(t, opInject, op)
	m.next()
	op = m.front()
	require.Equal(t, opSkip, op)
	m.next()
	op = m.front()
	require.Nil(t, op)
}

func TestInvalidSet(t *testing.T) {
	t.Parallel()
	m := newStreamModifier(log.L())

	wrongReq := &pb.HandleWorkerErrorRequest{
		Op: pb.ErrorOp_List,
	}
	err := m.Set(wrongReq, nil)
	require.ErrorContains(t, err, "invalid error op")

	wrongReq = &pb.HandleWorkerErrorRequest{
		Op: pb.ErrorOp_Inject,
	}
	err = m.Set(wrongReq, nil)
	require.ErrorContains(t, err, "Inject op should have non-empty events")

	err = m.Set(wrongReq, []*replication.BinlogEvent{{}, {}, {}})
	require.ErrorContains(t, err, "should be like (mysql-bin.000001, 2345)")

	wrongReq = &pb.HandleWorkerErrorRequest{
		Op:        pb.ErrorOp_Inject,
		BinlogPos: "wrong format",
	}
	err = m.Set(wrongReq, []*replication.BinlogEvent{{}, {}, {}})
	require.ErrorContains(t, err, "should be like (mysql-bin.000001, 2345)")
}

func TestSet(t *testing.T) {
	t.Parallel()
	m := newStreamModifier(log.L())

	req := &pb.HandleWorkerErrorRequest{
		Op:        pb.ErrorOp_Skip,
		BinlogPos: "(mysql.000001, 3000)",
	}
	err := m.Set(req, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(m.ops))
	op := m.front()
	require.Equal(t, pb.ErrorOp_Skip, op.op)

	req = &pb.HandleWorkerErrorRequest{
		Op:        pb.ErrorOp_Inject,
		BinlogPos: "(mysql.000001, 2000)",
	}
	err = m.Set(req, []*replication.BinlogEvent{
		{RawData: []byte("event1")},
		{RawData: []byte("event2")},
		{RawData: []byte("event3")},
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(m.ops))
	// check the order of the ops
	require.Equal(t, pb.ErrorOp_Inject, m.ops[0].op)
	require.Equal(t, pb.ErrorOp_Skip, m.ops[1].op)

	// front may be changed by Set
	op = m.front()
	require.Equal(t, pb.ErrorOp_Inject, op.op)

	// now caller finish processing an op
	m.next()
	op = m.front()
	require.Equal(t, pb.ErrorOp_Skip, op.op)

	req = &pb.HandleWorkerErrorRequest{
		Op:        pb.ErrorOp_Replace,
		BinlogPos: "(mysql.000001, 1000)",
	}
	// can Set before `front`
	err = m.Set(req, []*replication.BinlogEvent{
		{RawData: []byte("event4")},
	})
	require.NoError(t, err)
	require.Equal(t, 3, len(m.ops))
	// check the order of the ops
	require.Equal(t, pb.ErrorOp_Replace, m.ops[0].op)
	require.Equal(t, pb.ErrorOp_Inject, m.ops[1].op)
	require.Equal(t, pb.ErrorOp_Skip, m.ops[2].op)

	// front is not changed
	op = m.front()
	require.Equal(t, pb.ErrorOp_Skip, op.op)

	m.reset(binlog.Location{Position: mysql.Position{Name: "mysql.000001", Pos: 4}})
	// after reset, front will see the op we just Set
	op = m.front()
	require.Equal(t, pb.ErrorOp_Replace, op.op)
	require.Equal(t, []byte("event4"), op.events[0].RawData)

	// test Set can overwrite an operator after the front
	req = &pb.HandleWorkerErrorRequest{
		Op:        pb.ErrorOp_Replace,
		BinlogPos: "(mysql.000001, 3000)",
	}
	err = m.Set(req, []*replication.BinlogEvent{
		{RawData: []byte("event5")},
	})
	require.NoError(t, err)
	require.Equal(t, 3, len(m.ops))
	require.Equal(t, pb.ErrorOp_Replace, m.ops[0].op)
	require.Equal(t, pb.ErrorOp_Inject, m.ops[1].op)
	require.Equal(t, pb.ErrorOp_Replace, m.ops[2].op)

	m.next()
	op = m.front()
	require.Equal(t, pb.ErrorOp_Inject, op.op)
	// test Set can overwrite an operator before the front
	req = &pb.HandleWorkerErrorRequest{
		Op:        pb.ErrorOp_Skip,
		BinlogPos: "(mysql.000001, 1000)",
	}
	err = m.Set(req, []*replication.BinlogEvent{
		{RawData: []byte("event5")},
	})
	require.NoError(t, err)
	require.Equal(t, 3, len(m.ops))
	require.Equal(t, pb.ErrorOp_Skip, m.ops[0].op)
	require.Equal(t, pb.ErrorOp_Inject, m.ops[1].op)
	require.Equal(t, pb.ErrorOp_Replace, m.ops[2].op)

	op = m.front()
	require.Equal(t, pb.ErrorOp_Inject, op.op)
}

func TestDelete(t *testing.T) {
	t.Parallel()
	m := newStreamModifier(log.L())

	err := m.Delete("wrong format")
	require.ErrorContains(t, err, "should be like (mysql-bin.000001, 2345)")

	opInject := &operator{
		op:  pb.ErrorOp_Inject,
		pos: mysql.Position{Name: "mysql.000001", Pos: 1234},
		events: []*replication.BinlogEvent{
			{RawData: []byte("event1")},
		},
	}
	opSkip := &operator{
		op:  pb.ErrorOp_Skip,
		pos: mysql.Position{Name: "mysql.000001", Pos: 2345},
	}
	m.ops = []*operator{opInject, opSkip}

	// too early
	err = m.Delete("(mysql.000001, 1000)")
	require.ErrorContains(t, err, "error operator not exist")
	// too late
	err = m.Delete("(mysql.000001, 5000)")
	require.ErrorContains(t, err, "error operator not exist")
	// not match
	err = m.Delete("(mysql.000001, 1235)")
	require.ErrorContains(t, err, "error operator not exist")

	m.next()
	op := m.front()
	require.Equal(t, opSkip, op)

	// Delete before front will raise error https://github.com/pingcap/dm/issues/1249
	err = m.Delete("(mysql.000001, 1234)")
	require.ErrorContains(t, err, "error operator not exist")
	op = m.front()
	require.Equal(t, opSkip, op)

	// Delete after front
	err = m.Delete("(mysql.000001, 2345)")
	require.NoError(t, err)
	op = m.front()
	require.Nil(t, op)
	require.Len(t, m.ops, 1)
}

func TestRemoveOutdated(t *testing.T) {
	t.Parallel()
	m := newStreamModifier(log.L())

	m.RemoveOutdated(mysql.Position{Name: "mysql.000001", Pos: 9999})
	require.Len(t, m.ops, 0)

	opInject := &operator{
		op:  pb.ErrorOp_Inject,
		pos: mysql.Position{Name: "mysql.000001", Pos: 1234},
		events: []*replication.BinlogEvent{
			{RawData: []byte("event1")},
		},
	}
	opSkip := &operator{
		op:  pb.ErrorOp_Skip,
		pos: mysql.Position{Name: "mysql.000001", Pos: 2345},
	}
	m.ops = []*operator{opInject, opSkip}

	op := m.front()
	require.Equal(t, opInject, op)
	m.RemoveOutdated(mysql.Position{Name: "mysql.000001", Pos: 2000})
	// can't remove not processed op
	op = m.front()
	require.Equal(t, opInject, op)
	require.Len(t, m.ops, 2)

	m.next()
	op = m.front()
	require.Equal(t, opSkip, op)
	m.RemoveOutdated(mysql.Position{Name: "mysql.000001", Pos: 2000})
	// should not affect front
	op = m.front()
	require.Equal(t, opSkip, op)
	require.Len(t, m.ops, 1)

	m.next()
	op = m.front()
	require.Nil(t, op)
	m.RemoveOutdated(mysql.Position{Name: "mysql.000001", Pos: 9999})
	require.Len(t, m.ops, 0)
}
