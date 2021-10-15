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
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	. "github.com/pingcap/check"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

var _ = Suite(&testOperatorSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testOperatorSuite struct{}

func (o *testOperatorSuite) TestOperator(c *C) {
	logger := log.L()
	h := NewHolder(&logger)

	startLocation := binlog.Location{
		Position: mysql.Position{
			Name: "mysql-bin.000001",
			Pos:  233,
		},
	}
	endLocation := binlog.Location{
		Position: mysql.Position{
			Name: "mysql-bin.000001",
			Pos:  250,
		},
	}
	nextLocation := binlog.Location{
		Position: mysql.Position{
			Name: "mysql-bin.000001",
			Pos:  300,
		},
	}

	sql1 := "alter table tb add column a int"
	event1 := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.QUERY_EVENT,
			Timestamp: uint32(1623313992),
		},
		Event: &replication.QueryEvent{
			Schema: []byte("db"),
			Query:  []byte(sql1),
		},
	}
	sql2 := "alter table tb add column b int"
	event2 := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.QUERY_EVENT,
			Timestamp: uint32(1623313993),
		},
		Event: &replication.QueryEvent{
			Schema: []byte("db"),
			Query:  []byte(sql2),
		},
	}

	// revert not exist operator
	err := h.Set(startLocation.Position.String(), pb.ErrorOp_Revert, nil)
	c.Assert(terror.ErrSyncerOperatorNotExist.Equal(err), IsTrue)

	// skip event
	err = h.Set(startLocation.Position.String(), pb.ErrorOp_Skip, nil)
	c.Assert(err, IsNil)
	apply, op := h.MatchAndApply(startLocation, endLocation, event1.Header.Timestamp)
	c.Assert(apply, IsTrue)
	c.Assert(op, Equals, pb.ErrorOp_Skip)

	// overwrite operator
	err = h.Set(startLocation.Position.String(), pb.ErrorOp_Replace, []*replication.BinlogEvent{event1, event2})
	c.Assert(err, IsNil)
	apply, op = h.MatchAndApply(startLocation, endLocation, event2.Header.Timestamp)
	c.Assert(apply, IsTrue)
	c.Assert(op, Equals, pb.ErrorOp_Replace)

	// test GetEvent
	// get by endLocation
	e, err := h.GetEvent(endLocation)
	c.Assert(e, IsNil)
	c.Assert(terror.ErrSyncerReplaceEventNotExist.Equal(err), IsTrue)
	// get first event
	e, err = h.GetEvent(startLocation)
	c.Assert(err, IsNil)
	c.Assert(e.Header.LogPos, Equals, startLocation.Position.Pos)
	c.Assert(e.Header.Timestamp, Equals, event1.Header.Timestamp)
	c.Assert(e.Header.EventSize, Equals, uint32(0))
	c.Assert(e.Event, Equals, event1.Event)
	// get second event
	startLocation.Suffix++
	e, err = h.GetEvent(startLocation)
	c.Assert(err, IsNil)
	c.Assert(e.Header.LogPos, Equals, endLocation.Position.Pos)
	c.Assert(e.Header.Timestamp, Equals, event2.Header.Timestamp)
	c.Assert(e.Header.EventSize, Equals, endLocation.Position.Pos-startLocation.Position.Pos)
	c.Assert(e.Event, Equals, event2.Event)
	// get third event, out of index
	startLocation.Suffix++
	e, err = h.GetEvent(startLocation)
	c.Assert(terror.ErrSyncerReplaceEvent.Equal(err), IsTrue)
	c.Assert(e, IsNil)

	// revert exist operator
	err = h.Set(startLocation.Position.String(), pb.ErrorOp_Revert, nil)
	c.Assert(err, IsNil)
	apply, op = h.MatchAndApply(startLocation, endLocation, event1.Header.Timestamp)
	c.Assert(apply, IsFalse)
	c.Assert(op, Equals, pb.ErrorOp_InvalidErrorOp)

	// add two operators
	err = h.Set(startLocation.Position.String(), pb.ErrorOp_Replace, []*replication.BinlogEvent{event1, event2})
	c.Assert(err, IsNil)
	err = h.Set(endLocation.Position.String(), pb.ErrorOp_Replace, []*replication.BinlogEvent{event1, event2})
	c.Assert(err, IsNil)

	// test removeOutdated
	flushLocation := startLocation
	c.Assert(h.RemoveOutdated(flushLocation), IsNil)
	apply, op = h.MatchAndApply(startLocation, endLocation, event1.Header.Timestamp)
	c.Assert(apply, IsTrue)
	c.Assert(op, Equals, pb.ErrorOp_Replace)

	flushLocation = endLocation
	c.Assert(h.RemoveOutdated(flushLocation), IsNil)
	apply, op = h.MatchAndApply(startLocation, endLocation, event1.Header.Timestamp)
	c.Assert(apply, IsFalse)
	c.Assert(op, Equals, pb.ErrorOp_InvalidErrorOp)

	apply, op = h.MatchAndApply(endLocation, nextLocation, event1.Header.Timestamp)
	c.Assert(apply, IsTrue)
	c.Assert(op, Equals, pb.ErrorOp_Replace)
}
