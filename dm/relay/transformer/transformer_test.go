// Copyright 2019 PingCAP, Inc.
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

package transformer

import (
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/check"
	"github.com/pingcap/parser"

	"github.com/pingcap/dm/pkg/binlog/event"
	"github.com/pingcap/dm/pkg/gtid"
)

var _ = check.Suite(&testTransformerSuite{})

func TestSuite(t *testing.T) {
	check.TestingT(t)
}

type testTransformerSuite struct{}

type Case struct {
	event  *replication.BinlogEvent
	result Result
}

func (t *testTransformerSuite) TestTransform(c *check.C) {
	var (
		tran   = NewTransformer(parser.New())
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
			Flags:     0x01,
		}
		latestPos  uint32 = 456789
		gtidStr           = "9f61c5f9-1eef-11e9-b6cf-0242ac140003:5"
		gtidSet, _        = gtid.ParserGTID(mysql.MySQLFlavor, gtidStr)
		schema            = []byte("test_schema")
		cases             = make([]Case, 0, 10)
	)

	// RotateEvent
	nextLogName := "mysql-bin.000123"
	position := uint64(4)
	ev, err := event.GenRotateEvent(header, latestPos, []byte(nextLogName), position)
	c.Assert(err, check.IsNil)
	cases = append(cases, Case{
		event: ev,
		result: Result{
			LogPos:      uint32(position),
			NextLogName: nextLogName,
		},
	})

	// fake RotateEvent with zero timestamp
	header.Timestamp = 0
	ev, err = event.GenRotateEvent(header, latestPos, []byte(nextLogName), position)
	c.Assert(err, check.IsNil)
	cases = append(cases, Case{
		event: ev,
		result: Result{
			LogPos:      uint32(position),
			NextLogName: nextLogName,
		},
	})
	header.Timestamp = uint32(time.Now().Unix()) // set to non-zero

	// fake RotateEvent with zero logPos
	fakeRotateHeader := *header
	ev, err = event.GenRotateEvent(&fakeRotateHeader, latestPos, []byte(nextLogName), position)
	c.Assert(err, check.IsNil)
	ev.Header.LogPos = 0 // set to zero
	cases = append(cases, Case{
		event: ev,
		result: Result{
			LogPos:      uint32(position),
			NextLogName: nextLogName,
		},
	})

	// QueryEvent for DDL
	query := []byte("CREATE TABLE test_tbl (c1 INT)")
	ev, err = event.GenQueryEvent(header, latestPos, 0, 0, 0, nil, schema, query)
	c.Assert(err, check.IsNil)
	ev.Event.(*replication.QueryEvent).GSet = gtidSet.Origin() // set GTIDs manually
	cases = append(cases, Case{
		event: ev,
		result: Result{
			LogPos:      ev.Header.LogPos,
			GTIDSet:     gtidSet.Origin(),
			CanSaveGTID: true,
		},
	})

	// QueryEvent for non-DDL
	query = []byte("BEGIN")
	ev, err = event.GenQueryEvent(header, latestPos, 0, 0, 0, nil, schema, query)
	c.Assert(err, check.IsNil)
	cases = append(cases, Case{
		event: ev,
		result: Result{
			LogPos: ev.Header.LogPos,
		},
	})

	// XIDEvent
	xid := uint64(135)
	ev, err = event.GenXIDEvent(header, latestPos, xid)
	c.Assert(err, check.IsNil)
	ev.Event.(*replication.XIDEvent).GSet = gtidSet.Origin() // set GTIDs manually
	cases = append(cases, Case{
		event: ev,
		result: Result{
			LogPos:      ev.Header.LogPos,
			GTIDSet:     gtidSet.Origin(),
			CanSaveGTID: true,
		},
	})

	// GenericEvent, non-HEARTBEAT_EVENT
	ev = &replication.BinlogEvent{Header: header, Event: &replication.GenericEvent{}}
	cases = append(cases, Case{
		event: ev,
		result: Result{
			LogPos: ev.Header.LogPos,
		},
	})

	// GenericEvent, HEARTBEAT_EVENT
	genericHeader := *header
	ev = &replication.BinlogEvent{Header: &genericHeader, Event: &replication.GenericEvent{}}
	ev.Header.EventType = replication.HEARTBEAT_EVENT
	cases = append(cases, Case{
		event: ev,
		result: Result{
			Ignore:       true,
			IgnoreReason: ignoreReasonHeartbeat,
			LogPos:       ev.Header.LogPos,
		},
	})

	// other event type without LOG_EVENT_ARTIFICIAL_F
	ev, err = event.GenCommonGTIDEvent(mysql.MySQLFlavor, header.ServerID, latestPos, gtidSet)
	c.Assert(err, check.IsNil)
	cases = append(cases, Case{
		event: ev,
		result: Result{
			LogPos: ev.Header.LogPos,
		},
	})

	// other event type with LOG_EVENT_ARTIFICIAL_F
	ev, err = event.GenCommonGTIDEvent(mysql.MySQLFlavor, header.ServerID, latestPos, gtidSet)
	c.Assert(err, check.IsNil)
	ev.Header.Flags |= replication.LOG_EVENT_ARTIFICIAL_F
	cases = append(cases, Case{
		event: ev,
		result: Result{
			Ignore:       true,
			IgnoreReason: ignoreReasonArtificialFlag,
			LogPos:       ev.Header.LogPos,
		},
	})

	for _, cs := range cases {
		c.Assert(tran.Transform(cs.event), check.DeepEquals, cs.result)
	}
}
