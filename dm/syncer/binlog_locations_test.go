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

package syncer

import (
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	. "github.com/pingcap/check"

	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

var _ = Suite(&testLocationSuite{})

type testLocationSuite struct {
	eventsGenerator *event.Generator

	serverID       uint32
	binlogFile     string
	nextBinlogFile string
	binlogPos      uint32
	flavor         string
	prevGSetStr    string
	lastGTIDStr    string
	currGSetStr    string

	loc binlog.Location

	prevGSet gtid.Set
	lastGTID gtid.Set
	currGSet gtid.Set
}

func (s *testLocationSuite) SetUpTest(c *C) {
	s.serverID = 101
	s.binlogFile = "mysql-bin.000001"
	s.nextBinlogFile = "mysql-bin.000002"
	s.binlogPos = 123
	s.flavor = mysql.MySQLFlavor
	s.prevGSetStr = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14"
	s.lastGTIDStr = "3ccc475b-2343-11e7-be21-6c0b84d59f30:14"
	s.currGSetStr = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-15"

	var err error
	s.prevGSet, err = gtid.ParserGTID(s.flavor, s.prevGSetStr)
	c.Assert(err, IsNil)
	s.lastGTID, err = gtid.ParserGTID(s.flavor, s.lastGTIDStr)
	c.Assert(err, IsNil)
	s.currGSet, err = gtid.ParserGTID(s.flavor, s.currGSetStr)
	c.Assert(err, IsNil)

	s.loc = binlog.Location{
		Position: mysql.Position{
			Name: s.binlogFile,
			Pos:  s.binlogPos,
		},
	}
	prevGSet := s.prevGSet.Origin()
	c.Assert(s.loc.SetGTID(prevGSet), IsNil)

	s.eventsGenerator, err = event.NewGenerator(s.flavor, s.serverID, s.binlogPos, s.lastGTID, s.prevGSet, 0)
	c.Assert(err, IsNil)
}

func (s *testLocationSuite) generateEvents(binlogEvents mockBinlogEvents, c *C) []*replication.BinlogEvent {
	events := make([]*replication.BinlogEvent, 0, 1024)
	for _, e := range binlogEvents {
		switch e.typ {
		case DBCreate:
			evs, _, err := s.eventsGenerator.GenCreateDatabaseEvents(e.args[0].(string))
			c.Assert(err, IsNil)
			events = append(events, evs...)

		case Write:
			dmlData := []*event.DMLData{
				{
					TableID:    e.args[0].(uint64),
					Schema:     e.args[1].(string),
					Table:      e.args[2].(string),
					ColumnType: e.args[3].([]byte),
					Rows:       e.args[4].([][]interface{}),
				},
			}
			eventType := replication.WRITE_ROWS_EVENTv2
			evs, _, err := s.eventsGenerator.GenDMLEvents(eventType, dmlData, 0)
			c.Assert(err, IsNil)
			events = append(events, evs...)

		case DMLQuery:
			dmlData := []*event.DMLData{
				{
					Schema: e.args[0].(string),
					Query:  e.args[1].(string),
				},
			}
			evs, _, err := s.eventsGenerator.GenDMLEvents(replication.UNKNOWN_EVENT, dmlData, 0)
			c.Assert(err, IsNil)
			events = append(events, evs...)

		case Headers:
			filename := e.args[0].(string)
			fakeRotate, err := utils.GenFakeRotateEvent(filename, uint64(s.binlogPos), s.serverID)
			c.Assert(err, IsNil)
			events = append(events, fakeRotate)

			events1, _, err := event.GenCommonFileHeader(s.flavor, s.serverID, s.prevGSet, true, 0)
			c.Assert(err, IsNil)
			events = append(events, events1...)

		case Rotate:
			nextFile := e.args[0].(string)
			header := &replication.EventHeader{
				Timestamp: uint32(time.Now().Unix()),
				ServerID:  s.serverID,
			}
			e, err := event.GenRotateEvent(header, s.eventsGenerator.LatestPos, []byte(nextFile), 4)
			c.Assert(err, IsNil)
			events = append(events, e)
		}
	}
	return events
}

// updateLastEventGSet increase the GTID set of last event.
func (s *testLocationSuite) updateLastEventGSet(c *C, events []*replication.BinlogEvent) {
	e := events[len(events)-1]
	switch v := e.Event.(type) {
	case *replication.XIDEvent:
		v.GSet = s.currGSet.Origin()
	case *replication.QueryEvent:
		v.GSet = s.currGSet.Origin()
	default:
		c.Fatalf("last event is not expected, type %v", e.Header.EventType)
	}
}

func (s *testLocationSuite) generateDMLEvents(c *C) []*replication.BinlogEvent {
	events := s.generateEvents([]mockBinlogEvent{
		{Headers, []interface{}{s.binlogFile}},
		{Write, []interface{}{uint64(8), "foo", "bar", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(1)}}}},
	}, c)

	s.updateLastEventGSet(c, events)
	return events
}

func (s *testLocationSuite) generateDDLEvents(c *C) []*replication.BinlogEvent {
	events := s.generateEvents([]mockBinlogEvent{
		{Headers, []interface{}{s.binlogFile}},
		{DBCreate, []interface{}{"foo1"}},
	}, c)

	s.updateLastEventGSet(c, events)
	return events
}

// checkOneTxnEvents checks locationRecorder.update can correctly track binlog events of one transaction.
// the first one of `expected` is the location to reset streamer, the last one is the last event of a transaction.
func (s *testLocationSuite) checkOneTxnEvents(c *C, events []*replication.BinlogEvent, expected []binlog.Location) {
	r := &locationRecorder{}
	r.reset(expected[0])
	c.Assert(r.curStartLocation, DeepEquals, expected[0])
	c.Assert(r.curEndLocation, DeepEquals, expected[0])
	c.Assert(r.txnEndLocation, DeepEquals, expected[0])

	seenGTID := false
	for i, e := range events {
		r.update(e)
		c.Assert(r.curStartLocation, DeepEquals, expected[i])

		if e.Header.EventType == replication.GTID_EVENT || e.Header.EventType == replication.MARIADB_GTID_EVENT {
			seenGTID = true
		}
		if seenGTID {
			c.Assert(r.curEndLocation.Position, DeepEquals, expected[i+1].Position)
			c.Assert(r.curEndLocation.GetGTID(), DeepEquals, expected[len(expected)-1].GetGTID())
		} else {
			c.Assert(r.curEndLocation, DeepEquals, expected[i+1])
		}

		if i == len(events)-1 {
			switch e.Header.EventType {
			case replication.XID_EVENT, replication.QUERY_EVENT, replication.ROTATE_EVENT:
				c.Assert(r.txnEndLocation, DeepEquals, expected[i+1])
			default:
				c.Fatal("type of last event is not expect", e.Header.EventType)
			}
		} else {
			c.Assert(r.txnEndLocation, DeepEquals, expected[0])
		}
	}
}

// generateExpectedLocations generates binlog position part of location from given event.
func (s *testLocationSuite) generateExpectedLocations(
	initLoc binlog.Location,
	events []*replication.BinlogEvent,
) []binlog.Location {
	expected := make([]binlog.Location, len(events)+1)
	for i := range expected {
		if i == 0 {
			// before receive first event, it should be reset location
			expected[0] = initLoc
			continue
		}
		expected[i] = initLoc
		// those not-update-position events only occur in first events in these tests
		e := events[i-1]
		if shouldUpdatePos(e) && e.Header.EventType != replication.ROTATE_EVENT {
			expected[i].Position.Pos = e.Header.LogPos
		}
	}
	return expected
}

func (s *testLocationSuite) TestDMLUpdateLocationsGTID(c *C) {
	events := s.generateDMLEvents(c)

	// we have 8 events
	c.Assert(events, HasLen, 8)

	expected := s.generateExpectedLocations(s.loc, events)

	c.Assert(expected[8].SetGTID(s.currGSet.Origin()), IsNil)

	s.checkOneTxnEvents(c, events, expected)
}

func (s *testLocationSuite) TestDMLUpdateLocationsPos(c *C) {
	loc := s.loc
	err := loc.SetGTID(nil)
	c.Assert(err, IsNil)

	events := s.generateDMLEvents(c)

	// now we have 8 events, this case doesn't want to test GTID replication, so we remove them
	c.Assert(events, HasLen, 8)
	c.Assert(events[2].Header.EventType, Equals, replication.PREVIOUS_GTIDS_EVENT)
	c.Assert(events[3].Header.EventType, Equals, replication.GTID_EVENT)
	c.Assert(events[7].Header.EventType, Equals, replication.XID_EVENT)
	events[7].Event.(*replication.XIDEvent).GSet = nil
	events = append(events[:2], events[4:]...)

	expected := s.generateExpectedLocations(loc, events)

	s.checkOneTxnEvents(c, events, expected)
}

func (s *testLocationSuite) TestDDLUpdateLocationsGTID(c *C) {
	events := s.generateDDLEvents(c)

	// we have 5 events
	c.Assert(events, HasLen, 5)

	expected := s.generateExpectedLocations(s.loc, events)

	c.Assert(expected[5].SetGTID(s.currGSet.Origin()), IsNil)

	s.checkOneTxnEvents(c, events, expected)
}

func (s *testLocationSuite) TestDDLUpdateLocationsPos(c *C) {
	loc := s.loc
	err := loc.SetGTID(nil)
	c.Assert(err, IsNil)

	events := s.generateDDLEvents(c)

	// now we have 5 events, this case doesn't want to test GTID replication, so we remove them
	c.Assert(events, HasLen, 5)

	c.Assert(events[2].Header.EventType, Equals, replication.PREVIOUS_GTIDS_EVENT)
	c.Assert(events[3].Header.EventType, Equals, replication.GTID_EVENT)
	c.Assert(events[4].Header.EventType, Equals, replication.QUERY_EVENT)
	events[4].Event.(*replication.QueryEvent).GSet = nil
	events = append(events[:2], events[4:]...)

	// now we have 3 events, test about their 4 locations
	expected := s.generateExpectedLocations(loc, events)

	s.checkOneTxnEvents(c, events, expected)
}

func (s *testLocationSuite) generateDMLQueryEvents(c *C) []*replication.BinlogEvent {
	var err error
	s.eventsGenerator, err = event.NewGenerator(s.flavor, s.serverID, s.binlogPos, s.lastGTID, s.prevGSet, 0)
	c.Assert(err, IsNil)
	events := s.generateEvents([]mockBinlogEvent{
		{Headers, []interface{}{s.binlogFile}},
		{DMLQuery, []interface{}{"foo", "INSERT INTO v VALUES(1)"}},
	}, c)

	s.updateLastEventGSet(c, events)
	return events
}

func (s *testLocationSuite) TestDMLQueryUpdateLocationsGTID(c *C) {
	events := s.generateDMLQueryEvents(c)

	// we have 7 events
	c.Assert(events, HasLen, 7)

	expected := s.generateExpectedLocations(s.loc, events)

	c.Assert(expected[7].SetGTID(s.currGSet.Origin()), IsNil)

	s.checkOneTxnEvents(c, events, expected)
}

func (s *testLocationSuite) generateRotateAndDMLEvents(c *C) []*replication.BinlogEvent {
	var err error
	s.eventsGenerator, err = event.NewGenerator(s.flavor, s.serverID, s.binlogPos, s.lastGTID, s.prevGSet, 0)
	c.Assert(err, IsNil)
	events := s.generateEvents([]mockBinlogEvent{
		{Headers, []interface{}{s.binlogFile}},
		{Rotate, []interface{}{s.nextBinlogFile}},
		{Headers, []interface{}{s.nextBinlogFile}},
		{Write, []interface{}{uint64(8), "foo", "bar", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(1)}}}},
	}, c)

	s.updateLastEventGSet(c, events)
	return events
}

func (s *testLocationSuite) TestRotateEvent(c *C) {
	events := s.generateRotateAndDMLEvents(c)

	c.Assert(events, HasLen, 12)

	nextLoc := s.loc
	nextLoc.Position.Name = s.nextBinlogFile
	expected := s.generateExpectedLocations(nextLoc, events)

	// reset events of first binlog file
	expected[0].Position.Name = s.binlogFile
	c.Assert(events[0].Header.EventType, Equals, replication.ROTATE_EVENT)
	expected[1].Position.Name = s.binlogFile
	c.Assert(events[1].Header.EventType, Equals, replication.FORMAT_DESCRIPTION_EVENT)
	expected[2].Position.Name = s.binlogFile
	c.Assert(events[2].Header.EventType, Equals, replication.PREVIOUS_GTIDS_EVENT)
	expected[3].Position.Name = s.binlogFile
	c.Assert(events[3].Header.EventType, Equals, replication.ROTATE_EVENT)
	expected[4].Position.Pos = 4
	c.Assert(events[4].Header.EventType, Equals, replication.ROTATE_EVENT)
	expected[5].Position.Pos = 4
	c.Assert(events[5].Header.EventType, Equals, replication.FORMAT_DESCRIPTION_EVENT)
	expected[6].Position.Pos = 4
	c.Assert(events[6].Header.EventType, Equals, replication.PREVIOUS_GTIDS_EVENT)
	expected[7].Position.Pos = 4

	c.Assert(expected[12].SetGTID(s.currGSet.Origin()), IsNil)

	s.checkOneTxnEvents(c, events[:4], expected[:5])
	s.checkOneTxnEvents(c, events[4:], expected[4:])
}
