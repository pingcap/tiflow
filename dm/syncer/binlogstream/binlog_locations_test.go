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
	"fmt"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/stretchr/testify/suite"
)

type (
	mockBinlogEvent struct {
		typ  int
		args []interface{}
	}
)

const (
	DBCreate = iota

	Write

	DMLQuery

	Headers
	Rotate
)

type testLocationSuite struct {
	suite.Suite

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

	prevGSet mysql.GTIDSet
	lastGTID mysql.GTIDSet
	currGSet mysql.GTIDSet
}

func TestLocationSuite(t *testing.T) {
	suite.Run(t, new(testLocationSuite))
}

func (s *testLocationSuite) SetupTest() {
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
	s.Require().NoError(err)
	s.lastGTID, err = gtid.ParserGTID(s.flavor, s.lastGTIDStr)
	s.Require().NoError(err)
	s.currGSet, err = gtid.ParserGTID(s.flavor, s.currGSetStr)
	s.Require().NoError(err)

	s.loc = binlog.Location{
		Position: mysql.Position{
			Name: s.binlogFile,
			Pos:  s.binlogPos,
		},
	}
	prevGSet := s.prevGSet
	s.Require().NoError(s.loc.SetGTID(prevGSet))

	s.eventsGenerator, err = event.NewGenerator(s.flavor, s.serverID, s.binlogPos, s.lastGTID, s.prevGSet, 0)
	s.Require().NoError(err)
}

func (s *testLocationSuite) generateEvents(binlogEvents []mockBinlogEvent) []*replication.BinlogEvent {
	events := make([]*replication.BinlogEvent, 0, 1024)
	for _, e := range binlogEvents {
		switch e.typ {
		case DBCreate:
			evs, _, err := s.eventsGenerator.GenCreateDatabaseEvents(e.args[0].(string))
			s.Require().NoError(err)
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
			s.Require().NoError(err)
			events = append(events, evs...)

		case DMLQuery:
			dmlData := []*event.DMLData{
				{
					Schema: e.args[0].(string),
					Query:  e.args[1].(string),
				},
			}
			evs, _, err := s.eventsGenerator.GenDMLEvents(replication.UNKNOWN_EVENT, dmlData, 0)
			s.Require().NoError(err)
			events = append(events, evs...)

		case Headers:
			filename := e.args[0].(string)
			fakeRotate, err := utils.GenFakeRotateEvent(filename, uint64(s.binlogPos), s.serverID)
			s.Require().NoError(err)
			events = append(events, fakeRotate)

			events1, content, err := event.GenCommonFileHeader(s.flavor, s.serverID, s.prevGSet, true, 0)
			s.Require().NoError(err)
			events = append(events, events1...)
			s.eventsGenerator.LatestPos = uint32(len(content))

		case Rotate:
			nextFile := e.args[0].(string)
			header := &replication.EventHeader{
				Timestamp: uint32(time.Now().Unix()),
				ServerID:  s.serverID,
			}
			e, err := event.GenRotateEvent(header, s.eventsGenerator.LatestPos, []byte(nextFile), 4)
			s.Require().NoError(err)
			events = append(events, e)
		}
	}
	return events
}

// updateLastEventGSet increase the GTID set of last event.
func (s *testLocationSuite) updateLastEventGSet(events []*replication.BinlogEvent) {
	e := events[len(events)-1]
	switch v := e.Event.(type) {
	case *replication.XIDEvent:
		v.GSet = s.currGSet
	case *replication.QueryEvent:
		v.GSet = s.currGSet
	default:
		s.FailNow("last event is not expected, type %v", e.Header.EventType)
	}
}

func (s *testLocationSuite) generateDMLEvents() []*replication.BinlogEvent {
	events := s.generateEvents([]mockBinlogEvent{
		{Headers, []interface{}{s.binlogFile}},
		{Write, []interface{}{uint64(8), "foo", "bar", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(1)}}}},
	})

	s.updateLastEventGSet(events)
	return events
}

func (s *testLocationSuite) generateDDLEvents() []*replication.BinlogEvent {
	events := s.generateEvents([]mockBinlogEvent{
		{Headers, []interface{}{s.binlogFile}},
		{DBCreate, []interface{}{"foo1"}},
	})

	s.updateLastEventGSet(events)
	return events
}

// initAndCheckOneTxnEvents checks locationRecorder.update can correctly track binlog events of one transaction.
// the first one of `expected` is the location to reset streamer, the last one is the last event of a transaction.
func (s *testLocationSuite) initAndCheckOneTxnEvents(events []*replication.BinlogEvent, expected []binlog.Location) {
	r := newLocationRecorder()
	r.reset(expected[0])
	s.Require().Equal(expected[0], r.curStartLocation)
	s.Require().Equal(expected[0], r.curEndLocation)
	s.Require().Equal(expected[0], r.txnEndLocation)

	s.checkOneTxnEvents(r, events, expected)
}

func (s *testLocationSuite) checkOneTxnEvents(r *locationRecorder, events []*replication.BinlogEvent, expected []binlog.Location) {
	afterGTID := -1
	for i, e := range events {
		r.update(e)
		s.Require().Equal(expected[i], r.curStartLocation)

		if afterGTID >= 0 {
			afterGTID++
		}
		if e.Header.EventType == replication.GTID_EVENT || e.Header.EventType == replication.MARIADB_GTID_EVENT {
			afterGTID = 0
		}
		if afterGTID > 0 {
			s.Require().Equal(expected[i+1].Position, r.curEndLocation.Position)
			s.Require().Equal(expected[len(expected)-1].GetGTID(), r.curEndLocation.GetGTID())
		} else {
			s.Require().Equal(expected[i+1], r.curEndLocation)
		}

		if i == len(events)-1 {
			switch e.Header.EventType {
			case replication.XID_EVENT, replication.QUERY_EVENT, replication.ROTATE_EVENT:
				s.Require().Equal(expected[i+1], r.txnEndLocation)
			default:
				s.FailNow("type of last event is not expect", e.Header.EventType)
			}
		} else {
			s.Require().Equal(expected[0], r.txnEndLocation)
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

func (s *testLocationSuite) TestDMLUpdateLocationsGTID() {
	events := s.generateDMLEvents()

	expected := s.generateExpectedLocations(s.loc, events)

	// check each event, also provide readability
	s.Require().Len(events, 8)
	{
		s.Require().Equal(replication.ROTATE_EVENT, events[0].Header.EventType)
		s.Require().Equal(uint32(0), events[0].Header.LogPos)
	}
	{
		s.Require().Equal(replication.FORMAT_DESCRIPTION_EVENT, events[1].Header.EventType)
		s.Require().Equal(uint32(123), events[1].Header.LogPos)
	}
	{
		s.Require().Equal(replication.PREVIOUS_GTIDS_EVENT, events[2].Header.EventType)
		s.Require().Equal(uint32(194), events[2].Header.LogPos)
		gset := events[2].Event.(*replication.PreviousGTIDsEvent).GTIDSets
		s.Require().Equal("3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14", gset)
	}
	{
		s.Require().Equal(replication.GTID_EVENT, events[3].Header.EventType)
		s.Require().Equal(uint32(259), events[3].Header.LogPos)
		e := events[3].Event.(*replication.GTIDEvent)
		gtid := fmt.Sprintf("%s:%d", uuid.Must(uuid.FromBytes(e.SID)), e.GNO)
		s.Require().Equal("3ccc475b-2343-11e7-be21-6c0b84d59f30:15", gtid)
	}
	{
		s.Require().Equal(replication.QUERY_EVENT, events[4].Header.EventType)
		s.Require().Equal(uint32(301), events[4].Header.LogPos)
	}
	{
		s.Require().Equal(replication.TABLE_MAP_EVENT, events[5].Header.EventType)
		s.Require().Equal(uint32(346), events[5].Header.LogPos)
	}
	{
		s.Require().Equal(replication.WRITE_ROWS_EVENTv2, events[6].Header.EventType)
		s.Require().Equal(uint32(386), events[6].Header.LogPos)
	}
	{
		s.Require().Equal(replication.XID_EVENT, events[7].Header.EventType)
		s.Require().Equal(uint32(417), events[7].Header.LogPos)
	}

	err := expected[8].SetGTID(s.currGSet)
	s.Require().NoError(err)

	s.initAndCheckOneTxnEvents(events, expected)
}

func (s *testLocationSuite) TestDMLUpdateLocationsPos() {
	loc := s.loc
	err := loc.SetGTID(gtid.MustZeroGTIDSet(mysql.MySQLFlavor))
	s.Require().NoError(err)

	events := s.generateDMLEvents()

	// now we have 8 events, this case doesn't want to test GTID replication, so we remove them
	s.Require().Len(events, 8)
	s.Require().Equal(replication.PREVIOUS_GTIDS_EVENT, events[2].Header.EventType)
	s.Require().Equal(replication.GTID_EVENT, events[3].Header.EventType)
	s.Require().Equal(replication.XID_EVENT, events[7].Header.EventType)
	events[7].Event.(*replication.XIDEvent).GSet = nil
	events = append(events[:2], events[4:]...)

	// check each event, also provide readability
	// not that we support LogPos-EventSize not equal to previous LogPos
	s.Require().Len(events, 6)
	{
		s.Require().Equal(replication.ROTATE_EVENT, events[0].Header.EventType)
		s.Require().Equal(uint32(0), events[0].Header.LogPos)
	}
	{
		s.Require().Equal(replication.FORMAT_DESCRIPTION_EVENT, events[1].Header.EventType)
		s.Require().Equal(uint32(123), events[1].Header.LogPos)
	}
	{
		s.Require().Equal(replication.QUERY_EVENT, events[2].Header.EventType)
		s.Require().Equal(uint32(301), events[2].Header.LogPos)
	}
	{
		s.Require().Equal(replication.TABLE_MAP_EVENT, events[3].Header.EventType)
		s.Require().Equal(uint32(346), events[3].Header.LogPos)
	}
	{
		s.Require().Equal(replication.WRITE_ROWS_EVENTv2, events[4].Header.EventType)
		s.Require().Equal(uint32(386), events[4].Header.LogPos)
	}
	{
		s.Require().Equal(replication.XID_EVENT, events[5].Header.EventType)
		s.Require().Equal(uint32(417), events[5].Header.LogPos)
	}

	expected := s.generateExpectedLocations(loc, events)

	s.initAndCheckOneTxnEvents(events, expected)
}

func (s *testLocationSuite) TestDDLUpdateLocationsGTID() {
	events := s.generateDDLEvents()

	// we have 5 events
	s.Require().Len(events, 5)
	{
		s.Require().Equal(replication.ROTATE_EVENT, events[0].Header.EventType)
		s.Require().Equal(uint32(0), events[0].Header.LogPos)
	}
	{
		s.Require().Equal(replication.FORMAT_DESCRIPTION_EVENT, events[1].Header.EventType)
		s.Require().Equal(uint32(123), events[1].Header.LogPos)
	}
	{
		s.Require().Equal(replication.PREVIOUS_GTIDS_EVENT, events[2].Header.EventType)
		s.Require().Equal(uint32(194), events[2].Header.LogPos)
		gset := events[2].Event.(*replication.PreviousGTIDsEvent).GTIDSets
		s.Require().Equal("3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14", gset)
	}
	{
		s.Require().Equal(replication.GTID_EVENT, events[3].Header.EventType)
		s.Require().Equal(uint32(259), events[3].Header.LogPos)
		e := events[3].Event.(*replication.GTIDEvent)
		gtid := fmt.Sprintf("%s:%d", uuid.Must(uuid.FromBytes(e.SID)), e.GNO)
		s.Require().Equal("3ccc475b-2343-11e7-be21-6c0b84d59f30:15", gtid)
	}
	{
		s.Require().Equal(replication.QUERY_EVENT, events[4].Header.EventType)
		s.Require().Equal(uint32(322), events[4].Header.LogPos)
	}

	expected := s.generateExpectedLocations(s.loc, events)

	err := expected[5].SetGTID(s.currGSet)
	s.Require().NoError(err)

	s.initAndCheckOneTxnEvents(events, expected)
}

func (s *testLocationSuite) TestDDLUpdateLocationsPos() {
	loc := s.loc
	err := loc.SetGTID(gtid.MustZeroGTIDSet(mysql.MySQLFlavor))
	s.Require().NoError(err)

	events := s.generateDDLEvents()

	// now we have 5 events, this case doesn't want to test GTID replication, so we remove them
	s.Require().Len(events, 5)

	s.Require().Equal(replication.PREVIOUS_GTIDS_EVENT, events[2].Header.EventType)
	s.Require().Equal(replication.GTID_EVENT, events[3].Header.EventType)
	s.Require().Equal(replication.QUERY_EVENT, events[4].Header.EventType)
	events[4].Event.(*replication.QueryEvent).GSet = nil
	events = append(events[:2], events[4:]...)

	// check each event, also provide readability
	// not that we support LogPos-EventSize not equal to previous LogPos
	s.Require().Len(events, 3)
	{
		s.Require().Equal(replication.ROTATE_EVENT, events[0].Header.EventType)
		s.Require().Equal(uint32(0), events[0].Header.LogPos)
	}
	{
		s.Require().Equal(replication.FORMAT_DESCRIPTION_EVENT, events[1].Header.EventType)
		s.Require().Equal(uint32(123), events[1].Header.LogPos)
	}
	{
		s.Require().Equal(replication.QUERY_EVENT, events[2].Header.EventType)
		s.Require().Equal(uint32(322), events[2].Header.LogPos)
	}

	// now we have 3 events, test about their 4 locations
	expected := s.generateExpectedLocations(loc, events)

	s.initAndCheckOneTxnEvents(events, expected)
}

func (s *testLocationSuite) generateDMLQueryEvents() []*replication.BinlogEvent {
	var err error
	s.eventsGenerator, err = event.NewGenerator(s.flavor, s.serverID, s.binlogPos, s.lastGTID, s.prevGSet, 0)
	s.Require().NoError(err)
	events := s.generateEvents([]mockBinlogEvent{
		{Headers, []interface{}{s.binlogFile}},
		{DMLQuery, []interface{}{"foo", "INSERT INTO v VALUES(1)"}},
	})

	s.updateLastEventGSet(events)
	return events
}

func (s *testLocationSuite) TestDMLQueryUpdateLocationsGTID() {
	events := s.generateDMLQueryEvents()

	// we have 7 events
	s.Require().Len(events, 7)
	{
		s.Require().Equal(replication.ROTATE_EVENT, events[0].Header.EventType)
		s.Require().Equal(uint32(0), events[0].Header.LogPos)
	}
	{
		s.Require().Equal(replication.FORMAT_DESCRIPTION_EVENT, events[1].Header.EventType)
		s.Require().Equal(uint32(123), events[1].Header.LogPos)
	}
	{
		s.Require().Equal(replication.PREVIOUS_GTIDS_EVENT, events[2].Header.EventType)
		s.Require().Equal(uint32(194), events[2].Header.LogPos)
		gset := events[2].Event.(*replication.PreviousGTIDsEvent).GTIDSets
		s.Require().Equal("3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14", gset)
	}
	{
		s.Require().Equal(replication.GTID_EVENT, events[3].Header.EventType)
		s.Require().Equal(uint32(259), events[3].Header.LogPos)
		e := events[3].Event.(*replication.GTIDEvent)
		gtid := fmt.Sprintf("%s:%d", uuid.Must(uuid.FromBytes(e.SID)), e.GNO)
		s.Require().Equal("3ccc475b-2343-11e7-be21-6c0b84d59f30:15", gtid)
	}
	{
		s.Require().Equal(replication.QUERY_EVENT, events[4].Header.EventType)
		s.Require().Equal(uint32(301), events[4].Header.LogPos)
	}
	{
		s.Require().Equal(replication.QUERY_EVENT, events[5].Header.EventType)
		s.Require().Equal(uint32(364), events[5].Header.LogPos)
	}
	{
		s.Require().Equal(replication.XID_EVENT, events[6].Header.EventType)
		s.Require().Equal(uint32(395), events[6].Header.LogPos)
	}

	expected := s.generateExpectedLocations(s.loc, events)

	err := expected[7].SetGTID(s.currGSet)
	s.Require().NoError(err)

	s.initAndCheckOneTxnEvents(events, expected)
}

func (s *testLocationSuite) generateRotateAndDMLEvents() []*replication.BinlogEvent {
	var err error
	s.eventsGenerator, err = event.NewGenerator(s.flavor, s.serverID, s.binlogPos, s.lastGTID, s.prevGSet, 0)
	s.Require().NoError(err)
	events := s.generateEvents([]mockBinlogEvent{
		{Headers, []interface{}{s.binlogFile}},
		{Rotate, []interface{}{s.nextBinlogFile}},
		{Headers, []interface{}{s.nextBinlogFile}},
		{Write, []interface{}{uint64(8), "foo", "bar", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(1)}}}},
	})

	s.updateLastEventGSet(events)
	return events
}

func (s *testLocationSuite) TestRotateEvent() {
	events := s.generateRotateAndDMLEvents()

	s.Require().Len(events, 12)

	nextLoc := s.loc
	nextLoc.Position.Name = s.nextBinlogFile
	expected := s.generateExpectedLocations(nextLoc, events)

	// reset events of first binlog file
	expected[0].Position.Name = s.binlogFile
	s.Require().Equal(replication.ROTATE_EVENT, events[0].Header.EventType)
	expected[1].Position.Name = s.binlogFile
	s.Require().Equal(replication.FORMAT_DESCRIPTION_EVENT, events[1].Header.EventType)
	expected[2].Position.Name = s.binlogFile
	s.Require().Equal(replication.PREVIOUS_GTIDS_EVENT, events[2].Header.EventType)
	expected[3].Position.Name = s.binlogFile
	s.Require().Equal(replication.ROTATE_EVENT, events[3].Header.EventType)
	expected[4].Position.Pos = 4
	s.Require().Equal(replication.ROTATE_EVENT, events[4].Header.EventType)
	expected[5].Position.Pos = 4
	s.Require().Equal(replication.FORMAT_DESCRIPTION_EVENT, events[5].Header.EventType)
	expected[6].Position.Pos = 4
	s.Require().Equal(replication.PREVIOUS_GTIDS_EVENT, events[6].Header.EventType)
	expected[7].Position.Pos = 4

	err := expected[12].SetGTID(s.currGSet)
	s.Require().NoError(err)

	r := newLocationRecorder()
	r.reset(expected[0])
	s.Require().Equal(expected[0], r.curStartLocation)
	s.Require().Equal(expected[0], r.curEndLocation)
	s.Require().Equal(expected[0], r.txnEndLocation)

	s.checkOneTxnEvents(r, events[:4], expected[:5])
	s.checkOneTxnEvents(r, events[4:], expected[4:])
}
