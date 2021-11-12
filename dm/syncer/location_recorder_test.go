package syncer

import (
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	. "github.com/pingcap/check"

	"github.com/pingcap/ticdc/dm/pkg/binlog"
	"github.com/pingcap/ticdc/dm/pkg/binlog/event"
	"github.com/pingcap/ticdc/dm/pkg/gtid"
	"github.com/pingcap/ticdc/dm/pkg/utils"
)

var (
	serverID    uint32 = 101
	binlogFile         = "mysql-bin.000001"
	binlogPos   uint32 = 123
	flavor             = mysql.MySQLFlavor
	prevGSetStr        = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14"
	lastGTIDStr        = "3ccc475b-2343-11e7-be21-6c0b84d59f30:14"
)

func (s *testSyncerSuite) generateDMLEvents(c *C) []*replication.BinlogEvent {
	fakeRotate, err := utils.GenFakeRotateEvent(binlogFile, uint64(binlogPos), serverID)
	c.Assert(err, IsNil)
	events := []*replication.BinlogEvent{fakeRotate}

	prevGSet, err := gtid.ParserGTID(flavor, prevGSetStr)
	c.Assert(err, IsNil)

	events1, _, err := event.GenCommonFileHeader(flavor, serverID, prevGSet)
	c.Assert(err, IsNil)
	events = append(events, events1...)

	lastGTID, err := gtid.ParserGTID(flavor, lastGTIDStr)
	c.Assert(err, IsNil)

	s.eventsGenerator, err = event.NewGenerator(flavor, serverID, binlogPos, lastGTID, prevGSet, 0)
	c.Assert(err, IsNil)
	events2 := s.generateEvents([]mockBinlogEvent{{Write, []interface{}{uint64(8), "foo", "bar", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(1)}}}}}, c)
	events = append(events, events2...)

	// streamer will attach GTID to XID event, so we manually do it for our mock generator
	last := len(events) - 1
	c.Assert(events[last].Header.EventType, Equals, replication.XID_EVENT)
	newGSet := prevGSet.Origin()
	c.Assert(newGSet.Update(s.eventsGenerator.LatestGTID.String()), IsNil)
	// check not shallow copy
	c.Assert(newGSet.Equal(prevGSet.Origin()), IsFalse)
	events[last].Event.(*replication.XIDEvent).GSet = newGSet
	return events
}

func (s *testSyncerSuite) generateDDLEvents(c *C) []*replication.BinlogEvent {
	fakeRotate, err := utils.GenFakeRotateEvent(binlogFile, uint64(binlogPos), serverID)
	c.Assert(err, IsNil)
	events := []*replication.BinlogEvent{fakeRotate}

	prevGSet, err := gtid.ParserGTID(flavor, prevGSetStr)
	c.Assert(err, IsNil)

	events1, _, err := event.GenCommonFileHeader(flavor, serverID, prevGSet)
	c.Assert(err, IsNil)
	events = append(events, events1...)

	lastGTID, err := gtid.ParserGTID(flavor, lastGTIDStr)
	c.Assert(err, IsNil)

	s.eventsGenerator, err = event.NewGenerator(flavor, serverID, binlogPos, lastGTID, prevGSet, 0)
	c.Assert(err, IsNil)
	events2 := s.generateEvents([]mockBinlogEvent{{DBCreate, []interface{}{"foo1"}}}, c)
	events = append(events, events2...)

	// streamer will attach GTID to Query event, so we manually do it for our mock generator
	last := len(events) - 1
	c.Assert(events[last].Header.EventType, Equals, replication.QUERY_EVENT)
	newGSet := prevGSet.Origin()
	c.Assert(newGSet.Update(s.eventsGenerator.LatestGTID.String()), IsNil)
	// check not shallow copy
	c.Assert(newGSet.Equal(prevGSet.Origin()), IsFalse)
	events[last].Event.(*replication.QueryEvent).GSet = newGSet

	return events
}

// checkOneTxnEvents checks locationRecorder.update can correctly track binlog events of one transaction.
// the first one of `expected` is the location to reset streamer, the last one is the last event of a transaction.
func (s *testSyncerSuite) checkOneTxnEvents(c *C, events []*replication.BinlogEvent, expected []binlog.Location) {
	r := &locationRecorder{}
	r.reset(expected[0])
	c.Assert(r.curStartLocation, DeepEquals, expected[0])
	c.Assert(r.curEndLocation, DeepEquals, expected[0])
	c.Assert(r.txnEndLocation, DeepEquals, expected[0])

	for i, e := range events {
		r.update(e)
		c.Assert(r.curStartLocation, DeepEquals, expected[i])
		c.Assert(r.curEndLocation, DeepEquals, expected[i+1])

		if i == len(events)-1 {
			switch e.Header.EventType {
			case replication.XID_EVENT, replication.QUERY_EVENT:
                c.Assert(r.txnEndLocation, DeepEquals, expected[i+1])
			default:
				c.Fatal("type of last event is not expect", e.Header.EventType)
			}

		} else {
			c.Assert(r.txnEndLocation, DeepEquals, expected[0])
		}
	}
}

func (s *testSyncerSuite) TestDMLUpdateLocationsGTID(c *C) {
	loc := binlog.Location{
		Position: mysql.Position{
			Name: binlogFile,
			Pos:  binlogPos,
		},
	}
	prevGSetWrapped, err := gtid.ParserGTID(flavor, prevGSetStr)
	c.Assert(err, IsNil)
	prevGSet := prevGSetWrapped.Origin()
	err = loc.SetGTID(prevGSet)
	c.Assert(err, IsNil)

	events := s.generateDMLEvents(c)

	// we have 8 events
	c.Assert(events, HasLen, 8)

	expected := make([]binlog.Location, 9)
	for i := range expected {
		if i == 0 {
			// before receive first event, it should be reset location
			expected[0] = loc
			continue
		}
		expected[i] = loc
		expected[i].Position.Pos = events[i-1].Header.LogPos
	}

	lastGTID, err := gtid.ParserGTID(flavor, lastGTIDStr)
	c.Assert(err, IsNil)
	nextGTID, err := event.GTIDIncrease(flavor, lastGTID)
	c.Assert(err, IsNil)

	newGSet := prevGSet.Clone()
	c.Assert(newGSet.Update(nextGTID.String()), IsNil)
	c.Assert(expected[8].SetGTID(newGSet), IsNil)

	s.checkOneTxnEvents(c, events, expected)
}

func (s *testSyncerSuite) TestDMLUpdateLocationsPos(c *C) {
	loc := binlog.Location{
		Position: mysql.Position{
			Name: binlogFile,
			Pos:  binlogPos,
		},
	}
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

	// now we have 6 events, test about their 7 locations
	expected := make([]binlog.Location, 7)
	for i := range expected {
		if i == 0 {
			// before receive first event, it should be reset location
			expected[0] = loc
			continue
		}
		expected[i] = loc
		expected[i].Position.Pos = events[i-1].Header.LogPos
	}

	s.checkOneTxnEvents(c, events, expected)
}

func (s *testSyncerSuite) TestDDLUpdateLocationsGTID(c *C) {
	loc := binlog.Location{
		Position: mysql.Position{
			Name: binlogFile,
			Pos:  binlogPos,
		},
	}
	prevGSetWrapped, err := gtid.ParserGTID(flavor, prevGSetStr)
	c.Assert(err, IsNil)
	prevGSet := prevGSetWrapped.Origin()
	err = loc.SetGTID(nil)
	c.Assert(err, IsNil)

	events := s.generateDDLEvents(c)

	// we have 5 events
	c.Assert(events, HasLen, 5)

	expected := make([]binlog.Location, 6)
	for i := range expected {
		if i == 0 {
			// before receive first event, it should be reset location
			expected[0] = loc
			continue
		}
		expected[i] = loc
		expected[i].Position.Pos = events[i-1].Header.LogPos
	}

	lastGTID, err := gtid.ParserGTID(flavor, lastGTIDStr)
	c.Assert(err, IsNil)
	nextGTID, err := event.GTIDIncrease(flavor, lastGTID)
	c.Assert(err, IsNil)

	newGSet := prevGSet.Clone()
	c.Assert(newGSet.Update(nextGTID.String()), IsNil)
	c.Assert(expected[5].SetGTID(newGSet), IsNil)

	s.checkOneTxnEvents(c, events, expected)
}

func (s *testSyncerSuite) TestDDLUpdateLocationsPos(c *C) {
	loc := binlog.Location{
		Position: mysql.Position{
			Name: binlogFile,
			Pos:  binlogPos,
		},
	}
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
	expected := make([]binlog.Location, 4)
	for i := range expected {
		if i == 0 {
			// before receive first event, it should be reset location
			expected[0] = loc
			continue
		}
		expected[i] = loc
		expected[i].Position.Pos = events[i-1].Header.LogPos
	}

	s.checkOneTxnEvents(c, events, expected)
}

func (s *testSyncerSuite) generateDMLQueryEvents(c *C) []*replication.BinlogEvent {
	fakeRotate, err := utils.GenFakeRotateEvent(binlogFile, uint64(binlogPos), serverID)
	c.Assert(err, IsNil)
	events := []*replication.BinlogEvent{fakeRotate}

	prevGSet, err := gtid.ParserGTID(flavor, prevGSetStr)
	c.Assert(err, IsNil)

	events1, _, err := event.GenCommonFileHeader(flavor, serverID, prevGSet)
	c.Assert(err, IsNil)
	events = append(events, events1...)

	lastGTID, err := gtid.ParserGTID(flavor, lastGTIDStr)
	c.Assert(err, IsNil)

	s.eventsGenerator, err = event.NewGenerator(flavor, serverID, binlogPos, lastGTID, prevGSet, 0)
	c.Assert(err, IsNil)
	events2 := s.generateEvents([]mockBinlogEvent{{DMLQuery, []interface{}{"foo", "INSERT INTO v VALUES(1)"}}}, c)
	events = append(events, events2...)

	// streamer will attach GTID to XID event, so we manually do it for our mock generator
	last := len(events) - 1
	c.Assert(events[last].Header.EventType, Equals, replication.XID_EVENT)
	newGSet := prevGSet.Origin()
	c.Assert(newGSet.Update(s.eventsGenerator.LatestGTID.String()), IsNil)
	// check not shallow copy
	c.Assert(newGSet.Equal(prevGSet.Origin()), IsFalse)
	events[last].Event.(*replication.XIDEvent).GSet = newGSet
	return events
}

func (s *testSyncerSuite) TestDMLQueryUpdateLocationsGTID(c *C) {
	loc := binlog.Location{
		Position: mysql.Position{
			Name: binlogFile,
			Pos:  binlogPos,
		},
	}
	prevGSetWrapped, err := gtid.ParserGTID(flavor, prevGSetStr)
	c.Assert(err, IsNil)
	prevGSet := prevGSetWrapped.Origin()
	err = loc.SetGTID(prevGSet)
	c.Assert(err, IsNil)

	events := s.generateDMLQueryEvents(c)

	// we have 7 events
	c.Assert(events, HasLen, 7)

	expected := make([]binlog.Location, 8)
	for i := range expected {
		if i == 0 {
			// before receive first event, it should be reset location
			expected[0] = loc
			continue
		}
		expected[i] = loc
		expected[i].Position.Pos = events[i-1].Header.LogPos
	}

	lastGTID, err := gtid.ParserGTID(flavor, lastGTIDStr)
	c.Assert(err, IsNil)
	nextGTID, err := event.GTIDIncrease(flavor, lastGTID)
	c.Assert(err, IsNil)

	newGSet := prevGSet.Clone()
	c.Assert(newGSet.Update(nextGTID.String()), IsNil)
	c.Assert(expected[7].SetGTID(newGSet), IsNil)

	s.checkOneTxnEvents(c, events, expected)
}
