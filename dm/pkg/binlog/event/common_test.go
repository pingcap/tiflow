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

package event

import (
	"fmt"
	"os"
	"path/filepath"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	. "github.com/pingcap/check"

	"github.com/pingcap/tiflow/dm/pkg/gtid"
)

var _ = Suite(&testCommonSuite{})

type testCommonSuite struct{}

func (t *testCommonSuite) TestGenCommonFileHeader(c *C) {
	var (
		flavor          = gmysql.MySQLFlavor
		serverID uint32 = 101
		gSetStr         = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,406a3f61-690d-11e7-87c5-6c92bf46f384:1-94321383,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,686e1ab6-c47e-11e7-a42c-6c92bf46f384:1-34981190,03fc0263-28c7-11e7-a653-6c0b84d59f30:1-7041423,05474d3c-28c7-11e7-8352-203db246dd3d:1-170,10b039fc-c843-11e7-8f6a-1866daf8d810:1-308290454"
		gSet     gtid.Set
	)
	gSet, err := gtid.ParserGTID(flavor, gSetStr)
	c.Assert(err, IsNil)

	events, data, err := GenCommonFileHeader(flavor, serverID, gSet, true, 0)
	c.Assert(err, IsNil)
	c.Assert(len(events), Equals, 2)
	c.Assert(events[0].Header.EventType, Equals, replication.FORMAT_DESCRIPTION_EVENT)
	c.Assert(events[1].Header.EventType, Equals, replication.PREVIOUS_GTIDS_EVENT)

	// write to file then parse it
	dir := c.MkDir()
	mysqlFilename := filepath.Join(dir, "mysql-bin-test.000001")
	mysqlFile, err := os.Create(mysqlFilename)
	c.Assert(err, IsNil)
	defer mysqlFile.Close()

	_, err = mysqlFile.Write(data)
	c.Assert(err, IsNil)

	count := 0
	onEventFunc := func(e *replication.BinlogEvent) error {
		count++
		if count > 2 {
			c.Fatalf("too many binlog events got, current is %+v", e.Header)
		}
		c.Assert(e.Header, DeepEquals, events[count-1].Header)
		c.Assert(e.Event, DeepEquals, events[count-1].Event)
		c.Assert(e.RawData, DeepEquals, events[count-1].RawData)
		return nil
	}

	parser2 := replication.NewBinlogParser()
	parser2.SetVerifyChecksum(true)
	err = parser2.ParseFile(mysqlFilename, 0, onEventFunc)
	c.Assert(err, IsNil)

	// MariaDB
	flavor = gmysql.MariaDBFlavor
	gSetStr = "1-2-12,2-2-3,3-3-8,4-4-4"

	gSet, err = gtid.ParserGTID(flavor, gSetStr)
	c.Assert(err, IsNil)

	events, data, err = GenCommonFileHeader(flavor, serverID, gSet, true, 0)
	c.Assert(err, IsNil)
	c.Assert(len(events), Equals, 2)
	c.Assert(events[0].Header.EventType, Equals, replication.FORMAT_DESCRIPTION_EVENT)
	c.Assert(events[1].Header.EventType, Equals, replication.MARIADB_GTID_LIST_EVENT)

	mariadbFilename := filepath.Join(dir, "mariadb-bin-test.000001")
	mariadbFile, err := os.Create(mariadbFilename)
	c.Assert(err, IsNil)
	defer mariadbFile.Close()

	_, err = mariadbFile.Write(data)
	c.Assert(err, IsNil)

	count = 0 // reset to 0
	err = parser2.ParseFile(mariadbFilename, 0, onEventFunc)
	c.Assert(err, IsNil)
}

func (t *testCommonSuite) TestGenCommonGTIDEvent(c *C) {
	var (
		flavor           = gmysql.MySQLFlavor
		serverID  uint32 = 101
		gSet      gtid.Set
		latestPos uint32 = 123
	)

	// nil gSet, invalid
	gtidEv, err := GenCommonGTIDEvent(flavor, serverID, latestPos, gSet, false, 0)
	c.Assert(err, NotNil)
	c.Assert(gtidEv, IsNil)

	// multi GTID in set, invalid
	gSetStr := "03fc0263-28c7-11e7-a653-6c0b84d59f30:1-123,05474d3c-28c7-11e7-8352-203db246dd3d:1-456,10b039fc-c843-11e7-8f6a-1866daf8d810:1-789"
	gSet, err = gtid.ParserGTID(flavor, gSetStr)
	c.Assert(err, IsNil)
	gtidEv, err = GenCommonGTIDEvent(flavor, serverID, latestPos, gSet, false, 0)
	c.Assert(err, NotNil)
	c.Assert(gtidEv, IsNil)

	// multi intervals, invalid
	gSetStr = "03fc0263-28c7-11e7-a653-6c0b84d59f30:1-123:200-456"
	gSet, err = gtid.ParserGTID(flavor, gSetStr)
	c.Assert(err, IsNil)
	gtidEv, err = GenCommonGTIDEvent(flavor, serverID, latestPos, gSet, false, 0)
	c.Assert(err, NotNil)
	c.Assert(gtidEv, IsNil)

	// interval > 1, invalid
	gSetStr = "03fc0263-28c7-11e7-a653-6c0b84d59f30:1-123"
	gSet, err = gtid.ParserGTID(flavor, gSetStr)
	c.Assert(err, IsNil)
	gtidEv, err = GenCommonGTIDEvent(flavor, serverID, latestPos, gSet, false, 0)
	c.Assert(err, NotNil)
	c.Assert(gtidEv, IsNil)

	// valid
	gSetStr = "03fc0263-28c7-11e7-a653-6c0b84d59f30:123"
	gSet, err = gtid.ParserGTID(flavor, gSetStr)
	c.Assert(err, IsNil)
	sid, err := ParseSID(gSetStr[:len(gSetStr)-4])
	c.Assert(err, IsNil)
	gtidEv, err = GenCommonGTIDEvent(flavor, serverID, latestPos, gSet, false, 0)
	c.Assert(err, IsNil)
	c.Assert(gtidEv, NotNil)
	c.Assert(gtidEv.Header.EventType, Equals, replication.GTID_EVENT)

	// verify the body
	gtidEvBody1, ok := gtidEv.Event.(*replication.GTIDEvent)
	c.Assert(ok, IsTrue)
	c.Assert(gtidEvBody1, NotNil)
	c.Assert(gtidEvBody1.SID, DeepEquals, sid.Bytes())
	c.Assert(gtidEvBody1.GNO, Equals, int64(123))
	c.Assert(gtidEvBody1.CommitFlag, Equals, defaultGTIDFlags)
	c.Assert(gtidEvBody1.LastCommitted, Equals, defaultLastCommitted)
	c.Assert(gtidEvBody1.SequenceNumber, Equals, defaultSequenceNumber)

	// change flavor to MariaDB
	flavor = gmysql.MariaDBFlavor

	// GTID mismatch with flavor
	gtidEv, err = GenCommonGTIDEvent(flavor, serverID, latestPos, gSet, false, 0)
	c.Assert(err, NotNil)
	c.Assert(gtidEv, IsNil)

	// multi GTID in set, invalid
	gSetStr = "1-2-3,4-5-6"
	gSet, err = gtid.ParserGTID(flavor, gSetStr)
	c.Assert(err, IsNil)
	gtidEv, err = GenCommonGTIDEvent(flavor, serverID, latestPos, gSet, false, 0)
	c.Assert(err, NotNil)
	c.Assert(gtidEv, IsNil)

	// server_id mismatch, invalid
	gSetStr = "1-2-3"
	gSet, err = gtid.ParserGTID(flavor, gSetStr)
	c.Assert(err, IsNil)
	gtidEv, err = GenCommonGTIDEvent(flavor, serverID, latestPos, gSet, false, 0)
	c.Assert(err, NotNil)
	c.Assert(gtidEv, IsNil)

	// valid
	gSetStr = fmt.Sprintf("1-%d-3", serverID)
	gSet, err = gtid.ParserGTID(flavor, gSetStr)
	c.Assert(err, IsNil)
	gtidEv, err = GenCommonGTIDEvent(flavor, serverID, latestPos, gSet, false, 0)
	c.Assert(err, IsNil)
	c.Assert(gtidEv, NotNil)
	c.Assert(gtidEv.Header.EventType, Equals, replication.MARIADB_GTID_EVENT)

	// verify the body, we
	gtidEvBody2, ok := gtidEv.Event.(*replication.MariadbGTIDEvent)
	c.Assert(ok, IsTrue)
	c.Assert(gtidEvBody2.GTID.DomainID, Equals, uint32(1))
	c.Assert(gtidEvBody2.GTID.ServerID, Equals, serverID)
	c.Assert(gtidEvBody2.GTID.SequenceNumber, Equals, uint64(3))
}

func (t *testCommonSuite) TestGTIDIncrease(c *C) {
	var (
		flavor  = gmysql.MySQLFlavor
		gSetStr = "03fc0263-28c7-11e7-a653-6c0b84d59f30:123"
		gSetIn  gtid.Set
		gSetOut gtid.Set
	)

	// increase for MySQL
	gSetIn, err := gtid.ParserGTID(flavor, gSetStr)
	c.Assert(err, IsNil)
	gSetOut, err = GTIDIncrease(flavor, gSetIn)
	c.Assert(err, IsNil)
	c.Assert(gSetOut, NotNil)
	c.Assert(gSetOut.String(), Equals, "03fc0263-28c7-11e7-a653-6c0b84d59f30:124")

	// increase for MariaDB
	flavor = gmysql.MariaDBFlavor
	gSetStr = "1-2-3"
	gSetIn, err = gtid.ParserGTID(flavor, gSetStr)
	c.Assert(err, IsNil)
	gSetOut, err = GTIDIncrease(flavor, gSetIn)
	c.Assert(err, IsNil)
	c.Assert(gSetOut, NotNil)
	c.Assert(gSetOut.String(), Equals, "1-2-4")
}
