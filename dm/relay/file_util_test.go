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

package relay

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser"

	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
)

var _ = check.Suite(&testFileUtilSuite{})

type testFileUtilSuite struct{}

func (t *testFileUtilSuite) TestCheckBinlogHeaderExist(c *check.C) {
	// file not exists
	filename := filepath.Join(c.MkDir(), "test-mysql-bin.000001")
	exist, err := checkBinlogHeaderExist(filename)
	c.Assert(err, check.ErrorMatches, ".*(no such file or directory|The system cannot find the file specified).*")
	c.Assert(exist, check.IsFalse)

	// empty file
	err = os.WriteFile(filename, nil, 0o644)
	c.Assert(err, check.IsNil)
	exist, err = checkBinlogHeaderExist(filename)
	c.Assert(err, check.IsNil)
	c.Assert(exist, check.IsFalse)

	// no enough data
	err = os.WriteFile(filename, replication.BinLogFileHeader[:len(replication.BinLogFileHeader)-1], 0o644)
	c.Assert(err, check.IsNil)
	exist, err = checkBinlogHeaderExist(filename)
	c.Assert(err, check.ErrorMatches, ".*has no enough data.*")
	c.Assert(exist, check.IsFalse)

	// equal
	err = os.WriteFile(filename, replication.BinLogFileHeader, 0o644)
	c.Assert(err, check.IsNil)
	exist, err = checkBinlogHeaderExist(filename)
	c.Assert(err, check.IsNil)
	c.Assert(exist, check.IsTrue)

	// more data
	err = os.WriteFile(filename, bytes.Repeat(replication.BinLogFileHeader, 2), 0o644)
	c.Assert(err, check.IsNil)
	exist, err = checkBinlogHeaderExist(filename)
	c.Assert(err, check.IsNil)
	c.Assert(exist, check.IsTrue)

	// invalid data
	invalidData := make([]byte, len(replication.BinLogFileHeader))
	copy(invalidData, replication.BinLogFileHeader)
	invalidData[0]++
	err = os.WriteFile(filename, invalidData, 0o644)
	c.Assert(err, check.IsNil)
	exist, err = checkBinlogHeaderExist(filename)
	c.Assert(err, check.ErrorMatches, ".*header not valid.*")
	c.Assert(exist, check.IsFalse)
}

func (t *testFileUtilSuite) TestCheckFormatDescriptionEventExist(c *check.C) {
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
			Flags:     0x01,
		}
		latestPos uint32 = 4
	)
	formatDescEv, err := event.GenFormatDescriptionEvent(header, latestPos)
	c.Assert(err, check.IsNil)

	// file not exists
	filename := filepath.Join(c.MkDir(), "test-mysql-bin.000001")
	exist, err := checkFormatDescriptionEventExist(filename)
	c.Assert(err, check.ErrorMatches, ".*(no such file or directory|The system cannot find the file specified).*")
	c.Assert(exist, check.IsFalse)

	// empty file
	err = os.WriteFile(filename, nil, 0o644)
	c.Assert(err, check.IsNil)
	exist, err = checkFormatDescriptionEventExist(filename)
	c.Assert(err, check.ErrorMatches, ".*no binlog file header at the beginning.*")
	c.Assert(exist, check.IsFalse)

	// only file header
	err = os.WriteFile(filename, replication.BinLogFileHeader, 0o644)
	c.Assert(err, check.IsNil)
	exist, err = checkFormatDescriptionEventExist(filename)
	c.Assert(err, check.IsNil)
	c.Assert(exist, check.IsFalse)

	// no enough data, < EventHeaderSize
	var buff bytes.Buffer
	buff.Write(replication.BinLogFileHeader)
	buff.Write(formatDescEv.RawData[:replication.EventHeaderSize-1])
	err = os.WriteFile(filename, buff.Bytes(), 0o644)
	c.Assert(err, check.IsNil)
	exist, err = checkFormatDescriptionEventExist(filename)
	c.Assert(errors.Cause(err), check.Equals, io.EOF)
	c.Assert(exist, check.IsFalse)

	// no enough data, = EventHeaderSize
	buff.Reset()
	buff.Write(replication.BinLogFileHeader)
	buff.Write(formatDescEv.RawData[:replication.EventHeaderSize])
	err = os.WriteFile(filename, buff.Bytes(), 0o644)
	c.Assert(err, check.IsNil)
	exist, err = checkFormatDescriptionEventExist(filename)
	c.Assert(err, check.ErrorMatches, ".*get event err EOF.*")
	c.Assert(exist, check.IsFalse)

	// no enough data, > EventHeaderSize, < EventSize
	buff.Reset()
	buff.Write(replication.BinLogFileHeader)
	buff.Write(formatDescEv.RawData[:replication.EventHeaderSize+1])
	err = os.WriteFile(filename, buff.Bytes(), 0o644)
	c.Assert(err, check.IsNil)
	exist, err = checkFormatDescriptionEventExist(filename)
	c.Assert(err, check.ErrorMatches, ".*get event err EOF.*")
	c.Assert(exist, check.IsFalse)

	// exactly the event
	buff.Reset()
	buff.Write(replication.BinLogFileHeader)
	buff.Write(formatDescEv.RawData)
	dataCopy := make([]byte, buff.Len())
	copy(dataCopy, buff.Bytes())
	err = os.WriteFile(filename, buff.Bytes(), 0o644)
	c.Assert(err, check.IsNil)
	exist, err = checkFormatDescriptionEventExist(filename)
	c.Assert(err, check.IsNil)
	c.Assert(exist, check.IsTrue)

	// more than the event
	buff.Write([]byte("more data"))
	err = os.WriteFile(filename, buff.Bytes(), 0o644)
	c.Assert(err, check.IsNil)
	exist, err = checkFormatDescriptionEventExist(filename)
	c.Assert(err, check.IsNil)
	c.Assert(exist, check.IsTrue)

	// other event type
	queryEv, err := event.GenQueryEvent(header, latestPos, 0, 0, 0, nil, []byte("schema"), []byte("BEGIN"))
	c.Assert(err, check.IsNil)
	buff.Reset()
	buff.Write(replication.BinLogFileHeader)
	buff.Write(queryEv.RawData)
	err = os.WriteFile(filename, buff.Bytes(), 0o644)
	c.Assert(err, check.IsNil)
	exist, err = checkFormatDescriptionEventExist(filename)
	c.Assert(err, check.ErrorMatches, ".*expect FormatDescriptionEvent.*")
	c.Assert(exist, check.IsFalse)
}

func (t *testFileUtilSuite) TestCheckIsDuplicateEvent(c *check.C) {
	// use a binlog event generator to generate some binlog events.
	var (
		flavor                    = gmysql.MySQLFlavor
		serverID           uint32 = 11
		latestPos          uint32
		previousGTIDSetStr        = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,406a3f61-690d-11e7-87c5-6c92bf46f384:1-94321383,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,686e1ab6-c47e-11e7-a42c-6c92bf46f384:1-34981190,03fc0263-28c7-11e7-a653-6c0b84d59f30:1-7041423,05474d3c-28c7-11e7-8352-203db246dd3d:1-170,10b039fc-c843-11e7-8f6a-1866daf8d810:1-308290454"
		latestGTIDStr             = "3ccc475b-2343-11e7-be21-6c0b84d59f30:14"
		latestXID          uint64 = 10
		allEvents                 = make([]*replication.BinlogEvent, 0, 10)
		allData            bytes.Buffer
	)
	previousGTIDSet, err := gtid.ParserGTID(flavor, previousGTIDSetStr)
	c.Assert(err, check.IsNil)
	latestGTID, err := gtid.ParserGTID(flavor, latestGTIDStr)
	c.Assert(err, check.IsNil)
	g, err := event.NewGenerator(flavor, serverID, latestPos, latestGTID, previousGTIDSet, latestXID)
	c.Assert(err, check.IsNil)
	// file header with FormatDescriptionEvent and PreviousGTIDsEvent
	events, data, err := g.GenFileHeader(0)
	c.Assert(err, check.IsNil)
	allEvents = append(allEvents, events...)
	allData.Write(data)
	// CREATE DATABASE/TABLE
	queries := []string{
		"CRATE DATABASE `db`",
		"CREATE TABLE `db`.`tbl1` (c1 INT)",
		"CREATE TABLE `db`.`tbl2` (c1 INT)",
	}
	for _, query := range queries {
		events, data, err = g.GenDDLEvents("db", query, 0)
		c.Assert(err, check.IsNil)
		allEvents = append(allEvents, events...)
		allData.Write(data)
	}
	// write the events to a file
	filename := filepath.Join(c.MkDir(), "test-mysql-bin.000001")
	err = os.WriteFile(filename, allData.Bytes(), 0o644)
	c.Assert(err, check.IsNil)

	// all events in the file
	for _, ev := range allEvents {
		duplicate, err2 := checkIsDuplicateEvent(filename, ev)
		c.Assert(err2, check.IsNil)
		c.Assert(duplicate, check.IsTrue)
	}

	// event not in the file, because its start pos > file size
	events, _, err = g.GenDDLEvents("", "BEGIN", 0)
	c.Assert(err, check.IsNil)
	duplicate, err := checkIsDuplicateEvent(filename, events[0])
	c.Assert(err, check.IsNil)
	c.Assert(duplicate, check.IsFalse)

	// event not in the file, because event start pos < file size < event end pos, invalid
	lastEvent := allEvents[len(allEvents)-1]
	header := *lastEvent.Header // clone
	latestPos = lastEvent.Header.LogPos - lastEvent.Header.EventSize
	eventSize := lastEvent.Header.EventSize + 1 // greater event size
	dummyEv, err := event.GenDummyEvent(&header, latestPos, eventSize)
	c.Assert(err, check.IsNil)
	duplicate, err = checkIsDuplicateEvent(filename, dummyEv)
	c.Assert(err, check.ErrorMatches, ".*file size.*is between event's start pos.*")
	c.Assert(duplicate, check.IsFalse)

	// event's start pos not match any event in the file, invalid
	latestPos = lastEvent.Header.LogPos - lastEvent.Header.EventSize - 1 // start pos mismatch
	eventSize = lastEvent.Header.EventSize
	dummyEv, err = event.GenDummyEvent(&header, latestPos, eventSize)
	c.Assert(err, check.IsNil)
	duplicate, err = checkIsDuplicateEvent(filename, dummyEv)
	c.Assert(err, check.ErrorMatches, "*diff from passed-in event.*")
	c.Assert(duplicate, check.IsFalse)

	// event's start/end pos matched, but content mismatched, invalid
	latestPos = lastEvent.Header.LogPos - lastEvent.Header.EventSize
	eventSize = lastEvent.Header.EventSize
	dummyEv, err = event.GenDummyEvent(&header, latestPos, eventSize)
	c.Assert(err, check.IsNil)
	duplicate, err = checkIsDuplicateEvent(filename, dummyEv)
	c.Assert(err, check.ErrorMatches, ".*diff from passed-in event.*")
	c.Assert(duplicate, check.IsFalse)

	// file not exists, invalid
	filename += ".no-exist"
	duplicate, err = checkIsDuplicateEvent(filename, lastEvent)
	c.Assert(err, check.ErrorMatches, ".*get stat for.*")
	c.Assert(duplicate, check.IsFalse)
}

func (t *testFileUtilSuite) TestGetTxnPosGTIDsMySQL(c *check.C) {
	var (
		filename           = filepath.Join(c.MkDir(), "test-mysql-bin.000001")
		flavor             = gmysql.MySQLFlavor
		previousGTIDSetStr = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,406a3f61-690d-11e7-87c5-6c92bf46f384:123-456,686e1ab6-c47e-11e7-a42c-6c92bf46f384:234-567"
		latestGTIDStr1     = "3ccc475b-2343-11e7-be21-6c0b84d59f30:14"
		latestGTIDStr2     = "53bfca22-690d-11e7-8a62-18ded7a37b78:495"
		// 3 DDL + 10 DML
		expectedGTIDsStr1 = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-18,53bfca22-690d-11e7-8a62-18ded7a37b78:1-505,406a3f61-690d-11e7-87c5-6c92bf46f384:123-456,686e1ab6-c47e-11e7-a42c-6c92bf46f384:234-567"
		// 3 DDL + 11 DML
		expectedGTIDsStr2 = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-18,53bfca22-690d-11e7-8a62-18ded7a37b78:1-506,406a3f61-690d-11e7-87c5-6c92bf46f384:123-456,686e1ab6-c47e-11e7-a42c-6c92bf46f384:234-567"
	)

	t.testGetTxnPosGTIDs(c, filename, flavor, previousGTIDSetStr, latestGTIDStr1, latestGTIDStr2, expectedGTIDsStr1, expectedGTIDsStr2)
}

func (t *testFileUtilSuite) TestGetTxnPosGTIDMariaDB(c *check.C) {
	var (
		filename           = filepath.Join(c.MkDir(), "test-mysql-bin.000001")
		flavor             = gmysql.MariaDBFlavor
		previousGTIDSetStr = "1-11-1,2-11-2"
		latestGTIDStr1     = "1-11-1"
		latestGTIDStr2     = "2-11-2"
		// 3 DDL + 10 DML
		expectedGTIDsStr1 = "1-11-5,2-11-12"
		// 3 DDL + 11 DML
		expectedGTIDsStr2 = "1-11-5,2-11-13"
	)

	t.testGetTxnPosGTIDs(c, filename, flavor, previousGTIDSetStr, latestGTIDStr1, latestGTIDStr2, expectedGTIDsStr1, expectedGTIDsStr2)
}

func (t *testFileUtilSuite) testGetTxnPosGTIDs(c *check.C, filename, flavor, previousGTIDSetStr,
	latestGTIDStr1, latestGTIDStr2, expectedGTIDsStr1, expectedGTIDsStr2 string,
) {
	parser2 := parser.New()

	// different SIDs in GTID set
	previousGTIDSet, err := gtid.ParserGTID(flavor, previousGTIDSetStr)
	c.Assert(err, check.IsNil)
	latestGTID1, err := gtid.ParserGTID(flavor, latestGTIDStr1)
	c.Assert(err, check.IsNil)
	latestGTID2, err := gtid.ParserGTID(flavor, latestGTIDStr2)
	c.Assert(err, check.IsNil)

	g, _, baseData := genBinlogEventsWithGTIDs(c, flavor, previousGTIDSet, latestGTID1, latestGTID2)

	// expected latest pos/GTID set
	expectedPos := int64(len(baseData))
	expectedGTIDs, err := gtid.ParserGTID(flavor, expectedGTIDsStr1) // 3 DDL + 10 DML
	c.Assert(err, check.IsNil)

	// write the events to a file
	err = os.WriteFile(filename, baseData, 0o644)
	c.Assert(err, check.IsNil)

	// not extra data exists
	pos, gSet, err := getTxnPosGTIDs(context.Background(), filename, parser2)
	c.Assert(err, check.IsNil)
	c.Assert(pos, check.DeepEquals, expectedPos)
	c.Assert(gSet, check.DeepEquals, expectedGTIDs)

	// generate another transaction, DML
	var (
		tableID    uint64 = 9
		columnType        = []byte{gmysql.MYSQL_TYPE_LONG}
		eventType         = replication.UPDATE_ROWS_EVENTv2
		schema            = "db"
		table             = "tbl2"
	)
	updateRows := make([][]interface{}, 0, 2)
	updateRows = append(updateRows, []interface{}{int32(1)}, []interface{}{int32(2)})
	dmlData := []*event.DMLData{
		{
			TableID:    tableID,
			Schema:     schema,
			Table:      table,
			ColumnType: columnType,
			Rows:       updateRows,
		},
	}
	extraEvents, extraData, err := g.GenDMLEvents(eventType, dmlData, 0)
	c.Assert(err, check.IsNil)
	c.Assert(extraEvents, check.HasLen, 5) // [GTID, BEGIN, TableMap, UPDATE, XID]

	// write an incomplete event to the file
	corruptData := extraEvents[0].RawData[:len(extraEvents[0].RawData)-2]
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND, 0o644)
	c.Assert(err, check.IsNil)
	_, err = f.Write(corruptData)
	c.Assert(err, check.IsNil)
	c.Assert(f.Close(), check.IsNil)

	// check again
	pos, gSet, err = getTxnPosGTIDs(context.Background(), filename, parser2)
	c.Assert(err, check.IsNil)
	c.Assert(pos, check.DeepEquals, expectedPos)
	c.Assert(gSet, check.DeepEquals, expectedGTIDs)

	// truncate extra data
	f, err = os.OpenFile(filename, os.O_WRONLY, 0o644)
	c.Assert(err, check.IsNil)
	err = f.Truncate(expectedPos)
	c.Assert(err, check.IsNil)
	c.Assert(f.Close(), check.IsNil)

	// write an incomplete transaction with some completed events
	for i := 0; i < len(extraEvents)-1; i++ {
		f, err = os.OpenFile(filename, os.O_WRONLY|os.O_APPEND, 0o644)
		c.Assert(err, check.IsNil)
		_, err = f.Write(extraEvents[i].RawData) // write the event
		c.Assert(err, check.IsNil)
		c.Assert(f.Close(), check.IsNil)
		// check again
		pos, gSet, err = getTxnPosGTIDs(context.Background(), filename, parser2)
		c.Assert(err, check.IsNil)
		c.Assert(pos, check.DeepEquals, expectedPos)
		c.Assert(gSet, check.DeepEquals, expectedGTIDs)
	}

	// write a completed event (and a completed transaction) to the file
	f, err = os.OpenFile(filename, os.O_WRONLY|os.O_APPEND, 0o644)
	c.Assert(err, check.IsNil)
	_, err = f.Write(extraEvents[len(extraEvents)-1].RawData) // write the event
	c.Assert(err, check.IsNil)
	c.Assert(f.Close(), check.IsNil)

	// check again
	expectedPos += int64(len(extraData))
	expectedGTIDs, err = gtid.ParserGTID(flavor, expectedGTIDsStr2) // 3 DDL + 11 DML
	c.Assert(err, check.IsNil)
	pos, gSet, err = getTxnPosGTIDs(context.Background(), filename, parser2)
	c.Assert(err, check.IsNil)
	c.Assert(pos, check.DeepEquals, expectedPos)
	c.Assert(gSet, check.DeepEquals, expectedGTIDs)
}

func (t *testFileUtilSuite) TestGetTxnPosGTIDsNoGTID(c *check.C) {
	// generate some events but without GTID enabled
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
		}
		latestPos uint32 = 4
		filename         = filepath.Join(c.MkDir(), "test-mysql-bin.000001")
	)

	// FormatDescriptionEvent
	formatDescEv, err := event.GenFormatDescriptionEvent(header, latestPos)
	c.Assert(err, check.IsNil)
	latestPos = formatDescEv.Header.LogPos

	// QueryEvent, DDL
	queryEv, err := event.GenQueryEvent(header, latestPos, 0, 0, 0, nil, []byte("db"), []byte("CREATE DATABASE db"))
	c.Assert(err, check.IsNil)
	latestPos = queryEv.Header.LogPos

	// write events to the file
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0o644)
	c.Assert(err, check.IsNil)
	_, err = f.Write(replication.BinLogFileHeader)
	c.Assert(err, check.IsNil)
	_, err = f.Write(formatDescEv.RawData)
	c.Assert(err, check.IsNil)
	_, err = f.Write(queryEv.RawData)
	c.Assert(err, check.IsNil)
	c.Assert(f.Close(), check.IsNil)

	// check latest pos/GTID set
	pos, gSet, err := getTxnPosGTIDs(context.Background(), filename, parser.New())
	c.Assert(err, check.IsNil)
	c.Assert(pos, check.Equals, int64(latestPos))
	c.Assert(gSet, check.IsNil) // GTID not enabled
}

func (t *testFileUtilSuite) TestGetTxnPosGTIDsIllegalGTIDMySQL(c *check.C) {
	// generate some events with GTID enabled, but without PreviousGTIDEvent
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
		}
		latestPos uint32 = 4
	)

	// GTID event
	gtidEv, err := event.GenGTIDEvent(header, latestPos, 0, "3ccc475b-2343-11e7-be21-6c0b84d59f30", 14, 10, 10)
	c.Assert(err, check.IsNil)

	t.testGetTxnPosGTIDsIllegalGTID(c, gtidEv, ".*should have a PreviousGTIDsEvent before the GTIDEvent.*")
}

func (t *testFileUtilSuite) TestGetTxnPosGTIDsIllegalGTIDMairaDB(c *check.C) {
	// generate some events with GTID enabled, but without MariaDBGTIDEvent
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
		}
		latestPos uint32 = 4
	)

	// GTID event
	mariaDBGTIDEv, err := event.GenMariaDBGTIDEvent(header, latestPos, 10, 10)
	c.Assert(err, check.IsNil)

	t.testGetTxnPosGTIDsIllegalGTID(c, mariaDBGTIDEv, ".*should have a MariadbGTIDListEvent before the MariadbGTIDEvent.*")
}

func (t *testFileUtilSuite) testGetTxnPosGTIDsIllegalGTID(c *check.C, gtidEv *replication.BinlogEvent, errRegStr string) {
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
		}
		latestPos uint32 = 4
		filename         = filepath.Join(c.MkDir(), "test-mysql-bin.000001")
	)

	// FormatDescriptionEvent
	formatDescEv, err := event.GenFormatDescriptionEvent(header, latestPos)
	c.Assert(err, check.IsNil)

	// write events to the file
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0o644)
	c.Assert(err, check.IsNil)
	_, err = f.Write(replication.BinLogFileHeader)
	c.Assert(err, check.IsNil)
	_, err = f.Write(formatDescEv.RawData)
	c.Assert(err, check.IsNil)
	_, err = f.Write(gtidEv.RawData)
	c.Assert(err, check.IsNil)
	c.Assert(f.Close(), check.IsNil)

	// check latest pos/GTID set
	pos, gSet, err := getTxnPosGTIDs(context.Background(), filename, parser.New())
	c.Assert(err, check.ErrorMatches, errRegStr)
	c.Assert(pos, check.Equals, int64(0))
	c.Assert(gSet, check.IsNil)
}

func (t *testFileUtilSuite) TestDontTruncateOnlyHeader(c *check.C) {
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
		}
		latestPos              = 4
		previousGSetMySQL, _   = gtid.ParserGTID("mysql", "3ccc475b-2343-11e7-be21-6c0b84d59f30:14")
		previousGSetMariaDB, _ = gtid.ParserGTID("mariadb", "0-1-5")
	)

	formatDescEv, err := event.GenFormatDescriptionEvent(header, uint32(latestPos))
	c.Assert(err, check.IsNil)
	latestPos += len(formatDescEv.RawData)

	mysqlGTIDev, _ := event.GenPreviousGTIDsEvent(header, uint32(latestPos), previousGSetMySQL)
	mariaDBGTIDev, _ := event.GenMariaDBGTIDListEvent(header, uint32(latestPos), previousGSetMariaDB)

	t.testDontTruncate(c, []*replication.BinlogEvent{formatDescEv, mysqlGTIDev})
	t.testDontTruncate(c, []*replication.BinlogEvent{formatDescEv, mariaDBGTIDev})
}

func (t *testFileUtilSuite) testDontTruncate(c *check.C, events []*replication.BinlogEvent) {
	var (
		filename = filepath.Join(c.MkDir(), "dont-truncate.000001")
		parser2  = parser.New()
	)

	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0o644)
	c.Assert(err, check.IsNil)

	_, err = f.Write(replication.BinLogFileHeader)
	c.Assert(err, check.IsNil)

	for _, ev := range events {
		_, err = f.Write(ev.RawData)
		c.Assert(err, check.IsNil)

		stat, _ := f.Stat()
		pos, _, err := getTxnPosGTIDs(context.Background(), filename, parser2)
		c.Assert(err, check.IsNil)
		c.Assert(pos, check.Equals, stat.Size())
	}
}
