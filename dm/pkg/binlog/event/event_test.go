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

// binlog events generator for MySQL used to generate some binlog events for tests.
// Readability takes precedence over performance.

package event

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	. "github.com/pingcap/check"

	"github.com/pingcap/dm/pkg/gtid"
)

var _ = Suite(&testEventSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testEventSuite struct{}

func (t *testEventSuite) TestGenEventHeader(c *C) {
	var (
		latestPos uint32 = 4
		header           = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			EventType: replication.FORMAT_DESCRIPTION_EVENT,
			ServerID:  11,
			Flags:     0x01,
			LogPos:    latestPos + 109,
			EventSize: 109, // current binlog version, 109,
		}
	)

	data, err := GenEventHeader(header)
	c.Assert(err, IsNil)
	c.Assert(uint8(len(data)), Equals, eventHeaderLen)

	header2 := &replication.EventHeader{}
	err = header2.Decode(data)
	c.Assert(err, IsNil)
	verifyHeader(c, header2, header, header.EventType, latestPos, header.EventSize)
}

func verifyHeader(c *C, obtained, excepted *replication.EventHeader, eventType replication.EventType, latestPos, eventSize uint32) {
	c.Assert(obtained.Timestamp, Equals, excepted.Timestamp)
	c.Assert(obtained.ServerID, Equals, excepted.ServerID)
	c.Assert(obtained.Flags, Equals, excepted.Flags)
	c.Assert(obtained.EventType, Equals, eventType)
	c.Assert(obtained.EventSize, Equals, eventSize)
	c.Assert(obtained.LogPos, Equals, eventSize+latestPos)
}

func (t *testEventSuite) TestGenFormatDescriptionEvent(c *C) {
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
			Flags:     0x01,
		}
		latestPos uint32 = 4
	)
	ev, err := GenFormatDescriptionEvent(header, latestPos)
	c.Assert(err, IsNil)

	// verify the header
	verifyHeader(c, ev.Header, header, replication.FORMAT_DESCRIPTION_EVENT, latestPos, uint32(len(ev.RawData)))

	// some fields of FormatDescriptionEvent are a little hard to test, so we try to parse a binlog file.
	dir := c.MkDir()
	name := filepath.Join(dir, "mysql-bin-test.000001")
	f, err := os.Create(name)
	c.Assert(err, IsNil)
	defer f.Close()

	// write a binlog file header
	_, err = f.Write(replication.BinLogFileHeader)
	c.Assert(err, IsNil)

	// write the FormatDescriptionEvent
	_, err = f.Write(ev.RawData)
	c.Assert(err, IsNil)

	// should only receive one FormatDescriptionEvent
	onEventFunc := func(e *replication.BinlogEvent) error {
		c.Assert(e.Header, DeepEquals, ev.Header)
		c.Assert(e.Event, DeepEquals, ev.Event)
		c.Assert(e.RawData, DeepEquals, ev.RawData)
		return nil
	}

	parser2 := replication.NewBinlogParser()
	parser2.SetVerifyChecksum(true)
	err = parser2.ParseFile(name, 0, onEventFunc)
	c.Assert(err, IsNil)
}

func (t *testEventSuite) TestGenRotateEvent(c *C) {
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
			Flags:     0x01,
		}
		latestPos   uint32 = 4
		nextLogName []byte // nil
		position    uint64 = 123
	)

	// empty nextLogName, invalid
	rotateEv, err := GenRotateEvent(header, latestPos, nextLogName, position)
	c.Assert(err, NotNil)
	c.Assert(rotateEv, IsNil)

	// valid nextLogName
	nextLogName = []byte("mysql-bin.000010")
	rotateEv, err = GenRotateEvent(header, latestPos, nextLogName, position)
	c.Assert(err, IsNil)
	c.Assert(rotateEv, NotNil)

	// verify the header
	verifyHeader(c, rotateEv.Header, header, replication.ROTATE_EVENT, latestPos, uint32(len(rotateEv.RawData)))

	// verify the body
	rotateEvBody, ok := rotateEv.Event.(*replication.RotateEvent)
	c.Assert(ok, IsTrue)
	c.Assert(rotateEvBody, NotNil)
	c.Assert(rotateEvBody.NextLogName, DeepEquals, nextLogName)
	c.Assert(rotateEvBody.Position, Equals, position)
}

func (t *testEventSuite) TestGenPreviousGTIDsEvent(c *C) {
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
			Flags:     0x01,
		}
		latestPos uint32 = 4
		str              = "9f61c5f9-1eef-11e9-b6cf-0242ac140003:1-5"
	)

	// always needing a FormatDescriptionEvent in the binlog file.
	formatDescEv, err := GenFormatDescriptionEvent(header, latestPos)
	c.Assert(err, IsNil)

	// update latestPos
	latestPos = formatDescEv.Header.LogPos

	// generate a PreviousGTIDsEvent
	gSet, err := gtid.ParserGTID(gmysql.MySQLFlavor, str)
	c.Assert(err, IsNil)

	previousGTIDsEv, err := GenPreviousGTIDsEvent(header, latestPos, gSet)
	c.Assert(err, IsNil)

	dir := c.MkDir()
	name1 := filepath.Join(dir, "mysql-bin-test.000001")
	f1, err := os.Create(name1)
	c.Assert(err, IsNil)
	defer f1.Close()

	// write a binlog file header
	_, err = f1.Write(replication.BinLogFileHeader)
	c.Assert(err, IsNil)

	// write a FormatDescriptionEvent event
	_, err = f1.Write(formatDescEv.RawData)
	c.Assert(err, IsNil)

	// write the PreviousGTIDsEvent
	_, err = f1.Write(previousGTIDsEv.RawData)
	c.Assert(err, IsNil)

	count := 0
	onEventFunc := func(e *replication.BinlogEvent) error {
		count++
		switch count {
		case 1: // FormatDescriptionEvent
			c.Assert(e.Header, DeepEquals, formatDescEv.Header)
			c.Assert(e.Event, DeepEquals, formatDescEv.Event)
			c.Assert(e.RawData, DeepEquals, formatDescEv.RawData)
		case 2: // PreviousGTIDsEvent
			c.Assert(e.Header, DeepEquals, previousGTIDsEv.Header)
			c.Assert(e.Event, DeepEquals, previousGTIDsEv.Event)
			c.Assert(e.RawData, DeepEquals, previousGTIDsEv.RawData)
		default:
			c.Fatalf("too many binlog events got, current is %+v", e.Header)
		}
		return nil
	}

	parser2 := replication.NewBinlogParser()
	parser2.SetVerifyChecksum(true)
	err = parser2.ParseFile(name1, 0, onEventFunc)
	c.Assert(err, IsNil)

	// multi GTID
	str = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,406a3f61-690d-11e7-87c5-6c92bf46f384:1-94321383,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,686e1ab6-c47e-11e7-a42c-6c92bf46f384:1-34981190,03fc0263-28c7-11e7-a653-6c0b84d59f30:1-7041423,05474d3c-28c7-11e7-8352-203db246dd3d:1-170,10b039fc-c843-11e7-8f6a-1866daf8d810:1-308290454"
	gSet, err = gtid.ParserGTID(gmysql.MySQLFlavor, str)
	c.Assert(err, IsNil)

	previousGTIDsEv, err = GenPreviousGTIDsEvent(header, latestPos, gSet)
	c.Assert(err, IsNil)

	// write another file
	name2 := filepath.Join(dir, "mysql-bin-test.000002")
	f2, err := os.Create(name2)
	c.Assert(err, IsNil)
	defer f2.Close()

	// write a binlog file header
	_, err = f2.Write(replication.BinLogFileHeader)
	c.Assert(err, IsNil)

	// write a FormatDescriptionEvent event
	_, err = f2.Write(formatDescEv.RawData)
	c.Assert(err, IsNil)

	// write the PreviousGTIDsEvent
	_, err = f2.Write(previousGTIDsEv.RawData)
	c.Assert(err, IsNil)

	count = 0 // reset count
	err = parser2.ParseFile(name2, 0, onEventFunc)
	c.Assert(err, IsNil)
}

func (t *testEventSuite) TestGenGTIDEvent(c *C) {
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
			Flags:     0x01,
		}
		latestPos     uint32 = 4
		gtidFlags            = GTIDFlagsCommitYes
		prevGTIDsStr         = "9f61c5f9-1eef-11e9-b6cf-0242ac140003:1-5"
		uuid                 = "9f61c5f9-1eef-11e9-b6cf-0242ac140003"
		gno           int64  = 6
		lastCommitted int64
	)
	sid, err := ParseSID(uuid)
	c.Assert(err, IsNil)

	// always needing a FormatDescriptionEvent in the binlog file.
	formatDescEv, err := GenFormatDescriptionEvent(header, latestPos)
	c.Assert(err, IsNil)
	latestPos = formatDescEv.Header.LogPos // update latestPos

	// also needing a PreviousGTIDsEvent after FormatDescriptionEvent
	gSet, err := gtid.ParserGTID(gmysql.MySQLFlavor, prevGTIDsStr)
	c.Assert(err, IsNil)
	previousGTIDsEv, err := GenPreviousGTIDsEvent(header, latestPos, gSet)
	c.Assert(err, IsNil)
	latestPos = previousGTIDsEv.Header.LogPos // update latestPos

	gtidEv, err := GenGTIDEvent(header, latestPos, gtidFlags, uuid, gno, lastCommitted, lastCommitted+1)
	c.Assert(err, IsNil)

	// verify the header
	verifyHeader(c, gtidEv.Header, header, replication.GTID_EVENT, latestPos, uint32(len(gtidEv.RawData)))

	// verify the body
	gtidEvBody, ok := gtidEv.Event.(*replication.GTIDEvent)
	c.Assert(ok, IsTrue)
	c.Assert(gtidEvBody, NotNil)
	c.Assert(gtidEvBody.CommitFlag, Equals, gtidFlags)
	c.Assert(gtidEvBody.SID, DeepEquals, sid.Bytes())
	c.Assert(gtidEvBody.GNO, Equals, gno)
	c.Assert(gtidEvBody.LastCommitted, Equals, lastCommitted)
	c.Assert(gtidEvBody.SequenceNumber, Equals, lastCommitted+1)

	// write a binlog file, then try to parse it
	dir := c.MkDir()
	name := filepath.Join(dir, "mysql-bin-test.000001")
	f, err := os.Create(name)
	c.Assert(err, IsNil)
	defer f.Close()

	// write a binlog file.
	_, err = f.Write(replication.BinLogFileHeader)
	c.Assert(err, IsNil)
	_, err = f.Write(formatDescEv.RawData)
	c.Assert(err, IsNil)
	_, err = f.Write(previousGTIDsEv.RawData)
	c.Assert(err, IsNil)

	// write GTIDEvent.
	_, err = f.Write(gtidEv.RawData)
	c.Assert(err, IsNil)

	count := 0
	onEventFunc := func(e *replication.BinlogEvent) error {
		count++
		switch count {
		case 1: // FormatDescriptionEvent
			c.Assert(e.Header, DeepEquals, formatDescEv.Header)
			c.Assert(e.Event, DeepEquals, formatDescEv.Event)
			c.Assert(e.RawData, DeepEquals, formatDescEv.RawData)
		case 2: // PreviousGTIDsEvent
			c.Assert(e.Header, DeepEquals, previousGTIDsEv.Header)
			c.Assert(e.Event, DeepEquals, previousGTIDsEv.Event)
			c.Assert(e.RawData, DeepEquals, previousGTIDsEv.RawData)
		case 3: // GTIDEvent
			c.Assert(e.Header.EventType, Equals, replication.GTID_EVENT)
			c.Assert(e.RawData, DeepEquals, gtidEv.RawData)
		default:
			c.Fatalf("too many binlog events got, current is %+v", e.Header)
		}
		return nil
	}
	parser2 := replication.NewBinlogParser()
	parser2.SetVerifyChecksum(true)
	err = parser2.ParseFile(name, 0, onEventFunc)
	c.Assert(err, IsNil)
}

func (t *testEventSuite) TestGenQueryEvent(c *C) {
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
			Flags:     0x01,
		}
		latestPos     uint32 = 4
		slaveProxyID  uint32 = 2
		executionTime uint32 = 12
		errorCode     uint16 = 13
		statusVars    []byte // nil
		schema        []byte // nil
		query         []byte // nil
	)

	// empty query, invalid
	queryEv, err := GenQueryEvent(header, latestPos, slaveProxyID, executionTime, errorCode, statusVars, schema, query)
	c.Assert(err, NotNil)
	c.Assert(queryEv, IsNil)

	// valid query
	query = []byte("BEGIN")
	queryEv, err = GenQueryEvent(header, latestPos, slaveProxyID, executionTime, errorCode, statusVars, schema, query)
	c.Assert(err, IsNil)
	c.Assert(queryEv, NotNil)

	// verify the header
	verifyHeader(c, queryEv.Header, header, replication.QUERY_EVENT, latestPos, uint32(len(queryEv.RawData)))

	// verify the body
	queryEvBody, ok := queryEv.Event.(*replication.QueryEvent)
	c.Assert(ok, IsTrue)
	c.Assert(queryEvBody, NotNil)
	c.Assert(queryEvBody.SlaveProxyID, Equals, slaveProxyID)
	c.Assert(queryEvBody.ExecutionTime, Equals, executionTime)
	c.Assert(queryEvBody.ErrorCode, Equals, errorCode)
	c.Assert(queryEvBody.StatusVars, DeepEquals, []byte{})
	c.Assert(queryEvBody.Schema, DeepEquals, []byte{})
	c.Assert(queryEvBody.Query, DeepEquals, query)

	// non-empty schema
	schema = []byte("db")
	query = []byte("CREATE TABLE db.tbl (c1 int)")
	queryEv, err = GenQueryEvent(header, latestPos, slaveProxyID, executionTime, errorCode, statusVars, schema, query)
	c.Assert(err, IsNil)
	c.Assert(queryEv, NotNil)

	// verify the body
	queryEvBody, ok = queryEv.Event.(*replication.QueryEvent)
	c.Assert(ok, IsTrue)
	c.Assert(queryEvBody, NotNil)
	c.Assert(queryEvBody.Schema, DeepEquals, schema)
	c.Assert(queryEvBody.Query, DeepEquals, query)

	// non-empty statusVars
	statusVars = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x20, 0x00, 0xa0, 0x55, 0x00, 0x00, 0x00, 0x00, 0x06, 0x03, 0x73, 0x74, 0x64, 0x04, 0x21, 0x00, 0x21, 0x00, 0x08, 0x00, 0x0c, 0x01, 0x73, 0x68, 0x61, 0x72, 0x64, 0x5f, 0x64, 0x62, 0x5f, 0x31, 0x00}
	queryEv, err = GenQueryEvent(header, latestPos, slaveProxyID, executionTime, errorCode, statusVars, schema, query)
	c.Assert(err, IsNil)
	c.Assert(queryEv, NotNil)

	// verify the body
	queryEvBody, ok = queryEv.Event.(*replication.QueryEvent)
	c.Assert(ok, IsTrue)
	c.Assert(queryEvBody, NotNil)
	c.Assert(queryEvBody.StatusVars, DeepEquals, statusVars)
}

func (t *testEventSuite) TestGenTableMapEvent(c *C) {
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
			Flags:     0x01,
		}
		latestPos  uint32 = 123
		tableID    uint64 = 108
		schema     []byte // nil
		table      []byte // nil
		columnType []byte // nil
	)

	// invalid schema, table and columnType
	tableMapEv, err := GenTableMapEvent(header, latestPos, tableID, schema, table, columnType)
	c.Assert(err, NotNil)
	c.Assert(tableMapEv, IsNil)

	// valid schema, invalid table and columnType
	schema = []byte("db")
	tableMapEv, err = GenTableMapEvent(header, latestPos, tableID, schema, table, columnType)
	c.Assert(err, NotNil)
	c.Assert(tableMapEv, IsNil)

	// valid schema and table, invalid columnType
	table = []byte("tbl")
	tableMapEv, err = GenTableMapEvent(header, latestPos, tableID, schema, table, columnType)
	c.Assert(err, NotNil)
	c.Assert(tableMapEv, IsNil)

	// all valid
	columnType = []byte{gmysql.MYSQL_TYPE_LONG}
	tableMapEv, err = GenTableMapEvent(header, latestPos, tableID, schema, table, columnType)
	c.Assert(err, IsNil)
	c.Assert(tableMapEv, NotNil)

	// verify the header
	verifyHeader(c, tableMapEv.Header, header, replication.TABLE_MAP_EVENT, latestPos, uint32(len(tableMapEv.RawData)))

	// verify the body
	tableMapEvBody, ok := tableMapEv.Event.(*replication.TableMapEvent)
	c.Assert(ok, IsTrue)
	c.Assert(tableMapEvBody, NotNil)
	c.Assert(tableMapEvBody.TableID, Equals, tableID)
	c.Assert(tableMapEvBody.Flags, Equals, tableMapFlags)
	c.Assert(tableMapEvBody.Schema, DeepEquals, schema)
	c.Assert(tableMapEvBody.Table, DeepEquals, table)
	c.Assert(tableMapEvBody.ColumnCount, Equals, uint64(len(columnType)))
	c.Assert(tableMapEvBody.ColumnType, DeepEquals, columnType)

	// multi column type
	columnType = []byte{gmysql.MYSQL_TYPE_STRING, gmysql.MYSQL_TYPE_NEWDECIMAL, gmysql.MYSQL_TYPE_VAR_STRING, gmysql.MYSQL_TYPE_BLOB}
	tableMapEv, err = GenTableMapEvent(header, latestPos, tableID, schema, table, columnType)
	c.Assert(err, IsNil)
	c.Assert(tableMapEv, NotNil)

	// verify the body
	tableMapEvBody, ok = tableMapEv.Event.(*replication.TableMapEvent)
	c.Assert(ok, IsTrue)
	c.Assert(tableMapEvBody, NotNil)
	c.Assert(tableMapEvBody.ColumnCount, Equals, uint64(len(columnType)))
	c.Assert(tableMapEvBody.ColumnType, DeepEquals, columnType)

	// unsupported column type
	columnType = []byte{gmysql.MYSQL_TYPE_NEWDATE}
	tableMapEv, err = GenTableMapEvent(header, latestPos, tableID, schema, table, columnType)
	c.Assert(err, NotNil)
	c.Assert(tableMapEv, IsNil)
}

func (t *testEventSuite) TestGenRowsEvent(c *C) {
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
			Flags:     0x01,
		}
		latestPos  uint32 = 123
		tableID    uint64 = 108
		eventType         = replication.TABLE_MAP_EVENT
		rowsFlag          = RowFlagsEndOfStatement
		rows       [][]interface{}
		columnType []byte // nil
	)

	// invalid eventType, rows and columnType
	rowsEv, err := GenRowsEvent(header, latestPos, eventType, tableID, rowsFlag, rows, columnType, nil)
	c.Assert(err, NotNil)
	c.Assert(rowsEv, IsNil)

	// valid eventType, invalid rows and columnType
	eventType = replication.WRITE_ROWS_EVENTv0
	rowsEv, err = GenRowsEvent(header, latestPos, eventType, tableID, rowsFlag, rows, columnType, nil)
	c.Assert(err, NotNil)
	c.Assert(rowsEv, IsNil)

	// valid eventType and rows, invalid columnType
	row := []interface{}{int32(1)}
	rows = append(rows, row)
	rowsEv, err = GenRowsEvent(header, latestPos, eventType, tableID, rowsFlag, rows, columnType, nil)
	c.Assert(err, NotNil)
	c.Assert(rowsEv, IsNil)

	// all valid
	columnType = []byte{gmysql.MYSQL_TYPE_LONG}
	rowsEv, err = GenRowsEvent(header, latestPos, eventType, tableID, rowsFlag, rows, columnType, nil)
	c.Assert(err, IsNil)
	c.Assert(rowsEv, NotNil)

	// verify the header
	verifyHeader(c, rowsEv.Header, header, eventType, latestPos, uint32(len(rowsEv.RawData)))

	// verify the body
	rowsEvBody, ok := rowsEv.Event.(*replication.RowsEvent)
	c.Assert(ok, IsTrue)
	c.Assert(rowsEvBody, NotNil)
	c.Assert(rowsEvBody.Flags, Equals, rowsFlag)
	c.Assert(rowsEvBody.TableID, Equals, tableID)
	c.Assert(rowsEvBody.Table.TableID, Equals, tableID)
	c.Assert(rowsEvBody.ColumnCount, Equals, uint64(len(rows[0])))
	c.Assert(rowsEvBody.Version, Equals, 0) // WRITE_ROWS_EVENTv0
	c.Assert(rowsEvBody.ExtraData, IsNil)
	c.Assert(rowsEvBody.Rows, DeepEquals, rows)

	// multi rows, with different length, invalid
	rows = append(rows, []interface{}{int32(1), int32(2)})
	rowsEv, err = GenRowsEvent(header, latestPos, eventType, tableID, rowsFlag, rows, columnType, nil)
	c.Assert(err, NotNil)
	c.Assert(rowsEv, IsNil)

	// multi rows, multi columns, valid
	rows = make([][]interface{}, 0, 2)
	rows = append(rows, []interface{}{int32(1), int32(2)})
	rows = append(rows, []interface{}{int32(3), int32(4)})
	columnType = []byte{gmysql.MYSQL_TYPE_LONG, gmysql.MYSQL_TYPE_LONG}
	rowsEv, err = GenRowsEvent(header, latestPos, eventType, tableID, rowsFlag, rows, columnType, nil)
	c.Assert(err, IsNil)
	c.Assert(rowsEv, NotNil)
	// verify the body
	rowsEvBody, ok = rowsEv.Event.(*replication.RowsEvent)
	c.Assert(ok, IsTrue)
	c.Assert(rowsEvBody, NotNil)
	c.Assert(rowsEvBody.ColumnCount, Equals, uint64(len(rows[0])))
	c.Assert(rowsEvBody.Rows, DeepEquals, rows)

	// all valid event-type
	evTypes := []replication.EventType{
		replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2,
	}
	for _, eventType = range evTypes {
		rowsEv, err = GenRowsEvent(header, latestPos, eventType, tableID, rowsFlag, rows, columnType, nil)
		c.Assert(err, IsNil)
		c.Assert(rowsEv, NotNil)
		c.Assert(rowsEv.Header.EventType, Equals, eventType)
	}

	// more column types
	rows = make([][]interface{}, 0, 1)
	rows = append(rows, []interface{}{
		int32(1), int8(2), int16(3), int32(4), int64(5),
		float32(1.23), float64(4.56), "string with type STRING",
	})
	columnType = []byte{
		gmysql.MYSQL_TYPE_LONG, gmysql.MYSQL_TYPE_TINY, gmysql.MYSQL_TYPE_SHORT, gmysql.MYSQL_TYPE_INT24, gmysql.MYSQL_TYPE_LONGLONG,
		gmysql.MYSQL_TYPE_FLOAT, gmysql.MYSQL_TYPE_DOUBLE, gmysql.MYSQL_TYPE_STRING,
	}
	rowsEv, err = GenRowsEvent(header, latestPos, eventType, tableID, rowsFlag, rows, columnType, nil)
	c.Assert(err, IsNil)
	c.Assert(rowsEv, NotNil)
	// verify the body
	rowsEvBody, ok = rowsEv.Event.(*replication.RowsEvent)
	c.Assert(ok, IsTrue)
	c.Assert(rowsEvBody, NotNil)
	c.Assert(rowsEvBody.ColumnCount, Equals, uint64(len(rows[0])))
	c.Assert(rowsEvBody.Rows, DeepEquals, rows)

	// column type mismatch
	rows[0][0] = int8(1)
	rowsEv, err = GenRowsEvent(header, latestPos, eventType, tableID, rowsFlag, rows, columnType, nil)
	c.Assert(err, NotNil)
	c.Assert(rowsEv, IsNil)

	// NotSupported column type
	rows = make([][]interface{}, 0, 1)
	rows = append(rows, []interface{}{int32(1)})
	unsupportedTypes := []byte{
		gmysql.MYSQL_TYPE_VARCHAR, gmysql.MYSQL_TYPE_VAR_STRING,
		gmysql.MYSQL_TYPE_NEWDECIMAL, gmysql.MYSQL_TYPE_BIT,
		gmysql.MYSQL_TYPE_TIMESTAMP, gmysql.MYSQL_TYPE_TIMESTAMP2,
		gmysql.MYSQL_TYPE_DATETIME, gmysql.MYSQL_TYPE_DATETIME2,
		gmysql.MYSQL_TYPE_TIME, gmysql.MYSQL_TYPE_TIME2,
		gmysql.MYSQL_TYPE_YEAR, gmysql.MYSQL_TYPE_ENUM, gmysql.MYSQL_TYPE_SET,
		gmysql.MYSQL_TYPE_BLOB, gmysql.MYSQL_TYPE_JSON, gmysql.MYSQL_TYPE_GEOMETRY,
	}
	for i := range unsupportedTypes {
		columnType = unsupportedTypes[i : i+1]
		rowsEv, err = GenRowsEvent(header, latestPos, eventType, tableID, rowsFlag, rows, columnType, nil)
		c.Assert(err, NotNil)
		c.Assert(strings.Contains(err.Error(), "not supported"), IsTrue)
		c.Assert(rowsEv, IsNil)
	}
}

func (t *testEventSuite) TestGenXIDEvent(c *C) {
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
			Flags:     0x01,
		}
		latestPos uint32 = 4
		xid       uint64 = 123
	)

	xidEv, err := GenXIDEvent(header, latestPos, xid)
	c.Assert(err, IsNil)
	c.Assert(xidEv, NotNil)

	// verify the header
	verifyHeader(c, xidEv.Header, header, replication.XID_EVENT, latestPos, uint32(len(xidEv.RawData)))

	// verify the body
	xidEvBody, ok := xidEv.Event.(*replication.XIDEvent)
	c.Assert(ok, IsTrue)
	c.Assert(xidEvBody, NotNil)
	c.Assert(xidEvBody.XID, Equals, xid)
}

func (t *testEventSuite) TestGenMariaDBGTIDListEvent(c *C) {
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
			Flags:     0x01,
		}
		latestPos uint32   = 4
		gSet      gtid.Set // invalid
	)

	// invalid gSet
	gtidListEv, err := GenMariaDBGTIDListEvent(header, latestPos, gSet)
	c.Assert(err, NotNil)
	c.Assert(gtidListEv, IsNil)

	// valid gSet with single GTID
	gSet, err = gtid.ParserGTID(gmysql.MariaDBFlavor, "1-2-3")
	c.Assert(err, IsNil)
	c.Assert(gSet, NotNil)
	mGSet, ok := gSet.Origin().(*gmysql.MariadbGTIDSet)
	c.Assert(ok, IsTrue)
	c.Assert(mGSet, NotNil)

	gtidListEv, err = GenMariaDBGTIDListEvent(header, latestPos, gSet)
	c.Assert(err, IsNil)
	c.Assert(gtidListEv, NotNil)

	// verify the header
	verifyHeader(c, gtidListEv.Header, header, replication.MARIADB_GTID_LIST_EVENT, latestPos, uint32(len(gtidListEv.RawData)))

	// verify the body
	gtidListEvBody, ok := gtidListEv.Event.(*replication.MariadbGTIDListEvent)
	c.Assert(ok, IsTrue)
	c.Assert(gtidListEvBody, NotNil)
	c.Assert(gtidListEvBody.GTIDs, HasLen, 1)
	c.Assert(gtidListEvBody.GTIDs[0], DeepEquals, *mGSet.Sets[gtidListEvBody.GTIDs[0].DomainID])

	// valid gSet with multi GTIDs
	gSet, err = gtid.ParserGTID(gmysql.MariaDBFlavor, "1-2-12,2-2-3,3-3-8,4-4-4")
	c.Assert(err, IsNil)
	c.Assert(gSet, NotNil)
	mGSet, ok = gSet.Origin().(*gmysql.MariadbGTIDSet)
	c.Assert(ok, IsTrue)
	c.Assert(mGSet, NotNil)

	gtidListEv, err = GenMariaDBGTIDListEvent(header, latestPos, gSet)
	c.Assert(err, IsNil)
	c.Assert(gtidListEv, NotNil)

	// verify the body
	gtidListEvBody, ok = gtidListEv.Event.(*replication.MariadbGTIDListEvent)
	c.Assert(ok, IsTrue)
	c.Assert(gtidListEvBody, NotNil)
	c.Assert(gtidListEvBody.GTIDs, HasLen, 4)
	for _, mGTID := range gtidListEvBody.GTIDs {
		mGTID2, ok := mGSet.Sets[mGTID.DomainID]
		c.Assert(ok, IsTrue)
		c.Assert(mGTID, DeepEquals, *mGTID2)
	}
}

func (t *testEventSuite) TestGenMariaDBGTIDEvent(c *C) {
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
			Flags:     0x01,
		}
		latestPos uint32 = 4
		seqNum    uint64 = 123
		domainID  uint32 = 456
	)

	gtidEv, err := GenMariaDBGTIDEvent(header, latestPos, seqNum, domainID)
	c.Assert(err, IsNil)
	c.Assert(gtidEv, NotNil)

	// verify the header
	verifyHeader(c, gtidEv.Header, header, replication.MARIADB_GTID_EVENT, latestPos, uint32(len(gtidEv.RawData)))

	// verify the body
	gtidEvBody, ok := gtidEv.Event.(*replication.MariadbGTIDEvent)
	c.Assert(ok, IsTrue)
	c.Assert(gtidEvBody, NotNil)
	c.Assert(gtidEvBody.GTID.SequenceNumber, Equals, seqNum)
	c.Assert(gtidEvBody.GTID.DomainID, Equals, domainID)
}

func (t *testEventSuite) TestGenDummyEvent(c *C) {
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
			Flags:     replication.LOG_EVENT_THREAD_SPECIFIC_F | replication.LOG_EVENT_BINLOG_IN_USE_F,
		}
		expectedHeader = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
			Flags:     replication.LOG_EVENT_SUPPRESS_USE_F | replication.LOG_EVENT_RELAY_LOG_F | replication.LOG_EVENT_BINLOG_IN_USE_F,
		}
		latestPos uint32 = 4
	)

	// too small event size
	eventSize := MinUserVarEventLen - 1
	userVarEv, err := GenDummyEvent(header, latestPos, eventSize)
	c.Assert(err, ErrorMatches, ".*is too small.*")
	c.Assert(userVarEv, IsNil)

	// minimum event size, USER_VAR_EVENT with name-length==1
	eventSize = MinUserVarEventLen
	userVarEv, err = GenDummyEvent(header, latestPos, eventSize)
	c.Assert(err, IsNil)
	c.Assert(userVarEv, NotNil)
	// verify the header
	verifyHeader(c, userVarEv.Header, expectedHeader, replication.USER_VAR_EVENT, latestPos, uint32(len(userVarEv.RawData)))
	// verify the body
	nameStart := uint32(eventHeaderLen + 4)
	nameEnd := eventSize - 1 - crc32Len
	nameLen := nameEnd - nameStart
	c.Assert(nameLen, Equals, uint32(1)) // name-length==1
	c.Assert(userVarEv.RawData[nameStart:nameEnd], DeepEquals, dummyUserVarName[:nameLen])
	c.Assert(userVarEv.RawData[nameEnd:nameEnd+1], DeepEquals, []byte{0x01}) // is-null always 1

	// minimum, .., equal dummy query, longer, ...
	dummyQueryLen := uint32(len(dummyQuery))
	eventSizeList := []uint32{
		MinQueryEventLen, MinQueryEventLen + 5,
		MinQueryEventLen + dummyQueryLen - 1, MinQueryEventLen + dummyQueryLen, MinQueryEventLen + dummyQueryLen + 10,
	}
	for _, eventSize = range eventSizeList {
		queryEv, err := GenDummyEvent(header, latestPos, eventSize)
		c.Assert(err, IsNil)
		c.Assert(queryEv, NotNil)
		// verify the header
		verifyHeader(c, queryEv.Header, expectedHeader, replication.QUERY_EVENT, latestPos, uint32(len(queryEv.RawData)))
		// verify the body
		queryEvBody, ok := queryEv.Event.(*replication.QueryEvent)
		c.Assert(ok, IsTrue)
		c.Assert(queryEvBody, NotNil)
		c.Assert(queryEvBody.SlaveProxyID, Equals, uint32(0))
		c.Assert(queryEvBody.ExecutionTime, Equals, uint32(0))
		c.Assert(queryEvBody.ErrorCode, Equals, uint16(0))
		c.Assert(queryEvBody.StatusVars, DeepEquals, []byte{})
		c.Assert(queryEvBody.Schema, DeepEquals, []byte{})
		queryStart := uint32(eventHeaderLen + 4 + 4 + 1 + 2 + 2 + 1)
		queryEnd := eventSize - crc32Len
		queryLen := queryEnd - queryStart
		c.Assert(queryEvBody.Query, HasLen, int(queryLen))
		if queryLen <= dummyQueryLen {
			c.Assert(queryEvBody.Query, DeepEquals, dummyQuery[:queryLen])
		} else {
			c.Assert(queryEvBody.Query[:dummyQueryLen], DeepEquals, dummyQuery)
			zeroTail := make([]byte, queryLen-dummyQueryLen)
			c.Assert(queryEvBody.Query[dummyQueryLen:], DeepEquals, zeroTail)
		}
	}
}
