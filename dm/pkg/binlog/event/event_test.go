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
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/stretchr/testify/require"
)

func TestGenEventHeader(t *testing.T) {
	t.Parallel()
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
	require.Nil(t, err)
	require.Equal(t, eventHeaderLen, uint8(len(data)))

	header2 := &replication.EventHeader{}
	err = header2.Decode(data)
	require.Nil(t, err)
	verifyHeader(t, header2, header, header.EventType, latestPos, header.EventSize)
}

func verifyHeader(t *testing.T, obtained, excepted *replication.EventHeader, eventType replication.EventType, latestPos, eventSize uint32) {
	t.Helper()
	require.Equal(t, excepted.Timestamp, obtained.Timestamp)
	require.Equal(t, excepted.ServerID, obtained.ServerID)
	require.Equal(t, excepted.Flags, obtained.Flags)
	require.Equal(t, eventType, obtained.EventType)
	require.Equal(t, eventSize, obtained.EventSize)
	require.Equal(t, eventSize+latestPos, obtained.LogPos)
}

func TestGenFormatDescriptionEvent(t *testing.T) {
	t.Parallel()
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
			Flags:     0x01,
		}
		latestPos uint32 = 4
	)
	ev, err := GenFormatDescriptionEvent(header, latestPos)
	require.Nil(t, err)

	// verify the header
	verifyHeader(t, ev.Header, header, replication.FORMAT_DESCRIPTION_EVENT, latestPos, uint32(len(ev.RawData)))

	// some fields of FormatDescriptionEvent are a little hard to test, so we try to parse a binlog file.
	dir := t.TempDir()
	name := filepath.Join(dir, "mysql-bin-test.000001")
	f, err := os.Create(name)
	require.Nil(t, err)
	defer f.Close()

	// write a binlog file header
	_, err = f.Write(replication.BinLogFileHeader)
	require.Nil(t, err)

	// write the FormatDescriptionEvent
	_, err = f.Write(ev.RawData)
	require.Nil(t, err)

	// should only receive one FormatDescriptionEvent
	onEventFunc := func(e *replication.BinlogEvent) error {
		require.Equal(t, ev.Header, e.Header)
		require.Equal(t, ev.Event, e.Event)
		require.Equal(t, ev.RawData, e.RawData)
		return nil
	}

	parser2 := replication.NewBinlogParser()
	parser2.SetVerifyChecksum(true)
	err = parser2.ParseFile(name, 0, onEventFunc)
	require.Nil(t, err)
}

func TestGenRotateEvent(t *testing.T) {
	t.Parallel()
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
	require.NotNil(t, err)
	require.Nil(t, rotateEv)

	// valid nextLogName
	nextLogName = []byte("mysql-bin.000010")
	rotateEv, err = GenRotateEvent(header, latestPos, nextLogName, position)
	require.Nil(t, err)
	require.NotNil(t, rotateEv)

	// verify the header
	verifyHeader(t, rotateEv.Header, header, replication.ROTATE_EVENT, latestPos, uint32(len(rotateEv.RawData)))

	// verify the body
	rotateEvBody, ok := rotateEv.Event.(*replication.RotateEvent)
	require.True(t, ok)
	require.NotNil(t, rotateEvBody)
	require.Equal(t, nextLogName, rotateEvBody.NextLogName)
	require.Equal(t, position, rotateEvBody.Position)
}

func TestGenPreviousGTIDsEvent(t *testing.T) {
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
	require.Nil(t, err)

	// update latestPos
	latestPos = formatDescEv.Header.LogPos

	// generate a PreviousGTIDsEvent
	gSet, err := gtid.ParserGTID(gmysql.MySQLFlavor, str)
	require.Nil(t, err)

	previousGTIDsEv, err := GenPreviousGTIDsEvent(header, latestPos, gSet)
	require.Nil(t, err)

	dir := t.TempDir()
	name1 := filepath.Join(dir, "mysql-bin-test.000001")
	f1, err := os.Create(name1)
	require.Nil(t, err)
	defer f1.Close()

	// write a binlog file header
	_, err = f1.Write(replication.BinLogFileHeader)
	require.Nil(t, err)

	// write a FormatDescriptionEvent event
	_, err = f1.Write(formatDescEv.RawData)
	require.Nil(t, err)

	// write the PreviousGTIDsEvent
	_, err = f1.Write(previousGTIDsEv.RawData)
	require.Nil(t, err)

	count := 0
	onEventFunc := func(e *replication.BinlogEvent) error {
		count++
		switch count {
		case 1: // FormatDescriptionEvent
			require.Equal(t, formatDescEv.Header, e.Header)
			require.Equal(t, formatDescEv.Event, e.Event)
			require.Equal(t, formatDescEv.RawData, e.RawData)
		case 2: // PreviousGTIDsEvent
			require.Equal(t, previousGTIDsEv.Header, e.Header)
			require.Equal(t, previousGTIDsEv.Event, e.Event)
			require.Equal(t, previousGTIDsEv.RawData, e.RawData)
		default:
			t.Fatalf("too many binlog events got, current is %+v", e.Header)
		}
		return nil
	}

	parser2 := replication.NewBinlogParser()
	parser2.SetVerifyChecksum(true)
	err = parser2.ParseFile(name1, 0, onEventFunc)
	require.Nil(t, err)

	// multi GTID
	str = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,406a3f61-690d-11e7-87c5-6c92bf46f384:1-94321383,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,686e1ab6-c47e-11e7-a42c-6c92bf46f384:1-34981190,03fc0263-28c7-11e7-a653-6c0b84d59f30:1-7041423,05474d3c-28c7-11e7-8352-203db246dd3d:1-170,10b039fc-c843-11e7-8f6a-1866daf8d810:1-308290454"
	gSet, err = gtid.ParserGTID(gmysql.MySQLFlavor, str)
	require.Nil(t, err)

	previousGTIDsEv, err = GenPreviousGTIDsEvent(header, latestPos, gSet)
	require.Nil(t, err)

	// write another file
	name2 := filepath.Join(dir, "mysql-bin-test.000002")
	f2, err := os.Create(name2)
	require.Nil(t, err)
	defer f2.Close()

	// write a binlog file header
	_, err = f2.Write(replication.BinLogFileHeader)
	require.Nil(t, err)

	// write a FormatDescriptionEvent event
	_, err = f2.Write(formatDescEv.RawData)
	require.Nil(t, err)

	// write the PreviousGTIDsEvent
	_, err = f2.Write(previousGTIDsEv.RawData)
	require.Nil(t, err)

	count = 0 // reset count
	err = parser2.ParseFile(name2, 0, onEventFunc)
	require.Nil(t, err)
}

func TestGenGTIDEvent(t *testing.T) {
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
	require.Nil(t, err)

	// always needing a FormatDescriptionEvent in the binlog file.
	formatDescEv, err := GenFormatDescriptionEvent(header, latestPos)
	require.Nil(t, err)
	latestPos = formatDescEv.Header.LogPos // update latestPos

	// also needing a PreviousGTIDsEvent after FormatDescriptionEvent
	gSet, err := gtid.ParserGTID(gmysql.MySQLFlavor, prevGTIDsStr)
	require.Nil(t, err)
	previousGTIDsEv, err := GenPreviousGTIDsEvent(header, latestPos, gSet)
	require.Nil(t, err)
	latestPos = previousGTIDsEv.Header.LogPos // update latestPos

	gtidEv, err := GenGTIDEvent(header, latestPos, gtidFlags, uuid, gno, lastCommitted, lastCommitted+1)
	require.Nil(t, err)

	// verify the header
	verifyHeader(t, gtidEv.Header, header, replication.GTID_EVENT, latestPos, uint32(len(gtidEv.RawData)))

	// verify the body
	gtidEvBody, ok := gtidEv.Event.(*replication.GTIDEvent)
	require.True(t, ok)
	require.NotNil(t, gtidEvBody)
	require.Equal(t, gtidFlags, gtidEvBody.CommitFlag)
	require.Equal(t, sid.Bytes(), gtidEvBody.SID)
	require.Equal(t, gno, gtidEvBody.GNO)
	require.Equal(t, lastCommitted, gtidEvBody.LastCommitted)
	require.Equal(t, lastCommitted+1, gtidEvBody.SequenceNumber)

	// write a binlog file, then try to parse it
	dir := t.TempDir()
	name := filepath.Join(dir, "mysql-bin-test.000001")
	f, err := os.Create(name)
	require.Nil(t, err)
	defer f.Close()

	// write a binlog file.
	_, err = f.Write(replication.BinLogFileHeader)
	require.Nil(t, err)
	_, err = f.Write(formatDescEv.RawData)
	require.Nil(t, err)
	_, err = f.Write(previousGTIDsEv.RawData)
	require.Nil(t, err)

	// write GTIDEvent.
	_, err = f.Write(gtidEv.RawData)
	require.Nil(t, err)

	count := 0
	onEventFunc := func(e *replication.BinlogEvent) error {
		count++
		switch count {
		case 1: // FormatDescriptionEvent
			require.Equal(t, formatDescEv.Header, e.Header)
			require.Equal(t, formatDescEv.Event, e.Event)
			require.Equal(t, formatDescEv.RawData, e.RawData)
		case 2: // PreviousGTIDsEvent
			require.Equal(t, previousGTIDsEv.Header, e.Header)
			require.Equal(t, previousGTIDsEv.Event, e.Event)
			require.Equal(t, previousGTIDsEv.RawData, e.RawData)
		case 3: // GTIDEvent
			require.Equal(t, replication.GTID_EVENT, e.Header.EventType)
			require.Equal(t, gtidEv.RawData, e.RawData)
		default:
			t.Fatalf("too many binlog events got, current is %+v", e.Header)
		}
		return nil
	}
	parser2 := replication.NewBinlogParser()
	parser2.SetVerifyChecksum(true)
	err = parser2.ParseFile(name, 0, onEventFunc)
	require.Nil(t, err)
}

func TestGenQueryEvent(t *testing.T) {
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
	require.NotNil(t, err)
	require.Nil(t, queryEv)

	// valid query
	query = []byte("BEGIN")
	queryEv, err = GenQueryEvent(header, latestPos, slaveProxyID, executionTime, errorCode, statusVars, schema, query)
	require.Nil(t, err)
	require.NotNil(t, queryEv)

	// verify the header
	verifyHeader(t, queryEv.Header, header, replication.QUERY_EVENT, latestPos, uint32(len(queryEv.RawData)))

	// verify the body
	queryEvBody, ok := queryEv.Event.(*replication.QueryEvent)
	require.True(t, ok)
	require.NotNil(t, queryEvBody)
	require.Equal(t, slaveProxyID, queryEvBody.SlaveProxyID)
	require.Equal(t, executionTime, queryEvBody.ExecutionTime)
	require.Equal(t, errorCode, queryEvBody.ErrorCode)
	require.Equal(t, []byte{}, queryEvBody.StatusVars)
	require.Equal(t, []byte{}, queryEvBody.Schema)
	require.Equal(t, query, queryEvBody.Query)

	// non-empty schema
	schema = []byte("db")
	query = []byte("CREATE TABLE db.tbl (c1 int)")
	queryEv, err = GenQueryEvent(header, latestPos, slaveProxyID, executionTime, errorCode, statusVars, schema, query)
	require.Nil(t, err)
	require.NotNil(t, queryEv)

	// verify the body
	queryEvBody, ok = queryEv.Event.(*replication.QueryEvent)
	require.True(t, ok)
	require.NotNil(t, queryEvBody)
	require.Equal(t, schema, queryEvBody.Schema)
	require.Equal(t, query, queryEvBody.Query)

	// non-empty statusVars
	statusVars = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x20, 0x00, 0xa0, 0x55, 0x00, 0x00, 0x00, 0x00, 0x06, 0x03, 0x73, 0x74, 0x64, 0x04, 0x21, 0x00, 0x21, 0x00, 0x08, 0x00, 0x0c, 0x01, 0x73, 0x68, 0x61, 0x72, 0x64, 0x5f, 0x64, 0x62, 0x5f, 0x31, 0x00}
	queryEv, err = GenQueryEvent(header, latestPos, slaveProxyID, executionTime, errorCode, statusVars, schema, query)
	require.Nil(t, err)
	require.NotNil(t, queryEv)

	// verify the body
	queryEvBody, ok = queryEv.Event.(*replication.QueryEvent)
	require.True(t, ok)
	require.NotNil(t, queryEvBody)
	require.Equal(t, statusVars, queryEvBody.StatusVars)
}

func TestGenTableMapEvent(t *testing.T) {
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
	require.NotNil(t, err)
	require.Nil(t, tableMapEv)

	// valid schema, invalid table and columnType
	schema = []byte("db")
	tableMapEv, err = GenTableMapEvent(header, latestPos, tableID, schema, table, columnType)
	require.NotNil(t, err)
	require.Nil(t, tableMapEv)

	// valid schema and table, invalid columnType
	table = []byte("tbl")
	tableMapEv, err = GenTableMapEvent(header, latestPos, tableID, schema, table, columnType)
	require.NotNil(t, err)
	require.Nil(t, tableMapEv)

	// all valid
	columnType = []byte{gmysql.MYSQL_TYPE_LONG}
	tableMapEv, err = GenTableMapEvent(header, latestPos, tableID, schema, table, columnType)
	require.Nil(t, err)
	require.NotNil(t, tableMapEv)

	// verify the header
	verifyHeader(t, tableMapEv.Header, header, replication.TABLE_MAP_EVENT, latestPos, uint32(len(tableMapEv.RawData)))

	// verify the body
	tableMapEvBody, ok := tableMapEv.Event.(*replication.TableMapEvent)
	require.True(t, ok)
	require.NotNil(t, tableMapEvBody)
	require.Equal(t, tableID, tableMapEvBody.TableID)
	require.Equal(t, tableMapFlags, tableMapEvBody.Flags)
	require.Equal(t, schema, tableMapEvBody.Schema)
	require.Equal(t, table, tableMapEvBody.Table)
	require.Equal(t, uint64(len(columnType)), tableMapEvBody.ColumnCount)
	require.Equal(t, columnType, tableMapEvBody.ColumnType)

	// multi column type
	columnType = []byte{gmysql.MYSQL_TYPE_STRING, gmysql.MYSQL_TYPE_NEWDECIMAL, gmysql.MYSQL_TYPE_VAR_STRING, gmysql.MYSQL_TYPE_BLOB}
	tableMapEv, err = GenTableMapEvent(header, latestPos, tableID, schema, table, columnType)
	require.Nil(t, err)
	require.NotNil(t, tableMapEv)

	// verify the body
	tableMapEvBody, ok = tableMapEv.Event.(*replication.TableMapEvent)
	require.True(t, ok)
	require.NotNil(t, tableMapEvBody)
	require.Equal(t, uint64(len(columnType)), tableMapEvBody.ColumnCount)
	require.Equal(t, columnType, tableMapEvBody.ColumnType)

	// unsupported column type
	columnType = []byte{gmysql.MYSQL_TYPE_NEWDATE}
	tableMapEv, err = GenTableMapEvent(header, latestPos, tableID, schema, table, columnType)
	require.NotNil(t, err)
	require.Nil(t, tableMapEv)
}

func TestGenRowsEvent(t *testing.T) {
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
	require.NotNil(t, err)
	require.Nil(t, rowsEv)

	// valid eventType, invalid rows and columnType
	eventType = replication.WRITE_ROWS_EVENTv0
	rowsEv, err = GenRowsEvent(header, latestPos, eventType, tableID, rowsFlag, rows, columnType, nil)
	require.NotNil(t, err)
	require.Nil(t, rowsEv)

	// valid eventType and rows, invalid columnType
	row := []interface{}{int32(1)}
	rows = append(rows, row)
	rowsEv, err = GenRowsEvent(header, latestPos, eventType, tableID, rowsFlag, rows, columnType, nil)
	require.NotNil(t, err)
	require.Nil(t, rowsEv)

	// all valid
	columnType = []byte{gmysql.MYSQL_TYPE_LONG}
	rowsEv, err = GenRowsEvent(header, latestPos, eventType, tableID, rowsFlag, rows, columnType, nil)
	require.Nil(t, err)
	require.NotNil(t, rowsEv)

	// verify the header
	verifyHeader(t, rowsEv.Header, header, eventType, latestPos, uint32(len(rowsEv.RawData)))

	// verify the body
	rowsEvBody, ok := rowsEv.Event.(*replication.RowsEvent)
	require.True(t, ok)
	require.NotNil(t, rowsEvBody)
	require.Equal(t, rowsFlag, rowsEvBody.Flags)
	require.Equal(t, tableID, rowsEvBody.TableID)
	require.Equal(t, uint64(len(rows[0])), rowsEvBody.ColumnCount)
	require.Equal(t, 0, rowsEvBody.Version) // WRITE_ROWS_EVENTv0
	require.Equal(t, rows, rowsEvBody.Rows)

	// multi rows, with different length, invalid
	rows = append(rows, []interface{}{int32(1), int32(2)})
	rowsEv, err = GenRowsEvent(header, latestPos, eventType, tableID, rowsFlag, rows, columnType, nil)
	require.NotNil(t, err)
	require.Nil(t, rowsEv)

	// multi rows, multi columns, valid
	rows = make([][]interface{}, 0, 2)
	rows = append(rows, []interface{}{int32(1), int32(2)})
	rows = append(rows, []interface{}{int32(3), int32(4)})
	columnType = []byte{gmysql.MYSQL_TYPE_LONG, gmysql.MYSQL_TYPE_LONG}
	rowsEv, err = GenRowsEvent(header, latestPos, eventType, tableID, rowsFlag, rows, columnType, nil)
	require.Nil(t, err)
	require.NotNil(t, rowsEv)
	// verify the body
	rowsEvBody, ok = rowsEv.Event.(*replication.RowsEvent)
	require.True(t, ok)
	require.NotNil(t, rowsEvBody)
	require.Equal(t, uint64(len(rows[0])), rowsEvBody.ColumnCount)
	require.Equal(t, rows, rowsEvBody.Rows)

	// all valid event-type
	evTypes := []replication.EventType{
		replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2,
	}
	for _, eventType = range evTypes {
		rowsEv, err = GenRowsEvent(header, latestPos, eventType, tableID, rowsFlag, rows, columnType, nil)
		require.Nil(t, err)
		require.NotNil(t, rowsEv)
		require.Equal(t, eventType, rowsEv.Header.EventType)
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
	require.Nil(t, err)
	require.NotNil(t, rowsEv)
	// verify the body
	rowsEvBody, ok = rowsEv.Event.(*replication.RowsEvent)
	require.True(t, ok)
	require.NotNil(t, rowsEvBody)
	require.Equal(t, uint64(len(rows[0])), rowsEvBody.ColumnCount)
	require.Equal(t, rows, rowsEvBody.Rows)

	// column type mismatch
	rows[0][0] = int8(1)
	rowsEv, err = GenRowsEvent(header, latestPos, eventType, tableID, rowsFlag, rows, columnType, nil)
	require.NotNil(t, err)
	require.Nil(t, rowsEv)

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
		require.NotNil(t, err)
		require.True(t, strings.Contains(err.Error(), "not supported"))
		require.Nil(t, rowsEv)
	}
}

func TestGenXIDEvent(t *testing.T) {
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
	require.Nil(t, err)
	require.NotNil(t, xidEv)

	// verify the header
	verifyHeader(t, xidEv.Header, header, replication.XID_EVENT, latestPos, uint32(len(xidEv.RawData)))

	// verify the body
	xidEvBody, ok := xidEv.Event.(*replication.XIDEvent)
	require.True(t, ok)
	require.NotNil(t, xidEvBody)
	require.Equal(t, xid, xidEvBody.XID)
}

func TestGenMariaDBGTIDListEvent(t *testing.T) {
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
			Flags:     0x01,
		}
		latestPos uint32         = 4
		gSet      gmysql.GTIDSet // invalid
	)

	// invalid gSet
	gtidListEv, err := GenMariaDBGTIDListEvent(header, latestPos, gSet)
	require.NotNil(t, err)
	require.Nil(t, gtidListEv)

	// valid gSet with single GTID
	gSet, err = gtid.ParserGTID(gmysql.MariaDBFlavor, "1-2-3")
	require.Nil(t, err)
	require.NotNil(t, gSet)
	mGSet, ok := gSet.(*gmysql.MariadbGTIDSet)
	require.True(t, ok)
	require.NotNil(t, mGSet)

	gtidListEv, err = GenMariaDBGTIDListEvent(header, latestPos, gSet)
	require.Nil(t, err)
	require.NotNil(t, gtidListEv)

	// verify the header
	verifyHeader(t, gtidListEv.Header, header, replication.MARIADB_GTID_LIST_EVENT, latestPos, uint32(len(gtidListEv.RawData)))

	// verify the body
	gtidListEvBody, ok := gtidListEv.Event.(*replication.MariadbGTIDListEvent)
	require.True(t, ok)
	require.NotNil(t, gtidListEvBody)
	require.Len(t, gtidListEvBody.GTIDs, 1)
	require.Equal(t, *mGSet.Sets[gtidListEvBody.GTIDs[0].DomainID][gtidListEvBody.GTIDs[0].ServerID], gtidListEvBody.GTIDs[0])

	// valid gSet with multi GTIDs
	gSet, err = gtid.ParserGTID(gmysql.MariaDBFlavor, "1-2-12,2-2-3,3-3-8,4-4-4")
	require.Nil(t, err)
	require.NotNil(t, gSet)
	mGSet, ok = gSet.(*gmysql.MariadbGTIDSet)
	require.True(t, ok)
	require.NotNil(t, mGSet)

	gtidListEv, err = GenMariaDBGTIDListEvent(header, latestPos, gSet)
	require.Nil(t, err)
	require.NotNil(t, gtidListEv)

	// verify the body
	gtidListEvBody, ok = gtidListEv.Event.(*replication.MariadbGTIDListEvent)
	require.True(t, ok)
	require.NotNil(t, gtidListEvBody)
	require.Len(t, gtidListEvBody.GTIDs, 4)
	for _, mGTID := range gtidListEvBody.GTIDs {
		set, ok := mGSet.Sets[mGTID.DomainID]
		require.True(t, ok)
		mGTID2, ok := set[mGTID.ServerID]
		require.True(t, ok)
		require.Equal(t, *mGTID2, mGTID)
	}
}

func TestGenMariaDBGTIDEvent(t *testing.T) {
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
	require.Nil(t, err)
	require.NotNil(t, gtidEv)

	// verify the header
	verifyHeader(t, gtidEv.Header, header, replication.MARIADB_GTID_EVENT, latestPos, uint32(len(gtidEv.RawData)))

	// verify the body
	gtidEvBody, ok := gtidEv.Event.(*replication.MariadbGTIDEvent)
	require.True(t, ok)
	require.NotNil(t, gtidEvBody)
	require.Equal(t, seqNum, gtidEvBody.GTID.SequenceNumber)
	require.Equal(t, domainID, gtidEvBody.GTID.DomainID)
}

func TestGenDummyEvent(t *testing.T) {
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
	require.Regexp(t, ".*is too small.*", err)
	require.Nil(t, userVarEv)

	// minimum event size, USER_VAR_EVENT with name-length==1
	eventSize = MinUserVarEventLen
	userVarEv, err = GenDummyEvent(header, latestPos, eventSize)
	require.Nil(t, err)
	require.NotNil(t, userVarEv)
	// verify the header
	verifyHeader(t, userVarEv.Header, expectedHeader, replication.USER_VAR_EVENT, latestPos, uint32(len(userVarEv.RawData)))
	// verify the body
	nameStart := uint32(eventHeaderLen + 4)
	nameEnd := eventSize - 1 - crc32Len
	nameLen := nameEnd - nameStart
	require.Equal(t, uint32(1), nameLen) // name-length==1
	require.Equal(t, dummyUserVarName[:nameLen], userVarEv.RawData[nameStart:nameEnd])
	require.Equal(t, []byte{0x01}, userVarEv.RawData[nameEnd:nameEnd+1]) // is-null always 1

	// minimum, .., equal dummy query, longer, ...
	dummyQueryLen := uint32(len(dummyQuery))
	eventSizeList := []uint32{
		MinQueryEventLen, MinQueryEventLen + 5,
		MinQueryEventLen + dummyQueryLen - 1, MinQueryEventLen + dummyQueryLen, MinQueryEventLen + dummyQueryLen + 10,
	}
	for _, eventSize = range eventSizeList {
		queryEv, err := GenDummyEvent(header, latestPos, eventSize)
		require.Nil(t, err)
		require.NotNil(t, queryEv)
		// verify the header
		verifyHeader(t, queryEv.Header, expectedHeader, replication.QUERY_EVENT, latestPos, uint32(len(queryEv.RawData)))
		// verify the body
		queryEvBody, ok := queryEv.Event.(*replication.QueryEvent)
		require.True(t, ok)
		require.NotNil(t, queryEvBody)
		require.Equal(t, uint32(0), queryEvBody.SlaveProxyID)
		require.Equal(t, uint32(0), queryEvBody.ExecutionTime)
		require.Equal(t, uint16(0), queryEvBody.ErrorCode)
		require.Equal(t, []byte{}, queryEvBody.StatusVars)
		require.Equal(t, []byte{}, queryEvBody.Schema)
		queryStart := uint32(eventHeaderLen + 4 + 4 + 1 + 2 + 2 + 1)
		queryEnd := eventSize - crc32Len
		queryLen := queryEnd - queryStart
		require.Len(t, queryEvBody.Query, int(queryLen))
		if queryLen <= dummyQueryLen {
			require.Equal(t, dummyQuery[:queryLen], queryEvBody.Query)
		} else {
			require.Equal(t, dummyQuery, queryEvBody.Query[:dummyQueryLen])
			zeroTail := make([]byte, queryLen-dummyQueryLen)
			require.Equal(t, zeroTail, queryEvBody.Query[dummyQueryLen:])
		}
	}
}
