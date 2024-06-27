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
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
)

func TestInterfaceMethods(t *testing.T) {
	var (
		relayDir = t.TempDir()
		uuid     = "3ccc475b-2343-11e7-be21-6c0b84d59f30.000001"
		filename = "test-mysql-bin.000001"
		header   = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
			Flags:     0x01,
		}
		latestPos uint32 = 4
		ev, _            = event.GenFormatDescriptionEvent(header, latestPos)
	)

	require.NoError(t, os.MkdirAll(path.Join(relayDir, uuid), 0o755))

	w := NewFileWriter(log.L(), relayDir)
	require.NotNil(t, w)

	// not prepared
	_, err := w.WriteEvent(ev)
	require.ErrorContains(t, err, "not valid")

	w.Init(uuid, filename)

	// write event
	res, err := w.WriteEvent(ev)
	require.NoError(t, err)
	require.False(t, res.Ignore)

	// close the writer
	require.NoError(t, w.Close())
}

func TestRelayDir(t *testing.T) {
	var (
		relayDir = t.TempDir()
		uuid     = "3ccc475b-2343-11e7-be21-6c0b84d59f30.000001"
		header   = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
			Flags:     0x01,
		}
		latestPos uint32 = 4
	)
	ev, err := event.GenFormatDescriptionEvent(header, latestPos)
	require.NoError(t, err)

	// not inited
	w1 := NewFileWriter(log.L(), relayDir)
	defer w1.Close()
	_, err = w1.WriteEvent(ev)
	require.ErrorContains(t, err, "not valid")

	// invalid dir
	w2 := NewFileWriter(log.L(), relayDir)
	defer w2.Close()
	w2.Init("invalid\x00uuid", "bin.000001")
	_, err = w2.WriteEvent(ev)
	require.ErrorContains(t, err, "invalid argument")

	// valid directory, but no filename specified
	w3 := NewFileWriter(log.L(), relayDir)
	defer w3.Close()
	w3.Init(uuid, "")
	_, err = w3.WriteEvent(ev)
	require.ErrorContains(t, err, "not valid")

	// valid directory, but invalid filename
	w4 := NewFileWriter(log.L(), relayDir)
	defer w4.Close()
	w4.Init(uuid, "test-mysql-bin.666abc")
	_, err = w4.WriteEvent(ev)
	require.ErrorContains(t, err, "not valid")

	require.NoError(t, os.MkdirAll(path.Join(relayDir, uuid), 0o755))

	// valid directory, valid filename
	w5 := NewFileWriter(log.L(), relayDir)
	defer w5.Close()
	w5.Init(uuid, "test-mysql-bin.000001")
	result, err := w5.WriteEvent(ev)
	require.NoError(t, err)
	require.False(t, result.Ignore)
}

func TestFormatDescriptionEvent(t *testing.T) {
	var (
		relayDir = t.TempDir()
		filename = "test-mysql-bin.000001"
		uuid     = "3ccc475b-2343-11e7-be21-6c0b84d59f30.000001"
		header   = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
			Flags:     0x01,
		}
		latestPos uint32 = 4
	)
	formatDescEv, err := event.GenFormatDescriptionEvent(header, latestPos)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(path.Join(relayDir, uuid), 0o755))

	// write FormatDescriptionEvent to empty file
	w := NewFileWriter(log.L(), relayDir)
	defer w.Close()
	w.Init(uuid, filename)
	result, err := w.WriteEvent(formatDescEv)
	require.NoError(t, err)
	require.False(t, result.Ignore)
	fileSize := int64(len(replication.BinLogFileHeader) + len(formatDescEv.RawData))
	verifyFilenameOffset(t, w, filename, fileSize)
	latestPos = formatDescEv.Header.LogPos

	// write FormatDescriptionEvent again, ignore
	result, err = w.WriteEvent(formatDescEv)
	require.NoError(t, err)
	require.True(t, result.Ignore)
	require.Equal(t, ignoreReasonAlreadyExists, result.IgnoreReason)
	verifyFilenameOffset(t, w, filename, fileSize)

	// write another event
	queryEv, err := event.GenQueryEvent(header, latestPos, 0, 0, 0, nil, []byte("schema"), []byte("BEGIN"))
	require.NoError(t, err)
	result, err = w.WriteEvent(queryEv)
	require.NoError(t, err)
	require.False(t, result.Ignore)
	fileSize += int64(len(queryEv.RawData))
	verifyFilenameOffset(t, w, filename, fileSize)

	// write FormatDescriptionEvent again, ignore
	result, err = w.WriteEvent(formatDescEv)
	require.NoError(t, err)
	require.True(t, result.Ignore)
	require.Equal(t, ignoreReasonAlreadyExists, result.IgnoreReason)
	verifyFilenameOffset(t, w, filename, fileSize)

	// check events by reading them back
	events := make([]*replication.BinlogEvent, 0, 2)
	count := 0
	onEventFunc := func(e *replication.BinlogEvent) error {
		count++
		if count > 2 {
			t.Fatalf("too many events received, %+v", e.Header)
		}
		events = append(events, e)
		return nil
	}
	fullName := filepath.Join(relayDir, uuid, filename)
	err = replication.NewBinlogParser().ParseFile(fullName, 0, onEventFunc)
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, formatDescEv, events[0])
	require.Equal(t, queryEv, events[1])
}

func verifyFilenameOffset(t *testing.T, w Writer, filename string, offset int64) {
	t.Helper()

	wf, ok := w.(*FileWriter)
	require.True(t, ok)
	require.Equal(t, filename, wf.filename.Load())
	require.Equal(t, offset, wf.offset())
}

func TestRotateEventWithFormatDescriptionEvent(t *testing.T) {
	var (
		relayDir            = t.TempDir()
		uuid                = "3ccc475b-2343-11e7-be21-6c0b84d59f30.000001"
		filename            = "test-mysql-bin.000001"
		nextFilename        = "test-mysql-bin.000002"
		nextFilePos  uint64 = 4
		header              = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
			Flags:     0x01,
		}
		fakeHeader = &replication.EventHeader{
			Timestamp: 0, // mark as fake
			ServerID:  11,
			Flags:     0x01,
		}
		latestPos uint32 = 4
	)

	formatDescEv, err := event.GenFormatDescriptionEvent(header, latestPos)
	require.NoError(t, err)
	require.NotNil(t, formatDescEv)
	latestPos = formatDescEv.Header.LogPos

	rotateEv, err := event.GenRotateEvent(header, latestPos, []byte(nextFilename), nextFilePos)
	require.NoError(t, err)
	require.NotNil(t, rotateEv)

	fakeRotateEv, err := event.GenRotateEvent(fakeHeader, latestPos, []byte(nextFilename), nextFilePos)
	require.NoError(t, err)
	require.NotNil(t, fakeRotateEv)
	fakeRotateEv.Header.LogPos = 0

	// hole exists between formatDescEv and holeRotateEv, but the size is too small to fill
	holeRotateEv, err := event.GenRotateEvent(header, latestPos+event.MinUserVarEventLen-1, []byte(nextFilename), nextFilePos)
	require.NoError(t, err)
	require.NotNil(t, holeRotateEv)

	// 1: non-fake RotateEvent before FormatDescriptionEvent, invalid
	w1 := NewFileWriter(log.L(), relayDir)
	defer w1.Close()
	w1.Init(uuid, filename)
	_, err = w1.WriteEvent(rotateEv)
	require.ErrorContains(t, err, "no underlying writer opened")

	// 2. fake RotateEvent before FormatDescriptionEvent
	relayDir = t.TempDir() // use a new relay directory
	require.NoError(t, os.MkdirAll(path.Join(relayDir, uuid), 0o755))
	w2 := NewFileWriter(log.L(), relayDir)
	defer w2.Close()
	w2.Init(uuid, filename)
	result, err := w2.WriteEvent(fakeRotateEv)
	require.NoError(t, err)
	require.True(t, result.Ignore) // ignore fake RotateEvent
	require.Equal(t, ignoreReasonFakeRotate, result.IgnoreReason)

	result, err = w2.WriteEvent(formatDescEv)
	require.NoError(t, err)
	require.False(t, result.Ignore)

	fileSize := int64(len(replication.BinLogFileHeader) + len(formatDescEv.RawData))
	verifyFilenameOffset(t, w2, nextFilename, fileSize)

	// filename should be empty, next file should contain only one FormatDescriptionEvent
	filename1 := filepath.Join(relayDir, uuid, filename)
	filename2 := filepath.Join(relayDir, uuid, nextFilename)
	_, err = os.Stat(filename1)
	require.True(t, os.IsNotExist(err))
	require.NoError(t, w2.Flush())
	data, err := os.ReadFile(filename2)
	require.NoError(t, err)
	fileHeaderLen := len(replication.BinLogFileHeader)
	require.Equal(t, fileHeaderLen+len(formatDescEv.RawData), len(data))
	require.Equal(t, formatDescEv.RawData, data[fileHeaderLen:])

	// 3. FormatDescriptionEvent before fake RotateEvent
	relayDir = t.TempDir() // use a new relay directory
	require.NoError(t, os.MkdirAll(path.Join(relayDir, uuid), 0o755))
	w3 := NewFileWriter(log.L(), relayDir)
	defer w3.Close()
	w3.Init(uuid, filename)
	result, err = w3.WriteEvent(formatDescEv)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.False(t, result.Ignore)

	result, err = w3.WriteEvent(fakeRotateEv)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.Ignore)
	require.Equal(t, ignoreReasonFakeRotate, result.IgnoreReason)

	verifyFilenameOffset(t, w3, nextFilename, fileSize)

	// filename should contain only one FormatDescriptionEvent, next file should be empty
	filename1 = filepath.Join(relayDir, uuid, filename)
	filename2 = filepath.Join(relayDir, uuid, nextFilename)
	_, err = os.Stat(filename2)
	require.True(t, os.IsNotExist(err))
	require.NoError(t, w3.Flush())
	data, err = os.ReadFile(filename1)
	require.NoError(t, err)
	require.Equal(t, fileHeaderLen+len(formatDescEv.RawData), len(data))
	require.Equal(t, formatDescEv.RawData, data[fileHeaderLen:])

	// 4. FormatDescriptionEvent before non-fake RotateEvent
	relayDir = t.TempDir() // use a new relay directory
	require.NoError(t, os.MkdirAll(path.Join(relayDir, uuid), 0o755))
	w4 := NewFileWriter(log.L(), relayDir)
	defer w4.Close()
	w4.Init(uuid, filename)
	result, err = w4.WriteEvent(formatDescEv)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.False(t, result.Ignore)

	// try to write a rotateEv with hole exists
	_, err = w4.WriteEvent(holeRotateEv)
	require.Regexp(t, ".*required dummy event size.*is too small.*", err.Error())

	result, err = w4.WriteEvent(rotateEv)
	require.NoError(t, err)
	require.False(t, result.Ignore)

	fileSize += int64(len(rotateEv.RawData))
	verifyFilenameOffset(t, w4, nextFilename, fileSize)

	// write again, duplicate, but we already rotated and new binlog file not created
	_, err = w4.WriteEvent(rotateEv)
	require.Regexp(t, ".*(no such file or directory|The system cannot find the file specified).*", err.Error())

	// filename should contain both one FormatDescriptionEvent and one RotateEvent, next file should be empty
	filename1 = filepath.Join(relayDir, uuid, filename)
	filename2 = filepath.Join(relayDir, uuid, nextFilename)
	_, err = os.Stat(filename2)
	require.True(t, os.IsNotExist(err))
	data, err = os.ReadFile(filename1)
	require.NoError(t, err)
	require.Equal(t, fileHeaderLen+len(formatDescEv.RawData)+len(rotateEv.RawData), len(data))
	require.Equal(t, formatDescEv.RawData, data[fileHeaderLen:fileHeaderLen+len(formatDescEv.RawData)])
	require.Equal(t, rotateEv.RawData, data[fileHeaderLen+len(formatDescEv.RawData):])
}

func TestWriteMultiEvents(t *testing.T) {
	var (
		flavor                    = gmysql.MySQLFlavor
		serverID           uint32 = 11
		latestPos          uint32
		previousGTIDSetStr        = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,406a3f61-690d-11e7-87c5-6c92bf46f384:1-94321383,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,686e1ab6-c47e-11e7-a42c-6c92bf46f384:1-34981190,03fc0263-28c7-11e7-a653-6c0b84d59f30:1-7041423,05474d3c-28c7-11e7-8352-203db246dd3d:1-170,10b039fc-c843-11e7-8f6a-1866daf8d810:1-308290454"
		latestGTIDStr             = "3ccc475b-2343-11e7-be21-6c0b84d59f30:14"
		latestXID          uint64 = 10

		relayDir = t.TempDir()
		uuid     = "3ccc475b-2343-11e7-be21-6c0b84d59f30.000001"
		filename = "test-mysql-bin.000001"
	)
	previousGTIDSet, err := gtid.ParserGTID(flavor, previousGTIDSetStr)
	require.NoError(t, err)
	latestGTID, err := gtid.ParserGTID(flavor, latestGTIDStr)
	require.NoError(t, err)

	// use a binlog event generator to generate some binlog events.
	allEvents := make([]*replication.BinlogEvent, 0, 10)
	var allData bytes.Buffer
	g, err := event.NewGenerator(flavor, serverID, latestPos, latestGTID, previousGTIDSet, latestXID)
	require.NoError(t, err)

	// file header with FormatDescriptionEvent and PreviousGTIDsEvent
	events, data, err := g.GenFileHeader(0)
	require.NoError(t, err)
	allEvents = append(allEvents, events...)
	allData.Write(data)

	// CREATE DATABASE/TABLE
	queries := []string{"CRATE DATABASE `db`", "CREATE TABLE `db`.`tbl` (c1 INT)"}
	for _, query := range queries {
		events, data, err = g.GenDDLEvents("db", query, 0)
		require.NoError(t, err)
		allEvents = append(allEvents, events...)
		allData.Write(data)
	}

	// INSERT INTO `db`.`tbl` VALUES (1)
	var (
		tableID    uint64 = 8
		columnType        = []byte{gmysql.MYSQL_TYPE_LONG}
		insertRows        = make([][]interface{}, 1)
	)
	insertRows[0] = []interface{}{int32(1)}
	events, data, err = g.GenDMLEvents(replication.WRITE_ROWS_EVENTv2, []*event.DMLData{
		{TableID: tableID, Schema: "db", Table: "tbl", ColumnType: columnType, Rows: insertRows},
	}, 0)
	require.NoError(t, err)
	allEvents = append(allEvents, events...)
	allData.Write(data)

	require.NoError(t, os.MkdirAll(path.Join(relayDir, uuid), 0o755))

	// write the events to the file
	w := NewFileWriter(log.L(), relayDir)
	w.Init(uuid, filename)
	for _, ev := range allEvents {
		result, err2 := w.WriteEvent(ev)
		require.NoError(t, err2)
		require.False(t, result.Ignore) // no event is ignored
	}

	require.NoError(t, w.Flush())
	verifyFilenameOffset(t, w, filename, int64(allData.Len()))

	// read the data back from the file
	fullName := filepath.Join(relayDir, uuid, filename)
	obtainData, err := os.ReadFile(fullName)
	require.NoError(t, err)
	require.Equal(t, allData.Bytes(), obtainData)
}

func TestHandleFileHoleExist(t *testing.T) {
	var (
		relayDir = t.TempDir()
		uuid     = "3ccc475b-2343-11e7-be21-6c0b84d59f30.000001"
		filename = "test-mysql-bin.000001"
		header   = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
		}
		latestPos uint32 = 4
	)
	formatDescEv, err := event.GenFormatDescriptionEvent(header, latestPos)
	require.NoError(t, err)
	require.NotNil(t, formatDescEv)

	require.NoError(t, os.MkdirAll(path.Join(relayDir, uuid), 0o755))

	w := NewFileWriter(log.L(), relayDir)
	defer w.Close()
	w.Init(uuid, filename)

	// write the FormatDescriptionEvent, no hole exists
	result, err := w.WriteEvent(formatDescEv)
	require.NoError(t, err)
	require.False(t, result.Ignore)

	// hole exits, but the size is too small, invalid
	latestPos = formatDescEv.Header.LogPos + event.MinUserVarEventLen - 1
	queryEv, err := event.GenQueryEvent(header, latestPos, 0, 0, 0, nil, []byte("schema"), []byte("BEGIN"))
	require.NoError(t, err)
	_, err = w.WriteEvent(queryEv)
	require.ErrorContains(t, err, "generate dummy event")

	// hole exits, and the size is enough
	latestPos = formatDescEv.Header.LogPos + event.MinUserVarEventLen
	queryEv, err = event.GenQueryEvent(header, latestPos, 0, 0, 0, nil, []byte("schema"), []byte("BEGIN"))
	require.NoError(t, err)
	result, err = w.WriteEvent(queryEv)
	require.NoError(t, err)
	require.False(t, result.Ignore)
	require.NoError(t, w.Flush())
	fileSize := int64(queryEv.Header.LogPos)
	verifyFilenameOffset(t, w, filename, fileSize)

	// read events back from the file to check the dummy event
	events := make([]*replication.BinlogEvent, 0, 3)
	count := 0
	onEventFunc := func(e *replication.BinlogEvent) error {
		count++
		if count > 3 {
			t.Fatalf("too many events received, %+v", e.Header)
		}
		events = append(events, e)
		return nil
	}
	fullName := filepath.Join(relayDir, uuid, filename)
	err = replication.NewBinlogParser().ParseFile(fullName, 0, onEventFunc)
	require.NoError(t, err)
	require.Len(t, events, 3)
	require.Equal(t, formatDescEv, events[0])
	require.Equal(t, queryEv, events[2])
	// the second event is the dummy event
	dummyEvent := events[1]
	require.Equal(t, replication.USER_VAR_EVENT, dummyEvent.Header.EventType)
	require.Equal(t, latestPos, dummyEvent.Header.LogPos)
	require.Equal(t, latestPos-formatDescEv.Header.LogPos, dummyEvent.Header.EventSize) // hole size
}

func TestHandleDuplicateEventsExist(t *testing.T) {
	// NOTE: not duplicate event already tested in other cases

	var (
		relayDir = t.TempDir()
		uuid     = "3ccc475b-2343-11e7-be21-6c0b84d59f30.000001"
		filename = "test-mysql-bin.000001"
		header   = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
		}
		latestPos uint32 = 4
	)
	require.NoError(t, os.MkdirAll(path.Join(relayDir, uuid), 0o755))
	w := NewFileWriter(log.L(), relayDir)
	defer w.Close()
	w.Init(uuid, filename)

	// write a FormatDescriptionEvent, not duplicate
	formatDescEv, err := event.GenFormatDescriptionEvent(header, latestPos)
	require.NoError(t, err)
	result, err := w.WriteEvent(formatDescEv)
	require.NoError(t, err)
	require.False(t, result.Ignore)
	latestPos = formatDescEv.Header.LogPos

	// write a QueryEvent, the first time, not duplicate
	queryEv, err := event.GenQueryEvent(header, latestPos, 0, 0, 0, nil, []byte("schema"), []byte("BEGIN"))
	require.NoError(t, err)
	result, err = w.WriteEvent(queryEv)
	require.NoError(t, err)
	require.False(t, result.Ignore)

	// write the QueryEvent again, duplicate
	result, err = w.WriteEvent(queryEv)
	require.NoError(t, err)
	require.True(t, result.Ignore)
	require.Equal(t, ignoreReasonAlreadyExists, result.IgnoreReason)

	// write a start/end pos mismatched event
	latestPos--
	queryEv, err = event.GenQueryEvent(header, latestPos, 0, 0, 0, nil, []byte("schema"), []byte("BEGIN"))
	require.NoError(t, err)
	_, err = w.WriteEvent(queryEv)
	require.ErrorContains(t, err, "handle a potential duplicate event")
}
