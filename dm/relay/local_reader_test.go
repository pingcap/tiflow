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
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/pingcap/tiflow/dm/pkg/binlog/reader"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

var parseFileTimeout = 10 * time.Second

var _ = Suite(&testReaderSuite{})

type testReaderSuite struct {
	lastPos  uint32
	lastGTID gmysql.GTIDSet
}

func (t *testReaderSuite) SetUpSuite(c *C) {
	var err error
	t.lastPos = 0
	t.lastGTID, err = gtid.ParserGTID(gmysql.MySQLFlavor, "ba8f633f-1f15-11eb-b1c7-0242ac110002:1")
	c.Assert(err, IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/relay/SetHeartbeatInterval", "return(10000)"), IsNil)
}

func (t *testReaderSuite) TearDownSuite(c *C) {
	c.Assert(failpoint.Disable("github.com/pingcap/tiflow/dm/relay/SetHeartbeatInterval"), IsNil)
}

func newBinlogReaderForTest(logger log.Logger, cfg *BinlogReaderConfig, notify bool, uuid string) *BinlogReader {
	relay := NewRealRelay(&Config{Flavor: gmysql.MySQLFlavor})
	r := newBinlogReader(logger, cfg, relay)
	if notify {
		r.notifyCh <- struct{}{}
	}
	r.currentSubDir = uuid
	return r
}

func (t *testReaderSuite) setActiveRelayLog(r Process, uuid, filename string, offset int64) {
	relay := r.(*Relay)
	writer := relay.writer.(*FileWriter)
	writer.out.uuid.Store(uuid)
	writer.out.filename.Store(filename)
	writer.out.offset.Store(offset)
}

func (t *testReaderSuite) createBinlogFileParseState(c *C, relayLogDir, relayLogFile string, offset int64, possibleLast bool) *binlogFileParseState {
	fullPath := filepath.Join(relayLogDir, relayLogFile)
	f, err := os.Open(fullPath)
	c.Assert(err, IsNil)

	return &binlogFileParseState{
		possibleLast: possibleLast,
		fullPath:     fullPath,
		relayLogFile: relayLogFile,
		relayLogDir:  relayLogDir,
		f:            f,
		latestPos:    offset,
		skipGTID:     false,
	}
}

func (t *testReaderSuite) TestparseFileAsPossibleFileNotExist(c *C) {
	var (
		baseDir     = c.MkDir()
		currentUUID = "b60868af-5a6f-11e9-9ea3-0242ac160006.000001"
		filename    = "test-mysql-bin.000001"
		relayDir    = path.Join(baseDir, currentUUID)
	)
	cfg := &BinlogReaderConfig{RelayDir: baseDir, Flavor: gmysql.MySQLFlavor}
	r := newBinlogReaderForTest(log.L(), cfg, true, currentUUID)
	needSwitch, lastestPos, err := r.parseFileAsPossible(context.Background(), nil, filename, 4, relayDir, true, false)
	c.Assert(needSwitch, IsFalse)
	c.Assert(lastestPos, Equals, int64(0))
	c.Assert(err, ErrorMatches, ".*no such file or directory.*")
}

func (t *testReaderSuite) TestParseFileBase(c *C) {
	var (
		filename         = "test-mysql-bin.000001"
		baseDir          = c.MkDir()
		possibleLast     = false
		baseEvents, _, _ = t.genBinlogEvents(c, t.lastPos, t.lastGTID)
		s                = newLocalStreamer()
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// change to valid currentSubDir
	currentUUID := "b60868af-5a6f-11e9-9ea3-0242ac160006.000001"
	relayDir := filepath.Join(baseDir, currentUUID)
	fullPath := filepath.Join(relayDir, filename)
	err1 := os.MkdirAll(relayDir, 0o700)
	c.Assert(err1, IsNil)
	f, err1 := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY, 0o600)
	c.Assert(err1, IsNil)
	defer f.Close()

	// empty relay log file, got EOF when reading format description event separately and possibleLast = false
	{
		testCtx, testCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer testCancel()
		cfg := &BinlogReaderConfig{RelayDir: baseDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, true, currentUUID)
		t.setActiveRelayLog(r.relay, currentUUID, filename, 0)
		state := t.createBinlogFileParseState(c, relayDir, filename, 100, possibleLast)
		needSwitch, needReParse, err := r.parseFile(testCtx, s, true, state)
		c.Assert(errors.Cause(err), Equals, io.EOF)
		c.Assert(needSwitch, IsFalse)
		c.Assert(needReParse, IsFalse)
		c.Assert(state.latestPos, Equals, int64(100))
		c.Assert(state.formatDescEventRead, IsFalse)
		c.Assert(state.skipGTID, Equals, false)
	}

	// write some events to binlog file
	_, err1 = f.Write(replication.BinLogFileHeader)
	c.Assert(err1, IsNil)
	for _, ev := range baseEvents {
		_, err1 = f.Write(ev.RawData)
		c.Assert(err1, IsNil)
	}
	fileSize, _ := f.Seek(0, io.SeekCurrent)

	t.purgeStreamer(c, s)

	// base test with only one valid binlog file
	{
		testCtx, testCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer testCancel()
		cfg := &BinlogReaderConfig{RelayDir: baseDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, true, currentUUID)
		t.setActiveRelayLog(r.relay, currentUUID, filename, fileSize)
		state := t.createBinlogFileParseState(c, relayDir, filename, 4, possibleLast)
		needSwitch, needReParse, err := r.parseFile(testCtx, s, true, state)
		c.Assert(err, IsNil)
		c.Assert(needSwitch, IsFalse)
		c.Assert(needReParse, IsFalse)
		c.Assert(state.latestPos, Equals, int64(baseEvents[len(baseEvents)-1].Header.LogPos))
		c.Assert(state.formatDescEventRead, IsTrue)
		c.Assert(state.skipGTID, IsFalse)

		// try get events back, firstParse should have fake RotateEvent
		var fakeRotateEventCount int
		i := 0
		for {
			ev, err2 := s.GetEvent(ctx)
			c.Assert(err2, IsNil)
			if ev.Header.Timestamp == 0 || ev.Header.LogPos == 0 {
				if ev.Header.EventType == replication.ROTATE_EVENT {
					fakeRotateEventCount++
				}
				continue // ignore fake event
			}
			c.Assert(ev, DeepEquals, baseEvents[i])
			i++
			if i >= len(baseEvents) {
				break
			}
		}
		c.Assert(fakeRotateEventCount, Equals, 1)
		t.verifyNoEventsInStreamer(c, s)
	}

	// try get events back, since firstParse=false, should have no fake RotateEvent
	{
		testCtx, testCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer testCancel()
		cfg := &BinlogReaderConfig{RelayDir: baseDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, true, currentUUID)
		t.setActiveRelayLog(r.relay, currentUUID, filename, fileSize)
		state := t.createBinlogFileParseState(c, relayDir, filename, 4, possibleLast)
		needSwitch, needReParse, err := r.parseFile(testCtx, s, false, state)
		c.Assert(err, IsNil)
		c.Assert(needSwitch, IsFalse)
		c.Assert(needReParse, IsFalse)
		c.Assert(state.latestPos, Equals, int64(baseEvents[len(baseEvents)-1].Header.LogPos))
		c.Assert(state.formatDescEventRead, IsTrue)
		c.Assert(state.skipGTID, Equals, false)
		fakeRotateEventCount := 0
		i := 0
		for {
			ev, err2 := s.GetEvent(ctx)
			c.Assert(err2, IsNil)
			if ev.Header.Timestamp == 0 || ev.Header.LogPos == 0 {
				if ev.Header.EventType == replication.ROTATE_EVENT {
					fakeRotateEventCount++
				}
				continue // ignore fake event
			}
			c.Assert(ev, DeepEquals, baseEvents[i])
			i++
			if i >= len(baseEvents) {
				break
			}
		}
		c.Assert(fakeRotateEventCount, Equals, 0)
		t.verifyNoEventsInStreamer(c, s)
	}

	// generate another non-fake RotateEvent
	rotateEv, err := event.GenRotateEvent(baseEvents[0].Header, uint32(fileSize), []byte("mysql-bin.888888"), 4)
	c.Assert(err, IsNil)
	_, err = f.Write(rotateEv.RawData)
	c.Assert(err, IsNil)
	fileSize, _ = f.Seek(0, io.SeekCurrent)

	// latest is still the end_log_pos of the last event, not the next relay file log file's position
	{
		testCtx, testCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer testCancel()
		cfg := &BinlogReaderConfig{RelayDir: baseDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, true, currentUUID)
		t.setActiveRelayLog(r.relay, currentUUID, filename, fileSize)
		state := t.createBinlogFileParseState(c, relayDir, filename, 4, possibleLast)
		needSwitch, needReParse, err := r.parseFile(testCtx, s, true, state)
		c.Assert(err, IsNil)
		c.Assert(needSwitch, IsFalse)
		c.Assert(needReParse, IsFalse)
		c.Assert(state.latestPos, Equals, int64(rotateEv.Header.LogPos))
		c.Assert(state.formatDescEventRead, IsTrue)
		c.Assert(state.skipGTID, Equals, false)
		t.purgeStreamer(c, s)
	}

	// parse from offset > 4
	{
		testCtx, testCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer testCancel()
		cfg := &BinlogReaderConfig{RelayDir: baseDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, true, currentUUID)
		t.setActiveRelayLog(r.relay, currentUUID, filename, fileSize)
		offset := int64(rotateEv.Header.LogPos - rotateEv.Header.EventSize)
		state := t.createBinlogFileParseState(c, relayDir, filename, offset, possibleLast)
		needSwitch, needReParse, err := r.parseFile(testCtx, s, false, state)
		c.Assert(err, IsNil)
		c.Assert(needSwitch, IsFalse)
		c.Assert(needReParse, IsFalse)
		c.Assert(state.latestPos, Equals, int64(rotateEv.Header.LogPos))
		c.Assert(state.formatDescEventRead, IsTrue)
		c.Assert(state.skipGTID, Equals, false)

		// should only get a RotateEvent
		i := 0
		for {
			ev, err2 := s.GetEvent(ctx)
			c.Assert(err2, IsNil)
			switch ev.Header.EventType {
			case replication.ROTATE_EVENT:
				c.Assert(ev.RawData, DeepEquals, rotateEv.RawData)
				i++
			default:
				c.Fatalf("got unexpected event %+v", ev.Header)
			}
			if i >= 1 {
				break
			}
		}
		t.verifyNoEventsInStreamer(c, s)
	}
}

func (t *testReaderSuite) TestParseFileRelayNeedSwitchSubDir(c *C) {
	var (
		filename          = "test-mysql-bin.000001"
		nextFilename      = "test-mysql-bin.666888"
		notUsedGTIDSetStr = t.lastGTID.String()
		baseDir           = c.MkDir()
		offset            int64
		possibleLast      = true
		currentUUID       = "b60868af-5a6f-11e9-9ea3-0242ac160006.000001"
		switchedUUID      = "b60868af-5a6f-11e9-9ea3-0242ac160007.000002"
		relayDir          = filepath.Join(baseDir, currentUUID)
		nextRelayDir      = filepath.Join(baseDir, switchedUUID)
		fullPath          = filepath.Join(relayDir, filename)
		nextFullPath      = filepath.Join(nextRelayDir, nextFilename)
		s                 = newLocalStreamer()
		cfg               = &BinlogReaderConfig{RelayDir: baseDir, Flavor: gmysql.MySQLFlavor}
		r                 = newBinlogReaderForTest(log.L(), cfg, true, currentUUID)
	)

	// create the current relay log file and meta
	err := os.MkdirAll(relayDir, 0o700)
	c.Assert(err, IsNil)
	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY, 0o600)
	c.Assert(err, IsNil)
	defer f.Close()
	_, err = f.Write(replication.BinLogFileHeader)
	offset = 4
	c.Assert(err, IsNil)
	t.createMetaFile(c, relayDir, filename, uint32(offset), notUsedGTIDSetStr)

	r.subDirs = []string{currentUUID, switchedUUID}
	t.writeUUIDs(c, baseDir, r.subDirs)
	err = os.MkdirAll(nextRelayDir, 0o700)
	c.Assert(err, IsNil)
	err = os.WriteFile(nextFullPath, replication.BinLogFileHeader, 0o600)
	c.Assert(err, IsNil)

	// has relay log file in next sub directory, need to switch
	ctx2, cancel2 := context.WithTimeout(context.Background(), parseFileTimeout)
	defer cancel2()
	t.createMetaFile(c, nextRelayDir, filename, uint32(offset), notUsedGTIDSetStr)
	state := t.createBinlogFileParseState(c, relayDir, filename, offset, possibleLast)
	state.formatDescEventRead = true
	t.setActiveRelayLog(r.relay, "next", "next", 4)
	needSwitch, needReParse, err := r.parseFile(ctx2, s, true, state)
	c.Assert(err, IsNil)
	c.Assert(needSwitch, IsTrue)
	c.Assert(needReParse, IsFalse)
	c.Assert(state.latestPos, Equals, int64(4))
	c.Assert(state.formatDescEventRead, IsTrue)
	c.Assert(state.skipGTID, Equals, false)
	t.purgeStreamer(c, s)

	// NOTE: if we want to test the returned `needReParse` of `needSwitchSubDir`,
	// then we need to mock `fileSizeUpdated` or inject some delay or delay.
}

func (t *testReaderSuite) TestParseFileRelayWithIgnorableError(c *C) {
	var (
		filename          = "test-mysql-bin.000001"
		notUsedGTIDSetStr = t.lastGTID.String()
		baseDir           = c.MkDir()
		possibleLast      = true
		baseEvents, _, _  = t.genBinlogEvents(c, t.lastPos, t.lastGTID)
		currentUUID       = "b60868af-5a6f-11e9-9ea3-0242ac160006.000001"
		relayDir          = filepath.Join(baseDir, currentUUID)
		fullPath          = filepath.Join(relayDir, filename)
		s                 = newLocalStreamer()
		cfg               = &BinlogReaderConfig{RelayDir: baseDir, Flavor: gmysql.MySQLFlavor}
	)

	// create the current relay log file and write some events
	err := os.MkdirAll(relayDir, 0o700)
	c.Assert(err, IsNil)
	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY, 0o600)
	c.Assert(err, IsNil)
	defer f.Close()
	t.createMetaFile(c, relayDir, filename, 0, notUsedGTIDSetStr)

	_, err = f.Write(replication.BinLogFileHeader)
	c.Assert(err, IsNil)
	_, err = f.Write(baseEvents[0].RawData[:replication.EventHeaderSize])
	c.Assert(err, IsNil)

	// meet io.EOF error when read event and ignore it.
	{
		r := newBinlogReaderForTest(log.L(), cfg, true, currentUUID)
		state := t.createBinlogFileParseState(c, relayDir, filename, 4, possibleLast)
		state.formatDescEventRead = true
		t.setActiveRelayLog(r.relay, currentUUID, filename, 100)
		needSwitch, needReParse, err := r.parseFile(context.Background(), s, true, state)
		c.Assert(err, IsNil)
		c.Assert(needSwitch, IsFalse)
		c.Assert(needReParse, IsTrue)
		c.Assert(state.latestPos, Equals, int64(4))
		c.Assert(state.formatDescEventRead, IsTrue)
		c.Assert(state.skipGTID, Equals, false)
	}
}

func (t *testReaderSuite) TestUpdateUUIDs(c *C) {
	var (
		baseDir = c.MkDir()
		cfg     = &BinlogReaderConfig{RelayDir: baseDir, Flavor: gmysql.MySQLFlavor}
		r       = newBinlogReaderForTest(log.L(), cfg, true, "")
	)
	c.Assert(r.subDirs, HasLen, 0)

	// index file not exists, got nothing
	err := r.updateSubDirs()
	c.Assert(err, IsNil)
	c.Assert(r.subDirs, HasLen, 0)

	// valid UUIDs in the index file, got them back
	UUIDs := []string{
		"b60868af-5a6f-11e9-9ea3-0242ac160006.000001",
		"b60868af-5a6f-11e9-9ea3-0242ac160007.000002",
	}
	uuidBytes := t.uuidListToBytes(c, UUIDs)
	err = os.WriteFile(r.indexPath, uuidBytes, 0o600)
	c.Assert(err, IsNil)

	err = r.updateSubDirs()
	c.Assert(err, IsNil)
	c.Assert(r.subDirs, DeepEquals, UUIDs)
}

func (t *testReaderSuite) TestStartSyncByPos(c *C) {
	var (
		filenamePrefix                = "test-mysql-bin.00000"
		notUsedGTIDSetStr             = t.lastGTID.String()
		baseDir                       = c.MkDir()
		baseEvents, lastPos, lastGTID = t.genBinlogEvents(c, t.lastPos, t.lastGTID)
		eventsBuf                     bytes.Buffer
		UUIDs                         = []string{
			"b60868af-5a6f-11e9-9ea3-0242ac160006.000001",
			"b60868af-5a6f-11e9-9ea3-0242ac160007.000002",
			"b60868af-5a6f-11e9-9ea3-0242ac160008.000003",
		}
		cfg      = &BinlogReaderConfig{RelayDir: baseDir, Flavor: gmysql.MySQLFlavor}
		r        = newBinlogReaderForTest(log.L(), cfg, false, "")
		startPos = gmysql.Position{Name: "test-mysql-bin|000001.000001"} // from the first relay log file in the first sub directory
	)

	// prepare binlog data
	_, err := eventsBuf.Write(replication.BinLogFileHeader)
	c.Assert(err, IsNil)
	for _, ev := range baseEvents {
		_, err = eventsBuf.Write(ev.RawData)
		c.Assert(err, IsNil)
	}

	// create the index file
	uuidBytes := t.uuidListToBytes(c, UUIDs)
	err = os.WriteFile(r.indexPath, uuidBytes, 0o600)
	c.Assert(err, IsNil)

	// create sub directories
	for _, uuid := range UUIDs {
		subDir := filepath.Join(baseDir, uuid)
		err = os.MkdirAll(subDir, 0o700)
		c.Assert(err, IsNil)
	}

	// 1. generate relay log files
	// 1 for the first sub directory, 2 for the second directory and 3 for the third directory
	// so, write the same events data into (1+2+3) files.
	for i := 0; i < 3; i++ {
		for j := 1; j < i+2; j++ {
			filename := filepath.Join(baseDir, UUIDs[i], filenamePrefix+strconv.Itoa(j))
			var content []byte
			content = append(content, eventsBuf.Bytes()...)
			// don't add rotate event for the last file because we'll append more events to it.
			if !(i == 2 && j == i+1) {
				rotateEvent, err2 := event.GenRotateEvent(baseEvents[0].Header, lastPos, []byte(filenamePrefix+strconv.Itoa(j+1)), 4)
				c.Assert(err2, IsNil)
				content = append(content, rotateEvent.RawData...)
			}
			err = os.WriteFile(filename, content, 0o600)
			c.Assert(err, IsNil)
		}
		t.createMetaFile(c, path.Join(baseDir, UUIDs[i]), filenamePrefix+strconv.Itoa(i+1),
			startPos.Pos, notUsedGTIDSetStr)
	}

	// start the reader
	s, err := r.StartSyncByPos(startPos)
	c.Assert(err, IsNil)

	// get events from the streamer
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	obtainBaseEvents := readNEvents(ctx, c, s, (1+2+3)*(len(baseEvents)+1)-1, false)
	t.verifyNoEventsInStreamer(c, s)
	// verify obtain base events
	for i := 0; i < len(obtainBaseEvents); i += len(baseEvents) {
		c.Assert(obtainBaseEvents[i:i+len(baseEvents)], DeepEquals, baseEvents)
		// skip rotate event which is not added in baseEvents
		i++
	}

	// 2. write more events to the last file
	lastFilename := filepath.Join(baseDir, UUIDs[2], filenamePrefix+strconv.Itoa(3))
	extraEvents, _, _ := t.genBinlogEvents(c, lastPos, lastGTID)
	lastF, err := os.OpenFile(lastFilename, os.O_WRONLY|os.O_APPEND, 0o600)
	c.Assert(err, IsNil)
	defer lastF.Close()
	for _, ev := range extraEvents {
		_, err = lastF.Write(ev.RawData)
		c.Assert(err, IsNil)
	}
	r.notifyCh <- struct{}{}

	// read extra events back
	obtainExtraEvents := make([]*replication.BinlogEvent, 0, len(extraEvents))
	for {
		ev, err2 := s.GetEvent(ctx)
		c.Assert(err2, IsNil)
		if ev.Header.Timestamp == 0 || ev.Header.LogPos == 0 || ev.Header.EventType == replication.FORMAT_DESCRIPTION_EVENT {
			continue // ignore fake event and FormatDescriptionEvent, go-mysql may send extra FormatDescriptionEvent
		}
		obtainExtraEvents = append(obtainExtraEvents, ev)
		if len(obtainExtraEvents) == cap(obtainExtraEvents) {
			break
		}
	}
	t.verifyNoEventsInStreamer(c, s)

	// verify obtain extra events
	c.Assert(obtainExtraEvents, DeepEquals, extraEvents)

	// 3. create new file in the last directory
	lastFilename = filepath.Join(baseDir, UUIDs[2], filenamePrefix+strconv.Itoa(4))
	err = os.WriteFile(lastFilename, eventsBuf.Bytes(), 0o600)
	c.Assert(err, IsNil)
	t.createMetaFile(c, path.Join(baseDir, UUIDs[2]), lastFilename, lastPos, notUsedGTIDSetStr)
	r.notifyCh <- struct{}{}

	obtainExtraEvents2 := make([]*replication.BinlogEvent, 0, len(baseEvents)-1)
	for {
		ev, err2 := s.GetEvent(ctx)
		c.Assert(err2, IsNil)
		if ev.Header.Timestamp == 0 || ev.Header.LogPos == 0 || ev.Header.EventType == replication.FORMAT_DESCRIPTION_EVENT {
			continue // ignore fake event and FormatDescriptionEvent, go-mysql may send extra FormatDescriptionEvent
		}
		obtainExtraEvents2 = append(obtainExtraEvents2, ev)
		if len(obtainExtraEvents2) == cap(obtainExtraEvents2) {
			break
		}
	}
	t.verifyNoEventsInStreamer(c, s)

	// verify obtain extra events
	c.Assert(obtainExtraEvents2, DeepEquals, baseEvents[1:])

	// NOTE: load new UUIDs dynamically not supported yet

	// close the reader
	r.Close()
}

func readNEvents(ctx context.Context, c *C, s reader.Streamer, l int, tolerateMayDup bool) []*replication.BinlogEvent {
	var result []*replication.BinlogEvent
	for {
		ev, err2 := s.GetEvent(ctx)
		if tolerateMayDup {
			if err2 != nil {
				c.Assert(errors.ErrorEqual(ErrorMaybeDuplicateEvent, err2), IsTrue)
				continue
			}
		} else {
			c.Assert(err2, IsNil)
		}
		if ev.Header.Timestamp == 0 && ev.Header.LogPos == 0 {
			continue // ignore fake event
		}
		result = append(result, ev)
		// start from the first format description event
		if len(result) == l {
			break
		}
	}
	return result
}

func (t *testReaderSuite) TestStartSyncByGTID(c *C) {
	var (
		baseDir         = c.MkDir()
		events          []*replication.BinlogEvent
		cfg             = &BinlogReaderConfig{RelayDir: baseDir, Flavor: gmysql.MySQLFlavor}
		r               = newBinlogReaderForTest(log.L(), cfg, false, "")
		lastPos         uint32
		lastGTID        gmysql.GTIDSet
		previousGset, _ = gtid.ParserGTID(gmysql.MySQLFlavor, "")
	)

	type EventResult struct {
		eventType replication.EventType
		result    string // filename result of getPosByGTID
	}

	type FileEventResult struct {
		filename     string
		eventResults []EventResult
	}

	testCase := []struct {
		serverUUID      string
		uuid            string
		gtidStr         string
		fileEventResult []FileEventResult
	}{
		{
			"ba8f633f-1f15-11eb-b1c7-0242ac110002",
			"ba8f633f-1f15-11eb-b1c7-0242ac110002.000001",
			"ba8f633f-1f15-11eb-b1c7-0242ac110002:1",
			[]FileEventResult{
				{
					"mysql.000001",
					[]EventResult{
						{replication.PREVIOUS_GTIDS_EVENT, ""},
						{replication.QUERY_EVENT, "mysql|000001.000001"},
						{replication.XID_EVENT, "mysql|000001.000001"},
						{replication.QUERY_EVENT, "mysql|000001.000001"},
						{replication.XID_EVENT, "mysql|000001.000002"}, // next binlog filename
						{replication.ROTATE_EVENT, ""},
					},
				},
				{
					"mysql.000002",
					[]EventResult{
						{replication.PREVIOUS_GTIDS_EVENT, ""},
						{replication.QUERY_EVENT, "mysql|000001.000002"},
						{replication.XID_EVENT, "mysql|000001.000002"},
						{replication.QUERY_EVENT, "mysql|000001.000002"},
						{replication.XID_EVENT, "mysql|000001.000003"}, // next binlog filename
						{replication.ROTATE_EVENT, ""},
					},
				},
				{
					"mysql.000003",
					[]EventResult{
						{replication.PREVIOUS_GTIDS_EVENT, ""},
						{replication.QUERY_EVENT, "mysql|000001.000003"},
						{replication.XID_EVENT, "mysql|000001.000003"},
						{replication.QUERY_EVENT, "mysql|000001.000003"},
						{replication.XID_EVENT, "mysql|000002.000001"}, // next subdir
					},
				},
			},
		},
		{
			"bf6227a7-1f15-11eb-9afb-0242ac110004",
			"bf6227a7-1f15-11eb-9afb-0242ac110004.000002",
			"bf6227a7-1f15-11eb-9afb-0242ac110004:20",
			[]FileEventResult{
				{
					"mysql.000001",
					[]EventResult{
						{replication.PREVIOUS_GTIDS_EVENT, ""},
						{replication.QUERY_EVENT, "mysql|000002.000001"},
						{replication.XID_EVENT, "mysql|000002.000001"},
						{replication.QUERY_EVENT, "mysql|000002.000001"},
						{replication.XID_EVENT, "mysql|000002.000002"},
						{replication.ROTATE_EVENT, ""},
					},
				}, {
					"mysql.000002",
					[]EventResult{
						{replication.PREVIOUS_GTIDS_EVENT, ""},
						{replication.QUERY_EVENT, "mysql|000002.000002"},
						{replication.XID_EVENT, "mysql|000002.000002"},
						{replication.QUERY_EVENT, "mysql|000002.000002"},
						{replication.XID_EVENT, "mysql|000003.000001"},
						{replication.ROTATE_EVENT, ""},
					},
				}, {
					"mysql.000003",
					[]EventResult{
						{replication.PREVIOUS_GTIDS_EVENT, ""},
					},
				},
			},
		},
		{
			"bcbf9d42-1f15-11eb-a41c-0242ac110003",
			"bcbf9d42-1f15-11eb-a41c-0242ac110003.000003",
			"bcbf9d42-1f15-11eb-a41c-0242ac110003:30",
			[]FileEventResult{
				{
					"mysql.000001",
					[]EventResult{
						{replication.PREVIOUS_GTIDS_EVENT, ""},
						{replication.QUERY_EVENT, "mysql|000003.000001"},
						{replication.XID_EVENT, "mysql|000003.000001"},
						{replication.QUERY_EVENT, "mysql|000003.000001"},
						{replication.XID_EVENT, "mysql|000003.000001"},
					},
				},
			},
		},
	}

	for _, subDir := range testCase {
		r.subDirs = append(r.subDirs, subDir.uuid)
	}

	// write index file
	uuidBytes := t.uuidListToBytes(c, r.subDirs)
	err := os.WriteFile(r.indexPath, uuidBytes, 0o600)
	c.Assert(err, IsNil)

	var allEvents []*replication.BinlogEvent
	var allResults []string
	var eventsNumOfFirstServer int

	// generate binlog file
	for i, subDir := range testCase {
		lastPos = 4
		lastGTID, err = gtid.ParserGTID(gmysql.MySQLFlavor, subDir.gtidStr)
		c.Assert(err, IsNil)
		uuidDir := path.Join(baseDir, subDir.uuid)
		err = os.MkdirAll(uuidDir, 0o700)
		c.Assert(err, IsNil)

		for _, fileEventResult := range subDir.fileEventResult {
			eventTypes := []replication.EventType{}
			for _, eventResult := range fileEventResult.eventResults {
				eventTypes = append(eventTypes, eventResult.eventType)
				if len(eventResult.result) != 0 {
					allResults = append(allResults, eventResult.result)
				}
			}

			// generate events
			events, lastPos, lastGTID, previousGset = t.genEvents(c, eventTypes, lastPos, lastGTID, previousGset)
			allEvents = append(allEvents, events...)

			// write binlog file
			f, err2 := os.OpenFile(path.Join(uuidDir, fileEventResult.filename), os.O_CREATE|os.O_WRONLY, 0o600)
			c.Assert(err2, IsNil)
			_, err = f.Write(replication.BinLogFileHeader)
			c.Assert(err, IsNil)
			for _, ev := range events {
				_, err = f.Write(ev.RawData)
				c.Assert(err, IsNil)
			}
			f.Close()
			t.createMetaFile(c, uuidDir, fileEventResult.filename, lastPos, previousGset.String())
		}
		if i == 0 {
			eventsNumOfFirstServer = len(allEvents)
		}
	}

	startGTID, err := gtid.ParserGTID(gmysql.MySQLFlavor, "")
	c.Assert(err, IsNil)
	s, err := r.StartSyncByGTID(startGTID.Clone())
	c.Assert(err, IsNil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	obtainBaseEvents := readNEvents(ctx, c, s, len(allEvents), true)

	preGset, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, "")
	c.Assert(err, IsNil)

	gtidEventCount := 0
	for i, ev := range obtainBaseEvents {
		c.Assert(ev.Header, DeepEquals, allEvents[i].Header)
		if _, ok := ev.Event.(*replication.GTIDEvent); ok {
			gtidStr, _ := event.GetGTIDStr(ev)
			c.Assert(preGset.Update(gtidStr), IsNil)

			// get pos by preGset
			pos, err2 := r.getPosByGTID(preGset.Clone())
			c.Assert(err2, IsNil)
			// check result
			c.Assert(pos.Name, Equals, allResults[gtidEventCount])
			c.Assert(pos.Pos, Equals, uint32(4))
			gtidEventCount++
		}
	}

	r.Close()
	r = newBinlogReaderForTest(log.L(), cfg, true, "")

	excludeStrs := []string{}
	// exclude event except for first server
	includeServerUUID := testCase[0].serverUUID
	includeUUID := testCase[0].uuid
	for _, s := range strings.Split(preGset.String(), ",") {
		if !strings.Contains(s, includeServerUUID) {
			excludeStrs = append(excludeStrs, s)
		}
	}
	excludeStr := strings.Join(excludeStrs, ",")
	excludeGset, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, excludeStr)
	c.Assert(err, IsNil)

	// StartSyncByGtid exclude first uuid
	s, err = r.StartSyncByGTID(excludeGset)
	c.Assert(err, IsNil)
	obtainBaseEvents = readNEvents(ctx, c, s, eventsNumOfFirstServer, true)

	gset := excludeGset.Clone()
	// should not receive any event not from first server
	for i, event := range obtainBaseEvents {
		switch event.Header.EventType {
		case replication.HEARTBEAT_EVENT:
			c.FailNow()
		case replication.GTID_EVENT:
			// check gtid event comes from first uuid subdir
			ev, _ := event.Event.(*replication.GTIDEvent)
			u, _ := uuid.FromBytes(ev.SID)
			c.Assert(u.String(), Equals, includeServerUUID)
			c.Assert(event.Header, DeepEquals, allEvents[i].Header)
			c.Assert(gset.Update(fmt.Sprintf("%s:%d", u.String(), ev.GNO)), IsNil)
		default:
			c.Assert(event.Header, DeepEquals, allEvents[i].Header)
		}
	}
	// gset same as preGset now
	c.Assert(gset.Equal(preGset), IsTrue)

	// purge first uuid subdir's first binlog file
	c.Assert(os.Remove(path.Join(baseDir, includeUUID, "mysql.000001")), IsNil)

	r.Close()
	r = newBinlogReaderForTest(log.L(), cfg, true, "")
	_, err = r.StartSyncByGTID(preGset)
	c.Assert(err, IsNil)

	r.Close()
	r = newBinlogReaderForTest(log.L(), cfg, true, "")
	_, err = r.StartSyncByGTID(excludeGset)
	// error because file has been purge
	c.Assert(terror.ErrNoRelayPosMatchGTID.Equal(err), IsTrue)

	// purge first uuid subdir
	c.Assert(os.RemoveAll(path.Join(baseDir, includeUUID)), IsNil)

	r.Close()
	r = newBinlogReaderForTest(log.L(), cfg, true, "")
	_, err = r.StartSyncByGTID(preGset)
	c.Assert(err, IsNil)

	r.Close()
	r = newBinlogReaderForTest(log.L(), cfg, true, "")
	_, err = r.StartSyncByGTID(excludeGset)
	// error because subdir has been purge
	c.Assert(err, ErrorMatches, ".*no such file or directory.*")
	cancel()
}

func (t *testReaderSuite) TestStartSyncError(c *C) {
	var (
		baseDir = c.MkDir()
		UUIDs   = []string{
			"b60868af-5a6f-11e9-9ea3-0242ac160006.000001",
		}
		cfg      = &BinlogReaderConfig{RelayDir: baseDir, Flavor: gmysql.MySQLFlavor}
		startPos = gmysql.Position{Name: "test-mysql-bin|000001.000001"} // from the first relay log file in the first sub directory
	)

	r := newBinlogReaderForTest(log.L(), cfg, true, "")
	err := r.checkRelayPos(startPos)
	c.Assert(err, ErrorMatches, ".*empty UUIDs not valid.*")

	// no startup pos specified
	s, err := r.StartSyncByPos(gmysql.Position{})
	c.Assert(terror.ErrBinlogFileNotSpecified.Equal(err), IsTrue)
	c.Assert(s, IsNil)

	// empty UUIDs
	s, err = r.StartSyncByPos(startPos)
	c.Assert(err, ErrorMatches, ".*empty UUIDs not valid.*")
	c.Assert(s, IsNil)

	s, err = r.StartSyncByGTID(t.lastGTID.Clone())
	c.Assert(err, ErrorMatches, ".*no relay pos match gtid.*")
	c.Assert(s, IsNil)

	// write UUIDs into index file
	r = newBinlogReaderForTest(log.L(), cfg, true, "") // create a new reader
	uuidBytes := t.uuidListToBytes(c, UUIDs)
	err = os.WriteFile(r.indexPath, uuidBytes, 0o600)
	c.Assert(err, IsNil)

	// the startup relay log file not found
	s, err = r.StartSyncByPos(startPos)
	c.Assert(err, ErrorMatches, fmt.Sprintf(".*%s.*not found.*", startPos.Name))
	c.Assert(s, IsNil)

	s, err = r.StartSyncByGTID(t.lastGTID.Clone())
	c.Assert(err, ErrorMatches, ".*no such file or directory.*")
	c.Assert(s, IsNil)

	// can not re-start the reader
	r.running = true
	s, err = r.StartSyncByPos(startPos)
	c.Assert(terror.ErrReaderAlreadyRunning.Equal(err), IsTrue)
	c.Assert(s, IsNil)
	r.Close()

	r.running = true
	s, err = r.StartSyncByGTID(t.lastGTID.Clone())
	c.Assert(terror.ErrReaderAlreadyRunning.Equal(err), IsTrue)
	c.Assert(s, IsNil)
	r.Close()

	// too big startPos
	uuid := UUIDs[0]
	err = os.MkdirAll(filepath.Join(baseDir, uuid), 0o700)
	c.Assert(err, IsNil)
	parsedStartPosName := "test-mysql-bin.000001"
	relayLogFilePath := filepath.Join(baseDir, uuid, parsedStartPosName)
	err = os.WriteFile(relayLogFilePath, make([]byte, 100), 0o600)
	c.Assert(err, IsNil)
	startPos.Pos = 10000
	s, err = r.StartSyncByPos(startPos)
	c.Assert(terror.ErrRelayLogGivenPosTooBig.Equal(err), IsTrue)
	c.Assert(s, IsNil)
}

func (t *testReaderSuite) TestAdvanceCurrentGTIDSet(c *C) {
	var (
		baseDir        = c.MkDir()
		cfg            = &BinlogReaderConfig{RelayDir: baseDir, Flavor: gmysql.MySQLFlavor}
		r              = newBinlogReaderForTest(log.L(), cfg, true, "")
		mysqlGset, _   = gmysql.ParseMysqlGTIDSet("b60868af-5a6f-11e9-9ea3-0242ac160006:1-6")
		mariadbGset, _ = gmysql.ParseMariadbGTIDSet("0-1-5")
	)
	r.prevGset = mysqlGset.Clone()
	r.currGset = nil
	notUpdated, err := r.advanceCurrentGtidSet("b60868af-5a6f-11e9-9ea3-0242ac160006:6")
	c.Assert(err, IsNil)
	c.Assert(notUpdated, IsTrue)
	c.Assert(mysqlGset.Equal(r.currGset), IsTrue)
	notUpdated, err = r.advanceCurrentGtidSet("b60868af-5a6f-11e9-9ea3-0242ac160006:7")
	c.Assert(err, IsNil)
	c.Assert(notUpdated, IsFalse)
	c.Assert(mysqlGset.Equal(r.prevGset), IsTrue)
	c.Assert(r.currGset.String(), Equals, "b60868af-5a6f-11e9-9ea3-0242ac160006:1-7")

	r.cfg.Flavor = gmysql.MariaDBFlavor
	r.prevGset = mariadbGset.Clone()
	r.currGset = nil
	notUpdated, err = r.advanceCurrentGtidSet("0-1-3")
	c.Assert(err, IsNil)
	c.Assert(notUpdated, IsTrue)
	c.Assert(mariadbGset.Equal(r.currGset), IsTrue)
	notUpdated, err = r.advanceCurrentGtidSet("0-1-6")
	c.Assert(err, IsNil)
	c.Assert(notUpdated, IsFalse)
	c.Assert(mariadbGset.Equal(r.prevGset), IsTrue)
	c.Assert(r.currGset.String(), Equals, "0-1-6")
}

func (t *testReaderSuite) TestReParseUsingGTID(c *C) {
	var (
		baseDir   = c.MkDir()
		cfg       = &BinlogReaderConfig{RelayDir: baseDir, Flavor: gmysql.MySQLFlavor}
		r         = newBinlogReaderForTest(log.L(), cfg, true, "")
		uuid      = "ba8f633f-1f15-11eb-b1c7-0242ac110002.000001"
		gtidStr   = "ba8f633f-1f15-11eb-b1c7-0242ac110002:1"
		file      = "mysql.000001"
		latestPos uint32
	)

	startGTID, err := gtid.ParserGTID(gmysql.MySQLFlavor, "")
	c.Assert(err, IsNil)
	lastGTID, err := gtid.ParserGTID(gmysql.MySQLFlavor, gtidStr)
	c.Assert(err, IsNil)

	// prepare a minimal relay log file
	c.Assert(os.WriteFile(r.indexPath, []byte(uuid), 0o600), IsNil)

	uuidDir := path.Join(baseDir, uuid)
	c.Assert(os.MkdirAll(uuidDir, 0o700), IsNil)
	f, err := os.OpenFile(path.Join(uuidDir, file), os.O_CREATE|os.O_WRONLY, 0o600)
	c.Assert(err, IsNil)
	_, err = f.Write(replication.BinLogFileHeader)
	c.Assert(err, IsNil)

	meta := LocalMeta{BinLogName: file, BinLogPos: latestPos, BinlogGTID: startGTID.String()}
	metaFile, err := os.Create(path.Join(uuidDir, utils.MetaFilename))
	c.Assert(err, IsNil)
	c.Assert(toml.NewEncoder(metaFile).Encode(&meta), IsNil)
	c.Assert(metaFile.Close(), IsNil)

	// prepare some regular events,
	// FORMAT_DESC + PREVIOUS_GTIDS, some events generated from a DDL, some events generated from a DML
	genType := []replication.EventType{
		replication.PREVIOUS_GTIDS_EVENT,
		replication.QUERY_EVENT,
		replication.XID_EVENT,
	}
	events, _, _, latestGTIDSet := t.genEvents(c, genType, 4, lastGTID, startGTID)
	c.Assert(events, HasLen, 1+1+2+5)

	// write FORMAT_DESC + PREVIOUS_GTIDS
	_, err = f.Write(events[0].RawData)
	c.Assert(err, IsNil)
	_, err = f.Write(events[1].RawData)
	c.Assert(err, IsNil)

	// we use latestGTIDSet to start sync, which means we already received all binlog events, so expect no DML/DDL
	s, err := r.StartSyncByGTID(latestGTIDSet)
	c.Assert(err, IsNil)
	var wg sync.WaitGroup
	wg.Add(1)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		expected := map[uint32]replication.EventType{}
		for _, e := range events {
			// will not receive event for skipped GTID
			switch e.Event.(type) {
			case *replication.FormatDescriptionEvent, *replication.PreviousGTIDsEvent:
				expected[e.Header.LogPos] = e.Header.EventType
			}
		}
		// fake rotate
		expected[0] = replication.ROTATE_EVENT

		for {
			ev, err2 := s.GetEvent(ctx)
			if err2 == context.Canceled {
				break
			}
			c.Assert(err2, IsNil)
			c.Assert(ev.Header.EventType, Equals, expected[ev.Header.LogPos])
		}
		wg.Done()
	}()

	for i := 2; i < len(events); i++ {
		// hope a second is enough to trigger needReParse
		time.Sleep(time.Second)
		_, err = f.Write(events[i].RawData)
		c.Assert(err, IsNil)
		_ = f.Sync()
		select {
		case r.notifyCh <- struct{}{}:
		default:
		}
	}
	time.Sleep(time.Second)
	cancel()
	wg.Wait()
}

func (t *testReaderSuite) genBinlogEvents(c *C, latestPos uint32, latestGTID gmysql.GTIDSet) ([]*replication.BinlogEvent, uint32, gmysql.GTIDSet) {
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
		}
		events = make([]*replication.BinlogEvent, 0, 10)
	)

	if latestPos <= 4 { // generate a FormatDescriptionEvent if needed
		ev, err := event.GenFormatDescriptionEvent(header, 4)
		c.Assert(err, IsNil)
		latestPos = ev.Header.LogPos
		events = append(events, ev)
	}

	// for these tests, generates some DDL events is enough
	count := 5 + rand.Intn(5)
	for i := 0; i < count; i++ {
		evs, err := event.GenDDLEvents(gmysql.MySQLFlavor, 1, latestPos, latestGTID, fmt.Sprintf("db_%d", i), fmt.Sprintf("CREATE TABLE %d (c1 INT)", i), true, false, 0)
		c.Assert(err, IsNil)
		events = append(events, evs.Events...)
		latestPos = evs.LatestPos
		latestGTID = evs.LatestGTID
	}

	return events, latestPos, latestGTID
}

func (t *testReaderSuite) genEvents(
	c *C,
	eventTypes []replication.EventType,
	latestPos uint32,
	latestGTID gmysql.GTIDSet,
	previousGset gmysql.GTIDSet,
) ([]*replication.BinlogEvent, uint32, gmysql.GTIDSet, gmysql.GTIDSet) {
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
		}
		events    = make([]*replication.BinlogEvent, 0, 10)
		pGset     = previousGset.Clone()
		originSet = pGset
	)

	if latestPos <= 4 { // generate a FormatDescriptionEvent if needed
		ev, err := event.GenFormatDescriptionEvent(header, 4)
		c.Assert(err, IsNil)
		latestPos = ev.Header.LogPos
		events = append(events, ev)
	}

	for i, eventType := range eventTypes {
		switch eventType {
		case replication.QUERY_EVENT:
			evs, err := event.GenDDLEvents(gmysql.MySQLFlavor, 1, latestPos, latestGTID, fmt.Sprintf("db_%d", i), fmt.Sprintf("CREATE TABLE %d (c1 int)", i), true, false, 0)
			c.Assert(err, IsNil)
			events = append(events, evs.Events...)
			latestPos = evs.LatestPos
			latestGTID = evs.LatestGTID
			_, ok := evs.Events[0].Event.(*replication.GTIDEvent)
			c.Assert(ok, IsTrue)
			gtidStr, _ := event.GetGTIDStr(evs.Events[0])
			err = originSet.Update(gtidStr)
			c.Assert(err, IsNil)
		case replication.XID_EVENT:
			insertDMLData := []*event.DMLData{
				{
					TableID:    uint64(i),
					Schema:     fmt.Sprintf("db_%d", i),
					Table:      strconv.Itoa(i),
					ColumnType: []byte{gmysql.MYSQL_TYPE_INT24},
					Rows:       [][]interface{}{{int32(1)}, {int32(2)}},
				},
			}
			evs, err := event.GenDMLEvents(gmysql.MySQLFlavor, 1, latestPos, latestGTID, replication.WRITE_ROWS_EVENTv2, 10, insertDMLData, true, false, 0)
			c.Assert(err, IsNil)
			events = append(events, evs.Events...)
			latestPos = evs.LatestPos
			latestGTID = evs.LatestGTID
			_, ok := evs.Events[0].Event.(*replication.GTIDEvent)
			c.Assert(ok, IsTrue)
			gtidStr, _ := event.GetGTIDStr(evs.Events[0])
			err = originSet.Update(gtidStr)
			c.Assert(err, IsNil)
		case replication.ROTATE_EVENT:
			ev, err := event.GenRotateEvent(header, latestPos, []byte("next_log"), 4)
			c.Assert(err, IsNil)
			events = append(events, ev)
			latestPos = 4
		case replication.PREVIOUS_GTIDS_EVENT:
			ev, err := event.GenPreviousGTIDsEvent(header, latestPos, pGset)
			c.Assert(err, IsNil)
			events = append(events, ev)
			latestPos = ev.Header.LogPos
		}
	}
	return events, latestPos, latestGTID, originSet
}

func (t *testReaderSuite) purgeStreamer(c *C, s reader.Streamer) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	for {
		_, err := s.GetEvent(ctx)
		switch {
		case err == nil:
			continue
		case err == ctx.Err():
			return
		default:
			c.Fatalf("purge streamer with error %v", err)
		}
	}
}

func (t *testReaderSuite) verifyNoEventsInStreamer(c *C, s reader.Streamer) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	ev, err := s.GetEvent(ctx)
	if err != ctx.Err() {
		c.Fatalf("got event %v with error %v from streamer", ev, err)
	}
}

func (t *testReaderSuite) uuidListToBytes(c *C, uuids []string) []byte {
	var buf bytes.Buffer
	for _, uuid := range uuids {
		_, err := buf.WriteString(uuid)
		c.Assert(err, IsNil)
		_, err = buf.WriteString("\n")
		c.Assert(err, IsNil)
	}
	return buf.Bytes()
}

// nolint:unparam
func (t *testReaderSuite) writeUUIDs(c *C, relayDir string, uuids []string) []byte {
	indexPath := path.Join(relayDir, utils.UUIDIndexFilename)
	var buf bytes.Buffer
	for _, uuid := range uuids {
		_, err := buf.WriteString(uuid)
		c.Assert(err, IsNil)
		_, err = buf.WriteString("\n")
		c.Assert(err, IsNil)
	}

	// write the index file
	err := os.WriteFile(indexPath, buf.Bytes(), 0o600)
	c.Assert(err, IsNil)
	return buf.Bytes()
}

func (t *testReaderSuite) createMetaFile(c *C, relayDirPath, binlogFileName string, pos uint32, gtid string) {
	meta := LocalMeta{BinLogName: binlogFileName, BinLogPos: pos, BinlogGTID: gtid}
	metaFile, err2 := os.Create(path.Join(relayDirPath, utils.MetaFilename))
	c.Assert(err2, IsNil)
	err := toml.NewEncoder(metaFile).Encode(&meta)
	c.Assert(err, IsNil)
	metaFile.Close()
}

type mockActiveCase struct {
	active bool
	offset int64
}

type mockFileWriterForActiveTest struct {
	cnt   int
	cases []mockActiveCase
}

func (m *mockFileWriterForActiveTest) Init(uuid, filename string) {
	panic("should be used")
}

func (m *mockFileWriterForActiveTest) Close() error {
	panic("should be used")
}

func (m *mockFileWriterForActiveTest) Flush() error {
	panic("should be used")
}

func (m *mockFileWriterForActiveTest) WriteEvent(ev *replication.BinlogEvent) (WResult, error) {
	panic("should be used")
}

func (m *mockFileWriterForActiveTest) IsActive(uuid, filename string) (bool, int64) {
	v := m.cases[m.cnt]
	m.cnt++
	return v.active, v.offset
}

func (t *testReaderSuite) TestwaitBinlogChanged(c *C) {
	var (
		relayFiles = []string{
			"mysql-bin.000001",
			"mysql-bin.000002",
		}
		binlogPos  = uint32(4)
		binlogGTID = "ba8f633f-1f15-11eb-b1c7-0242ac110002:1"
		relayPaths = make([]string, len(relayFiles))
		data       = []byte("meaningless file content")
		size       = int64(len(data))
	)

	// create relay log dir
	subDir := c.MkDir()
	// join the file path
	for i, rf := range relayFiles {
		relayPaths[i] = filepath.Join(subDir, rf)
		f, _ := os.Create(relayPaths[i])
		_ = f.Close()
	}

	rotateRelayFile := func(filename string) {
		meta := LocalMeta{BinLogName: filename, BinLogPos: binlogPos, BinlogGTID: binlogGTID}
		metaFile, err2 := os.Create(path.Join(subDir, utils.MetaFilename))
		c.Assert(err2, IsNil)
		err := toml.NewEncoder(metaFile).Encode(&meta)
		c.Assert(err, IsNil)
		_ = metaFile.Close()
	}

	// meta not found
	{
		cfg := &BinlogReaderConfig{RelayDir: "", Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, false, "")
		t.setActiveRelayLog(r.relay, "next", "next", 0)
		state := t.createBinlogFileParseState(c, subDir, relayFiles[0], 0, true)
		needSwitch, reParse, err := r.waitBinlogChanged(context.Background(), state)
		c.Assert(needSwitch, IsFalse)
		c.Assert(reParse, IsFalse)
		c.Assert(err, NotNil)
		c.Assert(err, ErrorMatches, ".*no such file or directory*")
	}

	// write meta
	rotateRelayFile(relayFiles[0])

	// relay file not found
	{
		cfg := &BinlogReaderConfig{RelayDir: "", Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, false, "")
		t.setActiveRelayLog(r.relay, "next", "next", 0)
		state := &binlogFileParseState{
			relayLogDir:  subDir,
			relayLogFile: "not-exist-file",
		}
		needSwitch, reParse, err := r.waitBinlogChanged(context.Background(), state)
		c.Assert(needSwitch, IsFalse)
		c.Assert(reParse, IsFalse)
		c.Assert(err, NotNil)
		c.Assert(err, ErrorMatches, ".*no such file or directory*")
	}

	// create the first relay file
	err1 := os.WriteFile(relayPaths[0], data, 0o600)
	c.Assert(err1, IsNil)
	// rotate relay file
	rotateRelayFile(relayFiles[1])

	// file decreased when meta changed
	{
		cfg := &BinlogReaderConfig{RelayDir: "", Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, false, "")
		t.setActiveRelayLog(r.relay, "next", "next", 0)
		state := t.createBinlogFileParseState(c, subDir, relayFiles[0], size+100, true)
		needSwitch, reParse, err := r.waitBinlogChanged(context.Background(), state)
		c.Assert(needSwitch, IsFalse)
		c.Assert(reParse, IsFalse)
		c.Assert(err, NotNil)
		c.Assert(terror.ErrRelayLogFileSizeSmaller.Equal(err), IsTrue)
	}

	// return changed file in meta
	{
		cfg := &BinlogReaderConfig{RelayDir: "", Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, false, "")
		t.setActiveRelayLog(r.relay, "next", "next", 0)
		state := t.createBinlogFileParseState(c, subDir, relayFiles[0], size, true)
		needSwitch, reParse, err := r.waitBinlogChanged(context.Background(), state)
		c.Assert(needSwitch, IsFalse)
		c.Assert(reParse, IsFalse)
		c.Assert(err, IsNil)
	}

	// file increased when checking meta
	{
		cfg := &BinlogReaderConfig{RelayDir: "", Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, false, "")
		t.setActiveRelayLog(r.relay, "next", "next", 0)
		state := t.createBinlogFileParseState(c, subDir, relayFiles[0], 0, true)
		needSwitch, reParse, err := r.waitBinlogChanged(context.Background(), state)
		c.Assert(needSwitch, IsFalse)
		c.Assert(reParse, IsTrue)
		c.Assert(err, IsNil)
	}

	// context timeout (no new write)
	{
		newCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		cfg := &BinlogReaderConfig{RelayDir: "", Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, false, "current-uuid") // no notify
		t.setActiveRelayLog(r.relay, "current-uuid", relayFiles[0], 0)
		state := t.createBinlogFileParseState(c, subDir, relayFiles[0], 0, true)
		needSwitch, reParse, err := r.waitBinlogChanged(newCtx, state)
		c.Assert(needSwitch, IsFalse)
		c.Assert(reParse, IsFalse)
		c.Assert(err, IsNil)
	}

	// this dir is different from the dir of current binlog file, but for test it doesn't matter
	relayDir := c.MkDir()
	t.writeUUIDs(c, relayDir, []string{"xxx.000001", "invalid uuid"})

	// getSwitchPath return error(invalid uuid file)
	{
		cfg := &BinlogReaderConfig{RelayDir: relayDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, false, "xxx.000001")
		t.setActiveRelayLog(r.relay, "next", "next", 0)
		state := t.createBinlogFileParseState(c, subDir, relayFiles[1], 0, true)
		needSwitch, reParse, err := r.waitBinlogChanged(context.Background(), state)
		c.Assert(needSwitch, IsFalse)
		c.Assert(reParse, IsFalse)
		c.Assert(terror.ErrRelayParseUUIDSuffix.Equal(err), IsTrue)
	}

	t.writeUUIDs(c, relayDir, []string{"xxx.000001", "xxx.000002"})
	_ = os.MkdirAll(filepath.Join(relayDir, "xxx.000002"), 0o700)
	_ = os.WriteFile(filepath.Join(relayDir, "xxx.000002", "mysql.000001"), nil, 0o600)

	// binlog dir switched, but last file not exists so failed to check change of file length
	// should not happen in real, just for branch test
	{
		cfg := &BinlogReaderConfig{RelayDir: relayDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, false, "xxx.000001")
		t.setActiveRelayLog(r.relay, "next", "next", 0)
		state := t.createBinlogFileParseState(c, subDir, relayFiles[1], 0, true)
		_ = os.Remove(relayPaths[1])
		needSwitch, reParse, err := r.waitBinlogChanged(context.Background(), state)
		c.Assert(needSwitch, IsFalse)
		c.Assert(reParse, IsFalse)
		c.Assert(terror.ErrGetRelayLogStat.Equal(err), IsTrue)
	}

	err1 = os.WriteFile(relayPaths[1], nil, 0o600)
	c.Assert(err1, IsNil)

	// binlog dir switched, but last file smaller
	{
		cfg := &BinlogReaderConfig{RelayDir: relayDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, false, "xxx.000001")
		t.setActiveRelayLog(r.relay, "next", "next", 0)
		state := t.createBinlogFileParseState(c, subDir, relayFiles[1], size, true)
		needSwitch, reParse, err := r.waitBinlogChanged(context.Background(), state)
		c.Assert(needSwitch, IsFalse)
		c.Assert(reParse, IsFalse)
		c.Assert(terror.ErrRelayLogFileSizeSmaller.Equal(err), IsTrue)
	}

	err1 = os.WriteFile(relayPaths[1], data, 0o600)
	c.Assert(err1, IsNil)

	// binlog dir switched, but last file bigger
	{
		cfg := &BinlogReaderConfig{RelayDir: relayDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, false, "xxx.000001")
		t.setActiveRelayLog(r.relay, "next", "next", 0)
		state := t.createBinlogFileParseState(c, subDir, relayFiles[1], 0, true)
		needSwitch, reParse, err := r.waitBinlogChanged(context.Background(), state)
		c.Assert(needSwitch, IsFalse)
		c.Assert(reParse, IsTrue)
		c.Assert(err, IsNil)
	}

	// binlog dir switched, but last file not changed
	{
		cfg := &BinlogReaderConfig{RelayDir: relayDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, false, "xxx.000001")
		t.setActiveRelayLog(r.relay, "next", "next", 0)
		state := t.createBinlogFileParseState(c, subDir, relayFiles[1], size, true)
		needSwitch, reParse, err := r.waitBinlogChanged(context.Background(), state)
		c.Assert(needSwitch, IsTrue)
		c.Assert(reParse, IsFalse)
		c.Assert(err, IsNil)
	}

	// got notified and active pos > current read pos
	{
		cfg := &BinlogReaderConfig{RelayDir: relayDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, true, "xxx.000001")
		t.setActiveRelayLog(r.relay, r.currentSubDir, relayFiles[1], size)
		state := t.createBinlogFileParseState(c, subDir, relayFiles[1], 0, true)
		needSwitch, reParse, err := r.waitBinlogChanged(context.Background(), state)
		c.Assert(needSwitch, IsFalse)
		c.Assert(reParse, IsTrue)
		c.Assert(err, IsNil)
	}

	// got notified but not active
	{
		cfg := &BinlogReaderConfig{RelayDir: relayDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, true, "xxx.000001")
		relay := r.relay.(*Relay)
		relay.writer = &mockFileWriterForActiveTest{cases: []mockActiveCase{
			{true, 0},
			{false, 0},
		}}
		state := t.createBinlogFileParseState(c, subDir, relayFiles[1], 0, true)
		needSwitch, reParse, err := r.waitBinlogChanged(context.Background(), state)
		c.Assert(needSwitch, IsFalse)
		c.Assert(reParse, IsTrue)
		c.Assert(err, IsNil)
	}

	// got notified, first notified is active but has already read to that offset
	// second notify, got new data
	{
		cfg := &BinlogReaderConfig{RelayDir: relayDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, true, "xxx.000001")
		r.notifyCh = make(chan interface{}, 2)
		r.notifyCh <- struct{}{}
		r.notifyCh <- struct{}{}
		relay := r.relay.(*Relay)
		relay.writer = &mockFileWriterForActiveTest{cases: []mockActiveCase{
			{true, 0},
			{true, 0},
			{true, size},
		}}
		state := t.createBinlogFileParseState(c, subDir, relayFiles[1], 0, true)
		needSwitch, reParse, err := r.waitBinlogChanged(context.Background(), state)
		c.Assert(needSwitch, IsFalse)
		c.Assert(reParse, IsTrue)
		c.Assert(err, IsNil)
	}
}

func (t *testReaderSuite) TestGetSwitchPath(c *C) {
	var (
		relayDir = c.MkDir()
		UUIDs    = []string{
			"53ea0ed1-9bf8-11e6-8bea-64006a897c73.000001",
			"53ea0ed1-9bf8-11e6-8bea-64006a897c72.000002",
			"53ea0ed1-9bf8-11e6-8bea-64006a897c71.000003",
		}
		currentUUID = UUIDs[len(UUIDs)-1] // no next UUID
	)

	UUIDs = append(UUIDs, "invalid.uuid")

	// invalid UUID in UUIDs, error
	t.writeUUIDs(c, relayDir, UUIDs)
	{
		cfg := &BinlogReaderConfig{RelayDir: relayDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, true, currentUUID)
		switchPath, err := r.getSwitchPath()
		c.Assert(switchPath, IsNil)
		c.Assert(terror.ErrRelayParseUUIDSuffix.Equal(err), IsTrue)
	}

	UUIDs = UUIDs[:len(UUIDs)-1] // remove the invalid UUID
	t.writeUUIDs(c, relayDir, UUIDs)

	// no next sub directory
	{
		cfg := &BinlogReaderConfig{RelayDir: relayDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, true, UUIDs[0])
		switchPath, err := r.getSwitchPath()
		c.Assert(switchPath, IsNil)
		c.Assert(err, ErrorMatches, fmt.Sprintf(".*%s.*(no such file or directory|The system cannot find the file specified).*", UUIDs[1]))
	}

	err1 := os.Mkdir(filepath.Join(relayDir, UUIDs[1]), 0o700)
	c.Assert(err1, IsNil)

	// uuid directory exist, but no binlog file inside
	{
		cfg := &BinlogReaderConfig{RelayDir: relayDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, true, UUIDs[0])
		switchPath, err := r.getSwitchPath()
		c.Assert(switchPath, IsNil)
		c.Assert(err, IsNil)
	}

	// create a relay log file in the next sub directory
	nextBinlogPath := filepath.Join(relayDir, UUIDs[1], "mysql-bin.000001")
	err1 = os.MkdirAll(filepath.Dir(nextBinlogPath), 0o700)
	c.Assert(err1, IsNil)
	err1 = os.WriteFile(nextBinlogPath, nil, 0o600)
	c.Assert(err1, IsNil)

	// switch to the next
	{
		cfg := &BinlogReaderConfig{RelayDir: relayDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, true, UUIDs[0])
		switchPath, err := r.getSwitchPath()
		c.Assert(switchPath.nextUUID, Equals, UUIDs[1])
		c.Assert(switchPath.nextBinlogName, Equals, filepath.Base(nextBinlogPath))
		c.Assert(err, IsNil)
	}
}
