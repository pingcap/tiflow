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
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/pingcap/tiflow/dm/pkg/binlog/reader"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var parseFileTimeout = 10 * time.Second

func TestReaderSuite(t *testing.T) {
	suite.Run(t, new(testReaderSuite))
}

type testReaderSuite struct {
	suite.Suite
	lastPos  uint32
	lastGTID gmysql.GTIDSet
}

func (t *testReaderSuite) SetupSuite() {
	var err error
	t.lastPos = 0
	t.lastGTID, err = gtid.ParserGTID(gmysql.MySQLFlavor, "ba8f633f-1f15-11eb-b1c7-0242ac110002:1")
	t.Require().NoError(err)
	t.Require().NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/relay/SetHeartbeatInterval", "return(10000)"))
}

func (t *testReaderSuite) TearDownSuite() {
	t.Require().NoError(failpoint.Disable("github.com/pingcap/tiflow/dm/relay/SetHeartbeatInterval"))
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

func (t *testReaderSuite) createBinlogFileParseState(relayLogDir, relayLogFile string, offset int64, possibleLast bool) *binlogFileParseState {
	fullPath := filepath.Join(relayLogDir, relayLogFile)
	f, err := os.Open(fullPath)
	t.Require().NoError(err)

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

func (t *testReaderSuite) TestparseFileAsPossibleFileNotExist() {
	var (
		baseDir     = t.T().TempDir()
		currentUUID = "b60868af-5a6f-11e9-9ea3-0242ac160006.000001"
		filename    = "test-mysql-bin.000001"
		relayDir    = path.Join(baseDir, currentUUID)
	)
	cfg := &BinlogReaderConfig{RelayDir: baseDir, Flavor: gmysql.MySQLFlavor}
	r := newBinlogReaderForTest(log.L(), cfg, true, currentUUID)
	needSwitch, lastestPos, err := r.parseFileAsPossible(context.Background(), nil, filename, 4, relayDir, true, false)
	t.Require().False(needSwitch)
	t.Require().Equal(int64(0), lastestPos)
	t.Require().Error(err)
	t.Require().Regexp(".*no such file or directory.*", err.Error())
}

func (t *testReaderSuite) TestParseFileBase() {
	var (
		filename         = "test-mysql-bin.000001"
		baseDir          = t.T().TempDir()
		possibleLast     = false
		baseEvents, _, _ = t.genBinlogEvents(t.lastPos, t.lastGTID)
		s                = newLocalStreamer()
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// change to valid currentSubDir
	currentUUID := "b60868af-5a6f-11e9-9ea3-0242ac160006.000001"
	relayDir := filepath.Join(baseDir, currentUUID)
	fullPath := filepath.Join(relayDir, filename)
	err1 := os.MkdirAll(relayDir, 0o700)
	t.Require().Nil(err1)
	f, err1 := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY, 0o600)
	t.Require().Nil(err1)
	defer f.Close()

	// empty relay log file, got EOF when reading format description event separately and possibleLast = false
	{
		testCtx, testCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer testCancel()
		cfg := &BinlogReaderConfig{RelayDir: baseDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, true, currentUUID)
		t.setActiveRelayLog(r.relay, currentUUID, filename, 0)
		state := t.createBinlogFileParseState(relayDir, filename, 100, possibleLast)
		needSwitch, needReParse, err := r.parseFile(testCtx, s, true, state)
		t.Require().Equal(io.EOF, errors.Cause(err))
		t.Require().False(needSwitch)
		t.Require().False(needReParse)
		t.Require().Equal(int64(100), state.latestPos)
		t.Require().False(state.formatDescEventRead)
		t.Require().Equal(false, state.skipGTID)
	}

	// write some events to binlog file
	_, err1 = f.Write(replication.BinLogFileHeader)
	t.Require().Nil(err1)
	for _, ev := range baseEvents {
		_, err1 = f.Write(ev.RawData)
		t.Require().Nil(err1)
	}
	fileSize, _ := f.Seek(0, io.SeekCurrent)

	t.purgeStreamer(s)

	// base test with only one valid binlog file
	{
		testCtx, testCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer testCancel()
		cfg := &BinlogReaderConfig{RelayDir: baseDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, true, currentUUID)
		t.setActiveRelayLog(r.relay, currentUUID, filename, fileSize)
		state := t.createBinlogFileParseState(relayDir, filename, 4, possibleLast)
		needSwitch, needReParse, err := r.parseFile(testCtx, s, true, state)
		t.Require().NoError(err)
		t.Require().False(needSwitch)
		t.Require().False(needReParse)
		t.Require().Equal(int64(baseEvents[len(baseEvents)-1].Header.LogPos), state.latestPos)
		t.Require().True(state.formatDescEventRead)
		t.Require().False(state.skipGTID)

		// try get events back, firstParse should have fake RotateEvent
		var fakeRotateEventCount int
		i := 0
		for {
			ev, err2 := s.GetEvent(ctx)
			t.Require().Nil(err2)
			if ev.Header.Timestamp == 0 || ev.Header.LogPos == 0 {
				if ev.Header.EventType == replication.ROTATE_EVENT {
					fakeRotateEventCount++
				}
				continue // ignore fake event
			}
			t.Require().Equal(baseEvents[i], ev)
			i++
			if i >= len(baseEvents) {
				break
			}
		}
		t.Require().Equal(1, fakeRotateEventCount)
		t.verifyNoEventsInStreamer(s)
	}

	// try get events back, since firstParse=false, should have no fake RotateEvent
	{
		testCtx, testCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer testCancel()
		cfg := &BinlogReaderConfig{RelayDir: baseDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, true, currentUUID)
		t.setActiveRelayLog(r.relay, currentUUID, filename, fileSize)
		state := t.createBinlogFileParseState(relayDir, filename, 4, possibleLast)
		needSwitch, needReParse, err := r.parseFile(testCtx, s, false, state)
		t.Require().NoError(err)
		t.Require().False(needSwitch)
		t.Require().False(needReParse)
		t.Require().Equal(int64(baseEvents[len(baseEvents)-1].Header.LogPos), state.latestPos)
		t.Require().True(state.formatDescEventRead)
		t.Require().Equal(false, state.skipGTID)
		fakeRotateEventCount := 0
		i := 0
		for {
			ev, err2 := s.GetEvent(ctx)
			t.Require().Nil(err2)
			if ev.Header.Timestamp == 0 || ev.Header.LogPos == 0 {
				if ev.Header.EventType == replication.ROTATE_EVENT {
					fakeRotateEventCount++
				}
				continue // ignore fake event
			}
			t.Require().Equal(baseEvents[i], ev)
			i++
			if i >= len(baseEvents) {
				break
			}
		}
		t.Require().Equal(0, fakeRotateEventCount)
		t.verifyNoEventsInStreamer(s)
	}

	// generate another non-fake RotateEvent
	rotateEv, err := event.GenRotateEvent(baseEvents[0].Header, uint32(fileSize), []byte("mysql-bin.888888"), 4)
	t.Require().NoError(err)
	_, err = f.Write(rotateEv.RawData)
	t.Require().NoError(err)
	fileSize, _ = f.Seek(0, io.SeekCurrent)

	// latest is still the end_log_pos of the last event, not the next relay file log file's position
	{
		testCtx, testCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer testCancel()
		cfg := &BinlogReaderConfig{RelayDir: baseDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, true, currentUUID)
		t.setActiveRelayLog(r.relay, currentUUID, filename, fileSize)
		state := t.createBinlogFileParseState(relayDir, filename, 4, possibleLast)
		needSwitch, needReParse, err := r.parseFile(testCtx, s, true, state)
		t.Require().NoError(err)
		t.Require().False(needSwitch)
		t.Require().False(needReParse)
		t.Require().Equal(int64(rotateEv.Header.LogPos), state.latestPos)
		t.Require().True(state.formatDescEventRead)
		t.Require().Equal(false, state.skipGTID)
		t.purgeStreamer(s)
	}

	// parse from offset > 4
	{
		testCtx, testCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer testCancel()
		cfg := &BinlogReaderConfig{RelayDir: baseDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, true, currentUUID)
		t.setActiveRelayLog(r.relay, currentUUID, filename, fileSize)
		offset := int64(rotateEv.Header.LogPos - rotateEv.Header.EventSize)
		state := t.createBinlogFileParseState(relayDir, filename, offset, possibleLast)
		needSwitch, needReParse, err := r.parseFile(testCtx, s, false, state)
		t.Require().NoError(err)
		t.Require().False(needSwitch)
		t.Require().False(needReParse)
		t.Require().Equal(int64(rotateEv.Header.LogPos), state.latestPos)
		t.Require().True(state.formatDescEventRead)
		t.Require().Equal(false, state.skipGTID)

		// should only get a RotateEvent
		i := 0
		for {
			ev, err2 := s.GetEvent(ctx)
			t.Require().Nil(err2)
			switch ev.Header.EventType {
			case replication.ROTATE_EVENT:
				t.Require().Equal(rotateEv.RawData, ev.RawData)
				i++
			default:
				t.T().Fatalf("got unexpected event %+v", ev.Header)
			}
			if i >= 1 {
				break
			}
		}
		t.verifyNoEventsInStreamer(s)
	}
}

func (t *testReaderSuite) TestParseFileRelayNeedSwitchSubDir() {
	var (
		filename          = "test-mysql-bin.000001"
		nextFilename      = "test-mysql-bin.666888"
		notUsedGTIDSetStr = t.lastGTID.String()
		baseDir           = t.T().TempDir()
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
	t.Require().NoError(err)
	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY, 0o600)
	t.Require().NoError(err)
	defer f.Close()
	_, err = f.Write(replication.BinLogFileHeader)
	offset = 4
	t.Require().NoError(err)
	t.createMetaFile(relayDir, filename, uint32(offset), notUsedGTIDSetStr)

	r.subDirs = []string{currentUUID, switchedUUID}
	t.writeUUIDs(baseDir, r.subDirs)
	err = os.MkdirAll(nextRelayDir, 0o700)
	t.Require().NoError(err)
	err = os.WriteFile(nextFullPath, replication.BinLogFileHeader, 0o600)
	t.Require().NoError(err)

	// has relay log file in next sub directory, need to switch
	ctx2, cancel2 := context.WithTimeout(context.Background(), parseFileTimeout)
	defer cancel2()
	t.createMetaFile(nextRelayDir, filename, uint32(offset), notUsedGTIDSetStr)
	state := t.createBinlogFileParseState(relayDir, filename, offset, possibleLast)
	state.formatDescEventRead = true
	t.setActiveRelayLog(r.relay, "next", "next", 4)
	needSwitch, needReParse, err := r.parseFile(ctx2, s, true, state)
	t.Require().NoError(err)
	t.Require().True(needSwitch)
	t.Require().False(needReParse)
	t.Require().Equal(int64(4), state.latestPos)
	t.Require().True(state.formatDescEventRead)
	t.Require().Equal(false, state.skipGTID)
	t.purgeStreamer(s)

	// NOTE: if we want to test the returned `needReParse` of `needSwitchSubDir`,
	// then we need to mock `fileSizeUpdated` or inject some delay or delay.
}

func (t *testReaderSuite) TestParseFileRelayWithIgnorableError() {
	var (
		filename          = "test-mysql-bin.000001"
		notUsedGTIDSetStr = t.lastGTID.String()
		baseDir           = t.T().TempDir()
		possibleLast      = true
		baseEvents, _, _  = t.genBinlogEvents(t.lastPos, t.lastGTID)
		currentUUID       = "b60868af-5a6f-11e9-9ea3-0242ac160006.000001"
		relayDir          = filepath.Join(baseDir, currentUUID)
		fullPath          = filepath.Join(relayDir, filename)
		s                 = newLocalStreamer()
		cfg               = &BinlogReaderConfig{RelayDir: baseDir, Flavor: gmysql.MySQLFlavor}
	)

	// create the current relay log file and write some events
	err := os.MkdirAll(relayDir, 0o700)
	t.Require().NoError(err)
	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY, 0o600)
	t.Require().NoError(err)
	defer f.Close()
	t.createMetaFile(relayDir, filename, 0, notUsedGTIDSetStr)

	_, err = f.Write(replication.BinLogFileHeader)
	t.Require().NoError(err)
	_, err = f.Write(baseEvents[0].RawData[:replication.EventHeaderSize])
	t.Require().NoError(err)

	// meet io.EOF error when read event and ignore it.
	{
		r := newBinlogReaderForTest(log.L(), cfg, true, currentUUID)
		state := t.createBinlogFileParseState(relayDir, filename, 4, possibleLast)
		state.formatDescEventRead = true
		t.setActiveRelayLog(r.relay, currentUUID, filename, 100)
		needSwitch, needReParse, err := r.parseFile(context.Background(), s, true, state)
		t.Require().NoError(err)
		t.Require().False(needSwitch)
		t.Require().True(needReParse)
		t.Require().Equal(int64(4), state.latestPos)
		t.Require().True(state.formatDescEventRead)
		t.Require().Equal(false, state.skipGTID)
	}
}

func (t *testReaderSuite) TestUpdateUUIDs() {
	var (
		baseDir = t.T().TempDir()
		cfg     = &BinlogReaderConfig{RelayDir: baseDir, Flavor: gmysql.MySQLFlavor}
		r       = newBinlogReaderForTest(log.L(), cfg, true, "")
	)
	t.Require().Len(r.subDirs, 0)

	// index file not exists, got nothing
	err := r.updateSubDirs()
	t.Require().NoError(err)
	t.Require().Len(r.subDirs, 0)

	// valid UUIDs in the index file, got them back
	UUIDs := []string{
		"b60868af-5a6f-11e9-9ea3-0242ac160006.000001",
		"b60868af-5a6f-11e9-9ea3-0242ac160007.000002",
	}
	uuidBytes := t.uuidListToBytes(UUIDs)
	err = os.WriteFile(r.indexPath, uuidBytes, 0o600)
	t.Require().NoError(err)

	err = r.updateSubDirs()
	t.Require().NoError(err)
	t.Require().Equal(UUIDs, r.subDirs)
}

func (t *testReaderSuite) TestStartSyncByPos() {
	var (
		filenamePrefix                = "test-mysql-bin.00000"
		notUsedGTIDSetStr             = t.lastGTID.String()
		baseDir                       = t.T().TempDir()
		baseEvents, lastPos, lastGTID = t.genBinlogEvents(t.lastPos, t.lastGTID)
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
	t.Require().NoError(err)
	for _, ev := range baseEvents {
		_, err = eventsBuf.Write(ev.RawData)
		t.Require().NoError(err)
	}

	// create the index file
	uuidBytes := t.uuidListToBytes(UUIDs)
	err = os.WriteFile(r.indexPath, uuidBytes, 0o600)
	t.Require().NoError(err)

	// create sub directories
	for _, uuid := range UUIDs {
		subDir := filepath.Join(baseDir, uuid)
		err = os.MkdirAll(subDir, 0o700)
		t.Require().NoError(err)
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
				t.Require().Nil(err2)
				content = append(content, rotateEvent.RawData...)
			}
			err = os.WriteFile(filename, content, 0o600)
			t.Require().NoError(err)
		}
		t.createMetaFile(path.Join(baseDir, UUIDs[i]), filenamePrefix+strconv.Itoa(i+1),
			startPos.Pos, notUsedGTIDSetStr)
	}

	// start the reader
	s, err := r.StartSyncByPos(startPos)
	t.Require().NoError(err)

	// get events from the streamer
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	obtainBaseEvents := readNEvents(ctx, t.T(), s, (1+2+3)*(len(baseEvents)+1)-1, false)
	t.verifyNoEventsInStreamer(s)
	// verify obtain base events
	for i := 0; i < len(obtainBaseEvents); i += len(baseEvents) {
		t.Require().Equal(baseEvents, obtainBaseEvents[i:i+len(baseEvents)])
		// skip rotate event which is not added in baseEvents
		i++
	}

	// 2. write more events to the last file
	lastFilename := filepath.Join(baseDir, UUIDs[2], filenamePrefix+strconv.Itoa(3))
	extraEvents, _, _ := t.genBinlogEvents(lastPos, lastGTID)
	lastF, err := os.OpenFile(lastFilename, os.O_WRONLY|os.O_APPEND, 0o600)
	t.Require().NoError(err)
	defer lastF.Close()
	for _, ev := range extraEvents {
		_, err = lastF.Write(ev.RawData)
		t.Require().NoError(err)
	}
	r.notifyCh <- struct{}{}

	// read extra events back
	obtainExtraEvents := make([]*replication.BinlogEvent, 0, len(extraEvents))
	for {
		ev, err2 := s.GetEvent(ctx)
		t.Require().Nil(err2)
		if ev.Header.Timestamp == 0 || ev.Header.LogPos == 0 || ev.Header.EventType == replication.FORMAT_DESCRIPTION_EVENT {
			continue // ignore fake event and FormatDescriptionEvent, go-mysql may send extra FormatDescriptionEvent
		}
		obtainExtraEvents = append(obtainExtraEvents, ev)
		if len(obtainExtraEvents) == cap(obtainExtraEvents) {
			break
		}
	}
	t.verifyNoEventsInStreamer(s)

	// verify obtain extra events
	t.Require().Equal(extraEvents, obtainExtraEvents)

	// 3. create new file in the last directory
	lastFilename = filepath.Join(baseDir, UUIDs[2], filenamePrefix+strconv.Itoa(4))
	err = os.WriteFile(lastFilename, eventsBuf.Bytes(), 0o600)
	t.Require().NoError(err)
	t.createMetaFile(path.Join(baseDir, UUIDs[2]), lastFilename, lastPos, notUsedGTIDSetStr)
	r.notifyCh <- struct{}{}

	obtainExtraEvents2 := make([]*replication.BinlogEvent, 0, len(baseEvents)-1)
	for {
		ev, err2 := s.GetEvent(ctx)
		t.Require().Nil(err2)
		if ev.Header.Timestamp == 0 || ev.Header.LogPos == 0 || ev.Header.EventType == replication.FORMAT_DESCRIPTION_EVENT {
			continue // ignore fake event and FormatDescriptionEvent, go-mysql may send extra FormatDescriptionEvent
		}
		obtainExtraEvents2 = append(obtainExtraEvents2, ev)
		if len(obtainExtraEvents2) == cap(obtainExtraEvents2) {
			break
		}
	}
	t.verifyNoEventsInStreamer(s)

	// verify obtain extra events
	t.Require().Equal(baseEvents[1:], obtainExtraEvents2)

	// NOTE: load new UUIDs dynamically not supported yet

	// close the reader
	r.Close()
}

func readNEvents(ctx context.Context, t *testing.T, s reader.Streamer, l int, tolerateMayDup bool) []*replication.BinlogEvent {
	var result []*replication.BinlogEvent
	for {
		ev, err2 := s.GetEvent(ctx)
		if tolerateMayDup {
			if err2 != nil {
				require.True(t, errors.ErrorEqual(ErrorMaybeDuplicateEvent, err2))
				continue
			}
		} else {
			require.NoError(t, err2)
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

func (t *testReaderSuite) TestStartSyncByGTID() {
	var (
		baseDir         = t.T().TempDir()
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
	uuidBytes := t.uuidListToBytes(r.subDirs)
	err := os.WriteFile(r.indexPath, uuidBytes, 0o600)
	t.Require().NoError(err)

	var allEvents []*replication.BinlogEvent
	var allResults []string
	var eventsNumOfFirstServer int

	// generate binlog file
	for i, subDir := range testCase {
		lastPos = 4
		lastGTID, err = gtid.ParserGTID(gmysql.MySQLFlavor, subDir.gtidStr)
		t.Require().NoError(err)
		uuidDir := path.Join(baseDir, subDir.uuid)
		err = os.MkdirAll(uuidDir, 0o700)
		t.Require().NoError(err)

		for _, fileEventResult := range subDir.fileEventResult {
			eventTypes := []replication.EventType{}
			for _, eventResult := range fileEventResult.eventResults {
				eventTypes = append(eventTypes, eventResult.eventType)
				if len(eventResult.result) != 0 {
					allResults = append(allResults, eventResult.result)
				}
			}

			// generate events
			events, lastPos, lastGTID, previousGset = t.genEvents(eventTypes, lastPos, lastGTID, previousGset)
			allEvents = append(allEvents, events...)

			// write binlog file
			f, err2 := os.OpenFile(path.Join(uuidDir, fileEventResult.filename), os.O_CREATE|os.O_WRONLY, 0o600)
			t.Require().Nil(err2)
			_, err = f.Write(replication.BinLogFileHeader)
			t.Require().NoError(err)
			for _, ev := range events {
				_, err = f.Write(ev.RawData)
				t.Require().NoError(err)
			}
			f.Close()
			t.createMetaFile(uuidDir, fileEventResult.filename, lastPos, previousGset.String())
		}
		if i == 0 {
			eventsNumOfFirstServer = len(allEvents)
		}
	}

	startGTID, err := gtid.ParserGTID(gmysql.MySQLFlavor, "")
	t.Require().NoError(err)
	s, err := r.StartSyncByGTID(startGTID.Clone())
	t.Require().NoError(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	obtainBaseEvents := readNEvents(ctx, t.T(), s, len(allEvents), true)

	preGset, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, "")
	t.Require().NoError(err)

	gtidEventCount := 0
	for i, ev := range obtainBaseEvents {
		t.Require().Equal(allEvents[i].Header, ev.Header)
		if _, ok := ev.Event.(*replication.GTIDEvent); ok {
			gtidStr, _ := event.GetGTIDStr(ev)
			t.Require().NoError(preGset.Update(gtidStr))

			// get pos by preGset
			pos, err2 := r.getPosByGTID(preGset.Clone())
			t.Require().Nil(err2)
			// check result
			t.Require().Equal(allResults[gtidEventCount], pos.Name)
			t.Require().Equal(uint32(4), pos.Pos)
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
	t.Require().NoError(err)

	// StartSyncByGtid exclude first uuid
	s, err = r.StartSyncByGTID(excludeGset)
	t.Require().NoError(err)
	obtainBaseEvents = readNEvents(ctx, t.T(), s, eventsNumOfFirstServer, true)

	gset := excludeGset.Clone()
	// should not receive any event not from first server
	for i, event := range obtainBaseEvents {
		switch event.Header.EventType {
		case replication.HEARTBEAT_EVENT:
			t.T().FailNow()
		case replication.GTID_EVENT:
			// check gtid event comes from first uuid subdir
			ev, _ := event.Event.(*replication.GTIDEvent)
			u, _ := uuid.FromBytes(ev.SID)
			t.Require().Equal(includeServerUUID, u.String())
			t.Require().Equal(allEvents[i].Header, event.Header)
			t.Require().NoError(gset.Update(fmt.Sprintf("%s:%d", u.String(), ev.GNO)))
		default:
			t.Require().Equal(allEvents[i].Header, event.Header)
		}
	}
	// gset same as preGset now
	t.Require().True(gset.Equal(preGset))

	// purge first uuid subdir's first binlog file
	t.Require().Nil(os.Remove(path.Join(baseDir, includeUUID, "mysql.000001")))

	r.Close()
	r = newBinlogReaderForTest(log.L(), cfg, true, "")
	_, err = r.StartSyncByGTID(preGset)
	t.Require().NoError(err)

	r.Close()
	r = newBinlogReaderForTest(log.L(), cfg, true, "")
	_, err = r.StartSyncByGTID(excludeGset)
	// error because file has been purge
	t.Require().True(terror.ErrNoRelayPosMatchGTID.Equal(err))

	// purge first uuid subdir
	t.Require().Nil(os.RemoveAll(path.Join(baseDir, includeUUID)))

	r.Close()
	r = newBinlogReaderForTest(log.L(), cfg, true, "")
	_, err = r.StartSyncByGTID(preGset)
	t.Require().NoError(err)

	r.Close()
	r = newBinlogReaderForTest(log.L(), cfg, true, "")
	_, err = r.StartSyncByGTID(excludeGset)
	// error because subdir has been purge
	t.Require().Error(err)
	t.Require().Regexp(".*no such file or directory.*", err.Error())
	cancel()
}

func (t *testReaderSuite) TestStartSyncError() {
	var (
		baseDir = t.T().TempDir()
		UUIDs   = []string{
			"b60868af-5a6f-11e9-9ea3-0242ac160006.000001",
		}
		cfg      = &BinlogReaderConfig{RelayDir: baseDir, Flavor: gmysql.MySQLFlavor}
		startPos = gmysql.Position{Name: "test-mysql-bin|000001.000001"} // from the first relay log file in the first sub directory
	)

	r := newBinlogReaderForTest(log.L(), cfg, true, "")
	err := r.checkRelayPos(startPos)
	t.Require().Error(err)
	t.Require().Regexp(".*empty UUIDs not valid.*", err.Error())

	// no startup pos specified
	s, err := r.StartSyncByPos(gmysql.Position{})
	t.Require().True(terror.ErrBinlogFileNotSpecified.Equal(err))
	t.Require().Nil(s)

	// empty UUIDs
	s, err = r.StartSyncByPos(startPos)
	t.Require().Error(err)
	t.Require().Regexp(".*empty UUIDs not valid.*", err.Error())
	t.Require().Nil(s)

	s, err = r.StartSyncByGTID(t.lastGTID.Clone())
	t.Require().Error(err)
	t.Require().Regexp(".*no relay pos match gtid.*", err.Error())
	t.Require().Nil(s)

	// write UUIDs into index file
	r = newBinlogReaderForTest(log.L(), cfg, true, "") // create a new reader
	uuidBytes := t.uuidListToBytes(UUIDs)
	err = os.WriteFile(r.indexPath, uuidBytes, 0o600)
	t.Require().NoError(err)

	// the startup relay log file not found
	s, err = r.StartSyncByPos(startPos)
	t.Require().Error(err)
	t.Require().Regexp(fmt.Sprintf(".*%s.*not found.*", startPos.Name), err.Error())
	t.Require().Nil(s)

	s, err = r.StartSyncByGTID(t.lastGTID.Clone())
	t.Require().Error(err)
	t.Require().Regexp(".*no such file or directory.*", err.Error())
	t.Require().Nil(s)

	// can not re-start the reader
	r.running = true
	s, err = r.StartSyncByPos(startPos)
	t.Require().True(terror.ErrReaderAlreadyRunning.Equal(err))
	t.Require().Nil(s)
	r.Close()

	r.running = true
	s, err = r.StartSyncByGTID(t.lastGTID.Clone())
	t.Require().True(terror.ErrReaderAlreadyRunning.Equal(err))
	t.Require().Nil(s)
	r.Close()

	// too big startPos
	uuid := UUIDs[0]
	err = os.MkdirAll(filepath.Join(baseDir, uuid), 0o700)
	t.Require().NoError(err)
	parsedStartPosName := "test-mysql-bin.000001"
	relayLogFilePath := filepath.Join(baseDir, uuid, parsedStartPosName)
	err = os.WriteFile(relayLogFilePath, make([]byte, 100), 0o600)
	t.Require().NoError(err)
	startPos.Pos = 10000
	s, err = r.StartSyncByPos(startPos)
	t.Require().True(terror.ErrRelayLogGivenPosTooBig.Equal(err))
	t.Require().Nil(s)
}

func (t *testReaderSuite) TestAdvanceCurrentGTIDSet() {
	var (
		baseDir        = t.T().TempDir()
		cfg            = &BinlogReaderConfig{RelayDir: baseDir, Flavor: gmysql.MySQLFlavor}
		r              = newBinlogReaderForTest(log.L(), cfg, true, "")
		mysqlGset, _   = gmysql.ParseMysqlGTIDSet("b60868af-5a6f-11e9-9ea3-0242ac160006:1-6")
		mariadbGset, _ = gmysql.ParseMariadbGTIDSet("0-1-5")
	)
	r.prevGset = mysqlGset.Clone()
	r.currGset = nil
	notUpdated, err := r.advanceCurrentGtidSet("b60868af-5a6f-11e9-9ea3-0242ac160006:6")
	t.Require().NoError(err)
	t.Require().True(notUpdated)
	t.Require().True(mysqlGset.Equal(r.currGset))
	notUpdated, err = r.advanceCurrentGtidSet("b60868af-5a6f-11e9-9ea3-0242ac160006:7")
	t.Require().NoError(err)
	t.Require().False(notUpdated)
	t.Require().True(mysqlGset.Equal(r.prevGset))
	t.Require().Equal("b60868af-5a6f-11e9-9ea3-0242ac160006:1-7", r.currGset.String())

	r.cfg.Flavor = gmysql.MariaDBFlavor
	r.prevGset = mariadbGset.Clone()
	r.currGset = nil
	notUpdated, err = r.advanceCurrentGtidSet("0-1-3")
	t.Require().NoError(err)
	t.Require().True(notUpdated)
	t.Require().True(mariadbGset.Equal(r.currGset))
	notUpdated, err = r.advanceCurrentGtidSet("0-1-6")
	t.Require().NoError(err)
	t.Require().False(notUpdated)
	t.Require().True(mariadbGset.Equal(r.prevGset))
	t.Require().Equal("0-1-6", r.currGset.String())
}

func (t *testReaderSuite) TestReParseUsingGTID() {
	var (
		baseDir   = t.T().TempDir()
		cfg       = &BinlogReaderConfig{RelayDir: baseDir, Flavor: gmysql.MySQLFlavor}
		r         = newBinlogReaderForTest(log.L(), cfg, true, "")
		uuid      = "ba8f633f-1f15-11eb-b1c7-0242ac110002.000001"
		gtidStr   = "ba8f633f-1f15-11eb-b1c7-0242ac110002:1"
		file      = "mysql.000001"
		latestPos uint32
	)

	startGTID, err := gtid.ParserGTID(gmysql.MySQLFlavor, "")
	t.Require().NoError(err)
	lastGTID, err := gtid.ParserGTID(gmysql.MySQLFlavor, gtidStr)
	t.Require().NoError(err)

	// prepare a minimal relay log file
	t.Require().Nil(os.WriteFile(r.indexPath, []byte(uuid), 0o600))

	uuidDir := path.Join(baseDir, uuid)
	t.Require().Nil(os.MkdirAll(uuidDir, 0o700))
	f, err := os.OpenFile(path.Join(uuidDir, file), os.O_CREATE|os.O_WRONLY, 0o600)
	t.Require().NoError(err)
	_, err = f.Write(replication.BinLogFileHeader)
	t.Require().NoError(err)

	meta := LocalMeta{BinLogName: file, BinLogPos: latestPos, BinlogGTID: startGTID.String()}
	metaFile, err := os.Create(path.Join(uuidDir, utils.MetaFilename))
	t.Require().NoError(err)
	t.Require().NoError(toml.NewEncoder(metaFile).Encode(&meta))
	t.Require().NoError(metaFile.Close())

	// prepare some regular events,
	// FORMAT_DESC + PREVIOUS_GTIDS, some events generated from a DDL, some events generated from a DML
	genType := []replication.EventType{
		replication.PREVIOUS_GTIDS_EVENT,
		replication.QUERY_EVENT,
		replication.XID_EVENT,
	}
	events, _, _, latestGTIDSet := t.genEvents(genType, 4, lastGTID, startGTID)
	t.Require().Len(events, 1+1+2+5)

	// write FORMAT_DESC + PREVIOUS_GTIDS
	_, err = f.Write(events[0].RawData)
	t.Require().NoError(err)
	_, err = f.Write(events[1].RawData)
	t.Require().NoError(err)

	// we use latestGTIDSet to start sync, which means we already received all binlog events, so expect no DML/DDL
	s, err := r.StartSyncByGTID(latestGTIDSet)
	t.Require().NoError(err)
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
			t.Require().Nil(err2)
			t.Require().Equal(expected[ev.Header.LogPos], ev.Header.EventType)
		}
		wg.Done()
	}()

	for i := 2; i < len(events); i++ {
		// hope a second is enough to trigger needReParse
		time.Sleep(time.Second)
		_, err = f.Write(events[i].RawData)
		t.Require().NoError(err)
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

func (t *testReaderSuite) genBinlogEvents(latestPos uint32, latestGTID gmysql.GTIDSet) ([]*replication.BinlogEvent, uint32, gmysql.GTIDSet) {
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
		}
		events = make([]*replication.BinlogEvent, 0, 10)
	)

	if latestPos <= 4 { // generate a FormatDescriptionEvent if needed
		ev, err := event.GenFormatDescriptionEvent(header, 4)
		t.Require().NoError(err)
		latestPos = ev.Header.LogPos
		events = append(events, ev)
	}

	// for these tests, generates some DDL events is enough
	count := 5 + rand.Intn(5)
	for i := 0; i < count; i++ {
		evs, err := event.GenDDLEvents(gmysql.MySQLFlavor, 1, latestPos, latestGTID, fmt.Sprintf("db_%d", i), fmt.Sprintf("CREATE TABLE %d (c1 INT)", i), true, false, 0)
		t.Require().NoError(err)
		events = append(events, evs.Events...)
		latestPos = evs.LatestPos
		latestGTID = evs.LatestGTID
	}

	return events, latestPos, latestGTID
}

func (t *testReaderSuite) genEvents(
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
		t.Require().NoError(err)
		latestPos = ev.Header.LogPos
		events = append(events, ev)
	}

	for i, eventType := range eventTypes {
		switch eventType {
		case replication.QUERY_EVENT:
			evs, err := event.GenDDLEvents(gmysql.MySQLFlavor, 1, latestPos, latestGTID, fmt.Sprintf("db_%d", i), fmt.Sprintf("CREATE TABLE %d (c1 int)", i), true, false, 0)
			t.Require().NoError(err)
			events = append(events, evs.Events...)
			latestPos = evs.LatestPos
			latestGTID = evs.LatestGTID
			_, ok := evs.Events[0].Event.(*replication.GTIDEvent)
			t.Require().True(ok)
			gtidStr, _ := event.GetGTIDStr(evs.Events[0])
			err = originSet.Update(gtidStr)
			t.Require().NoError(err)
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
			t.Require().NoError(err)
			events = append(events, evs.Events...)
			latestPos = evs.LatestPos
			latestGTID = evs.LatestGTID
			_, ok := evs.Events[0].Event.(*replication.GTIDEvent)
			t.Require().True(ok)
			gtidStr, _ := event.GetGTIDStr(evs.Events[0])
			err = originSet.Update(gtidStr)
			t.Require().NoError(err)
		case replication.ROTATE_EVENT:
			ev, err := event.GenRotateEvent(header, latestPos, []byte("next_log"), 4)
			t.Require().NoError(err)
			events = append(events, ev)
			latestPos = 4
		case replication.PREVIOUS_GTIDS_EVENT:
			ev, err := event.GenPreviousGTIDsEvent(header, latestPos, pGset)
			t.Require().NoError(err)
			events = append(events, ev)
			latestPos = ev.Header.LogPos
		}
	}
	return events, latestPos, latestGTID, originSet
}

func (t *testReaderSuite) purgeStreamer(s reader.Streamer) {
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
			t.T().Fatalf("purge streamer with error %v", err)
		}
	}
}

func (t *testReaderSuite) verifyNoEventsInStreamer(s reader.Streamer) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	ev, err := s.GetEvent(ctx)
	if err != ctx.Err() {
		t.T().Fatalf("got event %v with error %v from streamer", ev, err)
	}
}

func (t *testReaderSuite) uuidListToBytes(uuids []string) []byte {
	var buf bytes.Buffer
	for _, uuid := range uuids {
		_, err := buf.WriteString(uuid)
		t.Require().NoError(err)
		_, err = buf.WriteString("\n")
		t.Require().NoError(err)
	}
	return buf.Bytes()
}

// nolint:unparam
func (t *testReaderSuite) writeUUIDs(relayDir string, uuids []string) []byte {
	indexPath := path.Join(relayDir, utils.UUIDIndexFilename)
	var buf bytes.Buffer
	for _, uuid := range uuids {
		_, err := buf.WriteString(uuid)
		t.Require().NoError(err)
		_, err = buf.WriteString("\n")
		t.Require().NoError(err)
	}

	// write the index file
	err := os.WriteFile(indexPath, buf.Bytes(), 0o600)
	t.Require().NoError(err)
	return buf.Bytes()
}

func (t *testReaderSuite) createMetaFile(relayDirPath, binlogFileName string, pos uint32, gtid string) {
	meta := LocalMeta{BinLogName: binlogFileName, BinLogPos: pos, BinlogGTID: gtid}
	metaFile, err2 := os.Create(path.Join(relayDirPath, utils.MetaFilename))
	t.Require().Nil(err2)
	err := toml.NewEncoder(metaFile).Encode(&meta)
	t.Require().NoError(err)
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

func (t *testReaderSuite) TestwaitBinlogChanged() {
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
	subDir := t.T().TempDir()
	// join the file path
	for i, rf := range relayFiles {
		relayPaths[i] = filepath.Join(subDir, rf)
		f, _ := os.Create(relayPaths[i])
		_ = f.Close()
	}

	rotateRelayFile := func(filename string) {
		meta := LocalMeta{BinLogName: filename, BinLogPos: binlogPos, BinlogGTID: binlogGTID}
		metaFile, err2 := os.Create(path.Join(subDir, utils.MetaFilename))
		t.Require().Nil(err2)
		err := toml.NewEncoder(metaFile).Encode(&meta)
		t.Require().NoError(err)
		_ = metaFile.Close()
	}

	// meta not found
	{
		cfg := &BinlogReaderConfig{RelayDir: "", Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, false, "")
		t.setActiveRelayLog(r.relay, "next", "next", 0)
		state := t.createBinlogFileParseState(subDir, relayFiles[0], 0, true)
		needSwitch, reParse, err := r.waitBinlogChanged(context.Background(), state)
		t.Require().False(needSwitch)
		t.Require().False(reParse)
		t.Require().Error(err)
		t.Require().Error(err)
		t.Require().Regexp(".*no such file or directory*", err.Error())
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
		t.Require().False(needSwitch)
		t.Require().False(reParse)
		t.Require().Error(err)
		t.Require().Error(err)
		t.Require().Regexp(".*no such file or directory*", err.Error())
	}

	// create the first relay file
	err1 := os.WriteFile(relayPaths[0], data, 0o600)
	t.Require().Nil(err1)
	// rotate relay file
	rotateRelayFile(relayFiles[1])

	// file decreased when meta changed
	{
		cfg := &BinlogReaderConfig{RelayDir: "", Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, false, "")
		t.setActiveRelayLog(r.relay, "next", "next", 0)
		state := t.createBinlogFileParseState(subDir, relayFiles[0], size+100, true)
		needSwitch, reParse, err := r.waitBinlogChanged(context.Background(), state)
		t.Require().False(needSwitch)
		t.Require().False(reParse)
		t.Require().Error(err)
		t.Require().True(terror.ErrRelayLogFileSizeSmaller.Equal(err))
	}

	// return changed file in meta
	{
		cfg := &BinlogReaderConfig{RelayDir: "", Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, false, "")
		t.setActiveRelayLog(r.relay, "next", "next", 0)
		state := t.createBinlogFileParseState(subDir, relayFiles[0], size, true)
		needSwitch, reParse, err := r.waitBinlogChanged(context.Background(), state)
		t.Require().False(needSwitch)
		t.Require().False(reParse)
		t.Require().NoError(err)
	}

	// file increased when checking meta
	{
		cfg := &BinlogReaderConfig{RelayDir: "", Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, false, "")
		t.setActiveRelayLog(r.relay, "next", "next", 0)
		state := t.createBinlogFileParseState(subDir, relayFiles[0], 0, true)
		needSwitch, reParse, err := r.waitBinlogChanged(context.Background(), state)
		t.Require().False(needSwitch)
		t.Require().True(reParse)
		t.Require().NoError(err)
	}

	// context timeout (no new write)
	{
		newCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		cfg := &BinlogReaderConfig{RelayDir: "", Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, false, "current-uuid") // no notify
		t.setActiveRelayLog(r.relay, "current-uuid", relayFiles[0], 0)
		state := t.createBinlogFileParseState(subDir, relayFiles[0], 0, true)
		needSwitch, reParse, err := r.waitBinlogChanged(newCtx, state)
		t.Require().False(needSwitch)
		t.Require().False(reParse)
		t.Require().NoError(err)
	}

	// this dir is different from the dir of current binlog file, but for test it doesn't matter
	relayDir := t.T().TempDir()
	t.writeUUIDs(relayDir, []string{"xxx.000001", "invalid uuid"})

	// getSwitchPath return error(invalid uuid file)
	{
		cfg := &BinlogReaderConfig{RelayDir: relayDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, false, "xxx.000001")
		t.setActiveRelayLog(r.relay, "next", "next", 0)
		state := t.createBinlogFileParseState(subDir, relayFiles[1], 0, true)
		needSwitch, reParse, err := r.waitBinlogChanged(context.Background(), state)
		t.Require().False(needSwitch)
		t.Require().False(reParse)
		t.Require().True(terror.ErrRelayParseUUIDSuffix.Equal(err))
	}

	t.writeUUIDs(relayDir, []string{"xxx.000001", "xxx.000002"})
	_ = os.MkdirAll(filepath.Join(relayDir, "xxx.000002"), 0o700)
	_ = os.WriteFile(filepath.Join(relayDir, "xxx.000002", "mysql.000001"), nil, 0o600)

	// binlog dir switched, but last file not exists so failed to check change of file length
	// should not happen in real, just for branch test
	{
		cfg := &BinlogReaderConfig{RelayDir: relayDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, false, "xxx.000001")
		t.setActiveRelayLog(r.relay, "next", "next", 0)
		state := t.createBinlogFileParseState(subDir, relayFiles[1], 0, true)
		_ = os.Remove(relayPaths[1])
		needSwitch, reParse, err := r.waitBinlogChanged(context.Background(), state)
		t.Require().False(needSwitch)
		t.Require().False(reParse)
		t.Require().True(terror.ErrGetRelayLogStat.Equal(err))
	}

	err1 = os.WriteFile(relayPaths[1], nil, 0o600)
	t.Require().Nil(err1)

	// binlog dir switched, but last file smaller
	{
		cfg := &BinlogReaderConfig{RelayDir: relayDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, false, "xxx.000001")
		t.setActiveRelayLog(r.relay, "next", "next", 0)
		state := t.createBinlogFileParseState(subDir, relayFiles[1], size, true)
		needSwitch, reParse, err := r.waitBinlogChanged(context.Background(), state)
		t.Require().False(needSwitch)
		t.Require().False(reParse)
		t.Require().True(terror.ErrRelayLogFileSizeSmaller.Equal(err))
	}

	err1 = os.WriteFile(relayPaths[1], data, 0o600)
	t.Require().Nil(err1)

	// binlog dir switched, but last file bigger
	{
		cfg := &BinlogReaderConfig{RelayDir: relayDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, false, "xxx.000001")
		t.setActiveRelayLog(r.relay, "next", "next", 0)
		state := t.createBinlogFileParseState(subDir, relayFiles[1], 0, true)
		needSwitch, reParse, err := r.waitBinlogChanged(context.Background(), state)
		t.Require().False(needSwitch)
		t.Require().True(reParse)
		t.Require().NoError(err)
	}

	// binlog dir switched, but last file not changed
	{
		cfg := &BinlogReaderConfig{RelayDir: relayDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, false, "xxx.000001")
		t.setActiveRelayLog(r.relay, "next", "next", 0)
		state := t.createBinlogFileParseState(subDir, relayFiles[1], size, true)
		needSwitch, reParse, err := r.waitBinlogChanged(context.Background(), state)
		t.Require().True(needSwitch)
		t.Require().False(reParse)
		t.Require().NoError(err)
	}

	// got notified and active pos > current read pos
	{
		cfg := &BinlogReaderConfig{RelayDir: relayDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, true, "xxx.000001")
		t.setActiveRelayLog(r.relay, r.currentSubDir, relayFiles[1], size)
		state := t.createBinlogFileParseState(subDir, relayFiles[1], 0, true)
		needSwitch, reParse, err := r.waitBinlogChanged(context.Background(), state)
		t.Require().False(needSwitch)
		t.Require().True(reParse)
		t.Require().NoError(err)
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
		state := t.createBinlogFileParseState(subDir, relayFiles[1], 0, true)
		needSwitch, reParse, err := r.waitBinlogChanged(context.Background(), state)
		t.Require().False(needSwitch)
		t.Require().True(reParse)
		t.Require().NoError(err)
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
		state := t.createBinlogFileParseState(subDir, relayFiles[1], 0, true)
		needSwitch, reParse, err := r.waitBinlogChanged(context.Background(), state)
		t.Require().False(needSwitch)
		t.Require().True(reParse)
		t.Require().NoError(err)
	}
}

func (t *testReaderSuite) TestGetSwitchPath() {
	var (
		relayDir = t.T().TempDir()
		UUIDs    = []string{
			"53ea0ed1-9bf8-11e6-8bea-64006a897c73.000001",
			"53ea0ed1-9bf8-11e6-8bea-64006a897c72.000002",
			"53ea0ed1-9bf8-11e6-8bea-64006a897c71.000003",
		}
		currentUUID = UUIDs[len(UUIDs)-1] // no next UUID
	)

	UUIDs = append(UUIDs, "invalid.uuid")

	// invalid UUID in UUIDs, error
	t.writeUUIDs(relayDir, UUIDs)
	{
		cfg := &BinlogReaderConfig{RelayDir: relayDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, true, currentUUID)
		switchPath, err := r.getSwitchPath()
		t.Require().Nil(switchPath)
		t.Require().True(terror.ErrRelayParseUUIDSuffix.Equal(err))
	}

	UUIDs = UUIDs[:len(UUIDs)-1] // remove the invalid UUID
	t.writeUUIDs(relayDir, UUIDs)

	// no next sub directory
	{
		cfg := &BinlogReaderConfig{RelayDir: relayDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, true, UUIDs[0])
		switchPath, err := r.getSwitchPath()
		t.Require().Nil(switchPath)
		t.Require().Error(err)
		t.Require().Regexp(fmt.Sprintf(".*%s.*(no such file or directory|The system cannot find the file specified).*", UUIDs[1]), err.Error())
	}

	err1 := os.Mkdir(filepath.Join(relayDir, UUIDs[1]), 0o700)
	t.Require().Nil(err1)

	// uuid directory exist, but no binlog file inside
	{
		cfg := &BinlogReaderConfig{RelayDir: relayDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, true, UUIDs[0])
		switchPath, err := r.getSwitchPath()
		t.Require().Nil(switchPath)
		t.Require().NoError(err)
	}

	// create a relay log file in the next sub directory
	nextBinlogPath := filepath.Join(relayDir, UUIDs[1], "mysql-bin.000001")
	err1 = os.MkdirAll(filepath.Dir(nextBinlogPath), 0o700)
	t.Require().Nil(err1)
	err1 = os.WriteFile(nextBinlogPath, nil, 0o600)
	t.Require().Nil(err1)

	// switch to the next
	{
		cfg := &BinlogReaderConfig{RelayDir: relayDir, Flavor: gmysql.MySQLFlavor}
		r := newBinlogReaderForTest(log.L(), cfg, true, UUIDs[0])
		switchPath, err := r.getSwitchPath()
		t.Require().Equal(UUIDs[1], switchPath.nextUUID)
		t.Require().Equal(filepath.Base(nextBinlogPath), switchPath.nextBinlogName)
		t.Require().NoError(err)
	}
}
