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

package reader

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"

	"github.com/pingcap/dm/pkg/binlog/common"
	"github.com/pingcap/dm/pkg/binlog/event"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/terror"
)

var _ = Suite(&testFileReaderSuite{})

type testFileReaderSuite struct{}

func (t *testFileReaderSuite) TestInterfaceMethods(c *C) {
	var (
		cfg                       = &FileReaderConfig{}
		gSet                      gtid.Set // nil GTID set
		timeoutCtx, timeoutCancel = context.WithTimeout(context.Background(), 10*time.Second)
	)
	defer timeoutCancel()

	r := NewFileReader(cfg)
	c.Assert(r, NotNil)

	// check status, stageNew
	status := r.Status()
	frStatus, ok := status.(*FileReaderStatus)
	c.Assert(ok, IsTrue)
	c.Assert(frStatus.Stage, Equals, common.StageNew.String())
	c.Assert(frStatus.ReadOffset, Equals, uint32(0))
	c.Assert(frStatus.SendOffset, Equals, uint32(0))
	frStatusStr := frStatus.String()
	c.Assert(frStatusStr, Matches, fmt.Sprintf(`.*"stage":"%s".*`, common.StageNew))

	// not prepared
	e, err := r.GetEvent(timeoutCtx)
	c.Assert(err, ErrorMatches, fmt.Sprintf(".*%s.*", common.StageNew))
	c.Assert(e, IsNil)

	// by GTID, not supported yet
	err = r.StartSyncByGTID(gSet)
	c.Assert(err, ErrorMatches, ".*not supported.*")

	// by pos
	err = r.StartSyncByPos(gmysql.Position{})
	c.Assert(err, IsNil)

	// check status, stagePrepared
	status = r.Status()
	frStatus, ok = status.(*FileReaderStatus)
	c.Assert(ok, IsTrue)
	c.Assert(frStatus.Stage, Equals, common.StagePrepared.String())
	c.Assert(frStatus.ReadOffset, Equals, uint32(0))
	c.Assert(frStatus.SendOffset, Equals, uint32(0))

	// re-prepare is invalid
	err = r.StartSyncByPos(gmysql.Position{})
	c.Assert(err, NotNil)

	// binlog file not exists
	e, err = r.GetEvent(timeoutCtx)
	c.Assert(os.IsNotExist(errors.Cause(err)), IsTrue)
	c.Assert(e, IsNil)

	// close the reader
	c.Assert(r.Close(), IsNil)

	// check status, stageClosed
	status = r.Status()
	frStatus, ok = status.(*FileReaderStatus)
	c.Assert(ok, IsTrue)
	c.Assert(frStatus.Stage, Equals, common.StageClosed.String())
	c.Assert(frStatus.ReadOffset, Equals, uint32(0))
	c.Assert(frStatus.SendOffset, Equals, uint32(0))

	// re-close is invalid
	c.Assert(r.Close(), NotNil)
}

func (t *testFileReaderSuite) TestGetEvent(c *C) {
	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer timeoutCancel()

	// create a empty file
	dir := c.MkDir()
	filename := filepath.Join(dir, "mysql-bin-test.000001")
	f, err := os.Create(filename)
	c.Assert(err, IsNil)
	defer f.Close()

	// start from the beginning
	startPos := gmysql.Position{Name: filename}

	// no data can be read, EOF
	r := NewFileReader(&FileReaderConfig{})
	c.Assert(r, NotNil)
	c.Assert(r.StartSyncByPos(startPos), IsNil)
	e, err := r.GetEvent(timeoutCtx)
	c.Assert(errors.Cause(err), Equals, io.EOF)
	c.Assert(e, IsNil)
	c.Assert(r.Close(), IsNil) // close the reader

	// writer a binlog file header
	_, err = f.Write(replication.BinLogFileHeader)
	c.Assert(err, IsNil)

	// no valid events can be read, but can cancel it by the context argument
	r = NewFileReader(&FileReaderConfig{})
	c.Assert(r, NotNil)
	c.Assert(r.StartSyncByPos(startPos), IsNil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	e, err = r.GetEvent(ctx)
	c.Assert(terror.ErrReaderReachEndOfFile.Equal(err), IsTrue)
	c.Assert(e, IsNil)
	c.Assert(r.Close(), IsNil) // close the reader

	// writer a FormatDescriptionEvent
	header := &replication.EventHeader{
		Timestamp: uint32(time.Now().Unix()),
		ServerID:  uint32(101),
	}
	latestPos := uint32(len(replication.BinLogFileHeader))
	formatDescEv, err := event.GenFormatDescriptionEvent(header, latestPos)
	c.Assert(err, IsNil)
	c.Assert(formatDescEv, NotNil)
	_, err = f.Write(formatDescEv.RawData)
	c.Assert(err, IsNil)
	latestPos = formatDescEv.Header.LogPos

	// got a FormatDescriptionEvent
	r = NewFileReader(&FileReaderConfig{})
	c.Assert(r, NotNil)
	c.Assert(r.StartSyncByPos(startPos), IsNil)
	e, err = r.GetEvent(timeoutCtx)
	c.Assert(err, IsNil)
	c.Assert(e, DeepEquals, formatDescEv)
	c.Assert(r.Close(), IsNil) // close the reader

	// check status, stageClosed
	fStat, err := f.Stat()
	c.Assert(err, IsNil)
	fSize := uint32(fStat.Size())
	status := r.Status()
	frStatus, ok := status.(*FileReaderStatus)
	c.Assert(ok, IsTrue)
	c.Assert(frStatus.Stage, Equals, common.StageClosed.String())
	c.Assert(frStatus.ReadOffset, Equals, fSize)
	c.Assert(frStatus.SendOffset, Equals, fSize)

	// write two QueryEvent
	var queryEv *replication.BinlogEvent
	for i := 0; i < 2; i++ {
		queryEv, err = event.GenQueryEvent(
			header, latestPos, 0, 0, 0, nil,
			[]byte(fmt.Sprintf("schema-%d", i)), []byte(fmt.Sprintf("query-%d", i)))
		c.Assert(err, IsNil)
		c.Assert(queryEv, NotNil)
		_, err = f.Write(queryEv.RawData)
		c.Assert(err, IsNil)
		latestPos = queryEv.Header.LogPos
	}

	// read from the middle
	startPos.Pos = latestPos - queryEv.Header.EventSize
	r = NewFileReader(&FileReaderConfig{})
	c.Assert(r, NotNil)
	c.Assert(r.StartSyncByPos(startPos), IsNil)
	e, err = r.GetEvent(timeoutCtx)
	c.Assert(err, IsNil)
	c.Assert(e.RawData, DeepEquals, formatDescEv.RawData) // always got a FormatDescriptionEvent first
	e, err = r.GetEvent(timeoutCtx)
	c.Assert(err, IsNil)
	c.Assert(e.RawData, DeepEquals, queryEv.RawData) // the last QueryEvent
	c.Assert(r.Close(), IsNil)                       // close the reader

	// read from an invalid pos
	startPos.Pos--
	r = NewFileReader(&FileReaderConfig{})
	c.Assert(r, NotNil)
	c.Assert(r.StartSyncByPos(startPos), IsNil)
	e, err = r.GetEvent(timeoutCtx)
	c.Assert(err, IsNil)
	c.Assert(e.RawData, DeepEquals, formatDescEv.RawData) // always got a FormatDescriptionEvent first
	e, err = r.GetEvent(timeoutCtx)
	c.Assert(err, ErrorMatches, ".*EOF.*")
	c.Assert(e, IsNil)
}

func (t *testFileReaderSuite) TestWithChannelBuffer(c *C) {
	var (
		cfg                       = &FileReaderConfig{ChBufferSize: 10}
		timeoutCtx, timeoutCancel = context.WithTimeout(context.Background(), 10*time.Second)
	)
	defer timeoutCancel()

	// create a empty file
	dir := c.MkDir()
	filename := filepath.Join(dir, "mysql-bin-test.000001")
	f, err := os.Create(filename)
	c.Assert(err, IsNil)
	defer f.Close()

	// writer a binlog file header
	_, err = f.Write(replication.BinLogFileHeader)
	c.Assert(err, IsNil)

	// writer a FormatDescriptionEvent
	header := &replication.EventHeader{
		Timestamp: uint32(time.Now().Unix()),
		ServerID:  uint32(101),
	}
	latestPos := uint32(len(replication.BinLogFileHeader))
	formatDescEv, err := event.GenFormatDescriptionEvent(header, latestPos)
	c.Assert(err, IsNil)
	c.Assert(formatDescEv, NotNil)
	_, err = f.Write(formatDescEv.RawData)
	c.Assert(err, IsNil)
	latestPos = formatDescEv.Header.LogPos

	// write channelBufferSize QueryEvent
	var queryEv *replication.BinlogEvent
	for i := 0; i < cfg.ChBufferSize; i++ {
		queryEv, err = event.GenQueryEvent(
			header, latestPos, 0, 0, 0, nil,
			[]byte(fmt.Sprintf("schema-%d", i)), []byte(fmt.Sprintf("query-%d", i)))
		c.Assert(err, IsNil)
		c.Assert(queryEv, NotNil)
		_, err = f.Write(queryEv.RawData)
		c.Assert(err, IsNil)
		latestPos = queryEv.Header.LogPos
	}

	r := NewFileReader(cfg)
	c.Assert(r, NotNil)
	c.Assert(r.StartSyncByPos(gmysql.Position{Name: filename}), IsNil)
	time.Sleep(time.Second) // wait events to be read

	// check status, stagePrepared
	readOffset := latestPos - queryEv.Header.EventSize // an FormatDescriptionEvent in the channel buffer
	status := r.Status()
	frStatus, ok := status.(*FileReaderStatus)
	c.Assert(ok, IsTrue)
	c.Assert(frStatus.Stage, Equals, common.StagePrepared.String())
	c.Assert(frStatus.ReadOffset, Equals, readOffset)
	c.Assert(frStatus.SendOffset, Equals, uint32(0)) // no event sent yet

	// get one event
	e, err := r.GetEvent(timeoutCtx)
	c.Assert(err, IsNil)
	c.Assert(e, NotNil)
	c.Assert(e.RawData, DeepEquals, formatDescEv.RawData)
	time.Sleep(time.Second) // wait events to be read

	// check status, again
	readOffset = latestPos // reach the end
	status = r.Status()
	frStatus, ok = status.(*FileReaderStatus)
	c.Assert(ok, IsTrue)
	c.Assert(frStatus.Stage, Equals, common.StagePrepared.String())
	c.Assert(frStatus.ReadOffset, Equals, readOffset)
	c.Assert(frStatus.SendOffset, Equals, formatDescEv.Header.LogPos) // already get formatDescEv

	c.Assert(r.Close(), IsNil)
}
