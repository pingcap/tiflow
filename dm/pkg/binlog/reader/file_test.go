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
	"testing"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/binlog/common"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

func TestInterfaceMethods(t *testing.T) {
	t.Parallel()
	var (
		cfg                       = &FileReaderConfig{}
		gSet                      gmysql.GTIDSet // nil GTID set
		timeoutCtx, timeoutCancel = context.WithTimeout(context.Background(), 10*time.Second)
	)
	defer timeoutCancel()

	r := NewFileReader(cfg)
	require.NotNil(t, r)

	// check status, stageNew
	status := r.Status()
	frStatus, ok := status.(*FileReaderStatus)
	require.True(t, ok)
	require.Equal(t, common.StageNew.String(), frStatus.Stage)
	require.Equal(t, uint32(0), frStatus.ReadOffset)
	require.Equal(t, uint32(0), frStatus.SendOffset)
	frStatusStr := frStatus.String()
	require.Regexp(t, fmt.Sprintf(`.*"stage":"%s".*`, common.StageNew), frStatusStr)

	// not prepared
	e, err := r.GetEvent(timeoutCtx)
	require.Regexp(t, fmt.Sprintf(".*%s.*", common.StageNew), err)
	require.Nil(t, e)

	// by GTID, not supported yet
	err = r.StartSyncByGTID(gSet)
	require.Regexp(t, ".*not supported.*", err)

	// by pos
	err = r.StartSyncByPos(gmysql.Position{})
	require.Nil(t, err)

	// check status, stagePrepared
	status = r.Status()
	frStatus, ok = status.(*FileReaderStatus)
	require.True(t, ok)
	require.Equal(t, common.StagePrepared.String(), frStatus.Stage)
	require.Equal(t, uint32(0), frStatus.ReadOffset)
	require.Equal(t, uint32(0), frStatus.SendOffset)

	// re-prepare is invalid
	err = r.StartSyncByPos(gmysql.Position{})
	require.NotNil(t, err)

	// binlog file not exists
	e, err = r.GetEvent(timeoutCtx)
	require.True(t, os.IsNotExist(errors.Cause(err)))
	require.Nil(t, e)

	// close the reader
	require.Nil(t, r.Close())

	// check status, stageClosed
	status = r.Status()
	frStatus, ok = status.(*FileReaderStatus)
	require.True(t, ok)
	require.Equal(t, common.StageClosed.String(), frStatus.Stage)
	require.Equal(t, uint32(0), frStatus.ReadOffset)
	require.Equal(t, uint32(0), frStatus.SendOffset)

	// re-close is invalid
	require.NotNil(t, r.Close())
}

func TestGetEvent(t *testing.T) {
	t.Parallel()
	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer timeoutCancel()

	// create a empty file
	dir := t.TempDir()
	filename := filepath.Join(dir, "mysql-bin-test.000001")
	f, err := os.Create(filename)
	require.Nil(t, err)
	defer f.Close()

	// start from the beginning
	startPos := gmysql.Position{Name: filename}

	// no data can be read, EOF
	r := NewFileReader(&FileReaderConfig{})
	require.NotNil(t, r)
	require.Nil(t, r.StartSyncByPos(startPos))
	e, err := r.GetEvent(timeoutCtx)
	require.Equal(t, io.EOF, errors.Cause(err))
	require.Nil(t, e)
	require.Nil(t, r.Close()) // close the reader

	// writer a binlog file header
	_, err = f.Write(replication.BinLogFileHeader)
	require.Nil(t, err)
	// no valid events can be read, but can cancel it by the context argument
	r = NewFileReader(&FileReaderConfig{})
	require.NotNil(t, r)
	require.Nil(t, r.StartSyncByPos(startPos))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	e, err = r.GetEvent(ctx)
	require.True(t, terror.ErrReaderReachEndOfFile.Equal(err))
	require.Nil(t, e)
	require.Nil(t, r.Close()) // close the reader

	// writer a FormatDescriptionEvent
	header := &replication.EventHeader{
		Timestamp: uint32(time.Now().Unix()),
		ServerID:  uint32(101),
	}
	latestPos := uint32(len(replication.BinLogFileHeader))
	formatDescEv, err := event.GenFormatDescriptionEvent(header, latestPos)
	require.Nil(t, err)
	require.NotNil(t, formatDescEv)
	_, err = f.Write(formatDescEv.RawData)
	require.Nil(t, err)
	latestPos = formatDescEv.Header.LogPos

	// got a FormatDescriptionEvent
	r = NewFileReader(&FileReaderConfig{})
	require.NotNil(t, r)
	require.Nil(t, r.StartSyncByPos(startPos))
	e, err = r.GetEvent(timeoutCtx)
	require.Nil(t, err)
	require.Equal(t, formatDescEv, e)
	require.Nil(t, r.Close()) // close the reader

	// check status, stageClosed
	fStat, err := f.Stat()
	require.Nil(t, err)
	fSize := uint32(fStat.Size())
	status := r.Status()
	frStatus, ok := status.(*FileReaderStatus)
	require.True(t, ok)
	require.Equal(t, common.StageClosed.String(), frStatus.Stage)
	require.Equal(t, fSize, frStatus.ReadOffset)
	require.Equal(t, fSize, frStatus.SendOffset)

	// write two QueryEvent
	var queryEv *replication.BinlogEvent
	for i := 0; i < 2; i++ {
		queryEv, err = event.GenQueryEvent(
			header, latestPos, 0, 0, 0, nil,
			[]byte(fmt.Sprintf("schema-%d", i)), []byte(fmt.Sprintf("query-%d", i)))
		require.Nil(t, err)
		require.NotNil(t, queryEv)
		_, err = f.Write(queryEv.RawData)
		require.Nil(t, err)
		latestPos = queryEv.Header.LogPos
	}

	// read from the middle
	startPos.Pos = latestPos - queryEv.Header.EventSize
	r = NewFileReader(&FileReaderConfig{})
	require.NotNil(t, r)
	require.Nil(t, r.StartSyncByPos(startPos))
	e, err = r.GetEvent(timeoutCtx)
	require.Nil(t, err)
	require.Equal(t, formatDescEv.RawData, e.RawData) // always got a FormatDescriptionEvent first
	e, err = r.GetEvent(timeoutCtx)
	require.Nil(t, err)
	require.Equal(t, queryEv.RawData, e.RawData) // the last QueryEvent
	require.Nil(t, r.Close())                    // close the reader

	// read from an invalid pos
	startPos.Pos--
	r = NewFileReader(&FileReaderConfig{})
	require.NotNil(t, r)
	require.Nil(t, r.StartSyncByPos(startPos))
	e, err = r.GetEvent(timeoutCtx)
	require.Nil(t, err)
	require.Equal(t, formatDescEv.RawData, e.RawData) // always got a FormatDescriptionEvent first
	e, err = r.GetEvent(timeoutCtx)
	require.Regexp(t, ".*EOF.*", err)
	require.Nil(t, e)
}

func TestWithChannelBuffer(t *testing.T) {
	t.Parallel()
	var (
		cfg                       = &FileReaderConfig{ChBufferSize: 10}
		timeoutCtx, timeoutCancel = context.WithTimeout(context.Background(), 10*time.Second)
	)
	defer timeoutCancel()

	// create a empty file
	dir := t.TempDir()
	filename := filepath.Join(dir, "mysql-bin-test.000001")
	f, err := os.Create(filename)
	require.Nil(t, err)
	defer f.Close()

	// writer a binlog file header
	_, err = f.Write(replication.BinLogFileHeader)
	require.Nil(t, err)

	// writer a FormatDescriptionEvent
	header := &replication.EventHeader{
		Timestamp: uint32(time.Now().Unix()),
		ServerID:  uint32(101),
	}
	latestPos := uint32(len(replication.BinLogFileHeader))
	formatDescEv, err := event.GenFormatDescriptionEvent(header, latestPos)
	require.Nil(t, err)
	require.NotNil(t, formatDescEv)
	_, err = f.Write(formatDescEv.RawData)
	require.Nil(t, err)
	latestPos = formatDescEv.Header.LogPos

	// write channelBufferSize QueryEvent
	var queryEv *replication.BinlogEvent
	for i := 0; i < cfg.ChBufferSize; i++ {
		queryEv, err = event.GenQueryEvent(
			header, latestPos, 0, 0, 0, nil,
			[]byte(fmt.Sprintf("schema-%d", i)), []byte(fmt.Sprintf("query-%d", i)))
		require.Nil(t, err)
		require.NotNil(t, queryEv)
		_, err = f.Write(queryEv.RawData)
		require.Nil(t, err)
		latestPos = queryEv.Header.LogPos
	}

	r := NewFileReader(cfg)
	require.NotNil(t, r)
	require.Nil(t, r.StartSyncByPos(gmysql.Position{Name: filename}))
	time.Sleep(time.Second) // wait events to be read

	// check status, stagePrepared
	readOffset := latestPos - queryEv.Header.EventSize // an FormatDescriptionEvent in the channel buffer
	status := r.Status()
	frStatus, ok := status.(*FileReaderStatus)
	require.True(t, ok)
	require.Equal(t, common.StagePrepared.String(), frStatus.Stage)
	require.Equal(t, readOffset, frStatus.ReadOffset)
	require.Equal(t, uint32(0), frStatus.SendOffset) // no event sent yet

	// get one event
	e, err := r.GetEvent(timeoutCtx)
	require.Nil(t, err)
	require.NotNil(t, e)
	require.Equal(t, formatDescEv.RawData, e.RawData)
	time.Sleep(time.Second) // wait events to be read

	// check status, again
	readOffset = latestPos // reach the end
	status = r.Status()
	frStatus, ok = status.(*FileReaderStatus)
	require.True(t, ok)
	require.Equal(t, common.StagePrepared.String(), frStatus.Stage)
	require.Equal(t, readOffset, frStatus.ReadOffset)
	require.Equal(t, formatDescEv.Header.LogPos, frStatus.SendOffset) // already get formatDescEv

	require.Nil(t, r.Close())
}
