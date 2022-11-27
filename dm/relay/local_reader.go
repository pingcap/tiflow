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
	"context"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/pingcap/tiflow/dm/pkg/binlog/reader"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"go.uber.org/zap"
)

// ErrorMaybeDuplicateEvent indicates that there may be duplicate event in next binlog file
// this is mainly happened when upstream master changed when relay log not finish reading a transaction.
var ErrorMaybeDuplicateEvent = errors.New("truncate binlog file found, event may be duplicated")

// BinlogReaderConfig is the configuration for BinlogReader.
type BinlogReaderConfig struct {
	RelayDir            string
	Timezone            *time.Location
	Flavor              string
	RowsEventDecodeFunc func(*replication.RowsEvent, []byte) error
}

// BinlogReader is a binlog reader.
type BinlogReader struct {
	cfg    *BinlogReaderConfig
	parser *replication.BinlogParser

	indexPath string // relay server-uuid index file path
	subDirs   []string

	latestServerID uint32 // latest server ID, got from relay log

	running bool
	wg      sync.WaitGroup
	cancel  context.CancelFunc

	tctx *tcontext.Context

	usingGTID          bool
	prevGset, currGset mysql.GTIDSet
	// ch with size = 1, we only need to be notified whether binlog file of relay changed, not how many times
	notifyCh chan interface{}
	relay    Process

	currentSubDir string // current UUID(with suffix)

	lastFileGracefulEnd bool
}

// newBinlogReader creates a new BinlogReader.
func newBinlogReader(logger log.Logger, cfg *BinlogReaderConfig, relay Process) *BinlogReader {
	ctx, cancel := context.WithCancel(context.Background()) // only can be canceled in `Close`
	parser := replication.NewBinlogParser()
	parser.SetVerifyChecksum(true)
	// use string representation of decimal, to replicate the exact value
	parser.SetUseDecimal(false)
	parser.SetRowsEventDecodeFunc(cfg.RowsEventDecodeFunc)
	if cfg.Timezone != nil {
		parser.SetTimestampStringLocation(cfg.Timezone)
	}

	newtctx := tcontext.NewContext(ctx, logger.WithFields(zap.String("component", "binlog reader")))

	binlogReader := &BinlogReader{
		cfg:                 cfg,
		parser:              parser,
		indexPath:           path.Join(cfg.RelayDir, utils.UUIDIndexFilename),
		cancel:              cancel,
		tctx:                newtctx,
		notifyCh:            make(chan interface{}, 1),
		relay:               relay,
		lastFileGracefulEnd: true,
	}
	binlogReader.relay.RegisterListener(binlogReader)
	return binlogReader
}

// checkRelayPos will check whether the given relay pos is too big.
func (r *BinlogReader) checkRelayPos(pos mysql.Position) error {
	currentSubDir, _, realPos, err := binlog.ExtractPos(pos, r.subDirs)
	if err != nil {
		return terror.Annotatef(err, "parse relay dir with pos %s", pos)
	}
	pos = realPos
	relayFilepath := path.Join(r.cfg.RelayDir, currentSubDir, pos.Name)
	r.tctx.L().Info("start to check relay log file", zap.String("path", relayFilepath), zap.Stringer("position", pos))
	fi, err := os.Stat(relayFilepath)
	if err != nil {
		return terror.ErrGetRelayLogStat.Delegate(err, relayFilepath)
	}
	if fi.Size() < int64(pos.Pos) {
		return terror.ErrRelayLogGivenPosTooBig.Generate(pos)
	}
	return nil
}

// IsGTIDCoverPreviousFiles check whether gset contains file's previous_gset.
func (r *BinlogReader) IsGTIDCoverPreviousFiles(ctx context.Context, filePath string, gset mysql.GTIDSet) (bool, error) {
	fileReader := reader.NewFileReader(&reader.FileReaderConfig{Timezone: r.cfg.Timezone})
	defer fileReader.Close()
	err := fileReader.StartSyncByPos(mysql.Position{Name: filePath, Pos: binlog.FileHeaderLen})
	if err != nil {
		return false, err
	}

	var gs mysql.GTIDSet

	for {
		select {
		case <-ctx.Done():
			return false, nil
		default:
		}

		ctx2, cancel := context.WithTimeout(ctx, time.Second)
		e, err := fileReader.GetEvent(ctx2)
		cancel()
		if err != nil {
			// reach end of file
			// Maybe we can only Parse the first three fakeRotate, Format_desc and Previous_gtids events.
			if terror.ErrReaderReachEndOfFile.Equal(err) {
				return false, terror.ErrPreviousGTIDNotExist.Generate(filePath)
			}
			return false, err
		}

		switch {
		case e.Header.EventType == replication.PREVIOUS_GTIDS_EVENT:
			gs, err = event.GTIDsFromPreviousGTIDsEvent(e)
		case e.Header.EventType == replication.MARIADB_GTID_LIST_EVENT:
			gs, err = event.GTIDsFromMariaDBGTIDListEvent(e)
		default:
			continue
		}

		if err != nil {
			return false, err
		}
		return gset.Contain(gs), nil
	}
}

// getPosByGTID gets file position by gtid, result should be (filename, 4).
func (r *BinlogReader) getPosByGTID(gset mysql.GTIDSet) (*mysql.Position, error) {
	// start from newest uuid dir
	for i := len(r.subDirs) - 1; i >= 0; i-- {
		subDir := r.subDirs[i]
		_, suffix, err := utils.ParseRelaySubDir(subDir)
		if err != nil {
			return nil, err
		}

		dir := path.Join(r.cfg.RelayDir, subDir)
		allFiles, err := CollectAllBinlogFiles(dir)
		if err != nil {
			return nil, err
		}

		// iterate files from the newest one
		for i := len(allFiles) - 1; i >= 0; i-- {
			file := allFiles[i]
			filePath := path.Join(dir, file)
			// if input `gset` not contain previous_gtids_event's gset (complementary set of `gset` overlap with
			// previous_gtids_event), that means there're some needed events in previous files.
			// so we go to previous one
			contain, err := r.IsGTIDCoverPreviousFiles(r.tctx.Ctx, filePath, gset)
			if err != nil {
				return nil, err
			}
			if contain {
				fileName, err := utils.ParseFilename(file)
				if err != nil {
					return nil, err
				}
				// Start at the beginning of the file
				return &mysql.Position{
					Name: utils.ConstructFilenameWithUUIDSuffix(fileName, utils.SuffixIntToStr(suffix)),
					Pos:  binlog.FileHeaderLen,
				}, nil
			}
		}
	}
	return nil, terror.ErrNoRelayPosMatchGTID.Generate(gset.String())
}

// StartSyncByPos start sync by pos
// TODO:  thread-safe?
func (r *BinlogReader) StartSyncByPos(pos mysql.Position) (reader.Streamer, error) {
	if pos.Name == "" {
		return nil, terror.ErrBinlogFileNotSpecified.Generate()
	}
	if r.running {
		return nil, terror.ErrReaderAlreadyRunning.Generate()
	}

	// load and update UUID list
	// NOTE: if want to support auto master-slave switching, then needing to re-load UUIDs when parsing.
	err := r.updateSubDirs()
	if err != nil {
		return nil, err
	}
	err = r.checkRelayPos(pos)
	if err != nil {
		return nil, err
	}

	r.latestServerID = 0
	r.running = true
	s := newLocalStreamer()

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.tctx.L().Info("start reading", zap.Stringer("position", pos))
		err = r.parseRelay(r.tctx.Context(), s, pos)
		if errors.Cause(err) == r.tctx.Context().Err() {
			r.tctx.L().Warn("parse relay finished", log.ShortError(err))
		} else if err != nil {
			s.closeWithError(err)
			r.tctx.L().Error("parse relay stopped", zap.Error(err))
		}
	}()

	return s, nil
}

// StartSyncByGTID start sync by gtid.
func (r *BinlogReader) StartSyncByGTID(gset mysql.GTIDSet) (reader.Streamer, error) {
	r.tctx.L().Info("begin to sync binlog", zap.Stringer("GTID Set", gset))
	r.usingGTID = true

	if r.running {
		return nil, terror.ErrReaderAlreadyRunning.Generate()
	}

	if err := r.updateSubDirs(); err != nil {
		return nil, err
	}

	pos, err := r.getPosByGTID(gset)
	if err != nil {
		return nil, err
	}
	r.tctx.L().Info("get pos by gtid", zap.Stringer("GTID Set", gset), zap.Stringer("Position", pos))

	r.prevGset = gset
	r.currGset = nil

	r.latestServerID = 0
	r.running = true
	s := newLocalStreamer()

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.tctx.L().Info("start reading", zap.Stringer("position", pos))
		err = r.parseRelay(r.tctx.Context(), s, *pos)
		if errors.Cause(err) == r.tctx.Context().Err() {
			r.tctx.L().Warn("parse relay finished", log.ShortError(err))
		} else if err != nil {
			s.closeWithError(err)
			r.tctx.L().Error("parse relay stopped", zap.Error(err))
		}
	}()

	return s, nil
}

// SwitchPath represents next binlog file path which should be switched.
type SwitchPath struct {
	nextUUID       string
	nextBinlogName string
}

// parseRelay parses relay root directory, it supports master-slave switch (switching to next sub directory).
func (r *BinlogReader) parseRelay(ctx context.Context, s *LocalStreamer, pos mysql.Position) error {
	currentSubDir, _, realPos, err := binlog.ExtractPos(pos, r.subDirs)
	if err != nil {
		return terror.Annotatef(err, "parse relay dir with pos %v", pos)
	}
	r.currentSubDir = currentSubDir
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		needSwitch, err := r.parseDirAsPossible(ctx, s, realPos)
		if err != nil {
			return err
		}
		if !needSwitch {
			return terror.ErrNoSubdirToSwitch.Generate()
		}
		// update new uuid
		if err = r.updateSubDirs(); err != nil {
			return err
		}
		switchPath, err := r.getSwitchPath()
		if err != nil {
			return err
		}
		if switchPath == nil {
			// should not happen, we have just called it inside parseDirAsPossible successfully.
			return errors.New("failed to get switch path")
		}

		r.currentSubDir = switchPath.nextUUID
		// update pos, so can switch to next sub directory
		realPos.Name = switchPath.nextBinlogName
		realPos.Pos = binlog.FileHeaderLen // start from pos 4 for next sub directory / file
		r.tctx.L().Info("switching to next ready sub directory", zap.String("next uuid", r.currentSubDir), zap.Stringer("position", pos))

		// when switching subdirectory, last binlog file may contain unfinished transaction, so we send a notification.
		if !r.lastFileGracefulEnd {
			s.ch <- &replication.BinlogEvent{
				RawData: []byte(ErrorMaybeDuplicateEvent.Error()),
				Header: &replication.EventHeader{
					EventType: replication.IGNORABLE_EVENT,
				},
			}
		}
	}
}

func (r *BinlogReader) getSwitchPath() (*SwitchPath, error) {
	// reload uuid
	subDirs, err := utils.ParseUUIDIndex(r.indexPath)
	if err != nil {
		return nil, err
	}
	nextSubDir, _, err := getNextRelaySubDir(r.currentSubDir, subDirs)
	if err != nil {
		return nil, err
	}
	if len(nextSubDir) == 0 {
		return nil, nil
	}

	// try to get the first binlog file in next subdirectory
	nextBinlogName, err := getFirstBinlogName(r.cfg.RelayDir, nextSubDir)
	if err != nil {
		// because creating subdirectory and writing relay log file are not atomic
		if terror.ErrBinlogFilesNotFound.Equal(err) {
			return nil, nil
		}
		return nil, err
	}

	return &SwitchPath{nextSubDir, nextBinlogName}, nil
}

// parseDirAsPossible parses relay subdirectory as far as possible.
func (r *BinlogReader) parseDirAsPossible(ctx context.Context, s *LocalStreamer, pos mysql.Position) (needSwitch bool, err error) {
	firstParse := true // the first parse time for the relay log file
	dir := path.Join(r.cfg.RelayDir, r.currentSubDir)
	r.tctx.L().Info("start to parse relay log files in sub directory", zap.String("directory", dir), zap.Stringer("position", pos))

	for {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}
		files, err := CollectBinlogFilesCmp(dir, pos.Name, FileCmpBiggerEqual)
		if err != nil {
			return false, terror.Annotatef(err, "parse relay dir %s with pos %s", dir, pos)
		} else if len(files) == 0 {
			return false, terror.ErrNoRelayLogMatchPos.Generate(dir, pos)
		}

		r.tctx.L().Debug("start read relay log files", zap.Strings("files", files), zap.String("directory", dir), zap.Stringer("position", pos))

		var (
			latestPos  int64
			latestName string
			offset     = int64(pos.Pos)
		)
		// TODO will this happen?
		// previously, we use ParseFile which will handle offset < 4, now we use ParseReader which won't
		if offset < binlog.FileHeaderLen {
			offset = binlog.FileHeaderLen
		}
		for i, relayLogFile := range files {
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			default:
			}
			if i == 0 {
				if !strings.HasSuffix(relayLogFile, pos.Name) {
					return false, terror.ErrFirstRelayLogNotMatchPos.Generate(relayLogFile, pos)
				}
			} else {
				offset = binlog.FileHeaderLen // for other relay log file, start parse from 4
				firstParse = true             // new relay log file need to parse
			}
			needSwitch, latestPos, err = r.parseFileAsPossible(ctx, s, relayLogFile, offset, dir, firstParse, i == len(files)-1)
			if err != nil {
				return false, terror.Annotatef(err, "parse relay log file %s from offset %d in dir %s", relayLogFile, offset, dir)
			}
			firstParse = false // already parsed
			if needSwitch {
				// need switch to next relay sub directory
				return true, nil
			}
			latestName = relayLogFile // record the latest file name
		}

		// update pos, so can re-collect files from the latest file and re start parse from latest pos
		pos.Pos = uint32(latestPos)
		pos.Name = latestName
	}
}

type binlogFileParseState struct {
	// readonly states
	possibleLast              bool
	fullPath                  string
	relayLogFile, relayLogDir string

	f *os.File

	// states may change
	skipGTID            bool
	lastSkipGTIDHeader  *replication.EventHeader
	formatDescEventRead bool
	latestPos           int64
}

// parseFileAsPossible parses single relay log file as far as possible.
func (r *BinlogReader) parseFileAsPossible(ctx context.Context, s *LocalStreamer, relayLogFile string, offset int64, relayLogDir string, firstParse bool, possibleLast bool) (bool, int64, error) {
	r.tctx.L().Debug("start to parse relay log file", zap.String("file", relayLogFile), zap.Int64("position", offset), zap.String("directory", relayLogDir))

	fullPath := filepath.Join(relayLogDir, relayLogFile)
	f, err := os.Open(fullPath)
	if err != nil {
		return false, 0, errors.Trace(err)
	}
	defer f.Close()

	state := &binlogFileParseState{
		possibleLast: possibleLast,
		fullPath:     fullPath,
		relayLogFile: relayLogFile,
		relayLogDir:  relayLogDir,
		f:            f,
		latestPos:    offset,
		skipGTID:     false,
	}

	for {
		select {
		case <-ctx.Done():
			return false, 0, ctx.Err()
		default:
		}
		needSwitch, needReParse, err := r.parseFile(ctx, s, firstParse, state)
		if err != nil {
			return false, 0, terror.Annotatef(err, "parse relay log file %s from offset %d in dir %s", relayLogFile, state.latestPos, relayLogDir)
		}
		firstParse = false // set to false to handle the `continue` below
		if needReParse {
			r.tctx.L().Debug("continue to re-parse relay log file", zap.String("file", relayLogFile), zap.String("directory", relayLogDir))
			continue // should continue to parse this file
		}
		return needSwitch, state.latestPos, nil
	}
}

// parseFile parses single relay log file from specified offset.
func (r *BinlogReader) parseFile(
	ctx context.Context,
	s *LocalStreamer,
	firstParse bool,
	state *binlogFileParseState,
) (needSwitch, needReParse bool, err error) {
	_, suffixInt, err := utils.ParseRelaySubDir(r.currentSubDir)
	if err != nil {
		return false, false, err
	}

	offset := state.latestPos
	r.lastFileGracefulEnd = false

	onEventFunc := func(e *replication.BinlogEvent) error {
		if ce := r.tctx.L().Check(zap.DebugLevel, ""); ce != nil {
			r.tctx.L().Debug("read event", zap.Reflect("header", e.Header))
		}
		r.latestServerID = e.Header.ServerID // record server_id

		lastSkipGTID := state.skipGTID

		switch ev := e.Event.(type) {
		case *replication.FormatDescriptionEvent:
			state.formatDescEventRead = true
			state.latestPos = int64(e.Header.LogPos)
		case *replication.RotateEvent:
			// add master UUID suffix to pos.Name
			parsed, _ := utils.ParseFilename(string(ev.NextLogName))
			uuidSuffix := utils.SuffixIntToStr(suffixInt) // current UUID's suffix, which will be added to binlog name
			nameWithSuffix := utils.ConstructFilenameWithUUIDSuffix(parsed, uuidSuffix)
			ev.NextLogName = []byte(nameWithSuffix)

			if e.Header.Timestamp != 0 && e.Header.LogPos != 0 {
				// not fake rotate event, update file pos
				state.latestPos = int64(e.Header.LogPos)
				r.lastFileGracefulEnd = true
			} else {
				r.tctx.L().Debug("skip fake rotate event", zap.Reflect("header", e.Header))
			}

			// currently, we do not switch to the next relay log file when we receive the RotateEvent,
			// because that next relay log file may not exists at this time,
			// and we *try* to switch to the next when `needReParse` is false.
			// so this `currentPos` only used for log now.
			currentPos := mysql.Position{
				Name: string(ev.NextLogName),
				Pos:  uint32(ev.Position),
			}
			r.tctx.L().Info("rotate binlog", zap.Stringer("position", currentPos))
		case *replication.GTIDEvent:
			if r.prevGset == nil {
				state.latestPos = int64(e.Header.LogPos)
				break
			}
			gtidStr, err2 := event.GetGTIDStr(e)
			if err2 != nil {
				return errors.Trace(err2)
			}
			state.skipGTID, err = r.advanceCurrentGtidSet(gtidStr)
			if err != nil {
				return errors.Trace(err)
			}
			state.latestPos = int64(e.Header.LogPos)
		case *replication.MariadbGTIDEvent:
			if r.prevGset == nil {
				state.latestPos = int64(e.Header.LogPos)
				break
			}
			gtidStr, err2 := event.GetGTIDStr(e)
			if err2 != nil {
				return errors.Trace(err2)
			}
			state.skipGTID, err = r.advanceCurrentGtidSet(gtidStr)
			if err != nil {
				return errors.Trace(err)
			}
			state.latestPos = int64(e.Header.LogPos)
		case *replication.XIDEvent:
			ev.GSet = r.getCurrentGtidSet()
			state.latestPos = int64(e.Header.LogPos)
		case *replication.QueryEvent:
			ev.GSet = r.getCurrentGtidSet()
			state.latestPos = int64(e.Header.LogPos)
		default:
			// update file pos
			state.latestPos = int64(e.Header.LogPos)
		}

		// align with MySQL
		// ref https://github.com/pingcap/tiflow/issues/5063#issuecomment-1082678211
		// heartbeat period is implemented in LocalStreamer.GetEvent
		if state.skipGTID {
			switch e.Event.(type) {
			// Only replace transaction event
			// Other events such as FormatDescriptionEvent, RotateEvent, etc. should be the same as before
			case *replication.RowsEvent, *replication.QueryEvent, *replication.GTIDEvent,
				*replication.MariadbGTIDEvent, *replication.XIDEvent, *replication.TableMapEvent:
				// replace with heartbeat event
				state.lastSkipGTIDHeader = e.Header
			default:
			}
			return nil
		} else if lastSkipGTID && state.lastSkipGTIDHeader != nil {
			// skipGTID is turned off after this event
			select {
			case s.ch <- event.GenHeartbeatEvent(state.lastSkipGTIDHeader):
			case <-ctx.Done():
			}
		}

		select {
		case s.ch <- e:
		case <-ctx.Done():
		}
		return nil
	}

	if firstParse {
		// if the file is the first time to parse, send a fake ROTATE_EVENT before parse binlog file
		// ref: https://github.com/mysql/mysql-server/blob/4f1d7cf5fcb11a3f84cff27e37100d7295e7d5ca/sql/rpl_binlog_sender.cc#L248
		e, err2 := utils.GenFakeRotateEvent(state.relayLogFile, uint64(offset), r.latestServerID)
		if err2 != nil {
			return false, false, terror.Annotatef(err2, "generate fake RotateEvent for (%s: %d)", state.relayLogFile, offset)
		}
		err2 = onEventFunc(e)
		if err2 != nil {
			return false, false, terror.Annotatef(err2, "send event %+v", e.Header)
		}
		r.tctx.L().Info("start parse relay log file", zap.String("file", state.fullPath), zap.Int64("offset", offset))
	} else {
		r.tctx.L().Debug("start parse relay log file", zap.String("file", state.fullPath), zap.Int64("offset", offset))
	}

	// parser needs the FormatDescriptionEvent to work correctly
	// if we start parsing from the middle, we need to read FORMAT DESCRIPTION event first
	if !state.formatDescEventRead && offset > binlog.FileHeaderLen {
		if err = r.parseFormatDescEvent(state); err != nil {
			if state.possibleLast && isIgnorableParseError(err) {
				return r.waitBinlogChanged(ctx, state)
			}
			return false, false, terror.ErrParserParseRelayLog.Delegate(err, state.fullPath)
		}
		state.formatDescEventRead = true
	}

	// we need to seek explicitly, as parser may read in-complete event and return error(ignorable) last time
	// and offset may be messed up
	if _, err = state.f.Seek(offset, io.SeekStart); err != nil {
		return false, false, terror.ErrParserParseRelayLog.Delegate(err, state.fullPath)
	}

	err = r.parser.ParseReader(state.f, onEventFunc)
	if err != nil && (!state.possibleLast || !isIgnorableParseError(err)) {
		r.tctx.L().Error("parse relay log file", zap.String("file", state.fullPath), zap.Int64("offset", offset), zap.Error(err))
		return false, false, terror.ErrParserParseRelayLog.Delegate(err, state.fullPath)
	}
	r.tctx.L().Debug("parse relay log file", zap.String("file", state.fullPath), zap.Int64("offset", state.latestPos))

	return r.waitBinlogChanged(ctx, state)
}

func (r *BinlogReader) waitBinlogChanged(ctx context.Context, state *binlogFileParseState) (needSwitch, needReParse bool, err error) {
	active, relayOffset := r.relay.IsActive(r.currentSubDir, state.relayLogFile)
	if active && relayOffset > state.latestPos {
		return false, true, nil
	}
	if !active {
		meta := &LocalMeta{}
		_, err := toml.DecodeFile(filepath.Join(state.relayLogDir, utils.MetaFilename), meta)
		if err != nil {
			return false, false, terror.Annotate(err, "decode relay meta toml file failed")
		}
		// current watched file size have no change means that no new writes have been made
		// our relay meta file will be updated immediately after receive the rotate event,
		// although we cannot ensure that the binlog filename in the meta is the next file after latestFile
		// but if we return a different filename with latestFile, the outer logic (parseDirAsPossible)
		// will find the right one
		if meta.BinLogName != state.relayLogFile {
			// we need check file size again, as the file may have been changed during our metafile check
			cmp, err2 := fileSizeUpdated(state.fullPath, state.latestPos)
			if err2 != nil {
				return false, false, terror.Annotatef(err2, "latestFilePath=%s endOffset=%d", state.fullPath, state.latestPos)
			}
			switch {
			case cmp < 0:
				return false, false, terror.ErrRelayLogFileSizeSmaller.Generate(state.fullPath)
			case cmp > 0:
				return false, true, nil
			default:
				nextFilePath := filepath.Join(state.relayLogDir, meta.BinLogName)
				log.L().Info("newer relay log file is already generated",
					zap.String("now file path", state.fullPath),
					zap.String("new file path", nextFilePath))
				return false, false, nil
			}
		}

		// maybe UUID index file changed
		switchPath, err := r.getSwitchPath()
		if err != nil {
			return false, false, err
		}
		if switchPath != nil {
			// we need check file size again, as the file may have been changed during path check
			cmp, err := fileSizeUpdated(state.fullPath, state.latestPos)
			if err != nil {
				return false, false, terror.Annotatef(err, "latestFilePath=%s endOffset=%d", state.fullPath, state.latestPos)
			}
			switch {
			case cmp < 0:
				return false, false, terror.ErrRelayLogFileSizeSmaller.Generate(state.fullPath)
			case cmp > 0:
				return false, true, nil
			default:
				log.L().Info("newer relay uuid path is already generated",
					zap.String("current path", state.relayLogDir),
					zap.Any("new path", switchPath))
				return true, false, nil
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			return false, false, nil
		case <-r.Notified():
			active, relayOffset = r.relay.IsActive(r.currentSubDir, state.relayLogFile)
			if active {
				if relayOffset > state.latestPos {
					return false, true, nil
				}
				// already read to relayOffset, try again
				continue
			}
			// file may have changed, try parse and check again
			return false, true, nil
		}
	}
}

func (r *BinlogReader) parseFormatDescEvent(state *binlogFileParseState) error {
	// FORMAT_DESCRIPTION event should always be read by default (despite that fact passed offset may be higher than 4)
	if _, err := state.f.Seek(binlog.FileHeaderLen, io.SeekStart); err != nil {
		return errors.Errorf("seek to 4, error %v", err)
	}

	onEvent := func(e *replication.BinlogEvent) error {
		if _, ok := e.Event.(*replication.FormatDescriptionEvent); ok {
			return nil
		}
		// the first event in binlog file must be FORMAT_DESCRIPTION event.
		return errors.New("corrupted binlog file")
	}
	eofWhenReadHeader, err := r.parser.ParseSingleEvent(state.f, onEvent)
	if err != nil {
		return errors.Annotatef(err, "parse FormatDescriptionEvent")
	}
	if eofWhenReadHeader {
		// when parser met EOF when reading event header, ParseSingleEvent returns nil error
		// return EOF so isIgnorableParseError can capture
		return io.EOF
	}
	return nil
}

// updateSubDirs re-parses UUID index file and updates subdirectory list.
func (r *BinlogReader) updateSubDirs() error {
	subDirs, err := utils.ParseUUIDIndex(r.indexPath)
	if err != nil {
		return terror.Annotatef(err, "index file path %s", r.indexPath)
	}
	oldSubDirs := r.subDirs
	r.subDirs = subDirs
	r.tctx.L().Info("update relay UUIDs", zap.Strings("old subDirs", oldSubDirs), zap.Strings("subDirs", subDirs))
	return nil
}

// Close closes BinlogReader.
func (r *BinlogReader) Close() {
	r.tctx.L().Info("binlog reader closing")
	r.running = false
	r.cancel()
	r.parser.Stop()
	r.wg.Wait()
	r.relay.UnRegisterListener(r)
	r.tctx.L().Info("binlog reader closed")
}

// GetSubDirs returns binlog reader's subDirs.
func (r *BinlogReader) GetSubDirs() []string {
	ret := make([]string, 0, len(r.subDirs))
	ret = append(ret, r.subDirs...)
	return ret
}

func (r *BinlogReader) getCurrentGtidSet() mysql.GTIDSet {
	if r.currGset == nil {
		return nil
	}
	return r.currGset.Clone()
}

// advanceCurrentGtidSet advance gtid set and return whether currGset not updated.
func (r *BinlogReader) advanceCurrentGtidSet(gtid string) (bool, error) {
	if r.currGset == nil {
		r.currGset = r.prevGset.Clone()
	}
	// Special treatment for Maridb
	// MaridbGTIDSet.Update(gtid) will replace gset with given gtid
	// ref https://github.com/go-mysql-org/go-mysql/blob/0c5789dd0bd378b4b84f99b320a2d35a80d8858f/mysql/mariadb_gtid.go#L96
	if r.cfg.Flavor == mysql.MariaDBFlavor {
		gset, err := mysql.ParseMariadbGTIDSet(gtid)
		if err != nil {
			return false, err
		}
		if r.currGset.Contain(gset) {
			return true, nil
		}
	}
	prev := r.currGset.Clone()
	err := r.currGset.Update(gtid)
	if err == nil {
		if !r.currGset.Equal(prev) {
			r.prevGset = prev
			return false, nil
		}
		return true, nil
	}
	return false, err
}

func (r *BinlogReader) Notified() chan interface{} {
	return r.notifyCh
}

func (r *BinlogReader) OnEvent(_ *replication.BinlogEvent) {
	// skip if there's pending notify
	select {
	case r.notifyCh <- struct{}{}:
	default:
	}
}
