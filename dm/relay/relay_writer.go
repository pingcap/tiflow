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
	"os"
	"path/filepath"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/parser"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/ticdc/dm/pkg/binlog"
	"github.com/pingcap/ticdc/dm/pkg/binlog/event"
	"github.com/pingcap/ticdc/dm/pkg/gtid"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"github.com/pingcap/ticdc/dm/pkg/terror"
	"github.com/pingcap/ticdc/dm/pkg/utils"
)

const (
	ignoreReasonAlreadyExists = "already exists"
	ignoreReasonFakeRotate    = "fake rotate event"
)

// WResult represents a write result.
type WResult struct {
	Ignore       bool   // whether the event ignored by the writer
	IgnoreReason string // why the writer ignore the event
}

// RecoverResult represents a result for a binlog recover operation.
type RecoverResult struct {
	// if truncate trailing incomplete events during recovering in relay log
	Truncated bool
	// the latest binlog position after recover operation has done.
	LatestPos gmysql.Position
	// the latest binlog GTID set after recover operation has done.
	LatestGTIDs gtid.Set
}

// Writer writes binlog events into disk or any other memory structure.
// The writer should support:
//   1. write binlog events and report the operation result
//   2. skip any obsolete binlog events
//   3. generate dummy events to fill the gap if needed
//   4. rotate binlog(relay) file if needed
//   5. rollback/discard unfinished binlog entries(events or transactions)
type Writer interface {
	// Close closes the writer and release the resource.
	Close() error

	// Recover tries to recover the binlog file or any other memory structure associate with this writer.
	// It is often used to recover a binlog file with some corrupt/incomplete binlog events/transactions at the end of the file.
	// It is not safe for concurrent use by multiple goroutines.
	// It should be called before writing to the file.
	Recover(ctx context.Context) (RecoverResult, error)

	// WriteEvent writes an binlog event's data into disk or any other places.
	// It is not safe for concurrent use by multiple goroutines.
	WriteEvent(ev *replication.BinlogEvent) (WResult, error)

	// Flush flushes the buffered data to a stable storage or sends through the network.
	// It is not safe for concurrent use by multiple goroutines.
	Flush() error
}

// FileConfig is the configuration used by the FileWriter.
type FileConfig struct {
	RelayDir string // directory to store relay log files.
	Filename string // the startup relay log filename, if not set then a fake RotateEvent must be the first event.
}

// FileWriter implements Writer interface.
type FileWriter struct {
	cfg *FileConfig

	// underlying binlog writer,
	// it will be created/started until needed.
	out *BinlogWriter

	// the parser often used to verify events's statement through parsing them.
	parser *parser.Parser

	filename atomic.String // current binlog filename

	logger log.Logger
}

// NewFileWriter creates a FileWriter instances.
func NewFileWriter(logger log.Logger, cfg *FileConfig, parser2 *parser.Parser) Writer {
	w := &FileWriter{
		cfg:    cfg,
		parser: parser2,
		logger: logger.WithFields(zap.String("sub component", "relay writer")),
	}
	w.filename.Store(cfg.Filename) // set the startup filename
	return w
}

// Close implements Writer.Close.
func (w *FileWriter) Close() error {
	var err error
	if w.out != nil {
		err = w.out.Close()
	}

	return err
}

// Recover implements Writer.Recover.
func (w *FileWriter) Recover(ctx context.Context) (RecoverResult, error) {
	return w.doRecovering(ctx)
}

// WriteEvent implements Writer.WriteEvent.
func (w *FileWriter) WriteEvent(ev *replication.BinlogEvent) (WResult, error) {
	switch ev.Event.(type) {
	case *replication.FormatDescriptionEvent:
		return w.handleFormatDescriptionEvent(ev)
	case *replication.RotateEvent:
		return w.handleRotateEvent(ev)
	default:
		return w.handleEventDefault(ev)
	}
}

// Flush implements Writer.Flush.
func (w *FileWriter) Flush() error {
	if w.out != nil {
		return w.out.Flush()
	}
	return terror.ErrRelayWriterNotOpened.Generate()
}

// offset returns the current offset of the binlog file.
// it is only used for testing now.
func (w *FileWriter) offset() int64 {
	if w.out == nil {
		return 0
	}
	status := w.out.Status().(*BinlogWriterStatus)
	return status.Offset
}

// handle FormatDescriptionEvent:
//   1. close the previous binlog file
//   2. open/create a new binlog file
//   3. write the binlog file header if not exists
//   4. write the FormatDescriptionEvent if not exists one
func (w *FileWriter) handleFormatDescriptionEvent(ev *replication.BinlogEvent) (WResult, error) {
	// close the previous binlog file
	if w.out != nil {
		w.logger.Info("closing previous underlying binlog writer", zap.Reflect("status", w.out.Status()))
		err := w.out.Close()
		if err != nil {
			return WResult{}, terror.Annotate(err, "close previous underlying binlog writer")
		}
	}

	// verify filename
	if !binlog.VerifyFilename(w.filename.Load()) {
		return WResult{}, terror.ErrRelayBinlogNameNotValid.Generatef("binlog filename %s not valid", w.filename.Load())
	}

	// open/create a new binlog file
	filename := filepath.Join(w.cfg.RelayDir, w.filename.Load())
	outCfg := &BinlogWriterConfig{
		Filename: filename,
	}
	out := NewBinlogWriter(w.logger, outCfg)
	err := out.Start()
	if err != nil {
		return WResult{}, terror.Annotatef(err, "start underlying binlog writer for %s", filename)
	}
	w.out = out
	w.logger.Info("open underlying binlog writer", zap.Reflect("status", w.out.Status()))

	// write the binlog file header if not exists
	exist, err := checkBinlogHeaderExist(filename)
	if err != nil {
		return WResult{}, terror.Annotatef(err, "check binlog file header for %s", filename)
	} else if !exist {
		err = w.out.Write(replication.BinLogFileHeader)
		if err != nil {
			return WResult{}, terror.Annotatef(err, "write binlog file header for %s", filename)
		}
	}

	// write the FormatDescriptionEvent if not exists one
	exist, err = checkFormatDescriptionEventExist(filename)
	if err != nil {
		return WResult{}, terror.Annotatef(err, "check FormatDescriptionEvent for %s", filename)
	} else if !exist {
		err = w.out.Write(ev.RawData)
		if err != nil {
			return WResult{}, terror.Annotatef(err, "write FormatDescriptionEvent %+v for %s", ev.Header, filename)
		}
	}
	var reason string
	if exist {
		reason = ignoreReasonAlreadyExists
	}

	return WResult{
		Ignore:       exist, // ignore if exists
		IgnoreReason: reason,
	}, nil
}

// handle RotateEvent:
//   1. update binlog filename if needed
//   2. write the RotateEvent if not fake
// NOTE: we only see fake event for RotateEvent in MySQL source code,
//       if see fake event for other event type, then handle them.
// NOTE: we do not create a new binlog file when received a RotateEvent,
//       instead, we create a new binlog file when received a FormatDescriptionEvent.
//       because a binlog file without any events has no meaning.
func (w *FileWriter) handleRotateEvent(ev *replication.BinlogEvent) (result WResult, err error) {
	rotateEv, ok := ev.Event.(*replication.RotateEvent)
	if !ok {
		return result, terror.ErrRelayWriterExpectRotateEv.Generate(ev.Header)
	}

	currFile := w.filename.Load()
	defer func() {
		if err == nil {
			// update binlog filename if needed
			nextFile := string(rotateEv.NextLogName)
			if gmysql.CompareBinlogFileName(nextFile, currFile) == 1 {
				// record the next filename, but not create it.
				// even it's a fake RotateEvent, we still need to record it,
				// because if we do not specify the filename when creating the writer (like Auto-Position),
				// we can only receive a fake RotateEvent before the FormatDescriptionEvent.
				w.filename.Store(nextFile)
			}
		}
	}()

	// write the RotateEvent if not fake
	if utils.IsFakeRotateEvent(ev.Header) {
		// skip fake rotate event
		return WResult{
			Ignore:       true,
			IgnoreReason: ignoreReasonFakeRotate,
		}, nil
	} else if w.out == nil {
		// if not open a binlog file yet, then non-fake RotateEvent can't be handled
		return result, terror.ErrRelayWriterRotateEvWithNoWriter.Generate(ev.Header)
	}

	result, err = w.handlePotentialHoleOrDuplicate(ev)
	if err != nil {
		return result, err
	} else if result.Ignore {
		return result, nil
	}

	err = w.out.Write(ev.RawData)
	if err != nil {
		return result, terror.Annotatef(err, "write RotateEvent %+v for %s", ev.Header, filepath.Join(w.cfg.RelayDir, currFile))
	}

	return WResult{
		Ignore: false,
	}, nil
}

// handle non-special event:
//   1. handle a potential hole if exists
//   2. handle any duplicate events if exist
//   3. write the non-duplicate event
func (w *FileWriter) handleEventDefault(ev *replication.BinlogEvent) (WResult, error) {
	result, err := w.handlePotentialHoleOrDuplicate(ev)
	if err != nil {
		return WResult{}, err
	} else if result.Ignore {
		return result, nil
	}

	// write the non-duplicate event
	err = w.out.Write(ev.RawData)
	return WResult{
		Ignore: false,
	}, terror.Annotatef(err, "write event %+v", ev.Header)
}

// handlePotentialHoleOrDuplicate combines handleFileHoleExist and handleDuplicateEventsExist.
func (w *FileWriter) handlePotentialHoleOrDuplicate(ev *replication.BinlogEvent) (WResult, error) {
	// handle a potential hole
	mayDuplicate, err := w.handleFileHoleExist(ev)
	if err != nil {
		return WResult{}, terror.Annotatef(err, "handle a potential hole in %s before %+v",
			w.filename.Load(), ev.Header)
	}

	if mayDuplicate {
		// handle any duplicate events if exist
		result, err2 := w.handleDuplicateEventsExist(ev)
		if err2 != nil {
			return WResult{}, terror.Annotatef(err2, "handle a potential duplicate event %+v in %s",
				ev.Header, w.filename.Load())
		}
		if result.Ignore {
			// duplicate, and can ignore it. now, we assume duplicate events can all be ignored
			return result, nil
		}
	}

	return WResult{
		Ignore: false,
	}, nil
}

// handleFileHoleExist tries to handle a potential hole after this event wrote.
// A hole exists often because some binlog events not sent by the master.
// If no hole exists, then ev may be a duplicate event.
// NOTE: handle cases when file size > 4GB.
func (w *FileWriter) handleFileHoleExist(ev *replication.BinlogEvent) (bool, error) {
	// 1. detect whether a hole exists
	evStartPos := int64(ev.Header.LogPos - ev.Header.EventSize)
	outFs, ok := w.out.Status().(*BinlogWriterStatus)
	if !ok {
		return false, terror.ErrRelayWriterStatusNotValid.Generate(w.out.Status())
	}
	fileOffset := outFs.Offset
	holeSize := evStartPos - fileOffset
	if holeSize <= 0 {
		// no hole exists, but duplicate events may exists, this should be handled in another place.
		return holeSize < 0, nil
	}
	w.logger.Info("hole exist from pos1 to pos2", zap.Int64("pos1", fileOffset), zap.Int64("pos2", evStartPos), zap.String("file", w.filename.Load()))

	// 2. generate dummy event
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  ev.Header.ServerID,
		}
		latestPos = uint32(fileOffset)
		eventSize = uint32(holeSize)
	)
	dummyEv, err := event.GenDummyEvent(header, latestPos, eventSize)
	if err != nil {
		return false, terror.Annotatef(err, "generate dummy event at %d with size %d", latestPos, eventSize)
	}

	// 3. write the dummy event
	err = w.out.Write(dummyEv.RawData)
	return false, terror.Annotatef(err, "write dummy event %+v to fill the hole", dummyEv.Header)
}

// handleDuplicateEventsExist tries to handle a potential duplicate event in the binlog file.
func (w *FileWriter) handleDuplicateEventsExist(ev *replication.BinlogEvent) (WResult, error) {
	filename := filepath.Join(w.cfg.RelayDir, w.filename.Load())
	duplicate, err := checkIsDuplicateEvent(filename, ev)
	if err != nil {
		return WResult{}, terror.Annotatef(err, "check event %+v whether duplicate in %s", ev.Header, filename)
	} else if duplicate {
		w.logger.Info("event is duplicate", zap.Reflect("header", ev.Header), zap.String("file", w.filename.Load()))
	}

	var reason string
	if duplicate {
		reason = ignoreReasonAlreadyExists
	}

	return WResult{
		Ignore:       duplicate,
		IgnoreReason: reason,
	}, nil
}

// doRecovering tries to recover the current binlog file.
// 1. read events from the file
// 2.
//    a. update the position with the event's position if the transaction finished
//    b. update the GTID set with the event's GTID if the transaction finished
// 3. truncate any incomplete events/transactions
// now, we think a transaction finished if we received a XIDEvent or DDL in QueryEvent
// NOTE: handle cases when file size > 4GB.
func (w *FileWriter) doRecovering(ctx context.Context) (RecoverResult, error) {
	filename := filepath.Join(w.cfg.RelayDir, w.filename.Load())
	fs, err := os.Stat(filename)
	if (err != nil && os.IsNotExist(err)) || (err == nil && len(w.filename.Load()) == 0) {
		return RecoverResult{}, nil // no file need to recover
	} else if err != nil {
		return RecoverResult{}, terror.ErrRelayWriterGetFileStat.Delegate(err, filename)
	}

	// get latest pos/GTID set for all completed transactions from the file
	latestPos, latestGTIDs, err := getTxnPosGTIDs(ctx, filename, w.parser)
	if err != nil {
		return RecoverResult{}, terror.Annotatef(err, "get latest pos/GTID set from %s", filename)
	}

	// mock file truncated by recover
	failpoint.Inject("MockRecoverRelayWriter", func() {
		w.logger.Info("mock recover relay writer")
		failpoint.Goto("bypass")
	})

	// in most cases, we think the file is fine, so compare the size is simpler.
	if fs.Size() == latestPos {
		return RecoverResult{
			Truncated:   false,
			LatestPos:   gmysql.Position{Name: w.filename.Load(), Pos: uint32(latestPos)},
			LatestGTIDs: latestGTIDs,
		}, nil
	} else if fs.Size() < latestPos {
		return RecoverResult{}, terror.ErrRelayWriterLatestPosGTFileSize.Generate(latestPos, fs.Size())
	}

	failpoint.Label("bypass")

	// truncate the file
	f, err := os.OpenFile(filename, os.O_WRONLY, 0o644)
	if err != nil {
		return RecoverResult{}, terror.Annotatef(terror.ErrRelayWriterFileOperate.New(err.Error()), "open %s", filename)
	}
	defer f.Close()
	err = f.Truncate(latestPos)
	if err != nil {
		return RecoverResult{}, terror.Annotatef(terror.ErrRelayWriterFileOperate.New(err.Error()), "truncate %s to %d", filename, latestPos)
	}

	return RecoverResult{
		Truncated:   true,
		LatestPos:   gmysql.Position{Name: w.filename.Load(), Pos: uint32(latestPos)},
		LatestGTIDs: latestGTIDs,
	}, nil
}
