// Copyright 2020 PingCAP, Inc.
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

package cdclog

import (
	"context"
	"net/url"
	"os"
	"path/filepath"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	parsemodel "github.com/pingcap/parser/model"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/codec"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/uber-go/atomic"
	"go.uber.org/zap"
)

const (
	defaultDirMode  = 0755
	defaultFileMode = 0644

	defaultFileName = "cdclog"

	maxRowFileSize = 10 << 20 // TODO update
)

type logPath struct {
	root string
	ddl  string
	meta string
}

type tableStream struct {
	dataCh  chan *model.RowChangedEvent
	rowFile *os.File

	encoder codec.EventBatchEncoder

	tableID    int64
	sendEvents *atomic.Int64
	sendSize   *atomic.Int64
}

func newTableStream(tableID int64) logUnit {
	return &tableStream{
		tableID: tableID,
		dataCh:  make(chan *model.RowChangedEvent, defaultBufferChanSize),

		sendEvents: atomic.NewInt64(0),
		sendSize:   atomic.NewInt64(0),
	}
}

func (ts *tableStream) dataChan() chan *model.RowChangedEvent {
	return ts.dataCh
}

func (ts *tableStream) TableID() int64 {
	return ts.tableID
}

func (ts *tableStream) Events() *atomic.Int64 {
	return ts.sendEvents
}

func (ts *tableStream) Size() *atomic.Int64 {
	return ts.sendSize
}

func (ts *tableStream) isEmpty() bool {
	return ts.sendEvents.Load() == 0
}

func (ts *tableStream) shouldFlush() bool {
	return ts.sendSize.Load() > maxPartFlushSize
}

func (ts *tableStream) flush(ctx context.Context, sink *logSink) error {
	var fileName string
	flushedEvents := ts.sendEvents.Load()
	flushedSize := ts.sendSize.Load()
	if flushedEvents == 0 {
		log.Info("[flushTableStreams] no events to flush")
		return nil
	}
	firstCreated := false
	if ts.encoder == nil {
		// create encoder for each file
		ts.encoder = sink.encoder()
		firstCreated = true
	}
	for event := int64(0); event < flushedEvents; event++ {
		row := <-ts.dataCh
		if event == flushedEvents-1 {
			// the last event
			fileName = makeTableFileName(row.CommitTs)
		}
		_, err := ts.encoder.AppendRowChangedEvent(row)
		if err != nil {
			return err
		}
	}
	rowDatas := ts.encoder.MixedBuild(firstCreated)
	defer func() {
		if ts.encoder != nil {
			ts.encoder.Reset()
		}
	}()

	log.Debug("[flushTableStreams] build cdc log data",
		zap.Int64("table id", ts.tableID),
		zap.Int64("flushed size", flushedSize),
		zap.Int64("flushed event", flushedEvents),
		zap.Int("encode size", len(rowDatas)),
		zap.String("file name", fileName),
	)

	tableDir := filepath.Join(sink.root(), makeTableDirectoryName(ts.tableID))

	if ts.rowFile == nil {
		// create new file to append data
		err := os.MkdirAll(tableDir, defaultDirMode)
		if err != nil {
			return err
		}
		file, err := os.OpenFile(filepath.Join(tableDir, defaultFileName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, defaultFileMode)
		if err != nil {
			return err
		}
		ts.rowFile = file
	}

	_, err := ts.rowFile.Write(rowDatas)
	if err != nil {
		return err
	}

	stat, err := ts.rowFile.Stat()
	if err != nil {
		return err
	}

	if stat.Size() > maxRowFileSize {
		// rotate file
		err := ts.rowFile.Close()
		if err != nil {
			return err
		}
		oldPath := filepath.Join(tableDir, defaultFileName)
		newPath := filepath.Join(tableDir, fileName)
		err = os.Rename(oldPath, newPath)
		if err != nil {
			return err
		}
		file, err := os.OpenFile(filepath.Join(tableDir, defaultFileName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, defaultFileMode)
		if err != nil {
			return err
		}
		ts.rowFile = file
		ts.encoder = nil
	}

	ts.sendEvents.Sub(flushedEvents)
	ts.sendSize.Sub(flushedSize)
	return nil
}

type fileSink struct {
	*logSink

	logMeta *logMeta
	logPath *logPath

	ddlFile *os.File

	ddlEncoder codec.EventBatchEncoder
}

func (f *fileSink) flushLogMeta() error {
	data, err := f.logMeta.Marshal()
	if err != nil {
		return cerror.WrapError(cerror.ErrMarshalFailed, err)
	}
	// FIXME: if initialize succeed, O_WRONLY is enough, but sometimes it will failed
	file, err := os.OpenFile(f.logPath.meta, os.O_CREATE|os.O_WRONLY, defaultFileMode)
	if err != nil {
		return cerror.WrapError(cerror.ErrFileSinkFileOp, err)
	}
	_, err = file.Write(data)
	return cerror.WrapError(cerror.ErrFileSinkFileOp, err)
}

func (f *fileSink) createDDLFile(commitTs uint64) (*os.File, error) {
	fileName := makeDDLFileName(commitTs)
	file, err := os.OpenFile(filepath.Join(f.logPath.ddl, fileName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, defaultFileMode)
	if err != nil {
		log.Error("[EmitDDLEvent] create ddl file failed", zap.Error(err))
		return nil, cerror.WrapError(cerror.ErrFileSinkFileOp, err)
	}
	return file, nil
}

func (f *fileSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	return f.emitRowChangedEvents(ctx, newTableStream, rows...)
}

func (f *fileSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) (uint64, error) {
	log.Debug("[FlushRowChangedEvents] enter", zap.Uint64("ts", resolvedTs))
	return f.flushRowChangedEvents(ctx, resolvedTs)
}

func (f *fileSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	log.Debug("[EmitCheckpointTs]", zap.Uint64("ts", ts))
	f.logMeta.GlobalResolvedTS = ts
	return f.flushLogMeta()
}

func (f *fileSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	switch ddl.Type {
	case parsemodel.ActionCreateTable:
		f.logMeta.Names[ddl.TableInfo.TableID] = model.QuoteSchema(ddl.TableInfo.Schema, ddl.TableInfo.Table)
		err := f.flushLogMeta()
		if err != nil {
			return err
		}
	case parsemodel.ActionRenameTable:
		delete(f.logMeta.Names, ddl.PreTableInfo.TableID)
		f.logMeta.Names[ddl.TableInfo.TableID] = model.QuoteSchema(ddl.TableInfo.Schema, ddl.TableInfo.Table)
		err := f.flushLogMeta()
		if err != nil {
			return err
		}
	}
	firstCreated := false
	if f.ddlEncoder == nil {
		// create ddl encoder once for each ddl log file
		f.ddlEncoder = f.encoder()
		firstCreated = true
	}
	_, err := f.ddlEncoder.EncodeDDLEvent(ddl)
	if err != nil {
		return err
	}
	data := f.ddlEncoder.MixedBuild(firstCreated)

	defer func() {
		if f.ddlEncoder != nil {
			f.ddlEncoder.Reset()
		}
	}()

	if f.ddlFile == nil {
		// create file stream
		file, err := f.createDDLFile(ddl.CommitTs)
		if err != nil {
			return err
		}
		f.ddlFile = file
	}

	stat, err := f.ddlFile.Stat()
	if err != nil {
		return cerror.WrapError(cerror.ErrFileSinkFileOp, err)
	}

	log.Debug("[EmitDDLEvent] current file stats",
		zap.String("name", stat.Name()),
		zap.Int64("size", stat.Size()),
		zap.Int("data size", len(data)),
	)

	if stat.Size() > maxDDLFlushSize {
		// rotate file
		err = f.ddlFile.Close()
		if err != nil {
			return cerror.WrapError(cerror.ErrFileSinkFileOp, err)
		}
		file, err := f.createDDLFile(ddl.CommitTs)
		if err != nil {
			return err
		}
		f.ddlFile = file
		// reset ddl encoder for new file
		f.ddlEncoder = nil
	}

	_, err = f.ddlFile.Write(data)
	if err != nil {
		return cerror.WrapError(cerror.ErrFileSinkFileOp, err)
	}
	return nil
}

func (f *fileSink) Initialize(ctx context.Context, tableInfo []*model.SimpleTableInfo) error {
	if tableInfo != nil {
		for _, table := range tableInfo {
			if table != nil {
				name := makeTableDirectoryName(table.TableID)
				err := os.MkdirAll(filepath.Join(f.logPath.root, name), defaultDirMode)
				if err != nil {
					return cerror.WrapError(cerror.ErrFileSinkCreateDir, err)
				}
			}
		}
		// update log meta to record the relationship about tableName and tableID
		f.logMeta = makeLogMetaContent(tableInfo)
		data, err := f.logMeta.Marshal()
		if err != nil {
			return cerror.WrapError(cerror.ErrMarshalFailed, err)
		}
		filePath := f.logPath.meta
		if _, err := os.Stat(filePath); !os.IsNotExist(err) {
			return cerror.WrapError(cerror.ErrFileSinkMetaAlreadyExists, err)
		}
		file, err := os.Create(filePath)
		if err != nil {
			return cerror.WrapError(cerror.ErrFileSinkCreateDir, err)
		}
		_, err = file.Write(data)
		if err != nil {
			return cerror.WrapError(cerror.ErrFileSinkFileOp, err)
		}
	}
	return nil
}

func (f *fileSink) Close() error {
	return nil
}

// NewLocalFileSink support log data to file.
func NewLocalFileSink(ctx context.Context, sinkURI *url.URL, errCh chan error) (*fileSink, error) {
	log.Info("[NewLocalFileSink]",
		zap.String("host", sinkURI.Host),
		zap.String("path", sinkURI.Path),
	)
	rootPath := sinkURI.Path + "/"
	logPath := &logPath{
		root: rootPath,
		meta: rootPath + logMetaFile,
		ddl:  rootPath + ddlEventsDir,
	}
	err := os.MkdirAll(logPath.ddl, defaultDirMode)
	if err != nil {
		log.Error("create ddl path failed",
			zap.String("ddl path", logPath.ddl),
			zap.Error(err))
		return nil, cerror.WrapError(cerror.ErrFileSinkCreateDir, err)
	}

	f := &fileSink{
		logMeta: newLogMeta(),
		logPath: logPath,
		logSink: newLogSink(logPath.root, nil),
	}

	// important! we should flush asynchronously in another goroutine
	go func() {
		if err := f.startFlush(ctx); err != nil && errors.Cause(err) != context.Canceled {
			select {
			case <-ctx.Done():
				return
			case errCh <- err:
			}
		}
	}()
	return f, nil
}
