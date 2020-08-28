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
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/uber-go/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	parsemodel "github.com/pingcap/parser/model"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/codec"
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

func newTableStream(tableID int64) *tableStream {
	return &tableStream{
		tableID: tableID,
		dataCh:  make(chan *model.RowChangedEvent, defaultBufferChanSize),

		sendEvents: atomic.NewInt64(0),
		sendSize:   atomic.NewInt64(0),
	}
}

type fileSink struct {
	logMeta *logMeta
	logPath *logPath

	ddlFile *os.File
	encoder func() codec.EventBatchEncoder

	ddlEncoder   codec.EventBatchEncoder
	hashMap      sync.Map
	tableStreams []*tableStream
}

func (f *fileSink) flushLogMeta(ctx context.Context) error {
	data, err := f.logMeta.Marshal()
	if err != nil {
		return errors.Annotate(err, "marshal meta to json failed")
	}
	// FIXME: if initialize succeed, O_WRONLY is enough, but sometimes it will failed
	file, err := os.OpenFile(f.logPath.meta, os.O_CREATE|os.O_WRONLY, defaultFileMode)
	if err != nil {
		return err
	}
	_, err = file.Write(data)
	return err
}

func (f *fileSink) flushTableStreams(ctx context.Context) error {
	// TODO use a fixed worker pool
	eg, _ := errgroup.WithContext(ctx)
	for _, ts := range f.tableStreams {
		tsReplica := ts
		eg.Go(func() error {
			var fileName string
			flushedEvents := tsReplica.sendEvents.Load()
			flushedSize := tsReplica.sendSize.Load()
			firstCreated := false
			if tsReplica.encoder == nil {
				// create encoder for each file
				tsReplica.encoder = f.encoder()
				firstCreated = true
			}
			for event := int64(0); event < flushedEvents; event++ {
				row := <-tsReplica.dataCh
				if event == flushedEvents-1 {
					// the last event
					fileName = makeTableFileName(row.CommitTs)
				}
				_, err := tsReplica.encoder.AppendRowChangedEvent(row)
				if err != nil {
					return err
				}
			}
			rowDatas := tsReplica.encoder.MixedBuild(firstCreated)
			defer func() {
				if tsReplica.encoder != nil {
					tsReplica.encoder.Reset()
				}
			}()

			log.Debug("[flushTableStreams] build cdc log data",
				zap.Int64("table id", tsReplica.tableID),
				zap.Int64("flushed size", flushedSize),
				zap.Int64("flushed event", flushedEvents),
				zap.Int("encode size", len(rowDatas)),
				zap.String("file name", fileName),
			)

			tableDir := filepath.Join(f.logPath.root, makeTableDirectoryName(tsReplica.tableID))

			if tsReplica.rowFile == nil {
				// create new file to append data
				err := os.MkdirAll(tableDir, defaultDirMode)
				if err != nil {
					return err
				}
				file, err := os.OpenFile(filepath.Join(tableDir, defaultFileName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, defaultFileMode)
				if err != nil {
					return err
				}
				tsReplica.rowFile = file
			}

			stat, err := tsReplica.rowFile.Stat()
			if err != nil {
				return err
			}

			if stat.Size() > maxRowFileSize {
				// rotate file
				err := tsReplica.rowFile.Close()
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
				tsReplica.rowFile = file
				tsReplica.encoder = nil
			}
			_, err = tsReplica.rowFile.Write(rowDatas)
			if err != nil {
				return err
			}

			tsReplica.sendEvents.Sub(flushedEvents)
			tsReplica.sendSize.Sub(flushedSize)
			return nil
		})
	}
	return eg.Wait()
}

func (f *fileSink) createDDLFile(commitTs uint64) (*os.File, error) {
	fileName := makeDDLFileName(commitTs)
	file, err := os.OpenFile(filepath.Join(f.logPath.ddl, fileName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, defaultFileMode)
	if err != nil {
		log.Error("[EmitDDLEvent] create ddl file failed", zap.Error(err))
		return nil, err
	}
	return file, err
}

func (f *fileSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	for _, row := range rows {
		// dispatch row event by tableID
		tableID := row.Table.GetTableID()
		var (
			ok   bool
			item interface{}
			hash int
		)
		if item, ok = f.hashMap.Load(tableID); !ok {
			// found new tableID
			f.tableStreams = append(f.tableStreams, newTableStream(tableID))
			hash = len(f.tableStreams) - 1
			f.hashMap.Store(tableID, hash)
		} else {
			hash = item.(int)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case f.tableStreams[hash].dataCh <- row:
			f.tableStreams[hash].sendEvents.Inc()
			f.tableStreams[hash].sendSize.Add(row.ApproximateSize)
		}
	}
	return nil
}

func (f *fileSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) (uint64, error) {
	log.Debug("[FlushRowChangedEvents] enter", zap.Uint64("ts", resolvedTs))
	// TODO update flush policy with size
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-time.After(defaultFlushRowChangedEventDuration):
		return resolvedTs, f.flushTableStreams(ctx)
	}
}

func (f *fileSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	log.Debug("[EmitCheckpointTs]", zap.Uint64("ts", ts))
	f.logMeta.GlobalResolvedTS = ts
	return f.flushLogMeta(ctx)
}

func (f *fileSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	switch ddl.Type {
	case parsemodel.ActionCreateTable:
		f.logMeta.Names[ddl.TableInfo.TableID] = model.QuoteSchema(ddl.TableInfo.Schema, ddl.TableInfo.Table)
		err := f.flushLogMeta(ctx)
		if err != nil {
			return err
		}
	case parsemodel.ActionRenameTable:
		delete(f.logMeta.Names, ddl.PreTableInfo.TableID)
		f.logMeta.Names[ddl.TableInfo.TableID] = model.QuoteSchema(ddl.TableInfo.Schema, ddl.TableInfo.Table)
		err := f.flushLogMeta(ctx)
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
	_, err := f.ddlEncoder.AppendDDLEvent(ddl)
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
		return err
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
			return err
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
		return err
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
					return errors.Annotatef(err, "create table directory for %s on failed", name)
				}
			}
		}
		// update log meta to record the relationship about tableName and tableID
		f.logMeta = makeLogMetaContent(tableInfo)
		data, err := f.logMeta.Marshal()
		if err != nil {
			return errors.Annotate(err, "marshal meta to json failed")
		}
		filePath := f.logPath.meta
		if _, err := os.Stat(filePath); !os.IsNotExist(err) {
			return errors.Annotate(err, "meta file already exists, please change the path of this sink")
		}
		file, err := os.Create(filePath)
		if err != nil {
			return err
		}
		_, err = file.Write(data)
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *fileSink) Close() error {
	return nil
}

// NewLocalFileSink support log data to file.
func NewLocalFileSink(sinkURI *url.URL) (*fileSink, error) {
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
		return nil, err
	}
	return &fileSink{
		logMeta: newLogMeta(),
		logPath: logPath,
		encoder: codec.NewJSONEventBatchEncoder,

		tableStreams: make([]*tableStream, 0),
	}, nil
}
