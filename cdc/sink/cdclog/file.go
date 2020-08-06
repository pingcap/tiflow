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
	"path"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/uber-go/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/ticdc/cdc/model"
)

const (
	defaultDirMode = 0755
	defaultFileMode = 0644

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

	hashMap      sync.Map
	tableStreams []*tableStream
}

func (f *fileSink) flushTableStreams() {
	var wg sync.WaitGroup
	errCh := make(chan error, len(f.tableStreams))
	for _, ts := range f.tableStreams {
		tsReplica := ts
		wg.Add(1)
		go func(ts *tableStream) {
			defer wg.Done()
			var fileName string
			flushedEvents := ts.sendEvents.Load()
			flushedSize := ts.sendSize.Load()
			rowDatas := make([]byte, 0, flushedSize)
			for event := int64(0); event < flushedEvents; event++ {
				row := <-ts.dataCh
				if event == flushedEvents-1 {
					// the last event
					fileName = makeTableFileName(row.CommitTs)
				}
				data, err := row.ToProtoBuf().Marshal()
				if err != nil {
					errCh <- err
				}
				rowDatas = append(rowDatas, append(data, '\n')...)
			}

			tableDir := path.Join(f.logPath.root, makeTableDirectoryName(ts.tableID))

			if ts.rowFile == nil {
				// create new file to append data
				err := os.MkdirAll(tableDir, defaultDirMode)
				if err != nil {
					errCh <- err
				}
				file, err := os.OpenFile(path.Join(tableDir, fileName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, defaultFileMode)
				if err != nil {
					errCh <- err
				}
				ts.rowFile = file
			}

			stat, err := ts.rowFile.Stat()
			if err != nil {
				errCh <- err
			}

			if stat.Size() > maxRowFileSize {
				// rotate file
				err := ts.rowFile.Close()
				if err != nil {
					errCh <- err
				}
				file, err := os.OpenFile(path.Join(tableDir, fileName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, defaultFileMode)
				if err != nil {
					errCh <- err
				}
				ts.rowFile = file
			}
			_, err = ts.rowFile.Write(rowDatas)
			if err != nil {
				errCh <- err
			}

			ts.sendEvents.Sub(flushedEvents)
			ts.sendSize.Sub(flushedSize)

		}(tsReplica)
	}
	wg.Wait()
}

func (f *fileSink) createDDLFile(commitTs uint64) (*os.File, error) {
	fileName := makeDDLFileName(commitTs)
	file, err := os.OpenFile(path.Join(f.logPath.ddl, fileName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, defaultFileMode)
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
			// TODO give a approximate size of this row
			f.tableStreams[hash].sendSize.Add(int64(len(row.Keys) * 128))
		}
	}
	return nil
}

func (f *fileSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) error {
	// TODO update flush policy with size
	select {
	case <-time.After(defaultFlushRowChangedEventDuration):
		f.flushTableStreams()
	}
	return nil
}

func (f *fileSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	if f.logMeta == nil {
		log.Debug("[EmitCheckpointTs] generate new logMeta when emit checkpoint ts", zap.Uint64("ts", ts))
		f.logMeta = new(logMeta)
	}

	f.logMeta.GlobalResolvedTS = ts
	data, err := f.logMeta.Marshal()
	if err != nil {
		return errors.Annotate(err, "marshal meta to json failed")
	}
	file, err := os.OpenFile(f.logPath.meta, os.O_WRONLY, defaultFileMode)
	if err != nil {
		return err
	}
	_, err = file.Write(data)
	return err
}

func (f *fileSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	data, err := ddl.ToProtoBuf().Marshal()
	if err != nil {
		return err
	}

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
	}

	_, err = f.ddlFile.Write(append(data, '\n'))
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
				err := os.MkdirAll(path.Join(f.logPath.root, name), defaultDirMode)
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
		logPath:      logPath,
		tableStreams: make([]*tableStream, 0),
	}, nil
}
