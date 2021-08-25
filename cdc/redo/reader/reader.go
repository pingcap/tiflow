//  Copyright 2021 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

package reader

import (
	"container/heap"
	"context"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/redo"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

// Reader ...
type Reader interface {
	// ReadNextLog ...
	// The returned redo logs sorted by commit-ts
	ReadNextLog(ctx context.Context, maxNumberOfEvents uint64) ([]*model.RedoRowChangedEvent, error)

	// ReadNextDDL ...
	ReadNextDDL(ctx context.Context, maxNumberOfEvents uint64) ([]*model.RedoDDLEvent, error)

	// ReadMeta reads meta from redo logs and returns the latest resovledTs and checkpointTs
	ReadMeta(ctx context.Context) (checkpointTs, resolvedTs uint64, err error)
}

// LogReaderConfig ...
type LogReaderConfig struct {
	Dir       string
	StartTs   uint64
	EndTs     uint64
	S3Storage bool
	// S3URI should be like SINK_URI="s3://logbucket/test-changefeed?endpoint=http://$S3_ENDPOINT/"
	S3URI *url.URL
}

// LogReader ...
type LogReader struct {
	cfg       *LogReaderConfig
	rowReader []fileReader
	ddlReader []fileReader
	rowHeap   logHeap
	ddlHeap   logHeap
	meta      *redo.LogMeta
	rowLock   sync.Mutex
	ddlLock   sync.Mutex
}

// NewLogReader creates a LogReader instance. need the client to guarantee only one LogReader per changefeed
// currently do not support read by offset, only read next batch.
func NewLogReader(ctx context.Context, cfg *LogReaderConfig) *LogReader {
	if cfg == nil {
		log.Panic("LogWriterConfig can not be nil")
		return nil
	}
	rowCfg := &readerConfig{
		dir:       cfg.Dir,
		fileType:  redo.DefaultRowLogFileName,
		startTs:   cfg.StartTs,
		endTs:     cfg.EndTs,
		s3Storage: cfg.S3Storage,
		s3URI:     cfg.S3URI,
	}
	ddlCfg := &readerConfig{
		dir:       cfg.Dir,
		fileType:  redo.DefaultDDLLogFileName,
		startTs:   cfg.StartTs,
		endTs:     cfg.EndTs,
		s3Storage: cfg.S3Storage,
		s3URI:     cfg.S3URI,
	}
	logReader := &LogReader{
		rowReader: newReader(ctx, rowCfg),
		ddlReader: newReader(ctx, ddlCfg),
		rowHeap:   logHeap{},
		ddlHeap:   logHeap{},
	}
	if cfg.S3Storage {
		s3storage, err := redo.InitS3storage(ctx, cfg.S3URI)
		if err != nil {
			log.Panic("initS3storage fail",
				zap.Error(err),
				zap.Any("S3URI", cfg.S3URI))
		}
		err = downLoadToLocal(ctx, cfg.Dir, s3storage, redo.DefaultMetaFileName)
		if err != nil {
			log.Panic("downLoadToLocal fail",
				zap.Error(err),
				zap.String("file type", redo.DefaultMetaFileName),
				zap.Any("s3URI", cfg.S3URI))
		}
	}
	return logReader
}

// ReadNextLog ...
func (l *LogReader) ReadNextLog(ctx context.Context, maxNumberOfEvents uint64) ([]*model.RedoRowChangedEvent, error) {
	select {
	case <-ctx.Done():
		return nil, errors.Trace(ctx.Err())
	default:
	}

	l.rowLock.Lock()
	defer l.rowLock.Unlock()

	// init heap
	if l.rowHeap.Len() == 0 {
		for i := 0; i < len(l.rowReader); i++ {
			rl := &model.RedoLog{}
			err := l.rowReader[i].Read(rl)
			if err != nil {
				if err != io.EOF {
					return nil, err
				}
				continue
			}

			ld := &logWithIdx{
				data: rl,
				idx:  i,
			}
			l.rowHeap = append(l.rowHeap, ld)
		}
		heap.Init(&l.rowHeap)
	}

	ret := []*model.RedoRowChangedEvent{}
	var i uint64
	for l.rowHeap.Len() != 0 && i < maxNumberOfEvents {
		item := heap.Pop(&l.rowHeap).(*logWithIdx)
		if item.data.Row.Row.CommitTs <= l.cfg.StartTs {
			continue
		}
		ret = append(ret, item.data.Row)
		i++

		rl := &model.RedoLog{}
		err := l.rowReader[item.idx].Read(rl)
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			continue
		}

		ld := &logWithIdx{
			data: rl,
			idx:  item.idx,
		}
		heap.Push(&l.rowHeap, ld)
	}

	return ret, nil
}

// ReadNextDDL ...
func (l *LogReader) ReadNextDDL(ctx context.Context, maxNumberOfEvents uint64) ([]*model.RedoDDLEvent, error) {
	select {
	case <-ctx.Done():
		return nil, errors.Trace(ctx.Err())
	default:
	}

	l.ddlLock.Lock()
	defer l.ddlLock.Unlock()

	// init heap
	if l.ddlHeap.Len() == 0 {
		for i := 0; i < len(l.ddlReader); i++ {
			rl := &model.RedoLog{}
			err := l.ddlReader[i].Read(rl)
			if err != nil {
				if err != io.EOF {
					return nil, err
				}
				continue
			}

			ld := &logWithIdx{
				data: rl,
				idx:  i,
			}
			l.ddlHeap = append(l.ddlHeap, ld)
		}
		heap.Init(&l.ddlHeap)
	}

	ret := []*model.RedoDDLEvent{}
	var i uint64
	for l.ddlHeap.Len() != 0 && i < maxNumberOfEvents {
		item := heap.Pop(&l.ddlHeap).(*logWithIdx)
		if item.data.DDL.DDL.CommitTs <= l.cfg.StartTs {
			continue
		}

		ret = append(ret, item.data.DDL)
		i++

		rl := &model.RedoLog{}
		err := l.ddlReader[item.idx].Read(rl)
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			continue
		}

		ld := &logWithIdx{
			data: rl,
			idx:  item.idx,
		}
		heap.Push(&l.ddlHeap, ld)
	}

	return ret, nil
}

// ReadMeta ...
func (l *LogReader) ReadMeta(ctx context.Context) (checkpointTs, resolvedTs uint64, err error) {
	select {
	case <-ctx.Done():
		return 0, 0, errors.Trace(ctx.Err())
	default:
	}

	if l.meta != nil {
		return l.meta.CheckPointTs, l.meta.ResolvedTs, nil
	}

	files, err := ioutil.ReadDir(l.cfg.Dir)
	if err != nil {
		return 0, 0, cerror.WrapError(cerror.ErrRedoFileOp, errors.Annotate(err, "can't read log file directory"))
	}

	for _, file := range files {
		if filepath.Ext(file.Name()) == redo.MetaEXT {
			fileData, err := os.ReadFile(file.Name())
			if err != nil {
				return 0, 0, cerror.WrapError(cerror.ErrRedoFileOp, err)
			}

			l.meta = &redo.LogMeta{}
			_, err = l.meta.UnmarshalMsg(fileData)
			if err != nil {
				return 0, 0, cerror.WrapError(cerror.ErrRedoFileOp, err)
			}
			break
		}
	}
	return l.meta.CheckPointTs, l.meta.ResolvedTs, nil
}

// Close ...
func (l *LogReader) Close() error {
	if l == nil || l.rowReader == nil || l.ddlReader == nil {
		return nil
	}

	var errs error
	for _, r := range l.rowReader {
		errs = multierr.Append(errs, r.Close())
	}
	for _, r := range l.ddlReader {
		errs = multierr.Append(errs, r.Close())
	}

	return errs
}

type logWithIdx struct {
	idx  int
	data *model.RedoLog
}

type logHeap []*logWithIdx

func (h logHeap) Len() int {
	return len(h)
}

func (h logHeap) Less(i, j int) bool {
	if h[i].data.Type == model.RedoLogTypeDDL {
		return h[i].data.DDL.DDL.CommitTs < h[j].data.DDL.DDL.CommitTs
	}

	return h[i].data.Row.Row.CommitTs < h[j].data.Row.Row.CommitTs
}

func (h logHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *logHeap) Push(x interface{}) {
	*h = append(*h, x.(*logWithIdx))
}

func (h *logHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
