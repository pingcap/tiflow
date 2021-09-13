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
	"github.com/pingcap/ticdc/cdc/redo/common"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

//go:generate mockery --name=Reader --inpackage
// Reader is a reader abstraction for redo log storage layer
type Reader interface {
	// ResetReader setup the reader boundary
	ResetReader(ctx context.Context, startTs, endTs uint64) error

	// ReadNextLog reads up to `maxNumberOfMessages` messages from current cursor.
	// The returned redo logs sorted by commit-ts
	ReadNextLog(ctx context.Context, maxNumberOfEvents uint64) ([]*model.RedoRowChangedEvent, error)

	// ReadNextDDL reads `maxNumberOfDDLs` ddl events from redo logs from current cursor
	ReadNextDDL(ctx context.Context, maxNumberOfEvents uint64) ([]*model.RedoDDLEvent, error)

	// ReadMeta reads meta from redo logs and returns the latest checkpointTs and resovledTs
	ReadMeta(ctx context.Context) (checkpointTs, resolvedTs uint64, err error)
}

// LogReaderConfig ...
type LogReaderConfig struct {
	// Dir is the folder contains the redo logs need to apply when OP environment or
	// the folder used to download redo logs to if s3 enabled
	Dir       string
	S3Storage bool
	// S3URI should be like S3URI="s3://logbucket/test-changefeed?endpoint=http://$S3_ENDPOINT/"
	S3URI   *url.URL
	startTs uint64
	endTs   uint64
}

// LogReader ...
type LogReader struct {
	cfg       *LogReaderConfig
	rowReader []fileReader
	ddlReader []fileReader
	rowHeap   logHeap
	ddlHeap   logHeap
	meta      *common.LogMeta
	rowLock   sync.Mutex
	ddlLock   sync.Mutex
	metaLock  sync.Mutex
	sync.Mutex
}

// NewLogReader creates a LogReader instance. Need the client to guarantee only one LogReader per changefeed
// currently support rewind operation by ResetReader api
// if s3 will download logs first, if OP environment need fetch the redo logs to local dir first
func NewLogReader(ctx context.Context, cfg *LogReaderConfig) *LogReader {
	if cfg == nil {
		log.Panic("LogWriterConfig can not be nil")
		return nil
	}
	logReader := &LogReader{
		cfg: cfg,
	}
	if cfg.S3Storage {
		s3storage, err := common.InitS3storage(ctx, *cfg.S3URI)
		if err != nil {
			log.Panic("initS3storage fail",
				zap.Error(err),
				zap.Any("S3URI", cfg.S3URI))
		}
		err = downLoadToLocal(ctx, cfg.Dir, s3storage, common.DefaultMetaFileType)
		if err != nil {
			log.Panic("downLoadToLocal fail",
				zap.Error(err),
				zap.String("file type", common.DefaultMetaFileType),
				zap.Any("s3URI", cfg.S3URI))
		}
	}
	return logReader
}

// ResetReader ...
func (l *LogReader) ResetReader(ctx context.Context, startTs, endTs uint64) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	if l.meta == nil {
		_, _, err := l.ReadMeta(ctx)
		if err != nil {
			return err
		}
	}
	if startTs > endTs || startTs > l.meta.ResolvedTs || endTs <= l.meta.CheckPointTs {
		return errors.Errorf(
			"startTs, endTs (%d, %d] should match the boundary: (%d, %d]",
			startTs, endTs, l.meta.CheckPointTs, l.meta.ResolvedTs)
	}
	return l.setUpReader(ctx, startTs, endTs)
}

func (l *LogReader) setUpReader(ctx context.Context, startTs, endTs uint64) error {
	l.Lock()
	defer l.Unlock()

	var errs error
	errs = multierr.Append(errs, l.setUpRowReader(ctx, startTs, endTs))
	errs = multierr.Append(errs, l.setUpDDLReader(ctx, startTs, endTs))

	return errs
}

func (l *LogReader) setUpRowReader(ctx context.Context, startTs, endTs uint64) error {
	l.rowLock.Lock()
	defer l.rowLock.Unlock()

	err := l.closeRowReader()
	if err != nil {
		return err
	}

	rowCfg := &readerConfig{
		dir:       l.cfg.Dir,
		fileType:  common.DefaultRowLogFileType,
		startTs:   startTs,
		endTs:     endTs,
		s3Storage: l.cfg.S3Storage,
		s3URI:     l.cfg.S3URI,
	}
	l.rowReader = newReader(ctx, rowCfg)
	l.rowHeap = logHeap{}
	l.cfg.startTs = startTs
	l.cfg.endTs = endTs
	return nil
}

func (l *LogReader) setUpDDLReader(ctx context.Context, startTs, endTs uint64) error {
	l.ddlLock.Lock()
	defer l.ddlLock.Unlock()

	err := l.closeDDLReader()
	if err != nil {
		return err
	}

	ddlCfg := &readerConfig{
		dir:       l.cfg.Dir,
		fileType:  common.DefaultDDLLogFileType,
		startTs:   startTs,
		endTs:     endTs,
		s3Storage: l.cfg.S3Storage,
		s3URI:     l.cfg.S3URI,
	}
	l.ddlReader = newReader(ctx, ddlCfg)
	l.ddlHeap = logHeap{}
	l.cfg.startTs = startTs
	l.cfg.endTs = endTs
	return nil
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
		if item.data.Row.Row.CommitTs <= l.cfg.startTs {
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
		if item.data.DDL.DDL.CommitTs <= l.cfg.startTs {
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

	l.metaLock.Lock()
	defer l.metaLock.Unlock()

	if l.meta != nil {
		return l.meta.CheckPointTs, l.meta.ResolvedTs, nil
	}

	files, err := ioutil.ReadDir(l.cfg.Dir)
	if err != nil {
		return 0, 0, cerror.WrapError(cerror.ErrRedoFileOp, errors.Annotate(err, "can't read log file directory"))
	}

	haveMeta := false
	metaList := map[uint64]*common.LogMeta{}
	var maxCheckPointTs uint64
	for _, file := range files {
		if filepath.Ext(file.Name()) == common.MetaEXT {
			path := filepath.Join(l.cfg.Dir, file.Name())
			fileData, err := os.ReadFile(path)
			if err != nil {
				return 0, 0, cerror.WrapError(cerror.ErrRedoFileOp, err)
			}

			l.meta = &common.LogMeta{}
			_, err = l.meta.UnmarshalMsg(fileData)
			if err != nil {
				return 0, 0, cerror.WrapError(cerror.ErrRedoFileOp, err)
			}

			if !haveMeta {
				haveMeta = true
			}
			metaList[l.meta.CheckPointTs] = l.meta
			// since checkPointTs is guaranteed to increase always
			if l.meta.CheckPointTs > maxCheckPointTs {
				maxCheckPointTs = l.meta.CheckPointTs
			}
		}
	}
	if !haveMeta {
		return 0, 0, errors.Errorf("no redo meta file found in dir:%s", l.cfg.Dir)
	}

	l.meta = metaList[maxCheckPointTs]
	return l.meta.CheckPointTs, l.meta.ResolvedTs, nil
}

func (l *LogReader) closeRowReader() error {
	var errs error
	for _, r := range l.rowReader {
		errs = multierr.Append(errs, r.Close())
	}
	return errs
}

func (l *LogReader) closeDDLReader() error {
	var errs error
	for _, r := range l.ddlReader {
		errs = multierr.Append(errs, r.Close())
	}
	return errs
}

// Close ...
func (l *LogReader) Close() error {
	if l == nil {
		return nil
	}

	var errs error

	l.rowLock.Lock()
	errs = multierr.Append(errs, l.closeRowReader())
	l.rowLock.Unlock()

	l.ddlLock.Lock()
	errs = multierr.Append(errs, l.closeDDLReader())
	l.ddlLock.Unlock()
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
