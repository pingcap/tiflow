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
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/common"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/redo"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

// RedoLogReader is a reader abstraction for redo log storage layer
type RedoLogReader interface {
	io.Closer

	// ResetReader setup the reader boundary
	ResetReader(ctx context.Context, startTs, endTs uint64) error

	// ReadNextLog reads up to `maxNumberOfMessages` messages from current cursor.
	// The returned redo logs sorted by commit-ts
	ReadNextLog(ctx context.Context, maxNumberOfEvents uint64) ([]*model.RedoRowChangedEvent, error)

	// ReadNextDDL reads `maxNumberOfDDLs` ddl events from redo logs from current cursor
	ReadNextDDL(ctx context.Context, maxNumberOfEvents uint64) ([]*model.RedoDDLEvent, error)

	// ReadMeta reads meta from redo logs and returns the latest checkpointTs and resolvedTs
	ReadMeta(ctx context.Context) (checkpointTs, resolvedTs uint64, err error)
}

// NewRedoLogReader creates a new redo log reader
func NewRedoLogReader(
	ctx context.Context, storageType string, cfg *LogReaderConfig,
) (rd RedoLogReader, err error) {
	if !redo.IsValidConsistentStorage(storageType) {
		return nil, cerror.ErrConsistentStorage.GenWithStackByArgs(storageType)
	}
	if redo.IsBlackholeStorage(storageType) {
		return newBlackHoleReader(), nil
	}
	return newLogReader(ctx, cfg)
}

// LogReaderConfig is the config for LogReader
type LogReaderConfig struct {
	startTs uint64
	endTs   uint64

	// Dir is the folder contains the redo logs need to apply when OP environment or
	// the folder used to download redo logs to if using external storage, such as s3
	// and gcs.
	Dir string

	// URI should be like "s3://logbucket/test-changefeed?endpoint=http://$S3_ENDPOINT/"
	URI                url.URL
	UseExternalStorage bool

	// WorkerNums is the num of workers used to sort the log file to sorted file,
	// will load the file to memory first then write the sorted file to disk
	// the memory used is WorkerNums * defaultMaxLogSize (64 * megabyte) total
	WorkerNums int
}

// LogReader implement RedoLogReader interface
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

// newLogReader creates a LogReader instance.
// Need the client to guarantee only one LogReader per changefeed
// currently support rewind operation by ResetReader api
// if s3 will download logs first, if OP environment need fetch the redo logs to local dir first
func newLogReader(ctx context.Context, cfg *LogReaderConfig) (*LogReader, error) {
	if cfg == nil {
		return nil, cerror.WrapError(cerror.ErrRedoConfigInvalid, errors.New("LogReaderConfig can not be nil"))
	}
	if cfg.WorkerNums == 0 {
		cfg.WorkerNums = defaultWorkerNum
	}

	logReader := &LogReader{
		cfg: cfg,
	}
	if cfg.UseExternalStorage {
		extStorage, err := redo.InitExternalStorage(ctx, cfg.URI)
		if err != nil {
			return nil, err
		}
		// remove logs in local dir first, if have logs left belongs to previous changefeed with the same name may have error when apply logs
		err = os.RemoveAll(cfg.Dir)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrRedoFileOp, err)
		}
		err = downLoadToLocal(ctx, cfg.Dir, extStorage, redo.RedoMetaFileType, cfg.WorkerNums)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrRedoDownloadFailed, err)
		}
	}
	return logReader, nil
}

// ResetReader implement ResetReader interface
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

	if startTs > endTs || startTs > l.meta.ResolvedTs || endTs <= l.meta.CheckpointTs {
		return errors.Errorf(
			"startTs, endTs (%d, %d] should match the boundary: (%d, %d]",
			startTs, endTs, l.meta.CheckpointTs, l.meta.ResolvedTs)
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
		startTs:            startTs,
		endTs:              endTs,
		dir:                l.cfg.Dir,
		fileType:           redo.RedoRowLogFileType,
		uri:                l.cfg.URI,
		useExternalStorage: l.cfg.UseExternalStorage,
		workerNums:         l.cfg.WorkerNums,
	}
	l.rowReader, err = newReader(ctx, rowCfg)
	if err != nil {
		return err
	}

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
		startTs:            startTs,
		endTs:              endTs,
		dir:                l.cfg.Dir,
		fileType:           redo.RedoDDLLogFileType,
		uri:                l.cfg.URI,
		useExternalStorage: l.cfg.UseExternalStorage,
		workerNums:         l.cfg.WorkerNums,
	}
	l.ddlReader, err = newReader(ctx, ddlCfg)
	if err != nil {
		return err
	}

	l.ddlHeap = logHeap{}
	l.cfg.startTs = startTs
	l.cfg.endTs = endTs
	return nil
}

// ReadNextLog implement ReadNextLog interface
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
		if item.data.RedoRow != nil && item.data.RedoRow.Row != nil &&
			// by design only data (startTs,endTs] is needed, so filter out data may beyond the boundary
			item.data.RedoRow.Row.CommitTs > l.cfg.startTs &&
			item.data.RedoRow.Row.CommitTs <= l.cfg.endTs {
			ret = append(ret, item.data.RedoRow)
			i++
		}

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

// ReadNextDDL implement ReadNextDDL interface
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
		if item.data.RedoDDL != nil && item.data.RedoDDL.DDL != nil &&
			// by design only data (startTs,endTs] is needed, so filter out data may beyond the boundary
			item.data.RedoDDL.DDL.CommitTs > l.cfg.startTs &&
			item.data.RedoDDL.DDL.CommitTs <= l.cfg.endTs {
			ret = append(ret, item.data.RedoDDL)
			i++
		}

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

// ReadMeta implement ReadMeta interface
func (l *LogReader) ReadMeta(ctx context.Context) (checkpointTs, resolvedTs uint64, err error) {
	select {
	case <-ctx.Done():
		return 0, 0, errors.Trace(ctx.Err())
	default:
	}

	l.metaLock.Lock()
	defer l.metaLock.Unlock()

	if l.meta != nil {
		return l.meta.CheckpointTs, l.meta.ResolvedTs, nil
	}

	files, err := ioutil.ReadDir(l.cfg.Dir)
	if err != nil {
		return 0, 0, cerror.WrapError(cerror.ErrRedoFileOp, errors.Annotate(err, "can't read log file directory"))
	}

	metas := make([]*common.LogMeta, 0, 64)
	for _, file := range files {
		if filepath.Ext(file.Name()) == redo.MetaEXT {
			path := filepath.Join(l.cfg.Dir, file.Name())
			fileData, err := os.ReadFile(path)
			if err != nil {
				return 0, 0, cerror.WrapError(cerror.ErrRedoFileOp, err)
			}

			log.Debug("unmarshal redo meta", zap.Int("size", len(fileData)))
			meta := &common.LogMeta{}
			_, err = meta.UnmarshalMsg(fileData)
			if err != nil {
				return 0, 0, cerror.WrapError(cerror.ErrRedoFileOp, err)
			}
			metas = append(metas, meta)
		}
	}

	if len(metas) == 0 {
		return 0, 0, cerror.ErrRedoMetaFileNotFound.GenWithStackByArgs(l.cfg.Dir)
	}

	common.ParseMeta(metas, &checkpointTs, &resolvedTs)
	if resolvedTs < checkpointTs {
		log.Panic("in all meta files, resolvedTs is less than checkpointTs",
			zap.Uint64("resolvedTs", resolvedTs),
			zap.Uint64("checkpointTs", checkpointTs))
	}
	l.meta = &common.LogMeta{CheckpointTs: checkpointTs, ResolvedTs: resolvedTs}
	return
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

// Close the backing file readers
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
		if h[i].data.RedoDDL == nil || h[i].data.RedoDDL.DDL == nil {
			return true
		}
		if h[j].data.RedoDDL == nil || h[j].data.RedoDDL.DDL == nil {
			return false
		}
		return h[i].data.RedoDDL.DDL.CommitTs < h[j].data.RedoDDL.DDL.CommitTs
	}

	if h[i].data.RedoRow == nil || h[i].data.RedoRow.Row == nil {
		return true
	}
	if h[j].data.RedoRow == nil || h[j].data.RedoRow.Row == nil {
		return false
	}

	if h[i].data.RedoRow.Row.CommitTs == h[j].data.RedoRow.Row.CommitTs &&
		h[i].data.RedoRow.Row.StartTs < h[j].data.RedoRow.Row.StartTs {
		return true
	}
	return h[i].data.RedoRow.Row.CommitTs < h[j].data.RedoRow.Row.CommitTs
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
