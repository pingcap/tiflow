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

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/pingcap/tiflow/pkg/sink/mysql"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	emitBatch             = mysql.DefaultMaxTxnRow
	defaultReaderChanSize = mysql.DefaultWorkerCount * emitBatch
)

// RedoLogReader is a reader abstraction for redo log storage layer
type RedoLogReader interface {
<<<<<<< HEAD
	io.Closer

	// ResetReader setup the reader boundary
	ResetReader(ctx context.Context, startTs, endTs uint64) error

	// ReadNextLog reads up to `maxNumberOfMessages` messages from current cursor.
	// The returned redo logs sorted by commit-ts
	ReadNextLog(ctx context.Context, maxNumberOfEvents uint64) ([]*model.RedoRowChangedEvent, error)

	// ReadNextDDL reads `maxNumberOfDDLs` ddl events from redo logs from current cursor
	ReadNextDDL(ctx context.Context, maxNumberOfEvents uint64) ([]*model.RedoDDLEvent, error)

=======
	// Run read and decode redo logs in background.
	Run(ctx context.Context) error
	// ReadNextRow read one row event from redo logs.
	ReadNextRow(ctx context.Context) (*model.RowChangedEvent, error)
	// ReadNextDDL read one ddl event from redo logs.
	ReadNextDDL(ctx context.Context) (*model.DDLEvent, error)
>>>>>>> 9499d6200d (redo(ticdc): support for applying ddl event in applier (#8362))
	// ReadMeta reads meta from redo logs and returns the latest checkpointTs and resolvedTs
	ReadMeta(ctx context.Context) (checkpointTs, resolvedTs uint64, err error)
}

// NewRedoLogReader creates a new redo log reader
func NewRedoLogReader(
	ctx context.Context, storageType string, cfg *LogReaderConfig,
) (rd RedoLogReader, err error) {
	if !redo.IsValidConsistentStorage(storageType) {
		return nil, errors.ErrConsistentStorage.GenWithStackByArgs(storageType)
	}
	if redo.IsBlackholeStorage(storageType) {
		return newBlackHoleReader(), nil
	}
	return newLogReader(ctx, cfg)
}

// LogReaderConfig is the config for LogReader
type LogReaderConfig struct {
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
	cfg      *LogReaderConfig
	meta     *common.LogMeta
	rowCh    chan *model.RowChangedEvent
	ddlCh    chan *model.DDLEvent
	metaLock sync.Mutex
	sync.Mutex
}

// newLogReader creates a LogReader instance.
// Need the client to guarantee only one LogReader per changefeed
// currently support rewind operation by ResetReader api
// if s3 will download logs first, if OP environment need fetch the redo logs to local dir first
func newLogReader(ctx context.Context, cfg *LogReaderConfig) (*LogReader, error) {
	if cfg == nil {
		err := errors.New("LogReaderConfig can not be nil")
		return nil, errors.WrapError(errors.ErrRedoConfigInvalid, err)
	}
	if cfg.WorkerNums == 0 {
		cfg.WorkerNums = defaultWorkerNum
	}

	logReader := &LogReader{
		cfg:   cfg,
		rowCh: make(chan *model.RowChangedEvent, defaultReaderChanSize),
		ddlCh: make(chan *model.DDLEvent, defaultReaderChanSize),
	}
	if cfg.UseExternalStorage {
		extStorage, err := redo.InitExternalStorage(ctx, cfg.URI)
		if err != nil {
			return nil, err
		}
		// remove logs in local dir first, if have logs left belongs to previous changefeed with the same name may have error when apply logs
		err = os.RemoveAll(cfg.Dir)
		if err != nil {
			return nil, errors.WrapError(errors.ErrRedoFileOp, err)
		}
		err = downLoadToLocal(ctx, cfg.Dir, extStorage, redo.RedoMetaFileType, cfg.WorkerNums)
		if err != nil {
			return nil, errors.WrapError(errors.ErrRedoDownloadFailed, err)
		}
	}
	return logReader, nil
}

// Run implements the `RedoLogReader` interface.
func (l *LogReader) Run(ctx context.Context) error {
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

	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return l.runRowReader(egCtx)
	})
	eg.Go(func() error {
		return l.runDDLReader(egCtx)
	})
	return eg.Wait()
}

func (l *LogReader) runRowReader(egCtx context.Context) error {
	defer close(l.rowCh)
	rowCfg := &readerConfig{
		startTs:            l.meta.CheckpointTs,
		endTs:              l.meta.ResolvedTs,
		dir:                l.cfg.Dir,
		fileType:           redo.RedoRowLogFileType,
		uri:                l.cfg.URI,
		useExternalStorage: l.cfg.UseExternalStorage,
		workerNums:         l.cfg.WorkerNums,
	}
	return l.runReader(egCtx, rowCfg)
}

func (l *LogReader) runDDLReader(egCtx context.Context) error {
	defer close(l.ddlCh)
	ddlCfg := &readerConfig{
		startTs:            l.meta.CheckpointTs - 1,
		endTs:              l.meta.ResolvedTs,
		dir:                l.cfg.Dir,
		fileType:           redo.RedoDDLLogFileType,
		uri:                l.cfg.URI,
		useExternalStorage: l.cfg.UseExternalStorage,
		workerNums:         l.cfg.WorkerNums,
	}
	return l.runReader(egCtx, ddlCfg)
}

func (l *LogReader) runReader(egCtx context.Context, cfg *readerConfig) error {
	fileReaders, err := newReader(egCtx, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		var errs error
		for _, r := range fileReaders {
			errs = multierr.Append(errs, r.Close())
		}
		if errs != nil {
			log.Error("close row reader failed", zap.Error(errs))
		}
	}()

	// init heap
	redoLogHeap, err := newLogHeap(fileReaders)
	if err != nil {
		return errors.Trace(err)
	}
	for i := 0; i < len(fileReaders); i++ {
		rl, err := fileReaders[i].Read()
		if err != nil {
			if err != io.EOF {
				return errors.Trace(err)
			}
			continue
		}

		ld := &logWithIdx{
			data: rl,
			idx:  i,
		}
		redoLogHeap = append(redoLogHeap, ld)
	}
	heap.Init(&redoLogHeap)

	for redoLogHeap.Len() != 0 {
		item := heap.Pop(&redoLogHeap).(*logWithIdx)

		switch cfg.fileType {
		case redo.RedoRowLogFileType:
			row := item.data.RedoRow.Row
			// By design only data (startTs,endTs] is needed,
			// so filter out data may beyond the boundary.
			if row != nil && row.CommitTs > cfg.startTs && row.CommitTs <= cfg.endTs {
				select {
				case <-egCtx.Done():
					return errors.Trace(egCtx.Err())
				case l.rowCh <- row:
				}
			}
		case redo.RedoDDLLogFileType:
			ddl := item.data.RedoDDL.DDL
			if ddl != nil && ddl.CommitTs > cfg.startTs && ddl.CommitTs <= cfg.endTs {
				select {
				case <-egCtx.Done():
					return errors.Trace(egCtx.Err())
				case l.ddlCh <- ddl:
				}
			}
		}

		// read next and push again
		rl, err := fileReaders[item.idx].Read()
		if err != nil {
			if err != io.EOF {
				return errors.Trace(err)
			}
			continue
		}
		ld := &logWithIdx{
			data: rl,
			idx:  item.idx,
		}
		heap.Push(&redoLogHeap, ld)
	}
	return nil
}

<<<<<<< HEAD
// ReadNextLog implement ReadNextLog interface
func (l *LogReader) ReadNextLog(ctx context.Context, maxNumberOfEvents uint64) ([]*model.RedoRowChangedEvent, error) {
=======
// ReadNextRow implement the `RedoLogReader` interface.
func (l *LogReader) ReadNextRow(ctx context.Context) (*model.RowChangedEvent, error) {
>>>>>>> 9499d6200d (redo(ticdc): support for applying ddl event in applier (#8362))
	select {
	case <-ctx.Done():
		return nil, errors.Trace(ctx.Err())
	case row := <-l.rowCh:
		return row, nil
	}
<<<<<<< HEAD

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
=======
}

// ReadNextDDL implement the `RedoLogReader` interface.
func (l *LogReader) ReadNextDDL(ctx context.Context) (*model.DDLEvent, error) {
>>>>>>> 9499d6200d (redo(ticdc): support for applying ddl event in applier (#8362))
	select {
	case <-ctx.Done():
		return nil, errors.Trace(ctx.Err())
	case ddl := <-l.ddlCh:
		return ddl, nil
	}
<<<<<<< HEAD

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
=======
>>>>>>> 9499d6200d (redo(ticdc): support for applying ddl event in applier (#8362))
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
		err = errors.Annotate(err, "can't read log file directory")
		return 0, 0, errors.WrapError(errors.ErrRedoFileOp, err)
	}

	metas := make([]*common.LogMeta, 0, 64)
	for _, file := range files {
		if filepath.Ext(file.Name()) == redo.MetaEXT {
			path := filepath.Join(l.cfg.Dir, file.Name())
			fileData, err := os.ReadFile(path)
			if err != nil {
				return 0, 0, errors.WrapError(errors.ErrRedoFileOp, err)
			}

			log.Debug("unmarshal redo meta", zap.Int("size", len(fileData)))
			meta := &common.LogMeta{}
			_, err = meta.UnmarshalMsg(fileData)
			if err != nil {
				return 0, 0, errors.WrapError(errors.ErrRedoFileOp, err)
			}
			metas = append(metas, meta)
		}
	}

	if len(metas) == 0 {
		return 0, 0, errors.ErrRedoMetaFileNotFound.GenWithStackByArgs(l.cfg.Dir)
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

type logWithIdx struct {
	idx  int
	data *model.RedoLog
}

type logHeap []*logWithIdx

func newLogHeap(fileReaders []fileReader) (logHeap, error) {
	h := logHeap{}
	for i := 0; i < len(fileReaders); i++ {
		rl, err := fileReaders[i].Read()
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
		h = append(h, ld)
	}
	heap.Init(&h)
	return h, nil
}

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
