//  Copyright 2021 PingCAP, Inc.
//  Copyright 2015 CoreOS, Inc.
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
	"bufio"
	"container/heap"
	"context"
	"encoding/binary"
	"io"
	"io/ioutil"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/writer"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/redo"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// frameSizeBytes is frame size in bytes, including record size and padding size.
	frameSizeBytes = 8

	// defaultWorkerNum is the num of workers used to sort the log file to sorted file,
	// will load the file to memory first then write the sorted file to disk
	// the memory used is defaultWorkerNum * defaultMaxLogSize (64 * megabyte) total
	defaultWorkerNum = 50
)

//go:generate mockery --name=fileReader --inpackage
type fileReader interface {
	io.Closer
	// Read return the log from log file
	Read(log *model.RedoLog) error
}

type readerConfig struct {
	startTs  uint64
	endTs    uint64
	dir      string
	fileType string

	uri                url.URL
	useExternalStorage bool
	workerNums         int
}

type reader struct {
	cfg      *readerConfig
	mu       sync.Mutex
	br       *bufio.Reader
	fileName string
	closer   io.Closer
	// lastValidOff file offset following the last valid decoded record
	lastValidOff int64
}

func newReader(ctx context.Context, cfg *readerConfig) ([]fileReader, error) {
	if cfg == nil {
		return nil, cerror.WrapError(cerror.ErrRedoConfigInvalid, errors.New("readerConfig can not be nil"))
	}
	if cfg.workerNums == 0 {
		cfg.workerNums = defaultWorkerNum
	}

	if cfg.useExternalStorage {
		extStorage, err := redo.InitExternalStorage(ctx, cfg.uri)
		if err != nil {
			return nil, err
		}

		err = downLoadToLocal(ctx, cfg.dir, extStorage, cfg.fileType, cfg.workerNums)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrRedoDownloadFailed, err)
		}
	}

	rr, err := openSelectedFiles(ctx, cfg.dir, cfg.fileType, cfg.startTs, cfg.workerNums)
	if err != nil {
		return nil, err
	}

	readers := []fileReader{}
	for i := range rr {
		readers = append(readers,
			&reader{
				cfg:      cfg,
				br:       bufio.NewReader(rr[i]),
				fileName: rr[i].(*os.File).Name(),
				closer:   rr[i],
			})
	}

	return readers, nil
}

func selectDownLoadFile(
	ctx context.Context, extStorage storage.ExternalStorage, fixedType string,
) ([]string, error) {
	files := []string{}
	err := extStorage.WalkDir(ctx, &storage.WalkOption{},
		func(path string, size int64) error {
			fileName := filepath.Base(path)
			_, fileType, err := redo.ParseLogFileName(fileName)
			if err != nil {
				return err
			}

			if fileType == fixedType {
				files = append(files, path)
			}
			return nil
		})
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrExternalStorageAPI, err)
	}

	return files, nil
}

func downLoadToLocal(
	ctx context.Context, dir string,
	extStorage storage.ExternalStorage,
	fixedType string, workerNum int,
) error {
	files, err := selectDownLoadFile(ctx, extStorage, fixedType)
	if err != nil {
		return err
	}

	limit := make(chan struct{}, workerNum)
	eg, eCtx := errgroup.WithContext(ctx)
	for _, file := range files {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case limit <- struct{}{}:
		}
		f := file
		eg.Go(func() error {
			defer func() { <-limit }()
			data, err := extStorage.ReadFile(eCtx, f)
			if err != nil {
				return cerror.WrapError(cerror.ErrExternalStorageAPI, err)
			}

			err = os.MkdirAll(dir, redo.DefaultDirMode)
			if err != nil {
				return cerror.WrapError(cerror.ErrRedoFileOp, err)
			}
			path := filepath.Join(dir, f)
			err = os.WriteFile(path, data, redo.DefaultFileMode)
			return cerror.WrapError(cerror.ErrRedoFileOp, err)
		})
	}

	return eg.Wait()
}

func openSelectedFiles(ctx context.Context, dir, fixedType string, startTs uint64, workerNum int) ([]io.ReadCloser, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrRedoFileOp, errors.Annotatef(err, "can't read log file directory: %s", dir))
	}

	sortedFileList := map[string]bool{}
	for _, file := range files {
		if filepath.Ext(file.Name()) == redo.SortLogEXT {
			sortedFileList[file.Name()] = false
		}
	}

	logFiles := []io.ReadCloser{}
	unSortedFile := []string{}
	for _, f := range files {
		name := f.Name()
		ret, err := shouldOpen(startTs, name, fixedType)
		if err != nil {
			log.Warn("check selected log file fail",
				zap.String("logFile", name),
				zap.Error(err))
			continue
		}

		if ret {
			sortedName := name
			if filepath.Ext(sortedName) != redo.SortLogEXT {
				sortedName += redo.SortLogEXT
			}
			if opened, ok := sortedFileList[sortedName]; ok {
				if opened {
					continue
				}
			} else {
				unSortedFile = append(unSortedFile, name)
				continue
			}
			path := filepath.Join(dir, sortedName)
			file, err := openReadFile(path)
			if err != nil {
				return nil, cerror.WrapError(cerror.ErrRedoFileOp, errors.Annotate(err, "can't open redo logfile"))
			}
			logFiles = append(logFiles, file)
			sortedFileList[sortedName] = true
		}
	}

	sortFiles, err := createSortedFiles(ctx, dir, unSortedFile, workerNum)
	if err != nil {
		return nil, err
	}
	logFiles = append(logFiles, sortFiles...)
	return logFiles, nil
}

func openReadFile(name string) (*os.File, error) {
	return os.OpenFile(name, os.O_RDONLY, redo.DefaultFileMode)
}

func readFile(file *os.File) (logHeap, error) {
	r := &reader{
		br:       bufio.NewReader(file),
		fileName: file.Name(),
		closer:   file,
	}
	defer r.Close()

	h := logHeap{}
	for {
		rl := &model.RedoLog{}
		err := r.Read(rl)
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			break
		}
		h = append(h, &logWithIdx{data: rl})
	}

	return h, nil
}

// writFile if not safely closed, the sorted file will end up with .sort.tmp as the file name suffix
func writFile(ctx context.Context, dir, name string, h logHeap) error {
	cfg := &writer.FileWriterConfig{
		Dir:        dir,
		MaxLogSize: math.MaxInt32,
	}
	w, err := writer.NewWriter(ctx, cfg, writer.WithLogFileName(func() string { return name }))
	if err != nil {
		return err
	}

	for h.Len() != 0 {
		item := heap.Pop(&h).(*logWithIdx).data
		data, err := item.MarshalMsg(nil)
		if err != nil {
			return cerror.WrapError(cerror.ErrMarshalFailed, err)
		}
		_, err = w.Write(data)
		if err != nil {
			return err
		}
	}

	return w.Close()
}

func createSortedFiles(ctx context.Context, dir string, names []string, workerNum int) ([]io.ReadCloser, error) {
	logFiles := []io.ReadCloser{}
	errCh := make(chan error)
	retCh := make(chan io.ReadCloser)

	var errs error
	i := 0
	for i != len(names) {
		nn := []string{}
		for i < len(names) {
			if len(nn) < workerNum {
				nn = append(nn, names[i])
				i++
				continue
			}
			break
		}

		for i := 0; i < len(nn); i++ {
			go createSortedFile(ctx, dir, nn[i], errCh, retCh)
		}
		for i := 0; i < len(nn); i++ {
			select {
			case err := <-errCh:
				errs = multierr.Append(errs, err)
			case ret := <-retCh:
				if ret != nil {
					logFiles = append(logFiles, ret)
				}
			}
		}
		if errs != nil {
			return nil, errs
		}
	}

	return logFiles, nil
}

func createSortedFile(ctx context.Context, dir string, name string, errCh chan error, retCh chan io.ReadCloser) {
	path := filepath.Join(dir, name)
	file, err := openReadFile(path)
	if err != nil {
		errCh <- cerror.WrapError(cerror.ErrRedoFileOp, errors.Annotate(err, "can't open redo logfile"))
		return
	}

	h, err := readFile(file)
	if err != nil {
		errCh <- err
		return
	}

	heap.Init(&h)
	if h.Len() == 0 {
		retCh <- nil
		return
	}

	sortFileName := name + redo.SortLogEXT
	err = writFile(ctx, dir, sortFileName, h)
	if err != nil {
		errCh <- err
		return
	}

	file, err = openReadFile(filepath.Join(dir, sortFileName))
	if err != nil {
		errCh <- cerror.WrapError(cerror.ErrRedoFileOp, errors.Annotate(err, "can't open redo logfile"))
		return
	}
	retCh <- file
}

func shouldOpen(startTs uint64, name, fixedType string) (bool, error) {
	// .sort.tmp will return error
	commitTs, fileType, err := redo.ParseLogFileName(name)
	if err != nil {
		return false, err
	}
	if fileType != fixedType {
		return false, nil
	}
	// always open .tmp
	if filepath.Ext(name) == redo.TmpEXT {
		return true, nil
	}
	// the commitTs=max(ts of log item in the file), if max > startTs then should open,
	// filter out ts in (startTs, endTs] for consume
	return commitTs > startTs, nil
}

// Read implement Read interface.
// TODO: more general reader pair with writer in writer pkg
func (r *reader) Read(redoLog *model.RedoLog) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	lenField, err := readInt64(r.br)
	if err != nil {
		if err == io.EOF {
			return err
		}
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	recBytes, padBytes := decodeFrameSize(lenField)
	data := make([]byte, recBytes+padBytes)
	_, err = io.ReadFull(r.br, data)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			log.Warn("read redo log have unexpected io error",
				zap.String("fileName", r.fileName),
				zap.Error(err))
			return io.EOF
		}
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	_, err = redoLog.UnmarshalMsg(data[:recBytes])
	if err != nil {
		if r.isTornEntry(data) {
			// just return io.EOF, since if torn write it is the last redoLog entry
			return io.EOF
		}
		return cerror.WrapError(cerror.ErrUnmarshalFailed, err)
	}

	// point last valid offset to the end of redoLog
	r.lastValidOff += frameSizeBytes + recBytes + padBytes
	return nil
}

func readInt64(r io.Reader) (int64, error) {
	var n int64
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}

// decodeFrameSize pair with encodeFrameSize in writer.file
// the func use code from etcd wal/decoder.go
func decodeFrameSize(lenField int64) (recBytes int64, padBytes int64) {
	// the record size is stored in the lower 56 bits of the 64-bit length
	recBytes = int64(uint64(lenField) & ^(uint64(0xff) << 56))
	// non-zero padding is indicated by set MSb / a negative length
	if lenField < 0 {
		// padding is stored in lower 3 bits of length MSB
		padBytes = int64((uint64(lenField) >> 56) & 0x7)
	}
	return recBytes, padBytes
}

// isTornEntry determines whether the last entry of the Log was partially written
// and corrupted because of a torn write.
// the func use code from etcd wal/decoder.go
// ref: https://github.com/etcd-io/etcd/pull/5250
func (r *reader) isTornEntry(data []byte) bool {
	fileOff := r.lastValidOff + frameSizeBytes
	curOff := 0
	chunks := [][]byte{}
	// split data on sector boundaries
	for curOff < len(data) {
		chunkLen := int(redo.MinSectorSize - (fileOff % redo.MinSectorSize))
		if chunkLen > len(data)-curOff {
			chunkLen = len(data) - curOff
		}
		chunks = append(chunks, data[curOff:curOff+chunkLen])
		fileOff += int64(chunkLen)
		curOff += chunkLen
	}

	// if any data for a sector chunk is all 0, it's a torn write
	for _, sect := range chunks {
		isZero := true
		for _, v := range sect {
			if v != 0 {
				isZero = false
				break
			}
		}
		if isZero {
			return true
		}
	}
	return false
}

// Close implement the Close interface
func (r *reader) Close() error {
	if r == nil || r.closer == nil {
		return nil
	}

	return cerror.WrapError(cerror.ErrRedoFileOp, r.closer.Close())
}
