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
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/model/codec"
	"github.com/pingcap/tiflow/cdc/redo/writer"
	"github.com/pingcap/tiflow/cdc/redo/writer/file"
	"github.com/pingcap/tiflow/pkg/compression"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/redo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// frameSizeBytes is frame size in bytes, including record size and padding size.
	frameSizeBytes = 8

	// defaultWorkerNum is the num of workers used to sort the log file to sorted file,
	// will load the file to memory first then write the sorted file to disk
	// the memory used is defaultWorkerNum * defaultMaxLogSize (64 * megabyte) total
	defaultWorkerNum = 16
)

// lz4MagicNumber is the magic number of lz4 compressed data
var lz4MagicNumber = []byte{0x04, 0x22, 0x4D, 0x18}

type fileReader interface {
	io.Closer
	// Read return the log from log file
	Read() (*model.RedoLog, error)
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
	br       io.Reader
	fileName string
	closer   io.Closer
	// lastValidOff file offset following the last valid decoded record
	lastValidOff int64
}

func newReaders(ctx context.Context, cfg *readerConfig) ([]fileReader, error) {
	if cfg == nil {
		return nil, cerror.WrapError(cerror.ErrRedoConfigInvalid, errors.New("readerConfig can not be nil"))
	}
	if !cfg.useExternalStorage {
		log.Panic("external storage is not enabled, please check your configuration")
	}
	if cfg.workerNums == 0 {
		cfg.workerNums = defaultWorkerNum
	}
	start := time.Now()

	sortedFiles, err := downLoadAndSortFiles(ctx, cfg)
	if err != nil {
		return nil, err
	}

	readers := []fileReader{}
	for i := range sortedFiles {
		readers = append(readers,
			&reader{
				cfg:      cfg,
				br:       bufio.NewReader(sortedFiles[i]),
				fileName: sortedFiles[i].(*os.File).Name(),
				closer:   sortedFiles[i],
			})
	}

	log.Info("succeed to download and sort redo logs",
		zap.String("type", cfg.fileType),
		zap.Duration("duration", time.Since(start)))
	return readers, nil
}

func downLoadAndSortFiles(ctx context.Context, cfg *readerConfig) ([]io.ReadCloser, error) {
	dir := cfg.dir
	// create temp dir in local storage
	err := os.MkdirAll(dir, redo.DefaultDirMode)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	// get all files
	extStorage, err := redo.InitExternalStorage(ctx, cfg.uri)
	if err != nil {
		return nil, err
	}
	files, err := selectDownLoadFile(ctx, extStorage, cfg.fileType, cfg.startTs)
	if err != nil {
		return nil, err
	}

	limit := make(chan struct{}, cfg.workerNums)
	eg, eCtx := errgroup.WithContext(ctx)
	sortedFileNames := make([]string, 0, len(files))
	for _, file := range files {
		select {
		case <-eCtx.Done():
			return nil, eCtx.Err()
		case limit <- struct{}{}:
		}

		fileName := file
		if strings.HasSuffix(fileName, redo.SortLogEXT) {
			log.Panic("should not download sorted log file")
		}
		sortedFileNames = append(sortedFileNames, getSortedFileName(fileName))
		eg.Go(func() error {
			defer func() { <-limit }()
			return sortAndWriteFile(ctx, extStorage, fileName, cfg)
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	// open all sorted files
	ret := []io.ReadCloser{}
	for _, sortedFileName := range sortedFileNames {
		path := filepath.Join(dir, sortedFileName)
		f, err := os.OpenFile(path, os.O_RDONLY, redo.DefaultFileMode)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, cerror.WrapError(cerror.ErrRedoFileOp, err)
		}
		ret = append(ret, f)
	}
	return ret, nil
}

func getSortedFileName(name string) string {
	return filepath.Base(name) + redo.SortLogEXT
}

func selectDownLoadFile(
	ctx context.Context, extStorage storage.ExternalStorage,
	fixedType string, startTs uint64,
) ([]string, error) {
	files := []string{}
	// add changefeed filter and endTs filter
	err := extStorage.WalkDir(ctx, &storage.WalkOption{},
		func(path string, size int64) error {
			fileName := filepath.Base(path)
			ret, err := shouldOpen(startTs, fileName, fixedType)
			if err != nil {
				log.Warn("check selected log file fail",
					zap.String("logFile", fileName),
					zap.Error(err))
				return err
			}
			if ret {
				files = append(files, path)
			}
			return nil
		})
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrExternalStorageAPI, err)
	}

	return files, nil
}

func isLZ4Compressed(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	return bytes.Equal(data[:4], lz4MagicNumber)
}

func readAllFromBuffer(buf []byte) (logHeap, error) {
	r := &reader{
		br: bytes.NewReader(buf),
	}
	defer r.Close()

	h := logHeap{}
	for {
		rl, err := r.Read()
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

// sortAndWriteFile read file from external storage, then sort the file and write
// to local storage.
func sortAndWriteFile(
	egCtx context.Context,
	extStorage storage.ExternalStorage,
	fileName string, cfg *readerConfig,
) error {
	sortedName := getSortedFileName(fileName)
	writerCfg := &writer.LogWriterConfig{
		Dir:               cfg.dir,
		MaxLogSizeInBytes: math.MaxInt32,
	}
	w, err := file.NewFileWriter(egCtx, writerCfg, writer.WithLogFileName(func() string {
		return sortedName
	}))
	if err != nil {
		return err
	}

	fileContent, err := extStorage.ReadFile(egCtx, fileName)
	if err != nil {
		return cerror.WrapError(cerror.ErrExternalStorageAPI, err)
	}
	if len(fileContent) == 0 {
		log.Warn("download file is empty", zap.String("file", fileName))
		return nil
	}
	// it's lz4 compressed, decompress it
	if isLZ4Compressed(fileContent) {
		if fileContent, err = compression.Decode(compression.LZ4, fileContent); err != nil {
			return err
		}
	}

	// sort data
	h, err := readAllFromBuffer(fileContent)
	if err != nil {
		return err
	}
	heap.Init(&h)
	for h.Len() != 0 {
		item := heap.Pop(&h).(*logWithIdx).data
		// This is min commitTs in log heap.
		if item.GetCommitTs() > cfg.endTs {
			// If the commitTs is greater than endTs, we should stop sorting
			// and ignore the rest of the logs.
			log.Info("ignore logs which commitTs is greater than resolvedTs",
				zap.Any("filename", fileName), zap.Uint64("endTs", cfg.endTs))
			break
		}
		if item.GetCommitTs() <= cfg.startTs {
			// If the commitTs is equal or less than startTs, we should skip this log.
			continue
		}
		data, err := codec.MarshalRedoLog(item, nil)
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
func (r *reader) Read() (*model.RedoLog, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	lenField, err := readInt64(r.br)
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	recBytes, padBytes := decodeFrameSize(lenField)
	data := make([]byte, recBytes+padBytes)
	_, err = io.ReadFull(r.br, data)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			log.Warn("read redo log have unexpected io error",
				zap.String("fileName", r.fileName),
				zap.Error(err))
			return nil, io.EOF
		}
		return nil, cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	redoLog, _, err := codec.UnmarshalRedoLog(data[:recBytes])
	if err != nil {
		if r.isTornEntry(data) {
			// just return io.EOF, since if torn write it is the last redoLog entry
			return nil, io.EOF
		}
		return nil, cerror.WrapError(cerror.ErrUnmarshalFailed, err)
	}

	// point last valid offset to the end of redoLog
	r.lastValidOff += frameSizeBytes + recBytes + padBytes
	return redoLog, nil
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
