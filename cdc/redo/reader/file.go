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

	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/redo/common"
	"github.com/pingcap/ticdc/cdc/redo/writer"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// frameSizeBytes is frame size in bytes, including record size and padding size.
	frameSizeBytes = 8
)

//go:generate mockery --name=fileReader --inpackage
type fileReader interface {
	io.Closer
	// Read ...
	Read(log *model.RedoLog) error
}

type readerConfig struct {
	dir       string
	fileType  string
	startTs   uint64
	endTs     uint64
	s3Storage bool
	s3URI     *url.URL
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

func newReader(ctx context.Context, cfg *readerConfig) []fileReader {
	if cfg == nil {
		log.Panic("readerConfig can not be nil")
		return nil
	}
	if cfg.s3Storage {
		s3storage, err := common.InitS3storage(ctx, cfg.s3URI)
		if err != nil {
			log.Panic("initS3storage fail",
				zap.Error(err),
				zap.Any("s3URI", cfg.s3URI))
		}
		err = downLoadToLocal(ctx, cfg.dir, s3storage, cfg.fileType)
		if err != nil {
			log.Panic("downLoadToLocal fail",
				zap.Error(err),
				zap.String("file type", cfg.fileType),
				zap.Any("s3URI", cfg.s3URI))
		}
	}

	rr, err := openSelectedFiles(ctx, cfg.dir, cfg.fileType, cfg.startTs, cfg.endTs)
	if err != nil {
		log.Panic("openSelectedFiles fail",
			zap.Error(err),
			zap.Any("cfg", cfg))
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

	return readers
}

func selectDownLoadFile(ctx context.Context, s3storage storage.ExternalStorage, fixedType string) ([]string, error) {
	files := []string{}
	err := s3storage.WalkDir(ctx, &storage.WalkOption{}, func(path string, size int64) error {
		fileName := filepath.Base(path)
		_, fileType, err := common.ParseLogFileName(fileName)
		if err != nil {
			return err
		}

		if fileType == fixedType {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrS3StorageAPI, err)
	}

	return files, nil
}

func downLoadToLocal(ctx context.Context, dir string, s3storage storage.ExternalStorage, fixedName string) error {
	files, err := selectDownLoadFile(ctx, s3storage, fixedName)
	if err != nil {
		return err
	}

	eg, eCtx := errgroup.WithContext(ctx)
	for _, file := range files {
		f := file
		eg.Go(func() error {
			data, err := s3storage.ReadFile(eCtx, f)
			if err != nil {
				return cerror.WrapError(cerror.ErrS3StorageAPI, err)
			}

			path := filepath.Join(dir, f)
			err = ioutil.WriteFile(path, data, common.DefaultFileMode)
			return cerror.WrapError(cerror.ErrRedoFileOp, err)
		})
	}

	return eg.Wait()
}

func openSelectedFiles(ctx context.Context, dir, fixedType string, startTs, endTs uint64) ([]io.ReadCloser, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrRedoFileOp, errors.Annotatef(err, "can't read log file directory: %s", dir))
	}

	sortedFileList := map[string]bool{}
	for _, file := range files {
		if filepath.Ext(file.Name()) == common.SortLogEXT {
			sortedFileList[file.Name()] = false
		}
	}

	logFiles := []io.ReadCloser{}
	unSortedFile := []string{}
	for _, f := range files {
		name := f.Name()
		ret, err := shouldOpen(startTs, endTs, name, fixedType)
		if err != nil {
			log.Warn("check selected log file fail",
				zap.String("log file", name),
				zap.Error(err))
			continue
		}

		if ret {
			sortedName := name
			if filepath.Ext(sortedName) != common.SortLogEXT {
				sortedName += common.SortLogEXT
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

	sortFiles, err := createSortedFile(ctx, dir, unSortedFile)
	if err != nil {
		return nil, err
	}
	logFiles = append(logFiles, sortFiles...)
	return logFiles, nil
}

func openReadFile(name string) (*os.File, error) {
	return os.OpenFile(name, os.O_RDONLY, common.DefaultFileMode)
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

func writFile(ctx context.Context, dir, name string, h logHeap) error {
	cfg := &writer.FileWriterConfig{
		Dir:        dir,
		MaxLogSize: math.MaxInt32,
	}
	w := writer.NewWriter(ctx, cfg, writer.WithLogFileName(func() string { return name }))
	defer w.Close()

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
	return nil
}

func createSortedFile(ctx context.Context, dir string, names []string) ([]io.ReadCloser, error) {
	logFiles := []io.ReadCloser{}

	// TODO: go func
	for _, name := range names {
		path := filepath.Join(dir, name)
		file, err := openReadFile(path)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrRedoFileOp, errors.Annotate(err, "can't open redo logfile"))
		}

		h, err := readFile(file)
		if err != nil {
			return nil, err
		}

		heap.Init(&h)
		if h.Len() == 0 {
			continue
		}

		sortFileName := name + common.SortLogEXT
		err = writFile(ctx, dir, sortFileName, h)
		if err != nil {
			return nil, err
		}

		file, err = openReadFile(filepath.Join(dir, sortFileName))
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrRedoFileOp, errors.Annotate(err, "can't open redo logfile"))
		}
		logFiles = append(logFiles, file)
	}

	return logFiles, nil
}

func shouldOpen(startTs, endTs uint64, name, fixedType string) (bool, error) {
	// .sort.tmp will return error
	commitTs, fileType, err := common.ParseLogFileName(name)
	if err != nil {
		return false, err
	}
	if fileType != fixedType {
		return false, nil
	}
	// always open .tmp
	if filepath.Ext(name) == common.TmpEXT {
		return true, nil
	}
	return commitTs > startTs && commitTs <= endTs, nil
}

// Read ...
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
// ref: https://github.com/etcd-io/etcd/pull/5250
func (r *reader) isTornEntry(data []byte) bool {
	fileOff := r.lastValidOff + frameSizeBytes
	curOff := 0
	chunks := [][]byte{}
	// split data on sector boundaries
	for curOff < len(data) {
		chunkLen := int(common.MinSectorSize - (fileOff % common.MinSectorSize))
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

// Close ...
func (r *reader) Close() error {
	if r == nil || r.closer == nil {
		return nil
	}

	return cerror.WrapError(cerror.ErrRedoFileOp, r.closer.Close())
}
