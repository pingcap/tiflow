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
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/redo"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/net/context"
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
	dir           string
	fixedFileType string
	startTs       uint64
	endTs         uint64
	s3Storage     bool
	s3URI         *url.URL
}

type reader struct {
	cfg    *readerConfig
	mu     sync.Mutex
	br     *bufio.Reader
	closer io.Closer
	// lastValidOff file offset following the last valid decoded record
	lastValidOff int64
}

func newReader(ctx context.Context, cfg *readerConfig) []fileReader {
	if cfg == nil {
		log.Panic("readerConfig can not be nil")
		return nil
	}
	if cfg.s3Storage {
		s3storage, err := redo.InitS3storage(ctx, cfg.s3URI)
		if err != nil {
			log.Panic("initS3storage fail",
				zap.Error(err),
				zap.Any("s3URI", cfg.s3URI))
		}
		exts := []string{redo.LogEXT, redo.LogEXT}
		err = downLoadToLocal(ctx, cfg.dir, s3storage, exts)
		if err != nil {
			log.Panic("downLoadToLocal fail",
				zap.Error(err),
				zap.Strings("file type", exts),
				zap.Any("s3URI", cfg.s3URI))
		}
	}

	rr, err := openSelectedFiles(cfg.dir, cfg.fixedFileType, cfg.startTs, cfg.endTs)
	if err != nil {
		log.Panic("openSelectedFiles fail",
			zap.Error(err),
			zap.Any("cfg", cfg))
	}

	readers := []fileReader{}
	for i := range rr {
		readers = append(readers,
			&reader{
				cfg:    cfg,
				br:     bufio.NewReader(rr[i]),
				closer: rr[i],
			})
	}

	return readers
}

func selectDownLoadFile(ctx context.Context, s3storage storage.ExternalStorage, exts []string) ([]string, error) {
	files := []string{}
	err := s3storage.WalkDir(ctx, &storage.WalkOption{}, func(path string, size int64) error {
		fileName := filepath.Base(path)
		for _, ext := range exts {
			if filepath.Ext(fileName) == ext {
				files = append(files, path)
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrS3StorageAPI, err)
	}

	return files, nil
}

func downLoadToLocal(ctx context.Context, dir string, s3storage storage.ExternalStorage, exts []string) error {
	files, err := selectDownLoadFile(ctx, s3storage, exts)
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
			err = ioutil.WriteFile(path, data, redo.DefaultFileMode)
			return cerror.WrapError(cerror.ErrRedoFileOp, err)
		})
	}

	return eg.Wait()
}

func openSelectedFiles(dir, fixedName string, startTs, endTs uint64) ([]io.ReadCloser, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrRedoFileOp, errors.Annotate(err, "can't read log file directory"))
	}

	logFiles := []io.ReadCloser{}
	for _, f := range files {
		ret, err := shouldOpen(startTs, endTs, f.Name(), fixedName)
		if err != nil {
			log.Warn("check selected log file fail",
				zap.String("log file", f.Name()),
				zap.Error(err))
			continue
		}

		if ret {
			path := filepath.Join(dir, f.Name())
			file, err := os.OpenFile(path, os.O_RDONLY, redo.DefaultFileMode)
			if err != nil {
				return nil, cerror.WrapError(cerror.ErrRedoFileOp, errors.Annotate(err, "can't open redo logfile"))
			}
			logFiles = append(logFiles, file)
		}
	}

	return logFiles, nil
}

func shouldOpen(startTs, endTs uint64, name, fixedName string) (bool, error) {
	// always open tmp file, because the file is writing into data when closed
	if filepath.Ext(name) == redo.TmpEXT {
		return true, nil
	}

	commitTs, fileName, err := parseLogFileName(name)
	if err != nil {
		return false, err
	}
	if fileName != fixedName {
		return false, nil
	}
	if commitTs > startTs && commitTs <= endTs {
		return true, nil
	}
	return false, nil
}

func parseLogFileName(name string) (uint64, string, error) {
	if filepath.Ext(name) != redo.LogEXT {
		return 0, "", errors.New("bad log name, file name extension not match")
	}

	var commitTs, d1 uint64
	var s1, s2, fileName string
	// TODO: extract to a common func
	_, err := fmt.Sscanf(name, "%s_%s_%d_%s_%d"+redo.LogEXT, &s1, &s2, &d1, &fileName, &commitTs)
	if err != nil {
		return 0, "", errors.New("bad log name")
	}
	return commitTs, fileName, nil
}

func (r *reader) Read(log *model.RedoLog) error {
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
		if err == io.EOF {
			return err
		}
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	_, err = log.UnmarshalMsg(data[:recBytes])
	if err != nil {
		if r.isTornEntry(data) {
			// just return io.EOF, since if torn write it is the last log entry
			return io.EOF
		}
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	// point last valid offset to the end of log
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

// Close ...
func (r *reader) Close() error {
	if r == nil || r.closer == nil {
		return nil
	}

	return cerror.WrapError(cerror.ErrRedoFileOp, r.closer.Close())
}
