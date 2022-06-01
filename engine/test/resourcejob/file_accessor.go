// Copyright 2022 PingCAP, Inc.
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

package resourcejob

import (
	"bufio"
	"context"
	"strconv"

	"github.com/pingcap/errors"
	brStorage "github.com/pingcap/tidb/br/pkg/storage"

	"github.com/pingcap/tiflow/dm/pkg/log"
)

type fileWriter struct {
	storage  brStorage.ExternalStorage
	exWriter brStorage.ExternalFileWriter
}

// newFileWriter creates a new fileWriter.
// If fileName already exists, it will be deleted.
func newFileWriter(
	ctx context.Context,
	storage brStorage.ExternalStorage,
	fileName string,
) (*fileWriter, error) {
	fileExists, err := storage.FileExists(ctx, fileName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if fileExists {
		err := storage.DeleteFile(ctx, fileName)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	exWriter, err := storage.Create(ctx, fileName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &fileWriter{
		storage:  storage,
		exWriter: exWriter,
	}, nil
}

// AppendInt64 appends an int64 to the file.
func (w *fileWriter) AppendInt64(ctx context.Context, num int64) error {
	str := strconv.FormatInt(num, 10)
	str += "\n"
	_, err := w.exWriter.Write(ctx, []byte(str))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Close closes the fileWriter.
func (w *fileWriter) Close(ctx context.Context) error {
	return errors.Trace(w.exWriter.Close(ctx))
}

type fileReader struct {
	storage  brStorage.ExternalStorage
	exReader brStorage.ExternalFileReader

	// use bufio to make parsing lines easier.
	buf *bufio.Reader
}

func newFileReader(
	ctx context.Context,
	storage brStorage.ExternalStorage,
	fileName string,
) (*fileReader, error) {
	exReader, err := storage.Open(ctx, fileName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	buf := bufio.NewReader(exReader)
	return &fileReader{
		storage:  storage,
		exReader: exReader,
		buf:      buf,
	}, nil
}

// ReadInt64 reads an int64 from the file. The integers are expected
// to be separated by "\n".
// Returns os.EOF if the end of file is reached.
func (r *fileReader) ReadInt64(ctx context.Context) (int64, error) {
	str, isPrefix, err := r.buf.ReadLine()
	if err != nil {
		return 0, errors.Trace(err)
	}

	if isPrefix {
		log.L().Panic("Failed to readline, buf too small?")
	}

	num, err := strconv.ParseInt(string(str), 10, 64)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return num, nil
}

// Close closes the fileReader.
func (r *fileReader) Close() error {
	return errors.Trace(r.exReader.Close())
}
