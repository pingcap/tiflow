// Copyright 2019 PingCAP, Inc.
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

package relay

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/ticdc/dm/pkg/log"
	"github.com/pingcap/ticdc/dm/pkg/terror"
)

// BinlogWriter is a binlog event writer which writes binlog events to a file.
type BinlogWriter struct {
	mu sync.RWMutex

	offset   atomic.Int64
	file     *os.File
	filename string

	logger log.Logger
}

// BinlogWriterStatus represents the status of a BinlogWriter.
type BinlogWriterStatus struct {
	Filename string `json:"filename"`
	Offset   int64  `json:"offset"`
}

// String implements Stringer.String.
func (s *BinlogWriterStatus) String() string {
	data, err := json.Marshal(s)
	if err != nil {
		// do not use %v/%+v for `s`, it will call this `String` recursively
		return fmt.Sprintf("marshal status %#v to json error %v", s, err)
	}
	return string(data)
}

// NewBinlogWriter creates a BinlogWriter instance.
func NewBinlogWriter(logger log.Logger) *BinlogWriter {
	return &BinlogWriter{
		logger: logger,
	}
}

func (w *BinlogWriter) Open(filename string) error {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return terror.ErrBinlogWriterOpenFile.Delegate(err)
	}
	fs, err := f.Stat()
	if err != nil {
		err2 := f.Close() // close the file opened before
		if err2 != nil {
			w.logger.Error("fail to close file", zap.String("component", "file writer"), zap.Error(err2))
		}
		return terror.ErrBinlogWriterGetFileStat.Delegate(err, f.Name())
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	w.offset.Store(fs.Size())
	w.file = f
	w.filename = filename

	return nil
}

func (w *BinlogWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var err error
	if w.file != nil {
		err2 := w.file.Sync() // try flush manually before close.
		if err2 != nil {
			w.logger.Error("fail to flush buffered data", zap.String("component", "file writer"), zap.Error(err2))
		}
		err = w.file.Close()
	}

	w.file = nil
	w.offset.Store(0)
	w.filename = ""

	return err
}

func (w *BinlogWriter) Write(rawData []byte) error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	n, err := w.file.Write(rawData)
	w.offset.Add(int64(n))

	return terror.ErrBinlogWriterWriteDataLen.Delegate(err, len(rawData))
}

func (w *BinlogWriter) Status() *BinlogWriterStatus {
	w.mu.RLock()
	w.mu.RUnlock()

	return &BinlogWriterStatus{
		Filename: w.filename,
		Offset:   w.offset.Load(),
	}
}

func (w *BinlogWriter) Offset() int64 {
	return w.offset.Load()
}
