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

	"github.com/pingcap/ticdc/dm/pkg/binlog/common"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"github.com/pingcap/ticdc/dm/pkg/terror"
)

// BinlogWriter is a binlog event writer which writes binlog events to a file.
type BinlogWriter struct {
	cfg *BinlogWriterConfig

	mu     sync.RWMutex
	stage  common.Stage
	offset atomic.Int64

	file *os.File

	logger log.Logger
}

// BinlogWriterStatus represents the status of a BinlogWriter.
type BinlogWriterStatus struct {
	Stage    string `json:"stage"`
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

// BinlogWriterConfig is the configuration used by a BinlogWriter.
type BinlogWriterConfig struct {
	Filename string
}

// NewBinlogWriter creates a BinlogWriter instance.
func NewBinlogWriter(logger log.Logger, cfg *BinlogWriterConfig) *BinlogWriter {
	return &BinlogWriter{
		cfg:    cfg,
		logger: logger,
	}
}

// Start implements Writer.Start.
func (w *BinlogWriter) Start() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.stage != common.StageNew {
		return terror.ErrBinlogWriterNotStateNew.Generate(w.stage, common.StageNew)
	}

	f, err := os.OpenFile(w.cfg.Filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
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

	w.offset.Store(fs.Size())
	w.file = f
	w.stage = common.StagePrepared
	return nil
}

// Close implements Writer.Close.
func (w *BinlogWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.stage != common.StagePrepared {
		return terror.ErrBinlogWriterStateCannotClose.Generate(w.stage, common.StagePrepared)
	}

	var err error
	if w.file != nil {
		err2 := w.flush() // try flush manually before close.
		if err2 != nil {
			w.logger.Error("fail to flush buffered data", zap.String("component", "file writer"), zap.Error(err2))
		}
		err = w.file.Close()
		w.file = nil
	}

	w.stage = common.StageClosed
	return err
}

// Write implements Writer.Write.
func (w *BinlogWriter) Write(rawData []byte) error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.stage != common.StagePrepared {
		return terror.ErrBinlogWriterNeedStart.Generate(w.stage, common.StagePrepared)
	}

	n, err := w.file.Write(rawData)
	w.offset.Add(int64(n))

	return terror.ErrBinlogWriterWriteDataLen.Delegate(err, len(rawData))
}

// Flush implements Writer.Flush.
func (w *BinlogWriter) Flush() error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.stage != common.StagePrepared {
		return terror.ErrBinlogWriterNeedStart.Generate(w.stage, common.StagePrepared)
	}

	return w.flush()
}

// Status implements Writer.Status.
func (w *BinlogWriter) Status() interface{} {
	w.mu.RLock()
	stage := w.stage
	w.mu.RUnlock()

	return &BinlogWriterStatus{
		Stage:    stage.String(),
		Filename: w.cfg.Filename,
		Offset:   w.offset.Load(),
	}
}

// flush flushes the buffered data to the disk.
func (w *BinlogWriter) flush() error {
	if w.file == nil {
		return terror.ErrBinlogWriterFileNotOpened.Generate(w.cfg.Filename)
	}
	return terror.ErrBinlogWriterFileSync.Delegate(w.file.Sync())
}
