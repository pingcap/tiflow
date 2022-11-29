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
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	bufferSize = 1 * 1024 * 1024 // 1MB
	chanSize   = 1024
)

var nilErr error

// BinlogWriter is a binlog event writer which writes binlog events to a file.
// Open/Write/Close cannot be called concurrently.
type BinlogWriter struct {
	offset   atomic.Int64
	file     *os.File
	relayDir string
	uuid     atomic.String
	filename atomic.String
	err      atomic.Error

	logger log.Logger

	input   chan []byte
	flushWg sync.WaitGroup
	wg      sync.WaitGroup
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
func NewBinlogWriter(logger log.Logger, relayDir string) *BinlogWriter {
	return &BinlogWriter{
		logger:   logger,
		relayDir: relayDir,
	}
}

// run starts the binlog writer.
func (w *BinlogWriter) run() {
	var (
		buf       = &bytes.Buffer{}
		errOccurs bool
	)

	// writeToFile writes buffer to file
	writeToFile := func() {
		if buf.Len() == 0 {
			return
		}

		if w.file == nil {
			w.err.CompareAndSwap(nilErr, terror.ErrRelayWriterNotOpened.Generate())
			errOccurs = true
			return
		}
		n, err := w.file.Write(buf.Bytes())
		if err != nil {
			w.err.CompareAndSwap(nilErr, terror.ErrBinlogWriterWriteDataLen.Delegate(err, n))
			errOccurs = true
			return
		}
		buf.Reset()
	}

	for bs := range w.input {
		if errOccurs {
			continue
		}
		if bs != nil {
			buf.Write(bs)
		}
		// we use bs = nil to mean flush
		if bs == nil || buf.Len() > bufferSize || len(w.input) == 0 {
			writeToFile()
		}
		if bs == nil {
			w.flushWg.Done()
		}
	}
	if !errOccurs {
		writeToFile()
	}
}

func (w *BinlogWriter) Open(uuid, filename string) error {
	fullName := filepath.Join(w.relayDir, uuid, filename)
	f, err := os.OpenFile(fullName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
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
	w.uuid.Store(uuid)
	w.filename.Store(filename)
	w.err.Store(nilErr)

	w.input = make(chan []byte, chanSize)
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.run()
	}()

	return nil
}

func (w *BinlogWriter) Close() error {
	if w.input != nil {
		close(w.input)
	}
	w.wg.Wait()

	if w.file != nil {
		if err := w.file.Sync(); err != nil {
			w.logger.Error("fail to flush buffered data", zap.String("component", "file writer"), zap.Error(err))
		}
		if err := w.file.Close(); err != nil {
			w.err.CompareAndSwap(nilErr, err)
		}
	}

	w.file = nil
	w.offset.Store(0)
	w.uuid.Store("")
	w.filename.Store("")
	w.input = nil
	return w.err.Swap(nilErr)
}

func (w *BinlogWriter) Write(rawData []byte) error {
	if w.file == nil {
		return terror.ErrRelayWriterNotOpened.Generate()
	}
	w.input <- rawData
	w.offset.Add(int64(len(rawData)))
	return w.err.Load()
}

func (w *BinlogWriter) Flush() error {
	w.flushWg.Add(1)
	if err := w.Write(nil); err != nil {
		return err
	}
	w.flushWg.Wait()
	return w.err.Load()
}

func (w *BinlogWriter) Status() *BinlogWriterStatus {
	return &BinlogWriterStatus{
		Filename: w.filename.Load(),
		Offset:   w.offset.Load(),
	}
}

func (w *BinlogWriter) Offset() int64 {
	return w.offset.Load()
}

func (w *BinlogWriter) isActive(uuid, filename string) (bool, int64) {
	return uuid == w.uuid.Load() && filename == w.filename.Load(), w.offset.Load()
}
