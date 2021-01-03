// Copyright 2020 PingCAP, Inc.
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

package sorter

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

const (
	fileBufferSize = 1 * 1024 * 1024 // 1MB
	magic          = 0xbeefbeef
)

var openFDCount int64

type fileBackEnd struct {
	fileName string
	serde    serializerDeserializer
	borrowed int32
	size     int64
}

func newFileBackEnd(fileName string, serde serializerDeserializer) (*fileBackEnd, error) {
	f, err := os.Create(fileName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = f.Close()
	if err != nil {
		return nil, errors.Trace(err)
	}

	log.Debug("new FileSorterBackEnd created", zap.String("filename", fileName))
	return &fileBackEnd{
		fileName: fileName,
		serde:    serde,
		borrowed: 0,
	}, nil
}

func (f *fileBackEnd) reader() (backEndReader, error) {
	fd, err := os.OpenFile(f.fileName, os.O_RDWR, 0o644)
	if err != nil {
		return nil, errors.Trace(err)
	}

	atomic.AddInt64(&openFDCount, 1)

	var totalSize int64
	failpoint.Inject("sorterDebug", func() {
		info, err := fd.Stat()
		if err != nil {
			failpoint.Return(nil, errors.Trace(err))
		}
		totalSize = info.Size()
	})

	failpoint.Inject("sorterDebug", func() {
		if atomic.SwapInt32(&f.borrowed, 1) != 0 {
			log.Panic("fileBackEnd: already borrowed", zap.String("fileName", f.fileName))
		}
	})

	return &fileBackEndReader{
		backEnd:   f,
		f:         fd,
		reader:    bufio.NewReaderSize(fd, fileBufferSize),
		totalSize: totalSize,
	}, nil
}

func (f *fileBackEnd) writer() (backEndWriter, error) {
	fd, err := os.OpenFile(f.fileName, os.O_TRUNC|os.O_RDWR, 0o644)
	if err != nil {
		return nil, errors.Trace(err)
	}

	atomic.AddInt64(&openFDCount, 1)

	failpoint.Inject("sorterDebug", func() {
		if atomic.SwapInt32(&f.borrowed, 1) != 0 {
			log.Panic("fileBackEnd: already borrowed", zap.String("fileName", f.fileName))
		}
	})

	return &fileBackEndWriter{
		backEnd: f,
		f:       fd,
		writer:  bufio.NewWriterSize(fd, fileBufferSize),
	}, nil
}

func (f *fileBackEnd) free() error {
	failpoint.Inject("sorterDebug", func() {
		if atomic.LoadInt32(&f.borrowed) != 0 {
			log.Panic("fileBackEnd: trying to free borrowed file", zap.String("fileName", f.fileName))
		}
	})

	log.Debug("Removing file", zap.String("file", f.fileName))

	f.cleanStats()

	err := os.Remove(f.fileName)
	if err != nil {
		failpoint.Inject("sorterDebug", func() {
			failpoint.Return(errors.Trace(err))
		})
		// ignore this error in production to provide some resilience
		log.Warn("fileBackEnd: failed to remove file", zap.Error(err))
	}

	return nil
}

func (f *fileBackEnd) cleanStats() {
	if pool != nil {
		atomic.AddInt64(&pool.onDiskDataSize, -f.size)
	}
	f.size = 0
}

type fileBackEndReader struct {
	backEnd     *fileBackEnd
	f           *os.File
	reader      *bufio.Reader
	rawBytesBuf []byte
	isEOF       bool

	// debug only fields
	readBytes int64
	totalSize int64
}

func (r *fileBackEndReader) readNext() (*model.PolymorphicEvent, error) {
	if r.isEOF {
		// guaranteed EOF idempotency
		return nil, nil
	}

	var m uint32
	err := binary.Read(r.reader, binary.LittleEndian, &m)
	if err != nil {
		if err == io.EOF {
			r.isEOF = true
			return nil, nil
		}
		return nil, errors.Trace(err)
	}

	if m != magic {
		log.Panic("fileSorterBackEnd: wrong magic. Damaged file or bug?", zap.Uint32("magic", m))
	}

	var size uint32
	err = binary.Read(r.reader, binary.LittleEndian, &size)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if cap(r.rawBytesBuf) < int(size) {
		r.rawBytesBuf = make([]byte, size)
	} else {
		r.rawBytesBuf = r.rawBytesBuf[:size]
	}

	// short reads are possible with bufio, hence the need for io.ReadFull
	n, err := io.ReadFull(r.reader, r.rawBytesBuf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if n != int(size) {
		return nil, errors.Errorf("fileSorterBackEnd: expected %d bytes, actually read %d bytes", size, n)
	}

	event := new(model.PolymorphicEvent)
	_, err = r.backEnd.serde.unmarshal(event, r.rawBytesBuf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	failpoint.Inject("sorterDebug", func() {
		r.readBytes += int64(4 + 4 + int(size))
		if r.readBytes > r.totalSize {
			log.Panic("fileSorterBackEnd: read more bytes than expected, check concurrent use of file",
				zap.String("fileName", r.backEnd.fileName))
		}
	})

	return event, nil
}

func (r *fileBackEndReader) resetAndClose() error {
	defer func() {
		// fail-fast for double-close
		r.f = nil

		r.backEnd.cleanStats()

		failpoint.Inject("sorterDebug", func() {
			atomic.StoreInt32(&r.backEnd.borrowed, 0)
		})
	}()

	if r.f == nil {
		failpoint.Inject("sorterDebug", func() {
			log.Panic("Double closing of file", zap.String("filename", r.backEnd.fileName))
		})
		log.Warn("Double closing of file", zap.String("filename", r.backEnd.fileName))
		return nil
	}

	err := r.f.Truncate(0)
	if err != nil {
		failpoint.Inject("sorterDebug", func() {
			info, err1 := r.f.Stat()
			if err1 != nil {
				failpoint.Return(errors.Trace(err))
			}

			log.Info("file debug info", zap.String("filename", info.Name()),
				zap.Int64("size", info.Size()))

			failpoint.Return(nil)
		})
		log.Warn("fileBackEndReader: could not truncate file", zap.Error(err))
	}

	err = r.f.Close()
	if err != nil {
		failpoint.Inject("sorterDebug", func() {
			failpoint.Return(errors.Trace(err))
		})
		log.Warn("fileBackEndReader: could not close file", zap.Error(err))
		return nil
	}

	atomic.AddInt64(&openFDCount, -1)

	return nil
}

type fileBackEndWriter struct {
	backEnd     *fileBackEnd
	f           *os.File
	writer      *bufio.Writer
	rawBytesBuf []byte

	bytesWritten  int64
	eventsWritten int64
}

func (w *fileBackEndWriter) writeNext(event *model.PolymorphicEvent) error {
	var err error
	w.rawBytesBuf, err = w.backEnd.serde.marshal(event, w.rawBytesBuf)
	if err != nil {
		return errors.Trace(err)
	}

	size := len(w.rawBytesBuf)
	if size == 0 {
		log.Panic("fileSorterBackEnd: serialized to empty byte array. Bug?")
	}

	err = binary.Write(w.writer, binary.LittleEndian, uint32(magic))
	if err != nil {
		return errors.Trace(err)
	}

	err = binary.Write(w.writer, binary.LittleEndian, uint32(size))
	if err != nil {
		return errors.Trace(err)
	}

	// short writes are possible with bufio
	offset := 0
	for offset < size {
		n, err := w.writer.Write(w.rawBytesBuf[offset:])
		if err != nil {
			return errors.Trace(err)
		}
		offset += n
	}
	if offset != size {
		return errors.Errorf("fileSorterBackEnd: expected to write %d bytes, actually wrote %d bytes", size, offset)
	}

	w.eventsWritten++
	w.bytesWritten += int64(size)
	return nil
}

func (w *fileBackEndWriter) writtenCount() int {
	return int(w.bytesWritten)
}

func (w *fileBackEndWriter) dataSize() uint64 {
	return uint64(w.eventsWritten)
}

func (w *fileBackEndWriter) flushAndClose() error {
	defer func() {
		// fail-fast for double-close
		w.f = nil
	}()

	err := w.writer.Flush()
	if err != nil {
		return errors.Trace(err)
	}

	err = w.f.Close()
	if err != nil {
		failpoint.Inject("sorterDebug", func() {
			failpoint.Return(errors.Trace(err))
		})
		log.Warn("fileBackEndReader: could not close file", zap.Error(err))
		return nil
	}

	atomic.AddInt64(&openFDCount, -1)
	w.backEnd.size = w.bytesWritten
	atomic.AddInt64(&pool.onDiskDataSize, w.bytesWritten)

	failpoint.Inject("sorterDebug", func() {
		atomic.StoreInt32(&w.backEnd.borrowed, 0)
	})

	return nil
}
