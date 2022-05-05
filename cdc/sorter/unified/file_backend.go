// Copyright 2021 PingCAP, Inc.
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

package unified

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sorter/encoding"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

const (
	fileBufferSize       = 4 * 1024 // 4KB
	fileMagic            = 0x12345678
	numFileEntriesOffset = 4
	blockMagic           = 0xbeefbeef
)

var openFDCount int64

type fileBackEnd struct {
	fileName string
	serde    encoding.SerializerDeserializer
	borrowed int32
	size     int64
}

func newFileBackEnd(fileName string, serde encoding.SerializerDeserializer) (*fileBackEnd, error) {
	f, err := os.Create(fileName)
	if err != nil {
		return nil, errors.Trace(wrapIOError(err))
	}

	err = f.Close()
	if err != nil {
		return nil, errors.Trace(wrapIOError(err))
	}

	log.Debug("new FileSorterBackEnd created", zap.String("filename", fileName))
	return &fileBackEnd{
		fileName: fileName,
		serde:    serde,
		borrowed: 0,
	}, nil
}

func (f *fileBackEnd) reader() (backEndReader, error) {
	fd, err := os.OpenFile(f.fileName, os.O_RDWR, 0o600)
	if err != nil {
		return nil, errors.Trace(wrapIOError(err))
	}

	atomic.AddInt64(&openFDCount, 1)

	var totalSize int64
	failpoint.Inject("sorterDebug", func() {
		info, err := fd.Stat()
		if err != nil {
			failpoint.Return(nil, errors.Trace(wrapIOError(err)))
		}
		totalSize = info.Size()
	})

	failpoint.Inject("sorterDebug", func() {
		if atomic.SwapInt32(&f.borrowed, 1) != 0 {
			log.Panic("fileBackEnd: already borrowed", zap.String("fileName", f.fileName))
		}
	})

	ret := &fileBackEndReader{
		backEnd:   f,
		f:         fd,
		reader:    bufio.NewReaderSize(fd, fileBufferSize),
		totalSize: totalSize,
	}

	err = ret.readHeader()
	if err != nil {
		return nil, errors.Trace(wrapIOError(err))
	}

	return ret, nil
}

func (f *fileBackEnd) writer() (backEndWriter, error) {
	fd, err := os.OpenFile(f.fileName, os.O_TRUNC|os.O_RDWR, 0o600)
	if err != nil {
		return nil, errors.Trace(wrapIOError(err))
	}

	atomic.AddInt64(&openFDCount, 1)

	failpoint.Inject("sorterDebug", func() {
		if atomic.SwapInt32(&f.borrowed, 1) != 0 {
			log.Panic("fileBackEnd: already borrowed", zap.String("fileName", f.fileName))
		}
	})

	ret := &fileBackEndWriter{
		backEnd: f,
		f:       fd,
		writer:  bufio.NewWriterSize(fd, fileBufferSize),
	}

	err = ret.writeFileHeader()
	if err != nil {
		return nil, errors.Trace(wrapIOError(err))
	}

	return ret, nil
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
			failpoint.Return(errors.Trace(wrapIOError(err)))
		})
		// ignore this error in production to provide some resilience
		log.Warn("fileBackEnd: failed to remove file", zap.Error(wrapIOError(err)))
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
	backEnd *fileBackEnd
	f       *os.File
	reader  *bufio.Reader
	isEOF   bool

	// to prevent truncation-like corruption
	totalEvents uint64
	readEvents  uint64

	// debug only fields
	readBytes int64
	totalSize int64
}

func (r *fileBackEndReader) readHeader() error {
	failpoint.Inject("sorterDebug", func() {
		pos, err := r.f.Seek(0, 1 /* relative to the current position */)
		if err != nil {
			failpoint.Return(errors.Trace(err))
		}
		// verify that we are reading from the beginning of the file
		if pos != 0 {
			log.Panic("unexpected file descriptor cursor position", zap.Int64("pos", pos))
		}
	})

	var m uint32
	err := binary.Read(r.reader, binary.LittleEndian, &m)
	if err != nil {
		return errors.Trace(err)
	}
	if m != fileMagic {
		log.Panic("fileSorterBackEnd: wrong fileMagic. Damaged file or bug?", zap.Uint32("actual", m))
	}

	err = binary.Read(r.reader, binary.LittleEndian, &r.totalEvents)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
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
			// verifies that the file has not been truncated unexpectedly.
			if r.totalEvents != r.readEvents {
				log.Panic("unexpected EOF",
					zap.String("file", r.backEnd.fileName),
					zap.Uint64("expectedNumEvents", r.totalEvents),
					zap.Uint64("actualNumEvents", r.readEvents))
			}
			return nil, nil
		}
		return nil, errors.Trace(wrapIOError(err))
	}

	if m != blockMagic {
		log.Panic("fileSorterBackEnd: wrong blockMagic. Damaged file or bug?", zap.Uint32("actual", m))
	}

	var size uint32
	err = binary.Read(r.reader, binary.LittleEndian, &size)
	if err != nil {
		return nil, errors.Trace(wrapIOError(err))
	}

	// Note, do not hold the buffer in reader to avoid hogging memory.
	rawBytesBuf := make([]byte, size)

	// short reads are possible with bufio, hence the need for io.ReadFull
	n, err := io.ReadFull(r.reader, rawBytesBuf)
	if err != nil {
		return nil, errors.Trace(wrapIOError(err))
	}

	if n != int(size) {
		return nil, errors.Errorf("fileSorterBackEnd: expected %d bytes, actually read %d bytes", size, n)
	}

	event := new(model.PolymorphicEvent)
	_, err = r.backEnd.serde.Unmarshal(event, rawBytesBuf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	r.readEvents++

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
				failpoint.Return(errors.Trace(wrapIOError(err)))
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
	backEnd *fileBackEnd
	f       *os.File
	writer  *bufio.Writer

	bytesWritten  int64
	eventsWritten int64
}

func (w *fileBackEndWriter) writeFileHeader() error {
	err := binary.Write(w.writer, binary.LittleEndian, uint32(fileMagic))
	if err != nil {
		return errors.Trace(err)
	}

	// reserves the space for writing the total number of entries in this file
	err = binary.Write(w.writer, binary.LittleEndian, uint64(0))
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (w *fileBackEndWriter) writeNext(event *model.PolymorphicEvent) error {
	var err error
	// Note, do not hold the buffer in writer to avoid hogging memory.
	var rawBytesBuf []byte
	rawBytesBuf, err = w.backEnd.serde.Marshal(event, rawBytesBuf)
	if err != nil {
		return errors.Trace(wrapIOError(err))
	}

	size := len(rawBytesBuf)
	if size == 0 {
		log.Panic("fileSorterBackEnd: serialized to empty byte array. Bug?")
	}

	err = binary.Write(w.writer, binary.LittleEndian, uint32(blockMagic))
	if err != nil {
		return errors.Trace(wrapIOError(err))
	}

	err = binary.Write(w.writer, binary.LittleEndian, uint32(size))
	if err != nil {
		return errors.Trace(wrapIOError(err))
	}

	// short writes are possible with bufio
	offset := 0
	for offset < size {
		n, err := w.writer.Write(rawBytesBuf[offset:])
		if err != nil {
			return errors.Trace(wrapIOError(err))
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
	return int(w.eventsWritten)
}

func (w *fileBackEndWriter) dataSize() uint64 {
	return uint64(w.bytesWritten)
}

func (w *fileBackEndWriter) flushAndClose() error {
	defer func() {
		// fail-fast for double-close
		w.f = nil
	}()

	err := w.writer.Flush()
	if err != nil {
		return errors.Trace(wrapIOError(err))
	}

	_, err = w.f.Seek(numFileEntriesOffset, 0 /* relative to the beginning of the file */)
	if err != nil {
		return errors.Trace(wrapIOError(err))
	}

	// write the total number of entries in the file to the header
	err = binary.Write(w.f, binary.LittleEndian, uint64(w.eventsWritten))
	if err != nil {
		return errors.Trace(wrapIOError(err))
	}

	err = w.f.Close()
	if err != nil {
		failpoint.Inject("sorterDebug", func() {
			failpoint.Return(errors.Trace(wrapIOError(err)))
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

// wrapIOError should be called when the error is to be returned to an caller outside this file and
// if the error could be caused by a filesystem-related error.
func wrapIOError(err error) error {
	cause := errors.Cause(err)
	switch cause.(type) {
	case *os.PathError:
		// We don't generate stack in this helper function to avoid confusion.
		return cerrors.ErrUnifiedSorterIOError.FastGenByArgs(err.Error())
	default:
		return err
	}
}
