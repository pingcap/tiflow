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
	"sync/atomic"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
)

type memoryBackEnd struct {
	events   []*model.PolymorphicEvent
	borrowed int32
}

func (m *memoryBackEnd) reader() (backEndReader, error) {
	failpoint.Inject("sorterDebug", func() {
		if atomic.SwapInt32(&m.borrowed, 1) != 0 {
			log.Fatal("memoryBackEnd: already borrowed")
		}
	})

	return &memoryBackEndReader{
		backEnd:   m,
		readIndex: 0,
	}, nil
}

func (m *memoryBackEnd) writer() (backEndWriter, error) {
	failpoint.Inject("sorterDebug", func() {
		if atomic.SwapInt32(&m.borrowed, 1) != 0 {
			log.Fatal("memoryBackEnd: already borrowed")
		}
	})

	return &memoryBackEndWriter{backEnd: m}, nil
}

func (m *memoryBackEnd) free() error {
	failpoint.Inject("sorterDebug", func() {
		if atomic.LoadInt32(&m.borrowed) != 0 {
			log.Fatal("fileBackEnd: trying to free borrowed file")
		}
	})

	return nil
}

type memoryBackEndReader struct {
	backEnd   *memoryBackEnd
	readIndex int
}

func (r *memoryBackEndReader) readNext() (*model.PolymorphicEvent, error) {
	// Check for "EOF"
	if r.readIndex >= len(r.backEnd.events) {
		return nil, nil
	}

	ret := r.backEnd.events[r.readIndex]
	r.readIndex++
	return ret, nil
}

func (r *memoryBackEndReader) resetAndClose() error {
	failpoint.Inject("sorterDebug", func() {
		atomic.StoreInt32(&r.backEnd.borrowed, 0)
	})

	return nil
}

type memoryBackEndWriter struct {
	backEnd      *memoryBackEnd
	bytesWritten int64
}

func (m memoryBackEndWriter) writeNext(event *model.PolymorphicEvent) error {
	panic("implement me")
}

func (m memoryBackEndWriter) writtenCount() int {
	panic("implement me")
}

func (m memoryBackEndWriter) dataSize() uint64 {
	panic("implement me")
}

func (m memoryBackEndWriter) flushAndClose() error {
	panic("implement me")
}
