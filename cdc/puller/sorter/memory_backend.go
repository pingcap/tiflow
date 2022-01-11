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

package sorter

import (
	"sync/atomic"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

type memoryBackEnd struct {
	events        []*model.PolymorphicEvent
	estimatedSize int64
	borrowed      int32
}

func newMemoryBackEnd() *memoryBackEnd {
	return &memoryBackEnd{}
}

func (m *memoryBackEnd) reader() (backEndReader, error) {
	failpoint.Inject("sorterDebug", func() {
		if atomic.SwapInt32(&m.borrowed, 1) != 0 {
			log.Panic("memoryBackEnd: already borrowed")
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
			log.Panic("memoryBackEnd: already borrowed")
		}
	})

	return &memoryBackEndWriter{backEnd: m}, nil
}

func (m *memoryBackEnd) free() error {
	failpoint.Inject("sorterDebug", func() {
		if atomic.LoadInt32(&m.borrowed) != 0 {
			log.Panic("fileBackEnd: trying to free borrowed file")
		}
	})

	if pool != nil {
		atomic.AddInt64(&pool.memoryUseEstimate, -m.estimatedSize)
	}

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
	// Sets the slot to nil to prevent delaying GC.
	r.backEnd.events[r.readIndex] = nil
	r.readIndex++
	return ret, nil
}

func (r *memoryBackEndReader) resetAndClose() error {
	failpoint.Inject("sorterDebug", func() {
		atomic.StoreInt32(&r.backEnd.borrowed, 0)
	})

	if pool != nil {
		atomic.AddInt64(&pool.memoryUseEstimate, -r.backEnd.estimatedSize)
	}
	r.backEnd.estimatedSize = 0

	return nil
}

type memoryBackEndWriter struct {
	backEnd      *memoryBackEnd
	bytesWritten int64
	// for debugging only
	maxTs uint64
}

func (w *memoryBackEndWriter) writeNext(event *model.PolymorphicEvent) error {
	w.backEnd.events = append(w.backEnd.events, event)
	// 8 * 5 is for the 5 fields in PolymorphicEvent, each of which is thought of as a 64-bit pointer
	w.bytesWritten += 8*5 + event.RawKV.ApproximateSize()

	failpoint.Inject("sorterDebug", func() {
		if event.CRTs < w.maxTs {
			log.Panic("memoryBackEnd: ts regressed, bug?",
				zap.Uint64("prev-ts", w.maxTs),
				zap.Uint64("cur-ts", event.CRTs))
		}
		w.maxTs = event.CRTs
	})
	return nil
}

func (w *memoryBackEndWriter) writtenCount() int {
	return len(w.backEnd.events)
}

// dataSize for the memoryBackEnd returns only an estimation, as there is no serialization taking place.
func (w *memoryBackEndWriter) dataSize() uint64 {
	return uint64(w.bytesWritten)
}

func (w *memoryBackEndWriter) flushAndClose() error {
	failpoint.Inject("sorterDebug", func() {
		atomic.StoreInt32(&w.backEnd.borrowed, 0)
	})

	w.backEnd.estimatedSize = w.bytesWritten
	if pool != nil {
		atomic.AddInt64(&pool.memoryUseEstimate, w.bytesWritten)
	}

	return nil
}
