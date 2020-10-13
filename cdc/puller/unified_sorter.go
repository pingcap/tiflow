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

package puller

import (
	"bufio"
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"go.uber.org/zap"
	"io"
	"math"
	"os"
	"reflect"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/mackerelio/go-osstat/memory"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
)

const (
	fileBufferSize      = 1 * 1024 * 1024  // 1MB
	heapSizeLimit       = 16 * 1024 * 1024 // 16MB
	numConcurrentHeaps  = 16
	memoryPressureThres = 70
	magic               = 0xbeefbeef
)

type sorterBackEnd interface {
	readNext() (*model.PolymorphicEvent, error)
	writeNext(event *model.PolymorphicEvent) error
	getSize() int
	flush() error
	reset() error
}

type fileSorterBackEnd struct {
	f          *os.File
	readWriter *bufio.ReadWriter
	serde      serializerDeserializer
	rawBytes   []byte
	name       string
	size       int
}

func (f *fileSorterBackEnd) flush() error {
	err := f.readWriter.Flush()
	if err != nil {
		return errors.AddStack(err)
	}

	_, err = f.f.Seek(0, 0)
	if err != nil {
		return errors.Trace(err)
	}
	f.readWriter.Reader.Reset(f.f)
	f.readWriter.Writer.Reset(f.f)
	return nil
}

func (f *fileSorterBackEnd) getSize() int {
	return f.size
}

func (f *fileSorterBackEnd) reset() error {
	err := f.f.Truncate(int64(0))
	if err != nil {
		return errors.AddStack(err)
	}

	_, err = f.f.Seek(0, 0)
	if err != nil {
		return errors.AddStack(err)
	}

	f.size = 0
	f.readWriter.Reader.Reset(f.f)
	f.readWriter.Writer.Reset(f.f)
	return nil
}

func (f *fileSorterBackEnd) free() error {
	err := f.f.Close()
	if err != nil {
		return errors.AddStack(err)
	}

	err = os.Remove(f.name)
	if err != nil {
		return errors.AddStack(err)
	}
	return nil
}

type serializerDeserializer interface {
	marshal(event *model.PolymorphicEvent, bytes []byte) ([]byte, error)
	unmarshal(event *model.PolymorphicEvent, bytes []byte) ([]byte, error)
}

type msgPackGenSerde struct {
}

func (m *msgPackGenSerde) marshal(event *model.PolymorphicEvent, bytes []byte) ([]byte, error) {
	bytes = bytes[:0]
	return event.RawKV.MarshalMsg(bytes)
}

func (m *msgPackGenSerde) unmarshal(event *model.PolymorphicEvent, bytes []byte) ([]byte, error) {
	if event.RawKV == nil {
		event.RawKV = new(model.RawKVEntry)
	}

	bytes, err := event.RawKV.UnmarshalMsg(bytes)
	if err != nil {
		return nil, errors.AddStack(err)
	}

	event.StartTs = event.RawKV.StartTs
	event.CRTs = event.RawKV.CRTs

	return bytes, nil
}

func newFileSorterBackEnd(fileName string, serde serializerDeserializer) (*fileSorterBackEnd, error) {
	f, err := os.Create(fileName)
	if err != nil {
		return nil, errors.AddStack(err)
	}

	reader := bufio.NewReaderSize(f, fileBufferSize)
	writer := bufio.NewWriterSize(f, fileBufferSize)
	readWriter := bufio.NewReadWriter(reader, writer)
	rawBytes := make([]byte, 0, 1024)

	log.Debug("new FileSorterBackEnd created", zap.String("filename", fileName))
	return &fileSorterBackEnd{
		f:          f,
		readWriter: readWriter,
		serde:      serde,
		rawBytes:   rawBytes,
		name:       fileName}, nil
}

func (f *fileSorterBackEnd) readNext() (*model.PolymorphicEvent, error) {
	var m uint32
	err := binary.Read(f.readWriter, binary.LittleEndian, &m)
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, errors.AddStack(err)
	}
	if m != magic {
		log.Fatal("fileSorterBackEnd: wrong magic. Damaged file or bug?", zap.Uint32("magic", m))
	}

	var size uint32
	err = binary.Read(f.readWriter, binary.LittleEndian, &size)
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, errors.AddStack(err)
	}

	if cap(f.rawBytes) < int(size) {
		f.rawBytes = make([]byte, 0, size)
	}
	f.rawBytes = f.rawBytes[:size]

	err = binary.Read(f.readWriter, binary.LittleEndian, f.rawBytes)
	if err != nil {
		return nil, errors.AddStack(err)
	}

	event := new(model.PolymorphicEvent)
	_, err = f.serde.unmarshal(event, f.rawBytes)
	if err != nil {
		return nil, errors.AddStack(err)
	}

	return event, nil
}

func (f *fileSorterBackEnd) writeNext(event *model.PolymorphicEvent) error {
	var err error
	f.rawBytes, err = f.serde.marshal(event, f.rawBytes)
	if err != nil {
		return errors.AddStack(err)
	}

	size := len(f.rawBytes)
	if size == 0 {
		log.Fatal("fileSorterBackEnd: serialized to empty byte array. Bug?")
	}

	err = binary.Write(f.readWriter, binary.LittleEndian, uint32(magic))
	if err != nil {
		return errors.AddStack(err)
	}

	err = binary.Write(f.readWriter, binary.LittleEndian, uint32(size))
	if err != nil {
		return errors.AddStack(err)
	}

	err = binary.Write(f.readWriter, binary.LittleEndian, f.rawBytes)
	if err != nil {
		return errors.AddStack(err)
	}

	f.size = f.size + 8 + size
	return nil
}

type memorySorterBackEnd struct {
	events    []*model.PolymorphicEvent
	readIndex int
}

func (m *memorySorterBackEnd) readNext() (*model.PolymorphicEvent, error) {
	if m.readIndex >= len(m.events) {
		return nil, nil
	}
	ret := m.events[m.readIndex]
	m.readIndex += 1
	return ret, nil
}

func (m *memorySorterBackEnd) writeNext(event *model.PolymorphicEvent) error {
	m.events = append(m.events, event)
	return nil
}

func (m *memorySorterBackEnd) getSize() int {
	return -1
}

func (m *memorySorterBackEnd) flush() error {
	return nil
}

func (m *memorySorterBackEnd) reset() error {
	m.events = m.events[0:0]
	m.readIndex = 0
	return nil
}

type backEndPool struct {
	memoryUseEstimate int64
	fileNameCounter   uint64
	memPressure       int32
	cache             [256]unsafe.Pointer
	dir               string
}

func newBackEndPool(dir string) *backEndPool {
	ret := &backEndPool{
		memoryUseEstimate: 0,
		fileNameCounter:   0,
		dir:               dir,
	}

	go func() {
		ticker := time.NewTicker(15 * time.Second)

		for {
			<-ticker.C

			// update memPressure
			memory, err := memory.Get()
			if err != nil {
				log.Fatal("unified sorter: getting system memory usage failed", zap.Error(err))
			}

			memPressure := memory.Used * 100 / memory.Total
			atomic.StoreInt32(&ret.memPressure, int32(memPressure))
			if memPressure > 50 {
				log.Debug("unified sorter: high memory pressure", zap.Uint64("memPressure", memPressure))
			}

			// garbage collect temporary files in batches
			freedCount := 0
			for i := range ret.cache {
				ptr := &ret.cache[i]
				innerPtr := atomic.SwapPointer(ptr, nil)
				if innerPtr == nil {
					continue
				}
				backEnd := (*fileSorterBackEnd)(innerPtr)
				err := backEnd.free()
				if err != nil {
					log.Fatal("Cannot remove temporary file for sorting", zap.String("file", backEnd.name))
				}
				log.Info("Temporary file removed", zap.String("file", backEnd.name))
				freedCount += 1

				if freedCount >= 16 {
					freedCount = 0
					break
				}
			}
		}
	}()

	return ret
}

func (p *backEndPool) alloc() (sorterBackEnd, error) {
	if atomic.LoadInt32(&p.memPressure) < memoryPressureThres {
		ret := new(memorySorterBackEnd)
		atomic.AddInt64(&p.memoryUseEstimate, heapSizeLimit)
		return ret, nil
	}

	for i := range p.cache {
		ptr := &p.cache[i]
		ret := atomic.SwapPointer(ptr, nil)
		if ret != nil {
			return (*fileSorterBackEnd)(ret), nil
		}
	}

	fname := fmt.Sprintf("%s/sort-%d", p.dir, atomic.AddUint64(&p.fileNameCounter, 1))
	log.Debug("Unified Sorter: trying to create file backEnd")
	ret, err := newFileSorterBackEnd(fname, &msgPackGenSerde{})
	if err != nil {
		return nil, errors.AddStack(err)
	}

	atomic.AddInt64(&p.memoryUseEstimate, heapSizeLimit)
	return ret, nil
}

func (p *backEndPool) dealloc(backEnd sorterBackEnd) error {
	err := backEnd.reset()
	if err != nil {
		return errors.Trace(err)
	}

	switch b := backEnd.(type) {
	case *memorySorterBackEnd:
		atomic.AddInt64(&p.memoryUseEstimate, -heapSizeLimit)
		// Let GC do its job
		return nil
	case *fileSorterBackEnd:
		for i := range p.cache {
			ptr := &p.cache[i]
			if atomic.CompareAndSwapPointer(ptr, nil, unsafe.Pointer(b)) {
				return nil
			}
		}
		// Cache is full. Let GC do its job
		return nil
	default:
		log.Fatal("backEndPool: unexpected backEnd type to be deallocated", zap.Reflect("type", reflect.TypeOf(backEnd)))
	}
	return nil
}

type flushTask struct {
	heapSorterID  int
	backend       sorterBackEnd
	maxResolvedTs uint64
	finished      chan error
	dealloc       func() error
	lastTs        uint64 // for debugging TODO remove
}

type heapSorter struct {
	id          int
	inputCh     chan *model.PolymorphicEvent
	outputCh    chan *flushTask
	heap        sortHeap
	backEndPool *backEndPool
}

func newHeapSorter(id int, pool *backEndPool, out chan *flushTask) *heapSorter {
	return &heapSorter{
		id:          id,
		inputCh:     make(chan *model.PolymorphicEvent, 1024*1024),
		outputCh:    out,
		heap:        make(sortHeap, 0, 65536),
		backEndPool: pool,
	}
}

// flush should only be called within the main loop in run().
func (h *heapSorter) flush(ctx context.Context, maxResolvedTs uint64) error {
	isEmptyFlush := h.heap.Len() == 0
	var backEnd sorterBackEnd = nil

	if !isEmptyFlush {
		var err error
		backEnd, err = h.backEndPool.alloc()
		if err != nil {
			return errors.AddStack(err)
		}
	}

	task := &flushTask{
		heapSorterID:  h.id,
		backend:       backEnd,
		maxResolvedTs: maxResolvedTs,
		finished:      make(chan error),
	}

	var oldHeap sortHeap
	if !isEmptyFlush {
		task.dealloc = func() error {
			return h.backEndPool.dealloc(backEnd)
		}
		oldHeap = h.heap
		h.heap = make(sortHeap, 0, 65536)
	} else {
		task.dealloc = func() error {
			return nil
		}
	}

	log.Debug("Unified Sorter new flushTask", zap.Int("heap-id", task.heapSorterID),
		zap.Uint64("resolvedTs", task.maxResolvedTs))
	go func() {
		defer close(task.finished)
		if isEmptyFlush {
			return
		}
		batchSize := oldHeap.Len()
		for oldHeap.Len() > 0 {
			event := heap.Pop(&oldHeap).(*sortItem).entry
			err := task.backend.writeNext(event)
			if err != nil {
				task.finished <- err
				return
			}
		}
		err := task.backend.flush()
		if err != nil {
			task.finished <- err
		}

		log.Debug("Unified Sorter flushTask finished",
			zap.Int("heap-id", task.heapSorterID),
			zap.Uint64("resolvedTs", task.maxResolvedTs),
			zap.Int("size", batchSize))
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case h.outputCh <- task:
	}
	return nil
}

func (h *heapSorter) run(ctx context.Context) error {
	var (
		maxResolved           uint64
		heapSizeBytesEstimate int64
	)
	maxResolved = 0
	heapSizeBytesEstimate = 0
	flushTicker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-h.inputCh:
			heap.Push(&h.heap, &sortItem{entry: event})
			isResolvedEvent := event.RawKV != nil && event.RawKV.OpType == model.OpTypeResolved
			if isResolvedEvent {
				if event.RawKV.CRTs < maxResolved {
					log.Fatal("ResolvedTs regression, bug?", zap.Uint64("event-resolvedTs", event.RawKV.CRTs),
						zap.Uint64("max-resolvedTs", maxResolved))
				}
				maxResolved = event.RawKV.CRTs
			}

			heapSizeBytesEstimate += event.RawKV.ApproximateSize() + 128

			needFlush := heapSizeBytesEstimate >= heapSizeLimit || isResolvedEvent
			select {
			case <-flushTicker.C:
				needFlush = true
			default:
			}

			if needFlush {
				err := h.flush(ctx, maxResolved)
				if err != nil {
					return errors.AddStack(err)
				}
				heapSizeBytesEstimate = 0
			}
		}
	}
}

func runMerger(ctx context.Context, numSorters int, in chan *flushTask, out chan *model.PolymorphicEvent) error {
	lastResolvedTs := make([]uint64, numSorters)
	minResolvedTs := uint64(0)

	pendingSet := make(map[*flushTask]*model.PolymorphicEvent)

	lastOutputTs := uint64(0)

	sendResolvedEvent := func(ts uint64) error {
		if ts < lastOutputTs {
			log.Fatal("unified sorter: output ts regressed, bug?", zap.Uint64("cur-ts", ts), zap.Uint64("last-ts", lastOutputTs))
		}
		lastOutputTs = ts
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- model.NewResolvedPolymorphicEvent(0, ts):
			return nil
		}
	}

	onMinResolvedTsUpdate := func() error {
		workingSet := make(map[*flushTask]struct{})
		sortHeap := new(sortHeap)
		for task, cache := range pendingSet {
			if task.maxResolvedTs <= minResolvedTs {
				var event *model.PolymorphicEvent
				if cache != nil {
					event = cache
				} else {
					var err error

					after := time.After(5 * time.Second)
					select {
					case <-ctx.Done():
						return ctx.Err()
					case err := <-task.finished:
						if err != nil {
							return errors.Trace(err)
						}
					case <-after:
						log.Warn("unified sorter: backEnd flush too long", zap.Uint64("minResolvedTs", task.maxResolvedTs))
					}

					event, err = task.backend.readNext()
					if err != nil {
						return errors.Trace(err)
					}

					if event == nil {
						log.Fatal("Unexpected end of backEnd data, bug?", zap.Uint64("minResolvedTs", task.maxResolvedTs))
					}
				}

				if event.CRTs > minResolvedTs {
					pendingSet[task] = event
					continue
				}

				pendingSet[task] = nil
				workingSet[task] = struct{}{}

				heap.Push(sortHeap, &sortItem{
					entry: event,
					data:  task,
				})
			}
		}

		resolvedTicker := time.NewTicker(1 * time.Second)
		defer resolvedTicker.Stop()

		retire := func(task *flushTask) error {
			delete(workingSet, task)
			if pendingSet[task] != nil {
				return nil
			}
			nextEvent, err := task.backend.readNext()
			if err != nil {
				return errors.Trace(err)
			}

			if nextEvent == nil {
				delete(pendingSet, task)

				err := task.dealloc()
				if err != nil {
					return errors.Trace(err)
				}
			} else {
				pendingSet[task] = nextEvent
			}
			return nil
		}

		counter := 0
		for sortHeap.Len() > 0 {
			item := heap.Pop(sortHeap).(*sortItem)
			task := item.data.(*flushTask)
			event := item.entry

			if event.RawKV != nil && event.RawKV.OpType != model.OpTypeResolved {
				if event.CRTs < lastOutputTs {
					log.Fatal("unified sorter: output ts regressed, bug?", zap.Uint64("cur-ts", event.CRTs), zap.Uint64("last-ts", lastOutputTs))
				}
				lastOutputTs = event.CRTs
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- event:
				}
			}
			counter += 1

			if event.CRTs < task.lastTs {
				log.Fatal("unified sorter: ts regressed in one backEnd, bug?", zap.Uint64("cur-ts", event.CRTs), zap.Uint64("last-ts", task.lastTs))
			}
			task.lastTs = event.CRTs

			select {
			case <-resolvedTicker.C:
				err := sendResolvedEvent(event.CRTs)
				if err != nil {
					return errors.Trace(err)
				}
			default:
			}

			event, err := task.backend.readNext()
			if err != nil {
				return errors.Trace(err)
			}

			if event == nil {
				// EOF
				delete(workingSet, task)
				delete(pendingSet, task)

				err := task.dealloc()
				if err != nil {
					return errors.Trace(err)
				}

				continue
			}

			if event.CRTs > minResolvedTs || (event.CRTs == minResolvedTs && event.RawKV.OpType == model.OpTypeResolved) {
				// we have processed all events from this task that need to be processed in this merge
				if event.CRTs > minResolvedTs || event.RawKV.OpType != model.OpTypeResolved {
					pendingSet[task] = event
				}
				err := retire(task)
				if err != nil {
					return errors.Trace(err)
				}
				continue
			}

			if counter%10000 == 0 {
				log.Debug("Merging progress", zap.Int("counter", counter))
			}
			heap.Push(sortHeap, &sortItem{
				entry: event,
				data:  task,
			})
		}

		if len(workingSet) != 0 {
			log.Fatal("unified sorter: merging ended prematurely, bug?", zap.Uint64("resolvedTs", minResolvedTs))
		}

		if counter >= 10000 {
			log.Debug("Unified Sorter: merging ended", zap.Uint64("resolvedTs", minResolvedTs), zap.Int("count", counter))
		}
		err := sendResolvedEvent(minResolvedTs)
		if err != nil {
			return errors.Trace(err)
		}

		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-in:
			if task == nil {
				return errors.New("Unified Sorter: nil flushTask, exiting")
			}

			if task.backend != nil {
				pendingSet[task] = nil
			} // otherwise it is an empty flush

			if lastResolvedTs[task.heapSorterID] < task.maxResolvedTs {
				lastResolvedTs[task.heapSorterID] = task.maxResolvedTs
			}

			minTemp := uint64(math.MaxUint64)
			for _, ts := range lastResolvedTs {
				if minTemp > ts {
					minTemp = ts
				}
			}

			if minTemp > minResolvedTs {
				minResolvedTs = minTemp
				err := onMinResolvedTsUpdate()
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	}
}

// UnifiedSorter provides both sorting in memory and in file. Memory pressure is used to determine which one to use.
type UnifiedSorter struct {
	inputCh  chan *model.PolymorphicEvent
	outputCh chan *model.PolymorphicEvent
	dir      string
	pool     *backEndPool
}

// NewUnifiedSorter creates a new UnifiedSorter
func NewUnifiedSorter(dir string) *UnifiedSorter {
	return &UnifiedSorter{
		inputCh:  make(chan *model.PolymorphicEvent, 128000),
		outputCh: make(chan *model.PolymorphicEvent, 128000),
		dir:      dir,
		pool:     newBackEndPool(dir),
	}
}

// Run implements the EventSorter interface
func (s *UnifiedSorter) Run(ctx context.Context) error {
	nextSorterID := 0
	heapSorters := make([]*heapSorter, numConcurrentHeaps)

	sorterOutCh := make(chan *flushTask, 4096)
	defer close(sorterOutCh)

	errCh := make(chan error)
	for i := range heapSorters {
		finalI := i
		heapSorters[finalI] = newHeapSorter(finalI, s.pool, sorterOutCh)
		go func() {
			errCh <- heapSorters[finalI].run(ctx)
		}()
	}

	go func() {
		errCh <- runMerger(ctx, numConcurrentHeaps, sorterOutCh, s.outputCh)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			if err != nil {
				return errors.Trace(err)
			}
		case event := <-s.inputCh:
			if event.RawKV != nil && event.RawKV.OpType == model.OpTypeResolved {
				// broadcast resolved events
				for _, sorter := range heapSorters {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case sorter.inputCh <- event:
					}
				}
				continue
			}

			// dispatch a row changed event
			targetID := nextSorterID % numConcurrentHeaps
			nextSorterID++
			select {
			case <-ctx.Done():
				return ctx.Err()
			case heapSorters[targetID].inputCh <- event:
			}
		}
	}
}

// AddEntry implements the EventSorter interface
func (s *UnifiedSorter) AddEntry(ctx context.Context, entry *model.PolymorphicEvent) {
	select {
	case <-ctx.Done():
		return
	case s.inputCh <- entry:
	}
}

// Output implements the EventSorter interface
func (s *UnifiedSorter) Output() <-chan *model.PolymorphicEvent {
	return s.outputCh
}
