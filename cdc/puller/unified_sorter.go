package puller

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"go.uber.org/zap"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
)

const (
	fileBufferSize     = 16 * 1024 * 1024
	heapSizeLimit      = 4 * 1024 * 1024 // 4MB
	numConcurrentHeaps = 16
	memoryLimit        = 1024 * 1024 * 1024 // 1GB
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
	return nil
}

func (f *fileSorterBackEnd) getSize() int {
	return f.size
}

func (f *fileSorterBackEnd) reset() error {
	err := f.f.Truncate(int64(f.size))
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

type serializerDeserializer interface {
	marshal(event *model.PolymorphicEvent, bytes []byte) ([]byte, error)
	unmarshal(event *model.PolymorphicEvent, bytes []byte) ([]byte, error)
}

type msgPackGenSerde struct {
}

func (m *msgPackGenSerde) marshal(event *model.PolymorphicEvent, bytes []byte) ([]byte, error) {
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
	f, err := os.Open(fileName)
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
	var size int
	err := binary.Read(f.readWriter, binary.LittleEndian, &size)
	if err != nil {
		return nil, errors.AddStack(err)
	}

	if cap(f.rawBytes) < size {
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
	_, err := f.serde.marshal(event, f.rawBytes)
	if err != nil {
		return errors.AddStack(err)
	}

	size := len(f.rawBytes)
	err = binary.Write(f.readWriter, binary.LittleEndian, size)
	if err != nil {
		return errors.AddStack(err)
	}

	err = binary.Write(f.readWriter, binary.LittleEndian, f.rawBytes)
	if err != nil {
		return errors.AddStack(err)
	}

	f.size += size + 8
	return nil
}

type memorySorterBackEnd struct {
	events    []*model.PolymorphicEvent
	readIndex int
}

func (m *memorySorterBackEnd) readNext() (*model.PolymorphicEvent, error) {
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
	memoryUseEstimate uint64
	fileNameCounter   uint64
	mu                sync.Mutex
	cache             []unsafe.Pointer
	dir               string
}

func newBackEndPool(dir string) *backEndPool {
	return &backEndPool{
		memoryUseEstimate: 0,
		fileNameCounter:   0,
		mu:                sync.Mutex{},
		cache:             make([]unsafe.Pointer, 256),
		dir:               dir,
	}
}

func (p *backEndPool) alloc() (sorterBackEnd, error) {
	if atomic.LoadUint64(&p.memoryUseEstimate) < memoryLimit {
		ret := new(memorySorterBackEnd)
		atomic.AddUint64(&p.memoryUseEstimate, heapSizeLimit)
		return ret, nil
	}

	for i := range p.cache {
		ptr := &p.cache[i]
		ret := atomic.SwapPointer(ptr, nil)
		if ret != nil {
			return *(*sorterBackEnd)(ret), nil
		}
	}

	fname := fmt.Sprintf("sort-%d", atomic.AddUint64(&p.fileNameCounter, 1))
	ret, err := newFileSorterBackEnd(fname, &msgPackGenSerde{})
	if err != nil {
		return nil, errors.AddStack(err)
	}

	atomic.AddUint64(&p.memoryUseEstimate, heapSizeLimit)
	return ret, nil
}

func (p *backEndPool) dealloc(backEnd sorterBackEnd) {
	switch b := backEnd.(type) {
	case *memorySorterBackEnd:
		// Let GC do its job
		return
	case *fileSorterBackEnd:
		for i := range p.cache {
			ptr := &p.cache[i]
			if atomic.CompareAndSwapPointer(ptr, nil, unsafe.Pointer(b)) {
				return
			}
		}
		// Cache is full. Let GC do its job
	}
}

type flushTask struct {
	heapSorterId  int
	backend       sorterBackEnd
	maxResolvedTs uint64
	finished      chan error
}

type heapSorter struct {
	id          int
	inputCh     chan *model.PolymorphicEvent
	outputCh    chan *flushTask
	heap        sortHeap
	backEndPool *backEndPool
}

func newHeapSorter(id int, pool *backEndPool) *heapSorter {
	return &heapSorter{
		id:          id,
		inputCh:     make(chan *model.PolymorphicEvent, 1024*1024),
		outputCh:    make(chan *flushTask, 1024),
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
		heapSorterId:  h.id,
		backend:       backEnd,
		maxResolvedTs: maxResolvedTs,
		finished:      make(chan error),
	}

	var oldHeap sortHeap
	if !isEmptyFlush {
		oldHeap = h.heap
		h.heap = make(sortHeap, 0, 65536)
	}

	go func() {
		defer close(task.finished)
		batchSize := oldHeap.Len()
		for oldHeap.Len() > 0 {
			event := oldHeap.Pop().(*sortItem).entry
			err := task.backend.writeNext(event)
			if err != nil {
				task.finished <- err
				return
			}
		}
		log.Debug("Unified Sorter flushTask finished",
			zap.Int("heap-id", task.heapSorterId),
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
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-h.inputCh:
			h.heap.Push(event)
			isResolvedEvent := event.RawKV != nil && event.RawKV.OpType == model.OpTypeResolved
			if isResolvedEvent {
				if event.RawKV.CRTs < maxResolved {
					log.Fatal("ResolvedTs regression, bug?", zap.Uint64("event-resolvedTs", event.RawKV.CRTs),
						zap.Uint64("max-resolvedTs", maxResolved))
				}
				maxResolved = event.RawKV.CRTs
			}

			heapSizeBytesEstimate += event.RawKV.ApproximateSize()
			if heapSizeBytesEstimate >= heapSizeLimit || isResolvedEvent {
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
	minResolvedTs := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-in:
			if lastResolvedTs[task.heapSorterId] < task.maxResolvedTs {
				lastResolvedTs[task.heapSorterId] = task.maxResolvedTs
			}
		}
	}
}

type UnifiedSorter struct {
	inputCh  chan *model.PolymorphicEvent
	outputCh chan *model.PolymorphicEvent
	dir      string
	pool     *backEndPool
}

func NewUnifiedSorter(dir string) *UnifiedSorter {
	return &UnifiedSorter{
		inputCh:  make(chan *model.PolymorphicEvent, 128000),
		outputCh: make(chan *model.PolymorphicEvent, 128000),
		dir:      dir,
		pool:     newBackEndPool(dir),
	}
}

func (s *UnifiedSorter) Run(ctx context.Context) error {
	nextSorterId := 0
	heapSorters := make([]*heapSorter, numConcurrentHeaps)
	for i := range heapSorters {
		heapSorters[i] = newHeapSorter(i, s.pool)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
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
				log.Debug("Unified Sorter: event broadcast", zap.Uint64("CRTs", event.CRTs))
				continue
			}

			// dispatch a row changed event
			targetId := nextSorterId % numConcurrentHeaps
			nextSorterId++
			select {
			case <-ctx.Done():
				return ctx.Err()
			case heapSorters[targetId].inputCh <- event:
			}

			log.Debug("Unified Sorter: event dispatched",
				zap.Uint64("CRTs", event.CRTs),
				zap.Int("heap-id", targetId))
		}
	}
}

func (s *UnifiedSorter) AddEntry(ctx context.Context, entry *model.PolymorphicEvent) {
	select {
	case <-ctx.Done():
		return
	case s.inputCh <- entry:
	}
}

func (s *UnifiedSorter) Output() <-chan *model.PolymorphicEvent {
	return s.outputCh
}
