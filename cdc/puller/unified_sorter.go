package puller

import (
	"bufio"
	"context"
	"encoding/binary"
	"go.uber.org/zap"
	"os"
	"sync"

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
	flushAndReset() error
}

type fileSorterBackEnd struct {
	f          *os.File
	readWriter *bufio.ReadWriter
	serde      serializerDeserializer
	rawBytes   []byte
	name       string
	size       int
}

func (f *fileSorterBackEnd) getSize() int {
	return f.size
}

func (f *fileSorterBackEnd) flushAndReset() error {
	err := f.readWriter.Flush()

	if err != nil {
		return errors.AddStack(err)
	}

	err = f.f.Truncate(int64(f.size))
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

func (m *memorySorterBackEnd) flushAndReset() error {
	m.events = m.events[0:0]
	m.readIndex = 0
	return nil
}

type backEndPool struct {
	memoryUseEstimate uint64
	fileNameCounter   uint64
	mu                sync.Mutex
	cache             []*sorterBackEnd
	dir               string
}

func newBackEndPool(dir string) *backEndPool {
	return &backEndPool{
		memoryUseEstimate: 0,
		fileNameCounter:   0,
		mu:                sync.Mutex{},
		cache:             make([]*sorterBackEnd, 1024),
		dir:               dir,
	}
}

func (p *backEndPool) alloc() (sorterBackEnd, error) {
	
}

type flushTask struct {
	heapSorterId  int
	backend       sorterBackEnd
	maxResolvedTs uint64
	finished      chan struct{}
}

type heapSorter struct {
	inputCh  chan *model.PolymorphicEvent
	outputCh chan *flushTask
	heap     sortHeap
}

// flush should only be called within the main loop in run().
func (h *heapSorter) flush(ctx context.Context, maxResolvedTs uint64) error {
	if h.heap.Len() == 0 {
		return nil
	}

}

func (h *heapSorter) run(ctx context.Context) error {
	var maxResolved uint64
	maxResolved = 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-h.inputCh:
			if event.RawKV != nil && event.RawKV.OpType == model.OpTypeResolved {
				if event.RawKV.CRTs < maxResolved {
					log.Fatal("ResolvedTs regression, bug?", zap.Uint64("event-resolvedTs", event.RawKV.CRTs),
						zap.Uint64("max-resolvedTs", maxResolved))
				}
				maxResolved = event.RawKV.CRTs
			}
			h.heap.Push(event)
		}
	}
}

type UnifiedSorter struct {
	inputCh  chan *model.PolymorphicEvent
	outputCh chan *model.PolymorphicEvent
	dir      string
}

func NewUnifiedSorter(dir string) *UnifiedSorter {
	return &UnifiedSorter{
		inputCh:  make(chan *model.PolymorphicEvent, 128000),
		outputCh: make(chan *model.PolymorphicEvent, 128000),
		dir:      dir,
	}
}

func (s *UnifiedSorter) Run(ctx context.Context) error {
	panic("implement me")
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
