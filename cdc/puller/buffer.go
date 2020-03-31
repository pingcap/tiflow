package puller

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/edwingeng/deque"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
)

// ErrReachLimit represents the buffer reach limit.
var ErrReachLimit = errors.New("reach limit")

const (
	defaultBufferSize = 128000
)

// EventBuffer in a interface for communicating kv entries.
type EventBuffer interface {
	// AddEntry adds an entry to the buffer, return ErrReachLimit if reach budget limit.
	AddEntry(ctx context.Context, entry model.RegionFeedEvent) error
	Get(ctx context.Context) (model.RegionFeedEvent, error)
}

// ChanBuffer buffers kv entries
type ChanBuffer chan model.RegionFeedEvent

var _ EventBuffer = makeChanBuffer()

func makeChanBuffer() ChanBuffer {
	return make(ChanBuffer, defaultBufferSize)
}

// AddEntry adds an entry to the buffer
func (b ChanBuffer) AddEntry(ctx context.Context, entry model.RegionFeedEvent) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b <- entry:
		return nil
	}
}

// Get waits for an entry from the input channel but will stop with respect to the context
func (b ChanBuffer) Get(ctx context.Context) (model.RegionFeedEvent, error) {
	select {
	case <-ctx.Done():
		return model.RegionFeedEvent{}, ctx.Err()
	case e := <-b:
		return e, nil
	}
}

var _ EventBuffer = &memBuffer{}

type memBuffer struct {
	limitter *BlurResourceLimitter

	mu struct {
		sync.Mutex
		entries deque.Deque
	}
	signalCh chan struct{}
}

// Passing nil will make a unlimited  buffer.
func makeMemBuffer(limitter *BlurResourceLimitter) *memBuffer {
	return &memBuffer{
		limitter: limitter,
		mu: struct {
			sync.Mutex
			entries deque.Deque
		}{
			entries: deque.NewDeque(),
		},

		signalCh: make(chan struct{}, 1),
	}
}

// AddEntry implements EventBuffer interface.
func (b *memBuffer) AddEntry(ctx context.Context, entry model.RegionFeedEvent) error {
	b.mu.Lock()
	if b.limitter != nil && b.limitter.OverBucget() {
		b.mu.Unlock()
		return ErrReachLimit
	}

	b.mu.entries.PushBack(entry)
	if b.limitter != nil {
		b.limitter.Add(int64(entrySize(entry)))
	}
	b.mu.Unlock()

	select {
	case b.signalCh <- struct{}{}:
	default:
	}

	return nil
}

// Get implements EventBuffer interface.
func (b *memBuffer) Get(ctx context.Context) (model.RegionFeedEvent, error) {
	for {
		b.mu.Lock()
		if !b.mu.entries.Empty() {
			e := b.mu.entries.PopFront().(model.RegionFeedEvent)
			if b.limitter != nil {
				b.limitter.Add(int64(-entrySize(e)))
			}
			b.mu.Unlock()
			return e, nil
		}

		b.mu.Unlock()

		select {
		case <-ctx.Done():
			return model.RegionFeedEvent{}, ctx.Err()
		case <-b.signalCh:
		}
	}
}

// Size returns the memory size of memBuffer
func (b *memBuffer) Size() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.limitter == nil {
		return 0
	}
	return b.limitter.used
}

var sizeOfVal = unsafe.Sizeof(model.RawKVEntry{})
var sizeOfResolve = unsafe.Sizeof(model.ResolvedSpan{})

func entrySize(e model.RegionFeedEvent) int {
	if e.Val != nil {
		return int(sizeOfVal) + len(e.Val.Key) + len(e.Val.Value)
	} else if e.Resolved != nil {
		return int(sizeOfResolve)
	} else {
		log.Fatal("unknow event type")
	}

	return 0
}

// BlurResourceLimitter limit resource use.
type BlurResourceLimitter struct {
	budget int64
	used   int64
}

// NewBlurResourceLimmter create a BlurResourceLimitter.
func NewBlurResourceLimmter(budget int64) *BlurResourceLimitter {
	return &BlurResourceLimitter{
		budget: budget,
	}
}

// Add used resource into limmter
func (rl *BlurResourceLimitter) Add(n int64) {
	atomic.AddInt64(&rl.used, n)
}

// OverBucget retun true if over budget.
func (rl *BlurResourceLimitter) OverBucget() bool {
	return atomic.LoadInt64(&rl.used) >= atomic.LoadInt64(&rl.budget)
}
