package chann

import (
	"sync/atomic"

	"github.com/pingcap/tiflow/pkg/deque"
)

// Opt represents an option to configure the created channel. The current possible
// option is Cap.
type Opt func(*config)

// Cap is the option to configure the capacity of a creating buffer.
// if the provided number is 0, Cap configures the creating buffer to a
// unbuffered channel; if the provided number is a positive integer, then
// Cap configures the creating buffer to a buffered channel with the given
// number of capacity  for caching. If n is a negative integer, then it
// configures the creating channel to become an unbounded channel.
func Cap(n int) Opt {
	return func(s *config) {
		switch {
		case n == 0:
			s.cap = int64(0)
			s.typ = unbuffered
		case n > 0:
			s.cap = int64(n)
			s.typ = buffered
		default:
			s.cap = int64(-1)
			s.typ = unbounded
		}
	}
}

// Chann is a generic channel abstraction that can be either buffered,
// unbuffered, or unbounded. To create a new channel, use New to allocate
// one, and use Cap to configure the capacity of the channel.
//
// Deprecated: Just Don't Use It. Use a channel please.
type Chann[T any] struct {
	queue   *deque.Deque[T]
	in, out chan T
	close   chan struct{}
	cfg     *config
}

// New returns a Chann that may represent a buffered, an unbuffered or
// an unbounded channel. To configure the type of the channel, one may
// pass Cap as the argument of this function.
//
// By default, or without specification, the function returns an unbounded
// channel which has unlimited capacity.
//
//	ch := chann.New[float64]()
//	or
//	ch := chann.New[float64](chann.Cap(-1))
//
// If the chann.Cap specified a non-negative integer, the returned channel
// is either unbuffered (0) or buffered (positive).
//
// Note that although the input arguments are  specified as variadic parameter
// list, however, the function panics if there is more than one option is
// provided.
// DEPRECATED: use NewAutoDrainChann instead.
func New[T any](opts ...Opt) *Chann[T] {
	cfg := &config{
		cap: -1, len: 0,
		typ: unbounded,
	}

	if len(opts) > 1 {
		panic("chann: too many arguments")
	}
	for _, o := range opts {
		o(cfg)
	}
	ch := &Chann[T]{cfg: cfg, close: make(chan struct{})}
	switch ch.cfg.typ {
	case unbuffered:
		ch.in = make(chan T)
		ch.out = ch.in
	case buffered:
		ch.in = make(chan T, ch.cfg.cap)
		ch.out = ch.in
	case unbounded:
		ch.in = make(chan T, 1)
		ch.out = make(chan T, 1)
		ch.queue = deque.NewDeque[T](32, 0)
		go ch.unboundedProcessing()
	}
	return ch
}

// In returns the send channel of the given Chann, which can be used to
// send values to the channel. If one closes the channel using close(),
// it will result in a runtime panic. Instead, use Close() method.
func (ch *Chann[T]) In() chan<- T { return ch.in }

// Out returns the receive channel of the given Chann, which can be used
// to receive values from the channel.
func (ch *Chann[T]) Out() <-chan T { return ch.out }

// Close closes the channel gracefully.
// DEPRECATED: use CloseAndDrain instead.
func (ch *Chann[T]) Close() {
	switch ch.cfg.typ {
	case buffered, unbuffered:
		close(ch.in)
		close(ch.close)
	default:
		ch.close <- struct{}{}
	}
}

// unboundedProcessing is a processing loop that implements unbounded
// channel semantics.
func (ch *Chann[T]) unboundedProcessing() {
	for {
		e, ok := ch.queue.Front()
		if ok {
			select {
			case ch.out <- e:
				atomic.AddInt64(&ch.cfg.len, -1)
				ch.queue.PopFront()
			case e, ok := <-ch.in:
				if !ok {
					panic("chann: send-only channel ch.In() closed unexpectedly")
				}
				atomic.AddInt64(&ch.cfg.len, 1)
				ch.queue.PushBack(e)
			case <-ch.close:
				ch.unboundedTerminate()
				return
			}
		} else {
			select {
			case e, ok := <-ch.in:
				if !ok {
					panic("chann: send-only channel ch.In() closed unexpectedly")
				}
				atomic.AddInt64(&ch.cfg.len, 1)
				ch.queue.PushBack(e)
			case <-ch.close:
				ch.unboundedTerminate()
				return
			}
		}
	}
}

// unboundedTerminate terminates the unbounde channel's processing loop
// and make sure all unprocessed elements be consumed if there is
// a pending receiver.
func (ch *Chann[T]) unboundedTerminate() {
	close(ch.in)
	for e := range ch.in {
		// ch.q = append(ch.q, e)
		ch.queue.PushBack(e)
	}

	for e, ok := ch.queue.PopFront(); ok; e, ok = ch.queue.PopFront() {
		// NOTICE: If no receiver is receiving the element, it will be blocked.
		// So the consumer have to deal with all the elements in the queue.
		ch.out <- e
		atomic.AddInt64(&ch.cfg.len, -1)
	}

	close(ch.out)
	close(ch.close)
}

// isClose reports the close status of a channel.
func (ch *Chann[T]) isClosed() bool {
	select {
	case <-ch.close:
		return true
	default:
		return false
	}
}

// Len returns an approximation of the length of the channel.
//
// Note that in a concurrent scenario, the returned length of a channel
// may never be accurate. Hence the function is named with an Approx prefix.
func (ch *Chann[T]) Len() int {
	switch ch.cfg.typ {
	case buffered, unbuffered:
		return len(ch.in)
	default:
		return int(atomic.LoadInt64(&ch.cfg.len)) + len(ch.in) + len(ch.out)
	}
}

// Cap returns the capacity of the channel.
func (ch *Chann[T]) Cap() int {
	switch ch.cfg.typ {
	case buffered, unbuffered:
		return cap(ch.in)
	default:
		return int(atomic.LoadInt64(&ch.cfg.cap)) + cap(ch.in) + cap(ch.out)
	}
}

type chanType int

const (
	unbuffered chanType = iota
	buffered
	unbounded
)

type config struct {
	typ      chanType
	len, cap int64
}
