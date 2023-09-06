// Copyright 2022 PingCAP, Inc.
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
//
// ============================================================
// Forked from https://github.com/golang-design/chann.
// Copyright 2021 The golang.design Initiative Authors.
// All rights reserved. Use of this source code is governed
// by a MIT license that can be found in the LICENSE file.
//
// Written by Changkun Ou <changkun.de>

// Package chann providesa a unified channel package.
//
// The package is compatible with existing buffered and unbuffered
// channels. For example, in Go, to create a buffered or unbuffered
// channel, one uses built-in function `make` to create a channel:
//
//	ch := make(chan int)     // unbuffered channel
//	ch := make(chan int, 42) // or buffered channel
//
// However, all these channels have a finite capacity for caching, and
// it is impossible to create a channel with unlimited capacity, namely,
// an unbounded channel.
//
// This package provides the ability to create all possible types of
// channels. To create an unbuffered or a buffered channel:
//
//	ch := chann.New[int](chann.Cap(0))  // unbuffered channel
//	ch := chann.New[int](chann.Cap(42)) // or buffered channel
//
// More importantly, when the capacity of the channel is unspecified,
// or provided as negative values, the created channel is an unbounded
// channel:
//
//	ch := chann.New[int]()               // unbounded channel
//	ch := chann.New[int](chann.Cap(-42)) // or unbounded channel
//
// Furthermore, all channels provides methods to send (In()),
// receive (Out()), and close (Close()).
//
// An unbounded channel is not a buffered channel with infinite capacity,
// and they have different memory model semantics in terms of receiving
// a value: The recipient of a buffered channel is immediately available
// after a send is complete. However, the recipient of an unbounded channel
// may be available within a bounded time frame after a send is complete.
//
// Note that to close a channel, must use Close() method instead of the
// language built-in method
// Two additional methods: ApproxLen and Cap returns the current status
// of the channel: an approximation of the current length of the channel,
// as well as the current capacity of the channel.
//
// See https://golang.design/research/ultimate-channel to understand
// the motivation of providing this package and the possible use cases
// with this package.
package chann

import (
	"sync/atomic"
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
type Chann[T any] struct {
	q       []T
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
		ch.in = make(chan T, 16)
		ch.out = make(chan T, 16)
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
	var nilT T

	for {
		select {
		case e, ok := <-ch.in:
			if !ok {
				panic("chann: send-only channel ch.In() closed unexpectedly")
			}
			atomic.AddInt64(&ch.cfg.len, 1)
			ch.q = append(ch.q, e)
		case <-ch.close:
			ch.unboundedTerminate()
			return
		}

		for len(ch.q) > 0 {
			select {
			case ch.out <- ch.q[0]:
				atomic.AddInt64(&ch.cfg.len, -1)
				ch.q[0] = nilT
				ch.q = ch.q[1:]
			case e, ok := <-ch.in:
				if !ok {
					panic("chann: send-only channel ch.In() closed unexpectedly")
				}
				atomic.AddInt64(&ch.cfg.len, 1)
				ch.q = append(ch.q, e)
			case <-ch.close:
				ch.unboundedTerminate()
				return
			}
		}
		ch.q = nil
	}
}

// unboundedTerminate terminates the unbounde channel's processing loop
// and make sure all unprocessed elements be consumed if there is
// a pending receiver.
func (ch *Chann[T]) unboundedTerminate() {
	var zeroT T

	close(ch.in)
	for e := range ch.in {
		ch.q = append(ch.q, e)
	}
	for len(ch.q) > 0 {
		// NOTICE: If no receiver is receiving the element, it will be blocked.
		// So the consumer have to deal with all the elements in the queue.
		ch.out <- ch.q[0]
		ch.q[0] = zeroT // de-reference earlier to help GC
		ch.q = ch.q[1:]
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
