// Copyright 2023 PingCAP, Inc.
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

package chann

// DrainableChann is a channel that will be drained when it is closed.
// It is a wrapper of Chann.
// NOTICE: Please make sure that it is safe to drain rest elements in the channel
// before closing the channel.
type DrainableChann[T any] struct {
	inner *Chann[T]
}

// NewAutoDrainChann creates a new DrainableChann.
func NewAutoDrainChann[T any](opts ...Opt) *DrainableChann[T] {
	return &DrainableChann[T]{
		inner: New[T](opts...),
	}
}

// In returns the send channel of the given Chann, which can be used to
// send values to the channel. If one closes the channel using close(),
// it will result in a runtime panic. Instead, use CloseAndDrain() method.
func (ch *DrainableChann[T]) In() chan<- T {
	return ch.inner.In()
}

// Out returns the receive channel of the given Chann, which can be used
// to receive values from the channel.
func (ch *DrainableChann[T]) Out() <-chan T {
	return ch.inner.Out()
}

// CloseAndDrain closes the channel and drains the channel to avoid the goroutine leak.
func (ch *DrainableChann[T]) CloseAndDrain() {
	ch.inner.Close()
	// NOTICE: Drain the channel to avoid the goroutine leak.
	for range ch.Out() {
	}
}

// Len returns an approximation of the length of the channel.
//
// Note that in a concurrent scenario, the returned length of a channel
// may never be accurate. Hence the function is named with an Approx prefix.
func (ch *DrainableChann[T]) Len() int {
	return ch.inner.Len()
}

// Cap returns the capacity of the channel.
func (ch *DrainableChann[T]) Cap() int {
	return ch.inner.Cap()
}
