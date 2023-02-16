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
	*Chann[T]
}

// NewAutoDrainChann creates a new DrainableChann.
func NewAutoDrainChann[T any](opts ...Opt) *DrainableChann[T] {
	return &DrainableChann[T]{
		Chann: New[T](opts...),
	}
}

// CloseAndDrain closes the channel and drains the channel to avoid the goroutine leak.
func (ch *Chann[T]) CloseAndDrain() {
	ch.Close()
	// NOTICE: Drain the channel to avoid the goroutine leak.
	for range ch.Out() {
	}
}
