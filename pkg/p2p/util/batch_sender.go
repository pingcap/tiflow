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

package util

import "github.com/pingcap/errors"

// State is the type for the internal state of the batcher.
// It should contain all batched messages and other relevant information.
type State = interface{}

// Item is the type containg the information for an individual message to be batched.
type Item = interface{}

// BatchSender is an abstract data structure used to implement
// batching some items and send them when Cond is met.
type BatchSender struct {
	Init   func() State
	Append func(State, Item) State
	SendFn func(State) error
	Cond   func(State) bool

	state State
}

// Push is used add a new item to the current batch.
// If Cond is met after the item is added, the SendFn
// will be called immediately.
func (s *BatchSender) Push(item Item) error {
	if s.state == nil {
		s.state = s.Init()
	}

	s.state = s.Append(s.state, item)

	if s.Cond(s.state) {
		if err := s.sendAndClear(); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// ForceSend forces sending out the batch.
func (s *BatchSender) ForceSend() error {
	return s.sendAndClear()
}

func (s *BatchSender) sendAndClear() error {
	if s.state == nil {
		return nil
	}
	if err := s.SendFn(s.state); err != nil {
		return errors.Trace(err)
	}
	s.state = nil
	return nil
}
