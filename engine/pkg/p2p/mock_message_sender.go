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

package p2p

import (
	"context"
	"sync"

	"github.com/edwingeng/deque"
)

type MockMessageSender struct {
	mu        sync.Mutex
	msgBox    map[msgBoxIndex]deque.Deque
	isBlocked bool

	injectedErrCh chan error
}

func NewMockMessageSender() *MockMessageSender {
	return &MockMessageSender{
		msgBox:        make(map[msgBoxIndex]deque.Deque),
		injectedErrCh: make(chan error, 1),
	}
}

type msgBoxIndex struct {
	topic  Topic
	target NodeID
}

func (m *MockMessageSender) SendToNodeB(
	ctx context.Context,
	targetNodeID NodeID,
	topic Topic,
	message interface{},
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	select {
	case err := <-m.injectedErrCh:
		return err
	default:
	}

	// TODO Handle the `m.isBlocked == true` case
	q := m.getQueue(targetNodeID, topic)
	q.PushBack(message)
	return nil
}

func (m *MockMessageSender) SendToNode(
	_ context.Context,
	targetNodeID NodeID,
	topic Topic,
	message interface{},
) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	select {
	case err := <-m.injectedErrCh:
		return false, err
	default:
	}

	if m.isBlocked {
		return false, nil
	}

	q := m.getQueue(targetNodeID, topic)
	q.PushBack(message)

	return true, nil
}

func (m *MockMessageSender) TryPop(targetNodeID NodeID, topic Topic) (interface{}, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	q := m.getQueue(targetNodeID, topic)
	if q.Empty() {
		return nil, false
	}

	return q.PopFront(), true
}

func (m *MockMessageSender) getQueue(target NodeID, topic Topic) deque.Deque {
	mapKey := msgBoxIndex{
		topic:  topic,
		target: target,
	}

	q, ok := m.msgBox[mapKey]
	if !ok {
		q = deque.NewDeque()
		m.msgBox[mapKey] = q
	}

	return q
}

func (m *MockMessageSender) SetBlocked(isBlocked bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.isBlocked = isBlocked
}

func (m *MockMessageSender) InjectError(err error) {
	m.injectedErrCh <- err
}
