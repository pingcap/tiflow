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
	"github.com/pingcap/tiflow/pkg/errors"
)

// MockMessageSender defines a mock message sender
type MockMessageSender struct {
	mu            sync.Mutex
	msgBox        map[msgBoxIndex]deque.Deque
	nodeBlackList map[NodeID]struct{}
	isBlocked     bool

	injectedErrCh chan error
}

// NewMockMessageSender creates a new MockMessageSender instance
func NewMockMessageSender() *MockMessageSender {
	return &MockMessageSender{
		msgBox:        make(map[msgBoxIndex]deque.Deque),
		nodeBlackList: make(map[NodeID]struct{}),
		injectedErrCh: make(chan error, 1),
	}
}

type msgBoxIndex struct {
	topic  Topic
	target NodeID
}

// SendToNodeB implements pkg/p2p.MessageSender.SendToNodeB
func (m *MockMessageSender) SendToNodeB(
	ctx context.Context,
	targetNodeID NodeID,
	topic Topic,
	message interface{},
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Handle mock offline nodes.
	if _, exists := m.nodeBlackList[targetNodeID]; exists {
		return errors.ErrExecutorNotFoundForMessage.GenWithStackByArgs()
	}

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

// SendToNode implements pkg/p2p.MessageSender.SendToNode
func (m *MockMessageSender) SendToNode(
	_ context.Context,
	targetNodeID NodeID,
	topic Topic,
	message interface{},
) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Handle mock offline nodes.
	if _, exists := m.nodeBlackList[targetNodeID]; exists {
		return false, nil
	}

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

// TryPop tries to get a message from message sender
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

// SetBlocked makes the message send blocking
func (m *MockMessageSender) SetBlocked(isBlocked bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.isBlocked = isBlocked
}

// InjectError injects error to simulate error scenario
func (m *MockMessageSender) InjectError(err error) {
	m.injectedErrCh <- err
}

// MarkNodeOffline marks a node as offline.
func (m *MockMessageSender) MarkNodeOffline(nodeID NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nodeBlackList[nodeID] = struct{}{}
}

// MarkNodeOnline marks a node as online.
// Note that by default all nodes are treated as online
// to facilitate testing.
func (m *MockMessageSender) MarkNodeOnline(nodeID NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.nodeBlackList, nodeID)
}
