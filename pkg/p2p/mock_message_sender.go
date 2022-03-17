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
}

func NewMockMessageSender() *MockMessageSender {
	return &MockMessageSender{
		msgBox: make(map[msgBoxIndex]deque.Deque),
	}
}

type msgBoxIndex struct {
	topic  Topic
	target NodeID
}

func (m *MockMessageSender) SendToNodeB(
	_ context.Context,
	targetNodeID NodeID,
	topic Topic,
	message interface{},
) error {
	panic("not implemented")
}

func (m *MockMessageSender) SendToNode(
	_ context.Context,
	targetNodeID NodeID,
	topic Topic,
	message interface{},
) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

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
