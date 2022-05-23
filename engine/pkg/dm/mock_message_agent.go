package dm

import (
	"context"
	"sync"

	"github.com/stretchr/testify/mock"
)

// MockMessageAgent implement MessageAgent
type MockMessageAgent struct {
	sync.Mutex
	mock.Mock
}

// RegisterHandler implement MessageAgent.RegisterHandler
func (m *MockMessageAgent) RegisterHandler(topic string, handler HandlerFunc) {}

// UpdateSender implement MessageAgent.UpdateSender
func (m *MockMessageAgent) UpdateSender(senderID string, sender Sender) {}

// SendMessage implement MessageAgent.SendMessage
func (m *MockMessageAgent) SendMessage(ctx context.Context, senderID string, topic string, msg interface{}) error {
	m.Lock()
	defer m.Unlock()
	args := m.Called()
	return args.Error(0)
}

// SendRequest implement MessageAgent.SendRequest
func (m *MockMessageAgent) SendRequest(ctx context.Context, senderID string, topic string, req interface{}) (interface{}, error) {
	m.Lock()
	defer m.Unlock()
	args := m.Called()
	return args.Get(0), args.Error(1)
}

// SendResponse implement MessageAgent.SendResponse
func (m *MockMessageAgent) SendResponse(ctx context.Context, senderID string, topic string, id messageID, resp interface{}) error {
	m.Lock()
	defer m.Unlock()
	args := m.Called()
	return args.Error(0)
}

// OnMessage implement MessageAgent.OnMessage
func (m *MockMessageAgent) OnMessage(senderID string, topic string, msg interface{}) error {
	m.Lock()
	defer m.Unlock()
	args := m.Called()
	return args.Error(0)
}
