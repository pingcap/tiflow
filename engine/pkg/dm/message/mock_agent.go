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

package message

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/stretchr/testify/mock"
)

// MockAgent implement Agent
type MockAgent struct {
	sync.Mutex
	mock.Mock
}

var _ Agent = &MockAgent{}

// GenerateTopic generate mock message topic.
func GenerateTopic(senderID, receiverID string) string {
	return generateTopic(senderID, receiverID)
}

// GenerateResponse generate mock response message.
func GenerateResponse(id messageID, command string, msg interface{}) interface{} {
	resp := message{ID: id, Type: responseTp, Command: command, Payload: msg}
	// nolint:errcheck
	bytes, _ := json.Marshal(resp)
	var resp2 message
	// nolint:errcheck
	json.Unmarshal(bytes, &resp2)
	return &resp2
}

// Tick implement Agent.Tick.
func (m *MockAgent) Tick(ctx context.Context) error { return nil }

// Close implement Agent.Close.
func (m *MockAgent) Close(ctx context.Context) error { return nil }

// UpdateClient implement Agent.UpdateClient.
func (m *MockAgent) UpdateClient(clientID string, client client) error { return nil }

// RemoveClient implement Agent.RemoveClient.
func (m *MockAgent) RemoveClient(clientID string) error { return nil }

// SendMessage implement Agent.SendMessage.
func (m *MockAgent) SendMessage(ctx context.Context, clientID string, command string, msg interface{}) error {
	m.Lock()
	defer m.Unlock()
	return m.Called().Error(0)
}

// SendRequest implement Agent.SendRequest.
func (m *MockAgent) SendRequest(ctx context.Context, clientID string, command string, req interface{}) (interface{}, error) {
	m.Lock()
	defer m.Unlock()
	args := m.Called(ctx, clientID, command, req)
	return args.Get(0), args.Error(1)
}
