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

package dm

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/stretchr/testify/mock"
)

// MockMessageAgent implement MessageAgent
type MockMessageAgent struct {
	sync.Mutex
	mock.Mock
}

var _ MessageAgent = &MockMessageAgent{}

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

// Tick implement MessageAgent.Tick.
func (m *MockMessageAgent) Tick(ctx context.Context) error { return nil }

// Close implement MessageAgent.Close.
func (m *MockMessageAgent) Close(ctx context.Context) error { return nil }

// UpdateClient implement MessageAgent.UpdateClient.
func (m *MockMessageAgent) UpdateClient(clientID string, client client) error { return nil }

// RemoveClient implement MessageAgent.RemoveClient.
func (m *MockMessageAgent) RemoveClient(clientID string) error { return nil }

// SendMessage implement MessageAgent.SendMessage.
func (m *MockMessageAgent) SendMessage(ctx context.Context, clientID string, command string, msg interface{}) error {
	m.Lock()
	defer m.Unlock()
	return m.Called().Error(0)
}

// SendRequest implement MessageAgent.SendRequest.
func (m *MockMessageAgent) SendRequest(ctx context.Context, clientID string, command string, req interface{}) (interface{}, error) {
	m.Lock()
	defer m.Unlock()
	args := m.Called(ctx, clientID, command, req)
	return args.Get(0), args.Error(1)
}
