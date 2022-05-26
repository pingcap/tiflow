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
	"sync"

	"github.com/stretchr/testify/mock"
)

// MockMessageAgent implement MessageAgent
type MockMessageAgent struct {
	sync.Mutex
	mock.Mock
}

// Init implement MessageAgent.Init
func (m *MockMessageAgent) Init(ctx context.Context) error { return nil }

// Tick implement MessageAgent.Tick
func (m *MockMessageAgent) Tick(ctx context.Context) error { return nil }

// Close implement MessageAgent.Close
func (m *MockMessageAgent) Close(ctx context.Context) error { return nil }

// UpdateSender implement MessageAgent.UpdateSender
func (m *MockMessageAgent) UpdateSender(senderID string, sender Sender) error { return nil }

// SendMessage implement MessageAgent.SendMessage
func (m *MockMessageAgent) SendMessage(ctx context.Context, senderID string, command string, msg interface{}) error {
	m.Lock()
	defer m.Unlock()
	return m.Called().Error(0)
}

// SendRequest implement MessageAgent.SendRequest
func (m *MockMessageAgent) SendRequest(ctx context.Context, senderID string, command string, req interface{}) (interface{}, error) {
	m.Lock()
	defer m.Unlock()
	args := m.Called()
	return args.Get(0), args.Error(1)
}
