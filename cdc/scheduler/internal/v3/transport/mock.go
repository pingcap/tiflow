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

package transport

import (
	"context"

	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
)

// MockTrans mocks transport, used in tests.
type MockTrans struct {
	SendBuffer []*schedulepb.Message
	RecvBuffer []*schedulepb.Message

	KeepRecvBuffer bool
}

// NewMockTrans returns a new mock transport.
func NewMockTrans() *MockTrans {
	return &MockTrans{
		SendBuffer: make([]*schedulepb.Message, 0),
		RecvBuffer: make([]*schedulepb.Message, 0),
	}
}

// Close mock transport.
func (m *MockTrans) Close() error {
	return nil
}

// Send sends messages.
func (m *MockTrans) Send(ctx context.Context, msgs []*schedulepb.Message) error {
	m.SendBuffer = append(m.SendBuffer, msgs...)
	return nil
}

// Recv receives messages.
func (m *MockTrans) Recv(ctx context.Context) ([]*schedulepb.Message, error) {
	if m.KeepRecvBuffer {
		return m.RecvBuffer, nil
	}
	messages := m.RecvBuffer[:len(m.RecvBuffer)]
	m.RecvBuffer = make([]*schedulepb.Message, 0)
	return messages, nil
}
