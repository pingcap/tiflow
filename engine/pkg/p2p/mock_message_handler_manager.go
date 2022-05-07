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
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

type MockMessageHandlerManager struct {
	mu       sync.RWMutex
	handlers map[Topic]HandlerFunc
	tpi      map[Topic]TypeInformation

	injectedError chan error
}

func NewMockMessageHandlerManager() *MockMessageHandlerManager {
	return &MockMessageHandlerManager{
		handlers:      make(map[Topic]HandlerFunc),
		tpi:           make(map[Topic]TypeInformation),
		injectedError: make(chan error, 1),
	}
}

func (m *MockMessageHandlerManager) AssertHasHandler(t *testing.T, topic Topic, tpi TypeInformation) {
	m.mu.Lock()
	defer m.mu.Unlock()

	require.Contains(t, m.handlers, topic)
	require.Contains(t, m.tpi, topic)
	require.Equal(t, tpi, m.tpi[topic])
}

func (m *MockMessageHandlerManager) AssertNoHandler(t *testing.T, topic Topic) {
	m.mu.Lock()
	defer m.mu.Unlock()

	require.NotContains(t, m.handlers, topic)
	require.NotContains(t, m.tpi, topic)
}

func (m *MockMessageHandlerManager) InvokeHandler(t *testing.T, topic Topic, senderID NodeID, message interface{}) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	require.Containsf(t, m.handlers, topic,
		"trying to invoke a non-existent handler for topic %s", topic)

	var err error
	require.NotPanicsf(t, func() {
		err = m.handlers[topic](senderID, message)
	}, "message handler panicked for topic %s", topic)
	return errors.Trace(err)
}

func (m *MockMessageHandlerManager) InjectError(err error) {
	m.injectedError <- err
}

func (m *MockMessageHandlerManager) RegisterHandler(
	ctx context.Context,
	topic Topic,
	tpi TypeInformation,
	fn HandlerFunc,
) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.handlers[topic]; ok {
		// If the handler already exists, we return false.
		return false, nil
	}

	m.handlers[topic] = fn
	m.tpi[topic] = tpi
	return true, nil
}

func (m *MockMessageHandlerManager) UnregisterHandler(ctx context.Context, topic Topic) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.handlers[topic]; !ok {
		return false, nil
	}

	delete(m.handlers, topic)
	delete(m.tpi, topic)
	return true, nil
}

func (m *MockMessageHandlerManager) CheckError(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case err := <-m.injectedError:
		return err
	default:
	}
	return nil
}

func (m *MockMessageHandlerManager) Clean(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for topic := range m.handlers {
		delete(m.handlers, topic)
		delete(m.tpi, topic)
	}

	return nil
}

func (m *MockMessageHandlerManager) SetTimeout(timeout time.Duration) {
	// This function is a dummy
}
