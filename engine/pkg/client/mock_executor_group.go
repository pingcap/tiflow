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

package client

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/pkg/errors"
)

// MockExecutorGroup is a stub implementation for ExecutorGroup.
type MockExecutorGroup struct {
	mu      sync.RWMutex
	clients map[model.ExecutorID]ExecutorClient

	updateNotifyCh chan struct{}
}

// NewMockExecutorGroup returns a new MockExecutorGroup.
func NewMockExecutorGroup() *MockExecutorGroup {
	return &MockExecutorGroup{
		clients:        make(map[model.ExecutorID]ExecutorClient),
		updateNotifyCh: make(chan struct{}, 1),
	}
}

// GetExecutorClient returns the ExecutorClient associated with id.
func (g *MockExecutorGroup) GetExecutorClient(id model.ExecutorID) (ExecutorClient, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if client, ok := g.clients[id]; ok {
		return client, true
	}
	return nil, false
}

// GetExecutorClientB tries to get the ExecutorClient blockingly.
func (g *MockExecutorGroup) GetExecutorClientB(ctx context.Context, id model.ExecutorID) (ExecutorClient, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, errors.Trace(ctx.Err())
		case <-ticker.C:
		case <-g.updateNotifyCh:
		}

		if client, ok := g.GetExecutorClient(id); ok {
			return client, nil
		}
	}
}

// AddClient adds a client to the client map.
func (g *MockExecutorGroup) AddClient(id model.ExecutorID, client ExecutorClient) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.clients[id] = client

	select {
	case g.updateNotifyCh <- struct{}{}:
	default:
	}
}

// RemoveClient removes a client from the client map.
func (g *MockExecutorGroup) RemoveClient(id model.ExecutorID) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	client, exists := g.clients[id]
	if !exists {
		return false
	}

	client.Close()
	delete(g.clients, id)

	return true
}
