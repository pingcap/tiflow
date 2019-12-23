// Copyright 2019 PingCAP, Inc.
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

package roles

import (
	"context"
	"sync/atomic"
)

var _ Manager = &mockManager{}

// mockManager represents the structure which is used for electing owner.
// It's used for local store and testing.
// So this worker will always be the owner.
type mockManager struct {
	owner    int32
	id       string // id is the ID of manager.
	cancel   context.CancelFunc
	retireCh chan struct{}
}

// NewMockManager creates a new mock Manager.
func NewMockManager(id string, cancel context.CancelFunc) Manager {
	return &mockManager{
		id:       id,
		cancel:   cancel,
		retireCh: make(chan struct{}, 1),
	}
}

// ID implements Manager.ID interface.
func (m *mockManager) ID() string {
	return m.id
}

// IsOwner implements Manager.IsOwner interface.
func (m *mockManager) IsOwner() bool {
	return atomic.LoadInt32(&m.owner) == 1
}

func (m *mockManager) toBeOwner() {
	atomic.StoreInt32(&m.owner, 1)
}

// RetireOwner implements Manager.RetireOwner interface.
func (m *mockManager) RetireOwner() {
	atomic.StoreInt32(&m.owner, 0)
}

// ResignOwner implements Manager.ResignOwner interface.
func (m *mockManager) ResignOwner(ctx context.Context) error {
	atomic.StoreInt32(&m.owner, 0)
	return nil
}

// Cancel implements Manager.Cancel interface.
func (m *mockManager) Cancel() {
	m.cancel()
}

// CampaignOwner implements Manager.CampaignOwner interface.
func (m *mockManager) CampaignOwner(_ context.Context) error {
	m.toBeOwner()
	return nil
}

// RetireNotify implements Manager.RetireNotify interface.
func (m *mockManager) RetireNotify() <-chan struct{} {
	return m.retireCh
}
