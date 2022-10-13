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

package worker

import (
	"context"
	"sync"

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"go.uber.org/atomic"
)

// MasterInfoProvider is an object that can provide the caller
// information on the master.
type MasterInfoProvider interface {
	MasterID() frameModel.MasterID
	MasterNode() p2p.NodeID
	Epoch() frameModel.Epoch
	SyncRefreshMasterInfo(ctx context.Context) error
	IsMasterSideClosed() bool
}

// MasterClient must implement MasterInfoProvider.
var _ MasterInfoProvider = (*MasterClient)(nil)

// MockMasterInfoProvider defines a mock provider that implements MasterInfoProvider
type MockMasterInfoProvider struct {
	mu         sync.RWMutex
	masterID   frameModel.MasterID
	masterNode p2p.NodeID
	epoch      frameModel.Epoch

	refreshCount     atomic.Int64
	masterSideClosed atomic.Bool
}

// NewMockMasterInfoProvider creates a new MockMasterInfoProvider
func NewMockMasterInfoProvider(
	masterID frameModel.MasterID,
	masterNode p2p.NodeID,
	epoch frameModel.Epoch,
) *MockMasterInfoProvider {
	return &MockMasterInfoProvider{
		masterID:   masterID,
		masterNode: masterNode,
		epoch:      epoch,
	}
}

// MasterID implements MasterInfoProvider.MasterID
func (p *MockMasterInfoProvider) MasterID() frameModel.MasterID {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.masterID
}

// MasterNode implements MasterInfoProvider.MasterNode
func (p *MockMasterInfoProvider) MasterNode() p2p.NodeID {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.masterNode
}

// Epoch implements MasterInfoProvider.Epoch
func (p *MockMasterInfoProvider) Epoch() frameModel.Epoch {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.epoch
}

// SyncRefreshMasterInfo implements MasterInfoProvider.RefreshMasterInfo
func (p *MockMasterInfoProvider) SyncRefreshMasterInfo(ctx context.Context) error {
	p.refreshCount.Add(1)
	return nil
}

// Set sets given information to the MockMasterInfoProvider
func (p *MockMasterInfoProvider) Set(masterID frameModel.MasterID, masterNode p2p.NodeID, epoch frameModel.Epoch) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.masterID = masterID
	p.masterNode = masterNode
	p.epoch = epoch
}

// SetMasterClosed marks the mock master has having marked the current
// worker closed.
func (p *MockMasterInfoProvider) SetMasterClosed() {
	p.masterSideClosed.Store(true)
}

// RefreshCount returns refresh time, it is used in unit test only
func (p *MockMasterInfoProvider) RefreshCount() int {
	return int(p.refreshCount.Load())
}

// IsMasterSideClosed implements MasterInfoProvider.IsMasterSideClosed
func (p *MockMasterInfoProvider) IsMasterSideClosed() bool {
	return p.masterSideClosed.Load()
}
