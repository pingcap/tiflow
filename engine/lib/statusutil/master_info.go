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

package statusutil

import (
	"sync"

	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
)

// MasterInfoProvider is an object that can provide necessary
// information so that the Writer can contact the master.
type MasterInfoProvider interface {
	MasterID() libModel.MasterID
	MasterNode() p2p.NodeID
	Epoch() libModel.Epoch
}

type MockMasterInfoProvider struct {
	mu         sync.RWMutex
	masterID   libModel.MasterID
	masterNode p2p.NodeID
	epoch      libModel.Epoch
}

func (p *MockMasterInfoProvider) MasterID() libModel.MasterID {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.masterID
}

func (p *MockMasterInfoProvider) MasterNode() p2p.NodeID {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.masterNode
}

func (p *MockMasterInfoProvider) Epoch() libModel.Epoch {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.epoch
}

func (p *MockMasterInfoProvider) Set(masterID libModel.MasterID, masterNode p2p.NodeID, epoch libModel.Epoch) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.masterID = masterID
	p.masterNode = masterNode
	p.epoch = epoch
}
