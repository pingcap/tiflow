package statusutil

import (
	"sync"

	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type (
	MasterID = string
	WorkerID = string
	Epoch    = int64
)

// MasterInfoProvider is an object that can provide necessary
// information so that the Writer can contact the master.
type MasterInfoProvider interface {
	MasterID() MasterID
	MasterNode() p2p.NodeID
	Epoch() Epoch
}

type MockMasterInfoProvider struct {
	mu         sync.RWMutex
	masterID   MasterID
	masterNode p2p.NodeID
	epoch      Epoch
}

func (p *MockMasterInfoProvider) MasterID() MasterID {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.masterID
}

func (p *MockMasterInfoProvider) MasterNode() p2p.NodeID {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.masterNode
}

func (p *MockMasterInfoProvider) Epoch() Epoch {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.epoch
}

func (p *MockMasterInfoProvider) Set(masterID MasterID, masterNode p2p.NodeID, epoch Epoch) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.masterID = masterID
	p.masterNode = masterNode
	p.epoch = epoch
}
