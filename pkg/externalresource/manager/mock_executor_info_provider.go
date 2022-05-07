package manager

import (
	"sync"

	resourcemeta "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
)

type MockExecutorInfoProvider struct {
	mu          sync.RWMutex
	executorSet map[resourcemeta.ExecutorID]struct{}
}

func NewMockExecutorInfoProvider() *MockExecutorInfoProvider {
	return &MockExecutorInfoProvider{
		executorSet: make(map[resourcemeta.ExecutorID]struct{}),
	}
}

func (p *MockExecutorInfoProvider) AddExecutor(executorID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.executorSet[resourcemeta.ExecutorID(executorID)] = struct{}{}
}

func (p *MockExecutorInfoProvider) RemoveExecutor(executorID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.executorSet, resourcemeta.ExecutorID(executorID))
}

func (p *MockExecutorInfoProvider) HasExecutor(executorID string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if _, exists := p.executorSet[resourcemeta.ExecutorID(executorID)]; exists {
		return true
	}
	return false
}

func (p *MockExecutorInfoProvider) ListExecutors() (ret []string) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for id := range p.executorSet {
		ret = append(ret, string(id))
	}
	return
}
