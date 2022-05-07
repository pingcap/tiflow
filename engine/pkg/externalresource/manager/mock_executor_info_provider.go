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

package manager

import (
	"sync"

	resourcemeta "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
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
