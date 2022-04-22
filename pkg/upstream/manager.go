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

package upstream

import (
	"context"
	"strings"
	"sync"

	"github.com/pingcap/tiflow/pkg/config"
)

var UpStreamManager *Manager

type Manager struct {
	upStreams map[uint64]*UpStream
	ctx       context.Context
	mu        sync.Mutex
}

func NewManager(ctx context.Context) *Manager {
	return &Manager{upStreams: make(map[uint64]*UpStream), ctx: ctx}
}

func (m *Manager) GetUpStream(clusterID uint64) (*UpStream, error) {
	if upStream, ok := m.upStreams[clusterID]; ok {
		m.mu.Lock()
		defer m.mu.Unlock()
		upStream.Count++
		return upStream, nil
	}

	pdEndpoints := strings.Split(config.GetGlobalServerConfig().Debug.ServerPdAddr, ",")
	securityConfig := config.GetGlobalServerConfig().Security
	upStream := newUpStream(pdEndpoints, securityConfig)
	// 之后的实现需要检查错误
	_ = upStream.Init(m.ctx)

	m.mu.Lock()
	defer m.mu.Unlock()
	m.upStreams[clusterID] = upStream
	upStream.Count++
	return upStream, nil
}

func (m *Manager) ReleaseUpStream(clusterID uint64) {
	if upStream, ok := m.upStreams[clusterID]; ok {
		m.mu.Lock()
		defer m.mu.Unlock()
		upStream.Count--

		if upStream.Count == 0 {
			upStream.close()
			delete(m.upStreams, clusterID)
		}
	}
}
