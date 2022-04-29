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
	"log"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/config"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// DefaultClusterID is a pseudo cluster id for now. It will be removed in the future.
const DefaultClusterID uint64 = 0

// Manager manages all upstream.
type Manager struct {
	// clusterID map to *Upstream.
	ups *sync.Map
	// all upstream should be spawn from this ctx.
	ctx context.Context
	// Only use in Close()
	cancel func()
}

// NewManager creates a new Manager.
// ctx will be use to initialize upstream spawned by this Manager.
func NewManager(ctx context.Context) *Manager {
	ctx, cancel := context.WithCancel(ctx)
	res := &Manager{ups: new(sync.Map), ctx: ctx, cancel: cancel}
	return res
}

// NewManager4Test returns a Manager for unit test.
func NewManager4Test(pdClient pd.Client) *Manager {
	res := &Manager{ups: new(sync.Map)}
	up := NewUpstream4Test(pdClient)
	atomic.StoreInt32(&up.status, normal)
	res.ups.Store(DefaultClusterID, up)
	return res
}

// Add adds a upstream and init it.
// TODO(dongmen): async init upstream and should not return any error in the future.
func (m *Manager) Add(clusterID uint64, pdEndpoints []string) error {
	if _, ok := m.ups.Load(clusterID); ok {
		return nil
	}
	securityConfig := config.GetGlobalServerConfig().Security
	up := newUpstream(DefaultClusterID, pdEndpoints, securityConfig)
	err := up.init(m.ctx)
	if err != nil {
		return errors.Trace(err)
	}
	m.ups.Store(DefaultClusterID, up)
	return nil
}

// Get gets a upstream by clusterID.
func (m *Manager) Get(clusterID uint64) *Upstream {
	v, ok := m.ups.Load(clusterID)
	if !ok {
		log.Panic("upstream not exists", zap.Uint64("cluster-id", clusterID))
	}
	return v.(*Upstream)
}

// Close closes all upstreams.
func (m *Manager) Close() {
	m.cancel()
	m.ups.Range(func(k, v interface{}) bool {
		v.(*Upstream).close()
		return true
	})
}
