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
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/config"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// DefaultUpstreamID is a pseudo upstreamID for now. It will be removed in the future.
const DefaultUpstreamID uint64 = 0

// Manager manages all upstream.
type Manager struct {
	// upstreamID map to *Upstream.
	ups *sync.Map
	// all upstream should be spawn from this ctx.
	ctx context.Context
	// Only use in Close().
	cancel func()
	// lock this mutex when add or delete a value of Manager.ups.
	mu sync.Mutex
}

// NewManager creates a new Manager.
// ctx will be used to initialize upstream spawned by this Manager.
func NewManager(ctx context.Context) *Manager {
	ctx, cancel := context.WithCancel(ctx)
	return &Manager{ups: new(sync.Map), ctx: ctx, cancel: cancel}
}

// NewManager4Test returns a Manager for unit test.
func NewManager4Test(pdClient pd.Client) *Manager {
	up := NewUpstream4Test(pdClient)
	res := &Manager{ups: new(sync.Map), ctx: context.Background()}
	res.ups.Store(DefaultUpstreamID, up)
	return res
}

// Add adds a upstream and init it.
// TODO(dongmen): async init upstream and should not return any error in the future.
func (m *Manager) Add(upstreamID uint64, pdEndpoints []string, securityConfig *config.SecurityConfig) error {
	select {
	case <-m.ctx.Done():
		// This would not happen if there were no errors in the code logic.
		panic("should not add a upstream to a closed upstream manager")
	default:
	}
	if _, ok := m.ups.Load(upstreamID); ok {
		return nil
	}
	up := newUpstream(upstreamID, pdEndpoints, securityConfig)
	err := up.init(m.ctx)
	if err != nil {
		return errors.Trace(err)
	}
	m.mu.Lock()
	m.ups.Store(upstreamID, up)
	m.mu.Unlock()
	return nil
}

// Get gets a upstream by upstreamID.
func (m *Manager) Get(upstreamID uint64) *Upstream {
	v, ok := m.ups.Load(upstreamID)
	if !ok {
		log.Panic("upstream not exists", zap.Uint64("upstreamID", upstreamID))
	}
	up := v.(*Upstream)
	up.hold()
	return up
}

// Close closes all upstreams.
// Please make sure it will only be called once when capture exits.
func (m *Manager) Close() {
	m.cancel()
	m.ups.Range(func(k, v interface{}) bool {
		v.(*Upstream).close()
		return true
	})
}

// Tick checks and frees upstream that have not been used
// for a long time to save resources.
// Tick should be only call in Owner.Tick().
func (m *Manager) Tick(ctx context.Context) error {
	var err error
	m.ups.Range(func(k, v interface{}) bool {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return false
		default:
		}
		id := k.(uint64)

		up := v.(*Upstream)
		if up.shouldClose() {
			go up.close()
		}

		if up.IsClosed() {
			m.mu.Lock()
			m.ups.Delete(id)
			m.mu.Unlock()
		}
		return true
	})
	return err
}
