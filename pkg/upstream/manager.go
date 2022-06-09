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

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/security"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// DefaultUpstreamID is a pseudo upstreamID for now. It will be removed in the future.
const DefaultUpstreamID uint64 = 0

// Manager manages all upstream.
type Manager struct {
	// gcServiceID identify the cdc cluster gc service id
	gcServiceID string
	// upstreamID map to *Upstream.
	ups *sync.Map
	// all upstream should be spawn from this ctx.
	ctx context.Context
	// Only use in Close().
	cancel func()
	// lock this mutex when add or delete a value of Manager.ups.
	mu sync.Mutex

	defaultUpstream *Upstream
}

// NewManager creates a new Manager.
// ctx will be used to initialize upstream spawned by this Manager.
func NewManager(ctx context.Context, gcServiceID string) *Manager {
	ctx, cancel := context.WithCancel(ctx)
	return &Manager{
		ups:         new(sync.Map),
		ctx:         ctx,
		cancel:      cancel,
		gcServiceID: gcServiceID,
	}
}

// NewManager4Test returns a Manager for unit test.
func NewManager4Test(pdClient pd.Client) *Manager {
	up := NewUpstream4Test(pdClient)
	res := &Manager{
		ups: new(sync.Map), ctx: context.Background(),
		gcServiceID:     etcd.GcServiceIDForTest(),
		defaultUpstream: up,
	}
	up.isDefaultUpstream = true
	up.hold()
	res.ups.Store(DefaultUpstreamID, up)
	return res
}

// AddDefaultUpstream add the default upstream
func (m *Manager) AddDefaultUpstream(pdEndpoint []string,
	conf *security.Credential,
) (*Upstream, error) {
	up := newUpstream(pdEndpoint,
		&security.Credential{
			CAPath:        conf.CAPath,
			CertPath:      conf.CertPath,
			KeyPath:       conf.KeyPath,
			CertAllowedCN: conf.CertAllowedCN,
		})
	if err := up.init(m.ctx, m.gcServiceID); err != nil {
		return nil, err
	}
	up.ID = up.PDClient.GetClusterID(m.ctx)
	up.isDefaultUpstream = true
	m.defaultUpstream = up
	up.hold()
	m.ups.Store(up.ID, up)
	return up, nil
}

// GetDefaultUpstream returns the default upstream
func (m *Manager) GetDefaultUpstream() *Upstream {
	m.defaultUpstream.hold()
	return m.defaultUpstream
}

func (m *Manager) add(upstreamID uint64,
	pdEndpoints []string, conf *config.SecurityConfig,
) *Upstream {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.ups.Load(upstreamID)
	if ok {
		up := v.(*Upstream)
		up.hold()
		return up
	}
	securityConf := &security.Credential{}
	if conf != nil {
		securityConf = &security.Credential{
			CAPath:        conf.CAPath,
			CertPath:      conf.CertPath,
			KeyPath:       conf.KeyPath,
			CertAllowedCN: conf.CertAllowedCN,
		}
	}
	up := newUpstream(pdEndpoints, securityConf)
	m.ups.Store(upstreamID, up)
	go func() {
		up.err = up.init(m.ctx, m.gcServiceID)
	}()
	up.hold()
	return up
}

// AddUpstream adds an upstream and init it.
func (m *Manager) AddUpstream(upstreamID model.UpstreamID,
	info *model.UpstreamInfo,
) *Upstream {
	return m.add(upstreamID,
		strings.Split(info.PDEndpoints, ","),
		&security.Credential{
			CAPath:        info.CAPath,
			CertPath:      info.CertPath,
			KeyPath:       info.KeyPath,
			CertAllowedCN: info.CertAllowedCN,
		})
}

// RemoveUpstream remove upstream from the manager
func (m *Manager) RemoveUpstream(upstreamID model.UpstreamID) {
	v, ok := m.ups.Load(upstreamID)
	if ok {
		up := v.(*Upstream)
		up.unhold()
	}
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
