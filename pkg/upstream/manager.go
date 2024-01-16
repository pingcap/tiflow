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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/security"
	pd "github.com/tikv/pd/client"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// testUpstreamID is a pseudo upstreamID for now. It will be removed in the future.
const testUpstreamID uint64 = 0

// tickInterval is the minimum interval that upstream manager to check upstreams
var tickInterval = 3 * time.Minute

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

	lastTickTime atomic.Time

	initUpstreamFunc func(ctx context.Context, up *Upstream, gcID string) error
}

// NewManager creates a new Manager.
// ctx will be used to initialize upstream spawned by this Manager.
func NewManager(ctx context.Context, gcServiceID string) *Manager {
	ctx, cancel := context.WithCancel(ctx)
	return &Manager{
		ups:              new(sync.Map),
		ctx:              ctx,
		cancel:           cancel,
		gcServiceID:      gcServiceID,
		initUpstreamFunc: initUpstream,
	}
}

// NewManager4Test returns a Manager for unit test.
func NewManager4Test(pdClient pd.Client) *Manager {
	up := NewUpstream4Test(pdClient)
	res := &Manager{
		ups: new(sync.Map), ctx: context.Background(),
		gcServiceID:     etcd.GcServiceIDForTest(),
		defaultUpstream: up,
		cancel:          func() {},
	}
	up.isDefaultUpstream = true
	res.ups.Store(testUpstreamID, up)
	return res
}

// AddDefaultUpstream add the default upstream
func (m *Manager) AddDefaultUpstream(pdEndpoints []string,
	conf *security.Credential,
	pdClient pd.Client,
) (*Upstream, error) {
	up := newUpstream(pdEndpoints, conf)
	// use the pdClient pass from cdc server as the default upstream
	// to reduce the creation times of pdClient to make cdc server more stable
	up.isDefaultUpstream = true
	up.PDClient = pdClient
	if err := m.initUpstreamFunc(m.ctx, up, m.gcServiceID); err != nil {
		return nil, err
	}
	m.defaultUpstream = up
	m.ups.Store(up.ID, up)
	log.Info("default upstream is added", zap.Uint64("id", up.ID))
	return up, nil
}

// GetDefaultUpstream returns the default upstream
func (m *Manager) GetDefaultUpstream() (*Upstream, error) {
	if m.defaultUpstream == nil {
		return nil, cerror.ErrUpstreamNotFound
	}
	return m.defaultUpstream, nil
}

func (m *Manager) add(upstreamID uint64,
	pdEndpoints []string, conf *security.Credential,
) *Upstream {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.ups.Load(upstreamID)
	if ok {
		up := v.(*Upstream)
		up.resetIdleTime()
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
		err := m.initUpstreamFunc(m.ctx, up, m.gcServiceID)
		up.err.Store(err)
	}()
	up.resetIdleTime()
	log.Info("new upstream is added", zap.Uint64("id", up.ID))
	return up
}

// AddUpstream adds an upstream and init it.
func (m *Manager) AddUpstream(info *model.UpstreamInfo) *Upstream {
	return m.add(info.ID,
		strings.Split(info.PDEndpoints, ","),
		&security.Credential{
			CAPath:        info.CAPath,
			CertPath:      info.CertPath,
			KeyPath:       info.KeyPath,
			CertAllowedCN: info.CertAllowedCN,
		})
}

// Get gets a upstream by upstreamID.
func (m *Manager) Get(upstreamID uint64) (*Upstream, bool) {
	v, ok := m.ups.Load(upstreamID)
	if !ok {
		return nil, false
	}
	up := v.(*Upstream)
	return up, true
}

// Close closes all upstreams.
// Please make sure it will only be called once when capture exits.
func (m *Manager) Close() {
	m.cancel()
	m.ups.Range(func(k, v interface{}) bool {
		v.(*Upstream).Close()
		m.ups.Delete(k)
		return true
	})
}

// Visit on each upstream, return error on the first
func (m *Manager) Visit(visitor func(up *Upstream) error) error {
	var err error
	m.ups.Range(func(k, v interface{}) bool {
		err = visitor(v.(*Upstream))
		return err == nil
	})
	return err
}

// Tick checks and frees upstream that have not been used
// for a long time to save resources.
// It's a thread-safe method.
func (m *Manager) Tick(ctx context.Context,
	globalState *orchestrator.GlobalReactorState,
) error {
	if time.Since(m.lastTickTime.Load()) < tickInterval {
		return nil
	}

	activeUpstreams := make(map[uint64]struct{})
	for _, cf := range globalState.Changefeeds {
		if cf != nil && cf.Info != nil {
			activeUpstreams[cf.Info.UpstreamID] = struct{}{}
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()

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
		if up.isDefaultUpstream {
			return true
		}
		// remove failed upstream
		if up.Error() != nil {
			log.Warn("upstream init failed, remove it from manager",
				zap.Uint64("id", up.ID),
				zap.Error(up.Error()))
			go up.Close()
			m.ups.Delete(id)
			return true
		}
		_, ok := activeUpstreams[id]
		if ok {
			return true
		}

		up.trySetIdleTime()
		log.Info("no active changefeed found, try to close upstream",
			zap.Uint64("id", up.ID))
		if up.shouldClose() {
			log.Info("upstream should be closed ,remove it from manager",
				zap.Uint64("id", up.ID))
			go up.Close()
			m.ups.Delete(id)
		}
		return true
	})
	m.lastTickTime.Store(time.Now())
	return err
}
