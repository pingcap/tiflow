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

	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/security"
	pd "github.com/tikv/pd/client"
)

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
	// defaultUpstream is the default upstream
	defaultUpstream *Upstream
}

// MapKey is the key of Manager's ups map
type MapKey struct {
	ID       uint64
	CAPath   string
	CertPath string
	KeyPath  string
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

var (
	testUpstreamID = uint64(1)
	testMapKey     = MapKey{
		ID:       testUpstreamID,
		CAPath:   "",
		CertPath: "",
		KeyPath:  "",
	}
	testConfig = Config{
		ID:       testUpstreamID,
		CAPath:   "",
		CertPath: "",
		KeyPath:  "",
	}
)

// NewManager4Test returns a Manager for unit test.
func NewManager4Test(pdClient pd.Client) *Manager {
	up := NewUpstream4Test(pdClient)
	res := &Manager{ups: new(sync.Map), ctx: context.Background(),
		gcServiceID:     etcd.GcServiceIDForTest(),
		defaultUpstream: up}
	res.ups.Store(testMapKey, up)
	return res
}

type Config struct {
	ID            uint64
	PDEndpoints   string
	KeyPath       string
	CertPath      string
	CAPath        string
	CertAllowedCN []string
}

// AddDefaultUpstream add the default upstream
func (m *Manager) AddDefaultUpstream(conf Config) (*Upstream, error) {
	up := newUpstream(conf.ID, strings.Split(conf.PDEndpoints, ","),
		&security.Credential{
			CAPath:        conf.CAPath,
			CertPath:      conf.CertPath,
			KeyPath:       conf.KeyPath,
			CertAllowedCN: conf.CertAllowedCN,
		})
	if err := up.init(m.ctx, m.gcServiceID); err != nil {
		return nil, err
	}
	mapKey := MapKey{
		ID:       up.PDClient.GetClusterID(m.ctx),
		CAPath:   conf.CAPath,
		CertPath: conf.CertPath,
		KeyPath:  conf.KeyPath,
	}
	up.ID = mapKey.ID
	up.isDefaultUpstream = true
	m.ups.Store(mapKey, up)
	return up, nil
}

func (m *Manager) GetDefaultUpstream() *Upstream {
	return m.defaultUpstream
}

func (m *Manager) Add(conf Config, sync bool) *Upstream {
	mapKey := MapKey{
		ID:       conf.ID,
		CAPath:   conf.CAPath,
		CertPath: conf.CertPath,
		KeyPath:  conf.KeyPath,
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// check again
	v, ok := m.ups.Load(mapKey)
	if ok {
		up := v.(*Upstream)
		up.hold()
		return up
	}
	up := newUpstream(conf.ID, strings.Split(conf.PDEndpoints, ","),
		&security.Credential{
			CAPath:        conf.CAPath,
			CertPath:      conf.CertPath,
			KeyPath:       conf.KeyPath,
			CertAllowedCN: conf.CertAllowedCN,
		})
	m.ups.Store(mapKey, up)
	if sync {
		up.err = up.init(m.ctx, m.gcServiceID)
	} else {
		go func() {
			up.err = up.init(m.ctx, m.gcServiceID)
		}()
	}
	up.hold()
	return up
}

// Get gets a upstream by upstreamID.
func (m *Manager) Get(conf Config) *Upstream {
	mapKey := MapKey{
		ID:       conf.ID,
		CAPath:   conf.CAPath,
		CertPath: conf.CertPath,
		KeyPath:  conf.KeyPath,
	}
	v, ok := m.ups.Load(mapKey)
	if ok {
		up := v.(*Upstream)
		up.hold()
		return up
	}
	return m.Add(conf, false)
}

func (m *Manager) GetByUpstreamID(id uint64) []*Upstream {
	var ret []*Upstream
	m.ups.Range(func(key, v any) bool {
		up := v.(*Upstream)
		if up.ID == id {
			ret = append(ret, up)
		}
		return true
	})
	for _, up := range ret {
		up.hold()
	}
	return ret
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
