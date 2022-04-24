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

	"github.com/benbjohnson/clock"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/config"
	"go.uber.org/zap"
)

const (
	tickDuration = 10 * time.Second
)

var UpStreamManager *Manager

type Manager struct {
	ups sync.Map
	c   clock.Clock
	ctx context.Context
}

// NewManager create a new Manager.
func NewManager(ctx context.Context) *Manager {
	return &Manager{c: clock.New(), ctx: ctx}
}

// Run will check all upStream in Manager periodly, if one upStream has not been
// use more than 30 mintues, close it.
func (m *Manager) Run(ctx context.Context) error {
	ticker := m.c.Ticker(tickDuration)
	for {
		select {
		case <-ctx.Done():
			m.closeUpstreams()
			log.Info("upstream manager exit")
			return ctx.Err()
		case <-ticker.C:
			m.checkUpstreams()
		}
	}
}

// Get gets a upStream by clusterID, if this upStream does not exis, create it.
func (m *Manager) Get(clusterID uint64) (*UpStream, error) {
	if up, ok := m.ups.Load(clusterID); ok {
		up.(*UpStream).hold()
		up.(*UpStream).clearIdealCount()
		return up.(*UpStream), nil
	}

	// TODO: use changefeed's pd addr in the future
	pdEndpoints := strings.Split(config.GetGlobalServerConfig().Debug.ServerPdAddr, ",")
	securityConfig := config.GetGlobalServerConfig().Security
	up := newUpStream(pdEndpoints, securityConfig)
	up.hold()
	// 之后的实现需要检查错误
	_ = up.Init(m.ctx)
	m.ups.Store(clusterID, up)
	return up, nil
}

// Release releases a upStream by clusterID
func (m *Manager) Release(clusterID uint64) {
	if up, ok := m.ups.Load(clusterID); ok {
		up.(*UpStream).unhold()
	}
}

func (m *Manager) checkUpstreams() {
	m.ups.Range(func(k, v interface{}) bool {
		up := v.(*UpStream)
		if !up.isHold() {
			up.addIdealCount()
		}
		if up.shouldClose() {
			up.close()
			m.ups.Delete(k)
		}
		return true
	})
}

func (m *Manager) closeUpstreams() {
	m.ups.Range(func(k, v interface{}) bool {
		id := k.(uint64)
		up := v.(*UpStream)
		up.close()
		log.Info("upStream closed", zap.Uint64("cluster id", id))
		m.ups.Delete(id)
		return true
	})
}
