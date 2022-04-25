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
	tickDuration     = 10 * time.Second
	notifierChanSize = 32
)

var UpStreamManager *Manager

type Manager struct {
	ups sync.Map
	c   clock.Clock
	// 用于通知需要初始化一个 upStream
	notifier chan uint64
}

// NewManager create a new Manager.
func NewManager() *Manager {
	return &Manager{c: clock.New(), notifier: make(chan uint64, notifierChanSize)}
}

// Run will check all upStream in Manager periodly, if one upStream has not been
// use more than 30 mintues, close it.
func (m *Manager) Run(ctx context.Context) error {
	ticker := m.c.Ticker(tickDuration)
	for {
		select {
		case <-ctx.Done():
			m.close()
			log.Info("upstream manager exited", zap.Error(ctx.Err()))
			return ctx.Err()
		case <-ticker.C:
			m.check(ctx)
		case id := <-m.notifier:
			if up, ok := m.ups.Load(id); ok {
				up.(*UpStream).asyncInit(ctx)
			}
		}
	}
}

// Get a upStream by clusterID, if this upStream does not exis, create it.
func (m *Manager) Get(clusterID uint64) (*UpStream, error) {
	if up, ok := m.ups.Load(clusterID); ok {
		up.(*UpStream).hold()
		up.(*UpStream).clearIc()
		return up.(*UpStream), nil
	}

	// TODO: use changefeed's pd addr in the future.
	pdEndpoints := strings.Split(config.GetGlobalServerConfig().Debug.ServerPdAddr, ",")
	securityConfig := config.GetGlobalServerConfig().Security
	up := newUpStream(clusterID, pdEndpoints, securityConfig)
	m.ups.Store(clusterID, up)
	up.hold()

	select {
	case m.notifier <- clusterID:
		log.Info("async init upstream", zap.Uint64("clusterID", clusterID))
	default:
	}
	return up, nil
}

// Release a upStream by clusterID
func (m *Manager) Release(clusterID uint64) {
	if up, ok := m.ups.Load(clusterID); ok {
		up.(*UpStream).unhold()
	}
}

// check go throuht all upStreams in Manager and check if it should be initialize
// or closed.
func (m *Manager) check(ctx context.Context) {
	m.ups.Range(func(k, v interface{}) bool {
		up := v.(*UpStream)
		if up.IsNormal() && !up.isHold() {
			up.addIc()
		}
		if up.shouldClose() {
			up.close()
			m.ups.Delete(k)
		}
		if up.IsReady() {
			up.asyncInit(ctx)
		}
		if up.IsColse() {
			m.ups.Delete(k)
		}
		return true
	})
}

// close closes all upStreams in Manager.
func (m *Manager) close() {
	m.ups.Range(func(k, v interface{}) bool {
		up := v.(*UpStream)
		up.close()
		m.ups.Delete(k)
		return true
	})
}
