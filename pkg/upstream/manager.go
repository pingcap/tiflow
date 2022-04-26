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
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// DefaultClusterID is a pseudo cluster for now. It will remove in the future.
const DefaultClusterID = 0

// UpManager is a global variable.
var UpManager *Manager

// Manager manages all upstream.
type Manager struct {
	ups               sync.Map
	defaultPDEnpoints []string
}

// NewManager create a new Manager.
func NewManager(pdEnpoints []string) *Manager {
	return &Manager{defaultPDEnpoints: pdEnpoints}
}

// Get gets a upStream.
// TODO: Get() will be changed to Get(clusterID uint64) in the future.
func (m *Manager) Get() *Upstream {
	if up, ok := m.ups.Load(DefaultClusterID); ok {
		return up.(*Upstream)
	}

	pdEndpoints := m.defaultPDEnpoints
	securityConfig := config.GetGlobalServerConfig().Security
	up := newUpstream(DefaultClusterID, pdEndpoints, securityConfig)

	m.ups.Store(DefaultClusterID, up)
	return up
}

// Run runs this manager.
func (m *Manager) Run(ctx context.Context) error {
	if up, ok := m.ups.Load(DefaultClusterID); ok {
		err := up.(*Upstream).init(ctx)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		return cerror.ErrUpStreamNotFound.GenWithStackByArgs(DefaultClusterID)
	}

	<-ctx.Done()
	m.close()
	log.Info("upstream manager exited", zap.Error(ctx.Err()))
	return ctx.Err()
}

func (m *Manager) close() {
	m.ups.Range(func(k, v interface{}) bool {
		id := k.(uint64)
		up := v.(*Upstream)
		up.close()
		m.ups.Delete(id)
		return true
	})
}
