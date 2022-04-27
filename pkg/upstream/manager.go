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

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/config"
	pd "github.com/tikv/pd/client"
)

// defaultClusterID is a pseudo cluster id for now. It will be removed in the future.
const defaultClusterID uint64 = 0

// Manager manages all upstream.
type Manager struct {
	defaultUpstream *Upstream
}

// NewManager creates a new Manager and initlizes a defaultUpstream.
// ctx is used to initialize the default upstream and defaultPDEndpoints
// is used to create the default upstream.
func NewManager(ctx context.Context, defaultPDEndpoints []string) (*Manager, error) {
	securityConfig := config.GetGlobalServerConfig().Security
	up := newUpstream(defaultClusterID, defaultPDEndpoints, securityConfig)
	err := up.init(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Manager{defaultUpstream: up}, nil
}

// NewManager4Test returns a Manager for unit test.
func NewManager4Test(pdClient pd.Client) *Manager {
	up := NewUpstream4Test(pdClient)
	return &Manager{defaultUpstream: up}
}

// GetDefaultUpstream returns defaultUpstream.
func (m *Manager) GetDefaultUpstream() *Upstream {
	return m.defaultUpstream
}

// Close closes defaultUpstream.
func (m *Manager) Close() {
	m.defaultUpstream.close()
}
