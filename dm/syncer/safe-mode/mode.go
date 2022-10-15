// Copyright 2019 PingCAP, Inc.
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

package mode

import (
	"sync"

	"github.com/pingcap/tidb/util/filter"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"go.uber.org/zap"
)

// SafeMode controls whether enable safe-mode through a mechanism similar to reference-count
// indicates enabled excepting the count is 0.
type SafeMode struct {
	mu     sync.RWMutex
	count  int32
	tables map[string]struct{}
}

// NewSafeMode creates a new SafeMode instance.
func NewSafeMode() *SafeMode {
	return &SafeMode{
		tables: make(map[string]struct{}),
	}
}

// Add adds n to the count, n can be negative.
func (m *SafeMode) Add(tctx *tcontext.Context, n int32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.setCount(tctx, m.count+n)
}

// IncrForTable tries to add 1 on the count if the table not added before
// can only be desc with DescForTable.
func (m *SafeMode) IncrForTable(tctx *tcontext.Context, table *filter.Table) error {
	tableID := utils.GenTableID(table)

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tables[tableID]; !ok {
		m.tables[tableID] = struct{}{}
		return m.setCount(tctx, m.count+1)
	}
	return nil
}

// DescForTable tries to add -1 on the count if the table added before.
func (m *SafeMode) DescForTable(tctx *tcontext.Context, table *filter.Table) error {
	tableID := utils.GenTableID(table)

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tables[tableID]; ok {
		delete(m.tables, tableID)
		return m.setCount(tctx, m.count-1)
	}
	return nil
}

// Reset resets to the state of not-enable.
func (m *SafeMode) Reset(tctx *tcontext.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	//nolint:errcheck
	m.setCount(tctx, 0)
	m.tables = make(map[string]struct{})
}

// Enable returns whether is enabled currently.
func (m *SafeMode) Enable() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.count != 0
}

// setCount sets the count, called internal.
func (m *SafeMode) setCount(tctx *tcontext.Context, n int32) error {
	if n < 0 {
		return terror.ErrSyncUnitSafeModeSetCount.Generatef("set negative count (%d) for safe-mode not valid", m.count)
	}

	prev := m.count
	m.count = n
	tctx.L().Info("change count", zap.Int32("previous count", prev), zap.Int32("new count", m.count))
	return nil
}
