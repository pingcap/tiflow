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

package unit

import (
	"context"
	"sync"

	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/stretchr/testify/mock"
)

// MockHolder implement Holder
type MockHolder struct {
	sync.Mutex
	mock.Mock
}

// Init implement Holder.Init
func (m *MockHolder) Init(ctx context.Context) error {
	return nil
}

// LazyProcess implement Holder.LazyProcess
func (m *MockHolder) LazyProcess(ctx context.Context) {}

// Tick implement Holder.Tick
func (m *MockHolder) Tick(ctx context.Context) error {
	return nil
}

// Stage implement Holder.Stage
func (m *MockHolder) Stage() metadata.TaskStage {
	m.Lock()
	defer m.Unlock()
	args := m.Called()
	return args.Get(0).(metadata.TaskStage)
}

// Status implement Holder.Status
func (m *MockHolder) Status(ctx context.Context) interface{} {
	m.Lock()
	defer m.Unlock()
	args := m.Called()
	return args.Get(0)
}

// Close implement Holder.Close
func (m *MockHolder) Close() {}
