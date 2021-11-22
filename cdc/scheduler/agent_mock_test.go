// Copyright 2021 PingCAP, Inc.
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

package scheduler

import (
	"testing"

	"github.com/pingcap/ticdc/cdc/model"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockProcessorMessenger struct {
	mock.Mock
}

func (m *mockProcessorMessenger) FinishTableOperation(ctx cdcContext.Context, tableID model.TableID) (bool, error) {
	args := m.Called(ctx, tableID)
	return args.Bool(0), args.Error(1)
}

func (m *mockProcessorMessenger) SyncTaskStatuses(ctx cdcContext.Context, running, adding, removing []model.TableID) (bool, error) {
	args := m.Called(ctx, running, adding, removing)
	return args.Bool(0), args.Error(1)
}

func (m *mockProcessorMessenger) SendCheckpoint(ctx cdcContext.Context, checkpointTs model.Ts, resolvedTs model.Ts) (bool, error) {
	args := m.Called(ctx, checkpointTs, resolvedTs)
	return args.Bool(0), args.Error(1)
}

func (m *mockProcessorMessenger) Barrier(ctx cdcContext.Context) (done bool) {
	args := m.Called(ctx)
	return args.Bool(0)
}

func (m *mockProcessorMessenger) OnOwnerChanged(ctx cdcContext.Context, newOwnerCaptureID model.CaptureID) {
	m.Called(ctx, newOwnerCaptureID)
}

func (m *mockProcessorMessenger) Close() error {
	args := m.Called()
	return args.Error(0)
}

type mockCheckpointSender struct {
	lastSentCheckpointTs model.Ts
	lastSentResolvedTs   model.Ts
}

func (s *mockCheckpointSender) SendCheckpoint(_ cdcContext.Context, provider checkpointProviderFunc) error {
	checkpointTs, resolvedTs, ok := provider()
	if !ok {
		return nil
	}
	s.lastSentCheckpointTs = checkpointTs
	s.lastSentResolvedTs = resolvedTs
	return nil
}

func (s *mockCheckpointSender) LastSentCheckpointTs() model.Ts {
	return s.lastSentCheckpointTs
}

type mockTableExecutor struct {
	mock.Mock

	t *testing.T

	adding, running, removing map[model.TableID]struct{}
}

func newMockTableExecutor(t *testing.T) *mockTableExecutor {
	return &mockTableExecutor{
		t:        t,
		adding:   map[model.TableID]struct{}{},
		running:  map[model.TableID]struct{}{},
		removing: map[model.TableID]struct{}{},
	}
}

func (e *mockTableExecutor) AddTable(ctx cdcContext.Context, tableID model.TableID) error {
	args := e.Called(ctx, tableID)
	require.NotContains(e.t, e.adding, tableID)
	require.NotContains(e.t, e.running, tableID)
	require.NotContains(e.t, e.removing, tableID)
	e.adding[tableID] = struct{}{}
	return args.Error(0)
}

func (e *mockTableExecutor) RemoveTable(ctx cdcContext.Context, tableID model.TableID) (bool, error) {
	args := e.Called(ctx, tableID)
	require.Contains(e.t, e.running, tableID)
	require.NotContains(e.t, e.removing, tableID)
	delete(e.running, tableID)
	e.removing[tableID] = struct{}{}
	return args.Bool(0), args.Error(1)
}

func (e *mockTableExecutor) IsAddTableFinished(ctx cdcContext.Context, tableID model.TableID) bool {
	_, ok := e.running[tableID]
	return ok
}

func (e *mockTableExecutor) IsRemoveTableFinished(ctx cdcContext.Context, tableID model.TableID) bool {
	_, ok := e.removing[tableID]
	return !ok
}

func (e *mockTableExecutor) GetAllCurrentTables() []model.TableID {
	var ret []model.TableID
	for tableID := range e.adding {
		ret = append(ret, tableID)
	}
	for tableID := range e.running {
		ret = append(ret, tableID)
	}
	for tableID := range e.removing {
		ret = append(ret, tableID)
	}

	return ret
}

func (e *mockTableExecutor) GetCheckpoint() (checkpointTs, resolvedTs model.Ts) {
	args := e.Called()
	return args.Get(0).(model.Ts), args.Get(1).(model.Ts)
}
