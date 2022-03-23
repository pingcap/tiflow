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

	"github.com/pingcap/log"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/cdc/model"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
)

type MockProcessorMessenger struct {
	mock.Mock
}

// FinishTableOperation marks this function as being called.
func (m *MockProcessorMessenger) FinishTableOperation(ctx cdcContext.Context, tableID model.TableID, epoch model.ProcessorEpoch) (bool, error) {
	args := m.Called(ctx, tableID, epoch)
	return args.Bool(0), args.Error(1)
}

// SyncTaskStatuses marks this function as being called.
func (m *MockProcessorMessenger) SyncTaskStatuses(ctx cdcContext.Context, epoch model.ProcessorEpoch, adding, removing, running []model.TableID) (bool, error) {
	args := m.Called(ctx, epoch, running, adding, removing)
	return args.Bool(0), args.Error(1)
}

func (m *MockProcessorMessenger) SendCheckpoint(ctx cdcContext.Context, checkpointTs model.Ts, resolvedTs model.Ts) (bool, error) {
	args := m.Called(ctx, checkpointTs, resolvedTs)
	return args.Bool(0), args.Error(1)
}

func (m *MockProcessorMessenger) Barrier(ctx cdcContext.Context) (done bool) {
	args := m.Called(ctx)
	return args.Bool(0)
}

// OnOwnerChanged marks this function as being called.
func (m *MockProcessorMessenger) OnOwnerChanged(
	ctx cdcContext.Context,
	newOwnerCaptureID model.CaptureID,
	newOwnerRev int64,
) {
	m.Called(ctx, newOwnerCaptureID, newOwnerRev)
}

func (m *MockProcessorMessenger) Close() error {
	args := m.Called()
	return args.Error(0)
}

type MockCheckpointSender struct {
	lastSentCheckpointTs model.Ts
	lastSentResolvedTs   model.Ts
}

func (s *MockCheckpointSender) SendCheckpoint(_ cdcContext.Context, provider checkpointProviderFunc) error {
	checkpointTs, resolvedTs, ok := provider()
	if !ok {
		return nil
	}
	s.lastSentCheckpointTs = checkpointTs
	s.lastSentResolvedTs = resolvedTs
	return nil
}

func (s *MockCheckpointSender) LastSentCheckpointTs() model.Ts {
	return s.lastSentCheckpointTs
}

type MockTableExecutor struct {
	mock.Mock

	t *testing.T

	Adding, Running, Removing map[model.TableID]struct{}
}

func NewMockTableExecutor(t *testing.T) *MockTableExecutor {
	return &MockTableExecutor{
		t:        t,
		Adding:   map[model.TableID]struct{}{},
		Running:  map[model.TableID]struct{}{},
		Removing: map[model.TableID]struct{}{},
	}
}

func (e *MockTableExecutor) AddTable(ctx cdcContext.Context, tableID model.TableID) (bool, error) {
	log.Info("AddTable", zap.Int64("table-id", tableID))
	require.NotContains(e.t, e.Adding, tableID)
	require.NotContains(e.t, e.Running, tableID)
	require.NotContains(e.t, e.Removing, tableID)
	args := e.Called(ctx, tableID)
	if args.Bool(0) {
		// If the mock return value indicates a success, then we record the added table.
		e.Adding[tableID] = struct{}{}
	}
	return args.Bool(0), args.Error(1)
}

func (e *MockTableExecutor) RemoveTable(ctx cdcContext.Context, tableID model.TableID) (bool, error) {
	log.Info("RemoveTable", zap.Int64("table-id", tableID))
	args := e.Called(ctx, tableID)
	require.Contains(e.t, e.Running, tableID)
	require.NotContains(e.t, e.Removing, tableID)
	delete(e.Running, tableID)
	e.Removing[tableID] = struct{}{}
	return args.Bool(0), args.Error(1)
}

func (e *MockTableExecutor) IsAddTableFinished(ctx cdcContext.Context, tableID model.TableID) bool {
	_, ok := e.Running[tableID]
	return ok
}

func (e *MockTableExecutor) IsRemoveTableFinished(ctx cdcContext.Context, tableID model.TableID) bool {
	_, ok := e.Removing[tableID]
	return !ok
}

func (e *MockTableExecutor) GetAllCurrentTables() []model.TableID {
	var ret []model.TableID
	for tableID := range e.Adding {
		ret = append(ret, tableID)
	}
	for tableID := range e.Running {
		ret = append(ret, tableID)
	}
	for tableID := range e.Removing {
		ret = append(ret, tableID)
	}

	return ret
}

func (e *MockTableExecutor) GetCheckpoint() (checkpointTs, resolvedTs model.Ts) {
	args := e.Called()
	return args.Get(0).(model.Ts), args.Get(1).(model.Ts)
}
