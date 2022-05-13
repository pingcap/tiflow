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

package base

import (
	"context"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/base/protocol"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// MockProcessorMessenger is a mock of ProcessorMessenger.
type MockProcessorMessenger struct {
	mock.Mock
}

// FinishTableOperation marks this function as being called.
func (m *MockProcessorMessenger) FinishTableOperation(
	ctx context.Context, tableID model.TableID, epoch protocol.ProcessorEpoch,
) (bool, error) {
	args := m.Called(ctx, tableID, epoch)
	return args.Bool(0), args.Error(1)
}

// SyncTaskStatuses marks this function as being called.
func (m *MockProcessorMessenger) SyncTaskStatuses(
	ctx context.Context, epoch protocol.ProcessorEpoch,
	adding, removing, running []model.TableID,
) (bool, error) {
	args := m.Called(ctx, epoch, running, adding, removing)
	return args.Bool(0), args.Error(1)
}

// SendCheckpoint marks this function as being called.
func (m *MockProcessorMessenger) SendCheckpoint(
	ctx context.Context, checkpointTs model.Ts, resolvedTs model.Ts,
) (bool, error) {
	args := m.Called(ctx, checkpointTs, resolvedTs)
	return args.Bool(0), args.Error(1)
}

// Barrier marks this function as being called.
func (m *MockProcessorMessenger) Barrier(ctx context.Context) (done bool) {
	args := m.Called(ctx)
	return args.Bool(0)
}

// OnOwnerChanged marks this function as being called.
func (m *MockProcessorMessenger) OnOwnerChanged(
	ctx context.Context,
	newOwnerCaptureID model.CaptureID,
	newOwnerRev int64,
) {
	m.Called(ctx, newOwnerCaptureID, newOwnerRev)
}

// Close marks this function as being called.
func (m *MockProcessorMessenger) Close() error {
	args := m.Called()
	return args.Error(0)
}

type mockCheckpointSender struct {
	lastSentCheckpointTs model.Ts
	lastSentResolvedTs   model.Ts
}

// SendCheckpoint sends a checkpoint.
func (s *mockCheckpointSender) SendCheckpoint(
	_ context.Context, provider checkpointProviderFunc,
) error {
	checkpointTs, resolvedTs, ok := provider()
	if !ok {
		return nil
	}
	s.lastSentCheckpointTs = checkpointTs
	s.lastSentResolvedTs = resolvedTs
	return nil
}

// LastSentCheckpointTs returns the last sent checkpoint ts.
func (s *mockCheckpointSender) LastSentCheckpointTs() model.Ts {
	return s.lastSentCheckpointTs
}

// MockTableExecutor is a mock implementation of TableExecutor.
type MockTableExecutor struct {
	mock.Mock

	t *testing.T

	Adding, Running, Removing map[model.TableID]struct{}
}

// NewMockTableExecutor creates a new mock table executor.
func NewMockTableExecutor(t *testing.T) *MockTableExecutor {
	return &MockTableExecutor{
		t:        t,
		Adding:   map[model.TableID]struct{}{},
		Running:  map[model.TableID]struct{}{},
		Removing: map[model.TableID]struct{}{},
	}
}

// AddTable adds a table to the executor.
func (e *MockTableExecutor) AddTable(
	ctx context.Context, tableID model.TableID, startTs model.Ts,
) (bool, error) {
	log.Info("AddTable", zap.Int64("tableID", tableID))
	require.NotContains(e.t, e.Adding, tableID)
	require.NotContains(e.t, e.Running, tableID)
	require.NotContains(e.t, e.Removing, tableID)
	args := e.Called(ctx, tableID, startTs)
	if args.Bool(0) {
		// If the mock return value indicates a success, then we record the added table.
		e.Adding[tableID] = struct{}{}
	}
	return args.Bool(0), args.Error(1)
}

// RemoveTable removes a table from the executor.
func (e *MockTableExecutor) RemoveTable(ctx context.Context, tableID model.TableID) (bool, error) {
	log.Info("RemoveTable", zap.Int64("tableID", tableID))
	args := e.Called(ctx, tableID)
	require.Contains(e.t, e.Running, tableID)
	require.NotContains(e.t, e.Removing, tableID)
	delete(e.Running, tableID)
	e.Removing[tableID] = struct{}{}
	return args.Bool(0), args.Error(1)
}

// IsAddTableFinished determines if the table has been added.
func (e *MockTableExecutor) IsAddTableFinished(ctx context.Context, tableID model.TableID) bool {
	_, ok := e.Running[tableID]
	return ok
}

// IsRemoveTableFinished determines if the table has been removed.
func (e *MockTableExecutor) IsRemoveTableFinished(ctx context.Context, tableID model.TableID) bool {
	_, ok := e.Removing[tableID]
	return !ok
}

// GetAllCurrentTables returns all tables that are currently being adding, running, or removing.
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

// GetCheckpoint returns the last checkpoint.
func (e *MockTableExecutor) GetCheckpoint() (checkpointTs, resolvedTs model.Ts) {
	args := e.Called()
	return args.Get(0).(model.Ts), args.Get(1).(model.Ts)
}
