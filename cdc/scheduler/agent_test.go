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
	"github.com/pingcap/ticdc/pkg/config"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

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

func (e *mockTableExecutor) AddTable(ctx cdcContext.Context, tableID model.TableID, boundaryTs model.Ts) error {
	args := e.Called(ctx, tableID, boundaryTs)
	require.NotContains(e.t, e.adding, tableID)
	require.NotContains(e.t, e.running, tableID)
	require.NotContains(e.t, e.removing, tableID)
	e.adding[tableID] = struct{}{}
	return args.Error(0)
}

func (e *mockTableExecutor) RemoveTable(ctx cdcContext.Context, tableID model.TableID, boundaryTs model.Ts) (bool, error) {
	args := e.Called(ctx, tableID, boundaryTs)
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

func TestAgentAddTable(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(false)
	oldCheckpointInterval := config.GetGlobalServerConfig().SchedulerV2.ProcessorCheckpointInterval
	config.GetGlobalServerConfig().SchedulerV2.ProcessorCheckpointInterval = 0
	defer func() {
		config.GetGlobalServerConfig().SchedulerV2.ProcessorCheckpointInterval = oldCheckpointInterval
	}()

	executor := newMockTableExecutor(t)
	messenger := &mockProcessorMessenger{}
	agent := NewBaseAgent("test-cf", executor, messenger)
	messenger.On("SyncTaskStatuses", mock.Anything, []model.TableID(nil), []model.TableID(nil), []model.TableID(nil)).
		Return(true, nil)
	err := agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	executor.ExpectedCalls = nil
	messenger.ExpectedCalls = nil
	agent.OnOwnerDispatchedTask("capture-1", 1, model.TableID(1), model.Ts(1000), false)
	executor.On("AddTable", mock.Anything, model.TableID(1), model.Ts(1000)).Return(nil)
	messenger.On("OnOwnerChanged", mock.Anything, "capture-1")

	err = agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	executor.ExpectedCalls = nil
	messenger.ExpectedCalls = nil
	delete(executor.adding, model.TableID(1))
	executor.running[model.TableID(1)] = struct{}{}
	executor.On("GetCheckpoint").Return(model.Ts(1002), model.Ts(1000))
	messenger.On("SendCheckpoint", mock.Anything, model.Ts(1002), model.Ts(1000)).Return(true, nil)
	messenger.On("FinishTableOperation", mock.Anything, model.TableID(1)).Return(true, nil)

	err = agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	executor.ExpectedCalls = nil
	messenger.ExpectedCalls = nil

	messenger.On("Barrier", mock.Anything).Return(true)
	executor.On("GetCheckpoint").Return(model.Ts(1003), model.Ts(1005))
	messenger.On("SendCheckpoint", mock.Anything, model.Ts(1003), model.Ts(1005)).Return(true, nil)

	err = agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)
}

func TestAgentRemoveTable(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(false)
	oldCheckpointInterval := config.GetGlobalServerConfig().SchedulerV2.ProcessorCheckpointInterval
	config.GetGlobalServerConfig().SchedulerV2.ProcessorCheckpointInterval = 0
	defer func() {
		config.GetGlobalServerConfig().SchedulerV2.ProcessorCheckpointInterval = oldCheckpointInterval
	}()

	executor := newMockTableExecutor(t)
	executor.running[model.TableID(1)] = struct{}{}
	executor.running[model.TableID(2)] = struct{}{}

	messenger := &mockProcessorMessenger{}
	agent := NewBaseAgent("test-cf", executor, messenger)
	agent.OnOwnerAnnounce("capture-2", 1)
	messenger.On("SyncTaskStatuses", mock.Anything,  []model.TableID{1, 2}, []model.TableID(nil), []model.TableID(nil)).
		Return(true, nil)
	messenger.On("OnOwnerChanged", mock.Anything, "capture-2")
	executor.On("GetCheckpoint").Return(model.Ts(1000), model.Ts(1000))
	messenger.On("SendCheckpoint", mock.Anything, model.Ts(1000), model.Ts(1000)).Return(true, nil)
	err := agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	executor.ExpectedCalls = nil
	messenger.ExpectedCalls = nil
	agent.OnOwnerDispatchedTask("capture-2", 1, model.TableID(1), model.Ts(1000), true)
	executor.On("GetCheckpoint").Return(model.Ts(1000), model.Ts(1000))
	messenger.On("SendCheckpoint", mock.Anything, model.Ts(1000), model.Ts(1000)).Return(true, nil)
	executor.On("RemoveTable", mock.Anything, model.TableID(1), model.Ts(1000)).Return(true, nil)
	messenger.On("Barrier", mock.Anything).Return(true)
	err = agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	executor.ExpectedCalls = nil
	messenger.ExpectedCalls = nil
	delete(executor.removing, model.TableID(1))
	executor.On("GetCheckpoint").Return(model.Ts(1002), model.Ts(1000))
	messenger.On("Barrier", mock.Anything).Return(true)
	messenger.On("FinishTableOperation", mock.Anything, model.TableID(1)).Return(true, nil)
	messenger.On("SendCheckpoint", mock.Anything, model.Ts(1002), model.Ts(1000)).Return(true, nil)

	err = agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)
}

func TestAgentOwnerChangedWhileAddingTable(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(false)
	oldCheckpointInterval := config.GetGlobalServerConfig().SchedulerV2.ProcessorCheckpointInterval
	config.GetGlobalServerConfig().SchedulerV2.ProcessorCheckpointInterval = 0
	defer func() {
		config.GetGlobalServerConfig().SchedulerV2.ProcessorCheckpointInterval = oldCheckpointInterval
	}()

	executor := newMockTableExecutor(t)
	messenger := &mockProcessorMessenger{}
	agent := NewBaseAgent("test-cf", executor, messenger)
	messenger.On("SyncTaskStatuses", mock.Anything, []model.TableID(nil), []model.TableID(nil), []model.TableID(nil)).
		Return(true, nil)
	err := agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	agent.OnOwnerDispatchedTask("capture-1", 1, model.TableID(1), model.Ts(1000), false)
	executor.On("AddTable", mock.Anything, model.TableID(1), model.Ts(1000)).Return(nil)
	messenger.On("OnOwnerChanged", mock.Anything, "capture-1")

	err = agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	executor.ExpectedCalls = nil
	messenger.ExpectedCalls = nil
	executor.On("GetCheckpoint").Return(model.Ts(1002), model.Ts(1000))
	messenger.On("SendCheckpoint", mock.Anything, model.Ts(1002), model.Ts(1000)).Return(true, nil)

	err = agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	executor.ExpectedCalls = nil
	messenger.ExpectedCalls = nil
	agent.OnOwnerAnnounce("capture-2", 2)
	messenger.On("OnOwnerChanged", mock.Anything, "capture-2")
	messenger.On("SyncTaskStatuses", mock.Anything, []model.TableID(nil), []model.TableID{1}, []model.TableID(nil)).
		Return(true, nil)
	messenger.On("Barrier", mock.Anything).Return(true)
	executor.On("GetCheckpoint").Return(model.Ts(1002), model.Ts(1000))
	messenger.On("SendCheckpoint", mock.Anything, model.Ts(1002), model.Ts(1000)).Return(true, nil)

	err = agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)
}

func TestAgentReceiveFromStaleOwner(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(false)
	oldCheckpointInterval := config.GetGlobalServerConfig().SchedulerV2.ProcessorCheckpointInterval
	config.GetGlobalServerConfig().SchedulerV2.ProcessorCheckpointInterval = 0
	defer func() {
		config.GetGlobalServerConfig().SchedulerV2.ProcessorCheckpointInterval = oldCheckpointInterval
	}()

	executor := newMockTableExecutor(t)
	messenger := &mockProcessorMessenger{}
	agent := NewBaseAgent("test-cf", executor, messenger)
	messenger.On("SyncTaskStatuses", mock.Anything, []model.TableID(nil), []model.TableID(nil), []model.TableID(nil)).
		Return(true, nil)
	err := agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	agent.OnOwnerDispatchedTask("capture-1", 1, model.TableID(1), model.Ts(1000), false)
	executor.On("AddTable", mock.Anything, model.TableID(1), model.Ts(1000)).Return(nil)
	messenger.On("OnOwnerChanged", mock.Anything, "capture-1")

	err = agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	executor.ExpectedCalls = nil
	messenger.ExpectedCalls = nil
	executor.On("GetCheckpoint").Return(model.Ts(1002), model.Ts(1000))
	messenger.On("SendCheckpoint", mock.Anything, model.Ts(1002), model.Ts(1000)).Return(true, nil)
	// Stale owner
	agent.OnOwnerDispatchedTask("capture-2", 0, model.TableID(2), model.Ts(1000), false)

	err = agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)
}

func TestAgentCheckpointBarrier(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(false)
	oldCheckpointInterval := config.GetGlobalServerConfig().SchedulerV2.ProcessorCheckpointInterval
	config.GetGlobalServerConfig().SchedulerV2.ProcessorCheckpointInterval = 0
	defer func() {
		config.GetGlobalServerConfig().SchedulerV2.ProcessorCheckpointInterval = oldCheckpointInterval
	}()

	executor := newMockTableExecutor(t)
	executor.running[model.TableID(1)] = struct{}{}
	executor.running[model.TableID(2)] = struct{}{}
	executor.running[model.TableID(3)] = struct{}{}

	messenger := &mockProcessorMessenger{}
	agent := NewBaseAgent("test-cf", executor, messenger)
	messenger.On("SyncTaskStatuses", mock.Anything, []model.TableID{1,2,3}, []model.TableID(nil), []model.TableID(nil)).
		Return(true, nil)
	executor.On("GetCheckpoint").Return(model.Ts(1000), model.Ts(1001))
	messenger.On("SendCheckpoint", mock.Anything, model.Ts(1000), model.Ts(1001)).Return(true, nil)

	err := agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	executor.ExpectedCalls = nil
	messenger.ExpectedCalls = nil
	executor.On("GetCheckpoint").Return(model.Ts(1001), model.Ts(1002))
	messenger.On("Barrier", mock.Anything).Return(false, nil)

	err = agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	lastCheckpointTs := agent.LastSentCheckpointTs()
	require.Equal(t, model.Ts(0), lastCheckpointTs)

	executor.ExpectedCalls = nil
	messenger.ExpectedCalls = nil
	executor.On("GetCheckpoint").Return(model.Ts(1002), model.Ts(1003))
	messenger.On("Barrier", mock.Anything).Return(true, nil)
	messenger.On("SendCheckpoint", mock.Anything, model.Ts(1002), model.Ts(1003)).Return(true, nil)

	err = agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	lastCheckpointTs = agent.LastSentCheckpointTs()
	require.Equal(t, model.Ts(1000), lastCheckpointTs)
}
