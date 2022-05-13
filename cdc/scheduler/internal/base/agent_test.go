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

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/base/protocol"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// read only
var agentConfigForTesting = &AgentConfig{SendCheckpointTsInterval: 0}

func TestAgentAddTable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	executor := NewMockTableExecutor(t)
	messenger := &MockProcessorMessenger{}
	agent := NewBaseAgent(model.DefaultChangeFeedID("test-cf"),
		executor, messenger, agentConfigForTesting)
	var epoch protocol.ProcessorEpoch
	messenger.On("SyncTaskStatuses", mock.Anything, mock.AnythingOfType("string"), []model.TableID(nil), []model.TableID(nil), []model.TableID(nil)).
		Return(true, nil).
		Run(func(args mock.Arguments) {
			epoch = args.String(1)
		})
	err := agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	executor.ExpectedCalls = nil
	messenger.ExpectedCalls = nil
	agent.OnOwnerDispatchedTask("capture-1", 1, model.TableID(1), model.Ts(1000), false, epoch)
	executor.On("AddTable", mock.Anything, model.TableID(1), model.Ts(1000)).Return(true, nil)
	messenger.On("OnOwnerChanged", mock.Anything, "capture-1", int64(1))

	err = agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	executor.ExpectedCalls = nil
	messenger.ExpectedCalls = nil
	delete(executor.Adding, model.TableID(1))
	executor.Running[model.TableID(1)] = struct{}{}
	executor.On("GetCheckpoint").Return(model.Ts(1002), model.Ts(1000))
	messenger.On("SendCheckpoint", mock.Anything, model.Ts(1002), model.Ts(1000)).Return(true, nil)
	messenger.On("FinishTableOperation", mock.Anything, model.TableID(1), epoch).Return(true, nil)

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	executor := NewMockTableExecutor(t)
	executor.Running[model.TableID(1)] = struct{}{}
	executor.Running[model.TableID(2)] = struct{}{}

	messenger := &MockProcessorMessenger{}
	agent := NewBaseAgent(model.DefaultChangeFeedID("test-cf"),
		executor, messenger, agentConfigForTesting)
	agent.OnOwnerAnnounce("capture-2", 1)

	var epoch protocol.ProcessorEpoch
	messenger.On("SyncTaskStatuses", mock.Anything, mock.AnythingOfType("string"), []model.TableID{1, 2}, []model.TableID(nil), []model.TableID(nil)).
		Return(true, nil).
		Run(func(args mock.Arguments) {
			epoch = args.String(1)
		})
	messenger.On("OnOwnerChanged", mock.Anything, "capture-2", int64(1))
	executor.On("GetCheckpoint").Return(model.Ts(1000), model.Ts(1000))
	messenger.On("SendCheckpoint", mock.Anything, model.Ts(1000), model.Ts(1000)).Return(true, nil)
	err := agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	executor.ExpectedCalls = nil
	messenger.ExpectedCalls = nil
	agent.OnOwnerDispatchedTask("capture-2", 1, model.TableID(1), model.Ts(1000), true, epoch)
	executor.On("GetCheckpoint").Return(model.Ts(1000), model.Ts(1000))
	messenger.On("SendCheckpoint", mock.Anything, model.Ts(1000), model.Ts(1000)).Return(true, nil)
	executor.On("RemoveTable", mock.Anything, model.TableID(1)).Return(true, nil)
	messenger.On("Barrier", mock.Anything).Return(true)
	err = agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	// Inject an owner change
	executor.ExpectedCalls = nil
	messenger.ExpectedCalls = nil
	executor.On("GetCheckpoint").Return(model.Ts(1000), model.Ts(1000))
	messenger.On("SyncTaskStatuses",
		mock.Anything,
		mock.AnythingOfType("string"),
		[]model.TableID{2},
		[]model.TableID(nil),
		[]model.TableID{1},
	).
		Return(true, nil).
		Run(func(args mock.Arguments) {
			epoch = args.String(1)
		})
	messenger.On("OnOwnerChanged", mock.Anything, "capture-3", int64(2))
	messenger.On("SendCheckpoint", mock.Anything, model.Ts(1000), model.Ts(1000)).Return(true, nil)
	messenger.On("Barrier", mock.Anything).Return(true)
	agent.OnOwnerAnnounce("capture-3", 2)
	err = agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	executor.ExpectedCalls = nil
	messenger.ExpectedCalls = nil
	delete(executor.Removing, model.TableID(1))
	executor.On("GetCheckpoint").Return(model.Ts(1002), model.Ts(1000))
	messenger.On("Barrier", mock.Anything).Return(true)
	messenger.On("FinishTableOperation", mock.Anything, model.TableID(1), epoch).Return(true, nil)
	messenger.On("SendCheckpoint", mock.Anything, model.Ts(1002), model.Ts(1000)).Return(true, nil)

	err = agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)
}

func TestAgentOwnerChangedWhileAddingTable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	executor := NewMockTableExecutor(t)
	messenger := &MockProcessorMessenger{}
	agent := NewBaseAgent(model.DefaultChangeFeedID("test-cf"),
		executor, messenger, agentConfigForTesting)

	var epoch protocol.ProcessorEpoch
	messenger.On("SyncTaskStatuses",
		mock.Anything,
		mock.AnythingOfType("string"),
		[]model.TableID(nil), []model.TableID(nil), []model.TableID(nil),
	).
		Return(true, nil).
		Run(func(args mock.Arguments) {
			epoch = args.String(1)
		})
	err := agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	agent.OnOwnerDispatchedTask("capture-1", 1, model.TableID(1), model.Ts(1000), false, epoch)
	executor.On("AddTable", mock.Anything, model.TableID(1), model.Ts(1000)).Return(true, nil)
	messenger.On("OnOwnerChanged", mock.Anything, "capture-1", int64(1))

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
	messenger.On("OnOwnerChanged", mock.Anything, "capture-2", int64(2))
	messenger.On(
		"SyncTaskStatuses",
		mock.Anything,
		mock.AnythingOfType("string"),
		[]model.TableID(nil), []model.TableID{1}, []model.TableID(nil),
	).
		Return(true, nil).
		Run(func(args mock.Arguments) {
			epoch = args.String(1)
		})
	messenger.On("Barrier", mock.Anything).Return(true)
	executor.On("GetCheckpoint").Return(model.Ts(1002), model.Ts(1000))
	messenger.On("SendCheckpoint", mock.Anything, model.Ts(1002), model.Ts(1000)).Return(true, nil)

	err = agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)
}

func TestAgentReceiveFromStaleOwner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	executor := NewMockTableExecutor(t)
	messenger := &MockProcessorMessenger{}
	agent := NewBaseAgent(model.DefaultChangeFeedID("test-cf"),
		executor, messenger, agentConfigForTesting)
	agent.checkpointSender = &mockCheckpointSender{}

	var epoch protocol.ProcessorEpoch
	messenger.On("SyncTaskStatuses", mock.Anything, mock.AnythingOfType("string"),
		[]model.TableID(nil), []model.TableID(nil), []model.TableID(nil)).
		Return(true, nil).Run(func(args mock.Arguments) {
		epoch = args.String(1)
	})
	err := agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	agent.OnOwnerDispatchedTask("capture-1", 1, model.TableID(1), model.Ts(1000), false, epoch)
	executor.On("AddTable", mock.Anything, model.TableID(1), model.Ts(1000)).Return(true, nil)
	messenger.On("OnOwnerChanged", mock.Anything, "capture-1", int64(1))

	err = agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	executor.ExpectedCalls = nil
	messenger.ExpectedCalls = nil
	executor.On("GetCheckpoint").Return(model.Ts(1002), model.Ts(1000))
	// Stale owner
	agent.OnOwnerDispatchedTask("capture-2", 0, model.TableID(2), model.Ts(1000), false, defaultEpoch)

	err = agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	executor.ExpectedCalls = nil
	messenger.ExpectedCalls = nil
	// Stale owner announce
	executor.On("GetCheckpoint").Return(model.Ts(1002), model.Ts(1000))
	agent.OnOwnerAnnounce("capture-2", 0)
	err = agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)
}

func TestOwnerMismatchShouldPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	executor := NewMockTableExecutor(t)
	messenger := &MockProcessorMessenger{}
	agent := NewBaseAgent(model.DefaultChangeFeedID("test-cf"),
		executor, messenger, agentConfigForTesting)
	agent.checkpointSender = &mockCheckpointSender{}
	messenger.On("SyncTaskStatuses", mock.Anything, mock.AnythingOfType("string"),
		[]model.TableID(nil), []model.TableID(nil), []model.TableID(nil)).
		Return(true, nil)
	err := agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	// capture-1 becomes owner with ownerRev == 1
	agent.OnOwnerAnnounce("capture-1", 1)
	messenger.On("OnOwnerChanged", mock.Anything, "capture-1", int64(1))

	err = agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	// capture-2 claims to be the owner with ownerRev == 1
	require.Panics(t, func() {
		agent.OnOwnerAnnounce("capture-2", 1)
	}, "should have panicked")
}

func TestIgnoreStaleEpoch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	executor := NewMockTableExecutor(t)
	messenger := &MockProcessorMessenger{}
	agent := NewBaseAgent(model.DefaultChangeFeedID("test-cf"),
		executor, messenger, agentConfigForTesting)
	agent.checkpointSender = &mockCheckpointSender{}

	var epoch, newEpoch protocol.ProcessorEpoch
	messenger.On("SyncTaskStatuses", mock.Anything, mock.AnythingOfType("string"),
		[]model.TableID(nil), []model.TableID(nil), []model.TableID(nil)).
		Return(true, nil).Run(func(args mock.Arguments) {
		epoch = args.String(1)
	})

	err := agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	agent.OnOwnerAnnounce("capture-1", 1)
	messenger.On("OnOwnerChanged", mock.Anything, "capture-1", int64(1))

	err = agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	messenger.ExpectedCalls = nil
	messenger.On("OnOwnerChanged", mock.Anything, "capture-1", int64(1))
	messenger.On("SyncTaskStatuses", mock.Anything, mock.AnythingOfType("string"),
		[]model.TableID(nil), []model.TableID(nil), []model.TableID(nil)).
		Return(true, nil).Run(func(args mock.Arguments) {
		newEpoch = args.String(1)
	})
	agent.OnOwnerAnnounce("capture-1", 1)

	err = agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)

	require.NotEqual(t, epoch, newEpoch)
	agent.OnOwnerDispatchedTask("capture-1", 1, model.TableID(2), model.Ts(1000), false, epoch)

	err = agent.Tick(ctx)
	require.NoError(t, err)
	messenger.AssertExpectations(t)
	executor.AssertNotCalled(t, "AddTable", mock.Anything, model.TableID(1), model.Ts(1000))
}
