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
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/context"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	defaultCheckpointIntervalForTesting = time.Second * 1
)

func TestCheckpointTsSenderBasics(t *testing.T) {
	ctx := context.NewBackendContext4Test(false)
	mockCommunicator := &MockProcessorMessenger{}
	sender := newCheckpointSender(mockCommunicator, zap.L(), defaultCheckpointIntervalForTesting)
	mockClock := clock.NewMock()
	sender.(*checkpointTsSenderImpl).clock = mockClock

	startTime := time.Now()
	mockClock.Set(startTime)

	// Test 1: SendCheckpoint returns false (message client could be congested)
	mockCommunicator.On("SendCheckpoint", mock.Anything, model.Ts(1000), model.Ts(1100)).
		Return(false, nil)
	err := sender.SendCheckpoint(ctx, func() (checkpointTs, resolvedTs model.Ts, ok bool) {
		return 1000, 1100, true
	})
	require.NoError(t, err)
	require.Equal(t, model.Ts(0), sender.LastSentCheckpointTs())
	mockCommunicator.AssertExpectations(t)

	// Test 2: SendCheckpoint returns true (message sent successfully)
	mockCommunicator.Calls = nil
	mockCommunicator.ExpectedCalls = nil
	mockCommunicator.On("SendCheckpoint", mock.Anything, model.Ts(1100), model.Ts(1200)).
		Return(true, nil)
	err = sender.SendCheckpoint(ctx, func() (checkpointTs, resolvedTs model.Ts, ok bool) {
		return 1100, 1200, true
	})
	require.NoError(t, err)
	require.Equal(t, model.Ts(0), sender.LastSentCheckpointTs())
	mockCommunicator.AssertExpectations(t)

	// Test 3: Barrier returns false (message still en route)
	mockCommunicator.Calls = nil
	mockCommunicator.ExpectedCalls = nil
	mockCommunicator.On("Barrier", mock.Anything).Return(false, nil)
	err = sender.SendCheckpoint(ctx, func() (checkpointTs, resolvedTs model.Ts, ok bool) {
		return 0, 0, false // this false here should not have mattered
	})
	require.NoError(t, err)
	require.Equal(t, model.Ts(0), sender.LastSentCheckpointTs())
	mockCommunicator.AssertExpectations(t)

	// Test 4: Barrier returns true (message sent successfully)
	mockCommunicator.Calls = nil
	mockCommunicator.ExpectedCalls = nil
	mockCommunicator.On("Barrier", mock.Anything).Return(true, nil)
	err = sender.SendCheckpoint(ctx, func() (checkpointTs, resolvedTs model.Ts, ok bool) {
		return 0, 0, false // this false here should not have mattered
	})
	require.NoError(t, err)
	require.Equal(t, model.Ts(1100), sender.LastSentCheckpointTs())
	mockCommunicator.AssertExpectations(t)

	// Test 5: No checkpoint is sent before the interval has elapsed.
	mockCommunicator.Calls = nil
	mockCommunicator.ExpectedCalls = nil
	err = sender.SendCheckpoint(ctx, func() (checkpointTs, resolvedTs model.Ts, ok bool) {
		return 2000, 2100, true
	})
	require.NoError(t, err)
	require.Equal(t, model.Ts(1100), sender.LastSentCheckpointTs())
	mockCommunicator.AssertExpectations(t)

	// Test 6: A new checkpoint is sent after the interval has elapsed.
	mockCommunicator.Calls = nil
	mockCommunicator.ExpectedCalls = nil
	mockClock.Add(2 * defaultCheckpointIntervalForTesting)
	mockCommunicator.On("SendCheckpoint", mock.Anything, model.Ts(2200), model.Ts(2300)).
		Return(true, nil)
	err = sender.SendCheckpoint(ctx, func() (checkpointTs, resolvedTs model.Ts, ok bool) {
		return 2200, 2300, true
	})
	require.NoError(t, err)
	require.Equal(t, model.Ts(1100), sender.LastSentCheckpointTs())
	mockCommunicator.AssertExpectations(t)
}
