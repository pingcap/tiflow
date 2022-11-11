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

package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tiflow/pkg/errors"
	p2pImpl "github.com/pingcap/tiflow/pkg/p2p"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// handlerRegistrar must be implemented by MessageServer
var _ handlerRegistrar = (*p2pImpl.MessageServer)(nil)

type mockHandlerRegistrar struct {
	mock.Mock
}

func (r *mockHandlerRegistrar) SyncAddHandler(
	ctx context.Context,
	topic Topic,
	information TypeInformation,
	handlerFunc HandlerFunc,
) (<-chan error, error) {
	args := r.Called(ctx, topic, information, handlerFunc)
	return args.Get(0).(<-chan error), args.Error(1)
}

func (r *mockHandlerRegistrar) SyncRemoveHandler(ctx context.Context, topic Topic) error {
	args := r.Called(ctx, topic)
	return args.Error(0)
}

type msgContent struct{}

func TestMessageHandlerManagerBasics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	registrar := &mockHandlerRegistrar{}
	manager := newMessageHandlerManager(registrar)

	errCh1 := make(chan error, 1)
	registrar.On("SyncAddHandler", mock.Anything, "test-topic-1", &msgContent{}, mock.Anything).
		Return((<-chan error)(errCh1), nil)
	ok, err := manager.RegisterHandler(ctx, "test-topic-1", &msgContent{}, func(NodeID, MessageValue) error {
		// This function does not matter here
		return nil
	})
	require.NoError(t, err)
	require.True(t, ok)
	registrar.AssertExpectations(t)

	// Test duplicate handler
	ok, err = manager.RegisterHandler(ctx, "test-topic-1", &msgContent{}, func(NodeID, MessageValue) error {
		// This function does not matter here
		return nil
	})
	require.NoError(t, err)
	require.False(t, ok)

	errCh2 := make(chan error, 1)
	registrar.ExpectedCalls = nil
	registrar.On("SyncAddHandler", mock.Anything, "test-topic-2", &msgContent{}, mock.Anything).
		Return((<-chan error)(errCh2), nil)
	ok, err = manager.RegisterHandler(ctx, "test-topic-2", &msgContent{}, func(NodeID, MessageValue) error {
		// This function does not matter here
		return nil
	})
	require.NoError(t, err)
	require.True(t, ok)
	registrar.AssertExpectations(t)

	err = manager.CheckError(ctx)
	require.NoError(t, err)

	errCh1 <- errors.New("fake error")
	err = manager.CheckError(ctx)
	require.Error(t, err)

	registrar.ExpectedCalls = nil
	registrar.On("SyncRemoveHandler", mock.Anything, "test-topic-1").Return(nil)
	ok, err = manager.UnregisterHandler(ctx, "test-topic-1")
	require.NoError(t, err)
	require.True(t, ok)
	registrar.AssertExpectations(t)

	// duplicate unregister
	ok, err = manager.UnregisterHandler(ctx, "test-topic-1")
	require.NoError(t, err)
	require.False(t, ok)

	registrar.ExpectedCalls = nil
	registrar.On("SyncRemoveHandler", mock.Anything, "test-topic-2").Return(nil)
	err = manager.Clean(ctx)
	require.NoError(t, err)
}

func TestMessageHandlerManagerTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	registrar := &mockHandlerRegistrar{}
	manager := newMessageHandlerManager(registrar)
	manager.SetTimeout(time.Duration(0))

	registrar.On("SyncAddHandler", mock.Anything, "test-topic-1", &msgContent{}, mock.Anything).
		Return((<-chan error)(nil), errors.New("fake error")).
		Run(func(args mock.Arguments) {
			ctx := args.Get(0).(context.Context)
			select {
			case <-ctx.Done():
			default:
				require.Fail(t, "context should have been canceled")
			}
		})

	_, err := manager.RegisterHandler(ctx, "test-topic-1", &msgContent{}, func(NodeID, MessageValue) error {
		// This function does not matter here
		return nil
	})
	require.Error(t, err)
}
