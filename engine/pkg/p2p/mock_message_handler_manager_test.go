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
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

var _ MessageHandlerManager = (*MockMessageHandlerManager)(nil)

func TestMockMessageHandlerManager(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	manager := NewMockMessageHandlerManager()

	called1 := atomic.NewBool(false)
	ok, err := manager.RegisterHandler(ctx, "topic-1", &msgForTesting{}, func(sender NodeID, value MessageValue) error {
		require.Equal(t, "node-1", sender)
		require.Equal(t, &msgForTesting{1}, value)
		require.False(t, called1.Swap(true))
		return nil
	})
	require.NoError(t, err)
	require.True(t, ok)

	called2 := atomic.NewBool(false)
	ok, err = manager.RegisterHandler(ctx, "topic-2", &msgForTesting{}, func(sender NodeID, value MessageValue) error {
		require.Equal(t, "node-2", sender)
		require.Equal(t, &msgForTesting{2}, value)
		require.False(t, called2.Swap(true))
		return nil
	})
	require.NoError(t, err)
	require.True(t, ok)

	manager.AssertHasHandler(t, "topic-1", &msgForTesting{})
	manager.AssertHasHandler(t, "topic-2", &msgForTesting{})

	err = manager.InvokeHandler(t, "topic-1", "node-1", &msgForTesting{1})
	require.NoError(t, err)
	require.True(t, called1.Load())

	err = manager.InvokeHandler(t, "topic-2", "node-2", &msgForTesting{2})
	require.NoError(t, err)
	require.True(t, called2.Load())

	ok, err = manager.UnregisterHandler(ctx, "topic-2")
	require.True(t, ok)
	require.NoError(t, err)
	manager.AssertNoHandler(t, "topic-2")

	err = manager.CheckError(ctx)
	require.NoError(t, err)

	manager.InjectError(errors.New("fake error"))
	err = manager.CheckError(ctx)
	require.Error(t, err)
	require.Regexp(t, "fake error", err.Error())

	err = manager.CheckError(ctx)
	require.NoError(t, err)

	err = manager.Clean(ctx)
	require.NoError(t, err)

	manager.AssertNoHandler(t, "topic-1")
}
