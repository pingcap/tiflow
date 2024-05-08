// Copyright 2024 PingCAP, Inc.
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

package async

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/vars"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

type fakePool struct {
	f           func()
	submitErr   error
	submitTimes int
}

func (f *fakePool) Go(_ context.Context, fn func()) error {
	f.f = fn
	f.submitTimes++
	return f.submitErr
}

func (f *fakePool) Run(_ context.Context) error {
	return nil
}

func TestTryInitialize(t *testing.T) {
	initializer := NewInitializer()
	pool := &vars.NonAsyncPool{}
	initialized, err := initializer.TryInitialize(context.Background(),
		func(ctx context.Context) error {
			return nil
		}, pool)
	require.Nil(t, err)
	require.True(t, initialized)
	// Try to initialize again
	initialized, err = initializer.TryInitialize(context.Background(), func(ctx context.Context) error {
		return nil
	}, pool)
	require.Nil(t, err)
	require.True(t, initialized)
	// init failed
	initializer = NewInitializer()
	initialized, err = initializer.TryInitialize(context.Background(), func(ctx context.Context) error {
		return errors.New("failed to init")
	}, pool)
	require.NotNil(t, err)
	require.False(t, initializer.initialized.Load())
	require.True(t, initializer.initializing.Load())
	require.False(t, initialized)
	initialized, err = initializer.TryInitialize(context.Background(), func(ctx context.Context) error {
		return errors.New("failed to init")
	}, pool)
	require.NotNil(t, err)
	require.False(t, initializer.initialized.Load())
	require.True(t, initializer.initializing.Load())
	require.False(t, initialized)

	// test submit error
	initializer = NewInitializer()
	initialized, err = initializer.TryInitialize(context.Background(), func(ctx context.Context) error {
		return nil
	}, &fakePool{submitErr: errors.New("submit error")})
	require.NotNil(t, err)
	require.False(t, initialized)
	require.False(t, initializer.initialized.Load())
	require.True(t, initializer.initializing.Load())
}

func TestTerminate(t *testing.T) {
	initializer := NewInitializer()
	pool := &vars.NonAsyncPool{}
	initialized, err := initializer.TryInitialize(context.Background(), func(ctx context.Context) error {
		return nil
	}, pool)
	require.Nil(t, err)
	require.True(t, initialized)
	initializer.Terminate()
	require.False(t, initializer.initialized.Load())
	require.False(t, initializer.initializing.Load())

	// test submit error
	initializer = NewInitializer()
	fpool := &fakePool{}
	initialized, err = initializer.TryInitialize(context.Background(), func(ctx context.Context) error {
		return nil
	}, fpool)
	require.Nil(t, err)
	require.False(t, initialized)
	require.True(t, initializer.initializing.Load())
	require.Equal(t, 1, fpool.submitTimes)

	initialized, err = initializer.TryInitialize(context.Background(), func(ctx context.Context) error {
		return nil
	}, fpool)
	require.Nil(t, err)
	require.False(t, initialized)
	require.True(t, initializer.initializing.Load())
	require.Equal(t, 1, fpool.submitTimes)

	wg := sync.WaitGroup{}
	wg.Add(1)
	terminated := atomic.NewInt32(1)
	go func() {
		defer wg.Done()
		initializer.Terminate()
		require.Equal(t, int32(2), terminated.Swap(3))
	}()
	require.Equal(t, int32(1), terminated.Swap(2))
	time.Sleep(1 * time.Second)
	fpool.f()
	wg.Wait()
	require.Equal(t, int32(3), terminated.Load())
}
