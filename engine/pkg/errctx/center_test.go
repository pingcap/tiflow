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

package errctx

import (
	"context"
	"sync"
	"testing"

	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestErrCenterMultipleErrCtx(t *testing.T) {
	center := NewErrCenter()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			ctx, cancel := center.WithCancelOnFirstError(context.Background())
			defer cancel()
			<-ctx.Done()
			require.Error(t, ctx.Err())
			require.EqualError(t, ctx.Err(), "fake error")
		}()
	}

	center.OnError(errors.New("fake error"))
	wg.Wait()

	require.Error(t, center.CheckError())
	require.EqualError(t, center.CheckError(), "fake error")
	require.Empty(t, center.children)
}

func TestErrCenterSingleErrCtx(t *testing.T) {
	center := NewErrCenter()
	ctx, cancel := center.WithCancelOnFirstError(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			<-ctx.Done()
			require.Error(t, ctx.Err())
			require.EqualError(t, ctx.Err(), "fake error")
		}()
	}

	center.OnError(errors.New("fake error"))
	wg.Wait()

	require.Error(t, center.CheckError())
	require.EqualError(t, center.CheckError(), "fake error")
	require.Empty(t, center.children)
}

func TestErrCenterRepeatedErrors(t *testing.T) {
	center := NewErrCenter()
	ctx, cancel := center.WithCancelOnFirstError(context.Background())
	defer cancel()

	center.OnError(nil) // no-op
	center.OnError(errors.New("first error"))
	center.OnError(errors.New("second error")) // no-op

	<-ctx.Done()
	require.Error(t, ctx.Err())
	require.EqualError(t, ctx.Err(), "first error")

	require.Error(t, center.CheckError())
	require.EqualError(t, center.CheckError(), "first error")
	require.Empty(t, center.children)
}

func TestErrCtxPropagate(t *testing.T) {
	center := NewErrCenter()
	parentCtx, cancelParent := context.WithCancel(context.Background())
	ctx, cancel := center.WithCancelOnFirstError(parentCtx)
	defer cancel()

	cancelParent()
	center.OnError(errors.New("fake error"))
	<-ctx.Done()
	require.Error(t, ctx.Err())
	require.EqualError(t, ctx.Err(), "context canceled")
	require.Empty(t, center.children)
}

func TestErrCtxDoneChSame(t *testing.T) {
	center := NewErrCenter()
	ctx, cancel := center.WithCancelOnFirstError(context.Background())
	cancel()

	doneCh1 := ctx.Done()
	doneCh2 := ctx.Done()

	require.Equal(t, doneCh1, doneCh2)
}

func TestErrCtxManualCancel(t *testing.T) {
	center := NewErrCenter()
	ctx, cancel := center.WithCancelOnFirstError(context.Background())
	cancel()

	<-ctx.Done()
	require.Error(t, ctx.Err())
	require.EqualError(t, ctx.Err(), "context canceled")
	require.Empty(t, center.children)

	// Multiple cancels should not panic.
	cancel()
	cancel()
}
