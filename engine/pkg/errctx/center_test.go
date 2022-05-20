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

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestErrCenterMultipleErrCtx(t *testing.T) {
	center := NewErrCenter()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			ctx := center.WithCancelOnFirstError(context.Background())
			<-ctx.Done()
			require.Error(t, ctx.Err())
			require.Regexp(t, ".*fake error.*", ctx.Err())
		}()
	}

	center.OnError(errors.New("fake error"))
	wg.Wait()

	require.Error(t, center.CheckError())
	require.Regexp(t, ".*fake error.*", center.CheckError())
}

func TestErrCenterSingleErrCtx(t *testing.T) {
	center := NewErrCenter()
	ctx := center.WithCancelOnFirstError(context.Background())

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			<-ctx.Done()
			require.Error(t, ctx.Err())
			require.Regexp(t, ".*fake error.*", ctx.Err())
		}()
	}

	center.OnError(errors.New("fake error"))
	wg.Wait()

	require.Error(t, center.CheckError())
	require.Regexp(t, ".*fake error.*", center.CheckError())
}

func TestErrCenterRepeatedErrors(t *testing.T) {
	center := NewErrCenter()
	ctx := center.WithCancelOnFirstError(context.Background())

	center.OnError(nil) // no-op
	center.OnError(errors.New("first error"))
	center.OnError(errors.New("second error")) // no-op

	<-ctx.Done()
	require.Error(t, ctx.Err())
	require.Regexp(t, ".*first error.*", ctx.Err())

	require.Error(t, center.CheckError())
	require.Regexp(t, ".*first error.*", center.CheckError())
}

func TestErrCtxPropagate(t *testing.T) {
	center := NewErrCenter()
	parentCtx, cancelParent := context.WithCancel(context.Background())
	ctx := center.WithCancelOnFirstError(parentCtx)

	cancelParent()
	<-ctx.Done()
	require.Error(t, ctx.Err())
	require.Regexp(t, ".*context canceled.*", ctx.Err())
}

func TestErrCtxDoneChSame(t *testing.T) {
	center := NewErrCenter()
	ctx := center.WithCancelOnFirstError(context.Background())

	doneCh1 := ctx.Done()
	doneCh2 := ctx.Done()

	require.Equal(t, doneCh1, doneCh2)
}
