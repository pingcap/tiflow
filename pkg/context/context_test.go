// Copyright 2020 PingCAP, Inc.
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

package context

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestVars(t *testing.T) {
	t.Parallel()
	stdCtx := context.Background()
	conf := config.GetDefaultReplicaConfig()
	conf.Filter.Rules = []string{"hello.world"}
	info := &model.ChangeFeedInfo{Config: conf}
	ctx := NewContext(stdCtx, &GlobalVars{
		CaptureInfo: &model.CaptureInfo{ID: "capture1"},
	})
	ctx = WithChangefeedVars(ctx, &ChangefeedVars{
		Info: info,
	})
	require.Equal(t, ctx.ChangefeedVars().Info, info)
	require.Equal(t, ctx.GlobalVars().CaptureInfo.ID, "capture1")
}

func TestStdCancel(t *testing.T) {
	t.Parallel()
	stdCtx := context.Background()
	stdCtx, cancel := context.WithCancel(stdCtx)
	ctx := NewContext(stdCtx, &GlobalVars{})
	cancel()
	<-ctx.Done()
}

func TestCancel(t *testing.T) {
	t.Parallel()
	stdCtx := context.Background()
	ctx := NewContext(stdCtx, &GlobalVars{})
	ctx, cancel := WithCancel(ctx)
	cancel()
	<-ctx.Done()
}

func TestCancelCascade(t *testing.T) {
	t.Parallel()
	startTime := time.Now()
	stdCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(1*time.Second))
	ctx := NewContext(stdCtx, &GlobalVars{})
	ctx1, _ := WithCancel(ctx)
	ctx2, cancel2 := WithCancel(ctx)
	cancel2()
	<-ctx2.Done()
	require.Less(t, time.Since(startTime), time.Second)
	<-ctx1.Done()
	require.GreaterOrEqual(t, time.Since(startTime), time.Second)
	cancel()
}

func TestThrow(t *testing.T) {
	t.Parallel()
	stdCtx := context.Background()
	ctx := NewContext(stdCtx, &GlobalVars{})
	ctx, cancel := WithCancel(ctx)
	ctx = WithErrorHandler(ctx, func(err error) error {
		require.Equal(t, err.Error(), "mock error")
		cancel()
		return nil
	})
	ctx.Throw(nil)
	ctx.Throw(errors.New("mock error"))
	<-ctx.Done()
}

func TestThrowCascade(t *testing.T) {
	t.Parallel()
	stdCtx := context.Background()
	ctx := NewContext(stdCtx, &GlobalVars{})
	ctx, cancel := WithCancel(ctx)
	var errNum1, errNum2, errNum3 int
	ctx = WithErrorHandler(ctx, func(err error) error {
		if err.Error() == "mock error" {
			errNum1++
		} else if err.Error() == "mock error2" {
			errNum2++
		} else {
			require.Fail(t, "TestThrowCascade fail")
		}
		return nil
	})
	ctx2 := WithErrorHandler(ctx, func(err error) error {
		if err.Error() == "mock error2" {
			errNum2++
			return err
		} else if err.Error() == "mock error3" {
			errNum3++
		} else {
			require.Fail(t, "TestThrowCascade fail")
		}
		return nil
	})
	ctx2.Throw(errors.New("mock error2"))
	ctx2.Throw(errors.New("mock error3"))
	ctx.Throw(errors.New("mock error"))
	require.Equal(t, errNum1, 1)
	require.Equal(t, errNum2, 2)
	require.Equal(t, errNum3, 1)
	cancel()
	<-ctx.Done()
}

func TestThrowPanic(t *testing.T) {
	t.Parallel()
	defer func() {
		panicMsg := recover()
		require.Equal(t, panicMsg, "an error has escaped, please report a bug")
	}()
	stdCtx := context.Background()
	ctx := NewContext(stdCtx, &GlobalVars{})
	ctx.Throw(nil)
	ctx.Throw(errors.New("mock error"))
}
