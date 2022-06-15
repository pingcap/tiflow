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

package worker

import (
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/pingcap/tiflow/engine/pkg/errctx"
	derrors "github.com/pingcap/tiflow/engine/pkg/errors"
)

func TestExitControllerHappyPath(t *testing.T) {
	t.Parallel()

	masterCli := NewMockMasterInfoProvider("master-1", "executor-1", 1)
	errc := errctx.NewErrCenter()
	clk := clock.NewMock()

	var cbCalled atomic.Bool
	ec := NewExitController(
		masterCli,
		errc,
		WithClock(clk),
		WithPrepareExitFunc(func() {
			require.False(t, cbCalled.Swap(true))
		}))

	err := ec.PollExit()
	require.NoError(t, err)

	// Sets an error with the ErrCenter.
	errc.OnError(errors.New("injected error"))

	err = ec.PollExit()
	require.Error(t, err)
	require.Regexp(t, ".*ErrWorkerHalfExit.*", err)

	require.True(t, ec.IsExiting())

	err = ec.PollExit()
	require.Error(t, err)
	require.Regexp(t, ".*ErrWorkerHalfExit.*", err)

	// simulates master's acknowledgement.
	masterCli.SetMasterClosed()
	err = ec.PollExit()
	require.Error(t, err)
	require.Regexp(t, ".*injected error.*", err)

	require.True(t, cbCalled.Load())
}

func TestExitControllerMasterTimesOut(t *testing.T) {
	t.Parallel()

	masterCli := NewMockMasterInfoProvider("master-1", "executor-1", 1)
	errc := errctx.NewErrCenter()
	clk := clock.NewMock()

	ec := NewExitController(masterCli, errc, WithClock(clk))

	// Sets an error with the ErrCenter.
	errc.OnError(errors.New("injected error"))

	err := ec.PollExit()
	require.Error(t, err)
	require.Regexp(t, ".*ErrWorkerHalfExit.*", err)

	require.True(t, ec.IsExiting())

	err = ec.PollExit()
	require.Error(t, err)
	require.Regexp(t, ".*ErrWorkerHalfExit.*", err)

	// simulates a timeout
	clk.Add(1 * time.Hour)
	err = ec.PollExit()
	require.Error(t, err)
	require.Regexp(t, ".*injected error.*", err)
}

func TestExitControllerForceExit(t *testing.T) {
	t.Parallel()

	masterCli := NewMockMasterInfoProvider("master-1", "executor-1", 1)
	errc := errctx.NewErrCenter()
	clk := clock.NewMock()

	ec := NewExitController(masterCli, errc, WithClock(clk))

	// Sets an error with the ErrCenter.
	errc.OnError(errors.New("injected error"))

	ec.ForceExit(derrors.ErrWorkerSuicide.GenWithStackByArgs())

	err := ec.PollExit()
	require.Error(t, err)
	require.Regexp(t, ".*injected error.*", err)
}
