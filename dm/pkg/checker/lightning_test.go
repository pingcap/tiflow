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

package checker

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/lightning/pkg/precheck"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

type mockPrecheckItem struct {
	err  error
	pass bool
	msg  string
}

func (m mockPrecheckItem) Check(ctx context.Context) (*precheck.CheckResult, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &precheck.CheckResult{
		Passed:  m.pass,
		Message: m.msg,
	}, nil
}

func (m mockPrecheckItem) GetCheckItemID() precheck.CheckItemID {
	return "mock"
}

// nolint:dupl
func TestEmptyRegionChecker(t *testing.T) {
	ctx := context.Background()
	c := &LightningEmptyRegionChecker{inner: mockPrecheckItem{pass: true}}
	result := c.Check(ctx)
	require.Equal(t, StateSuccess, result.State)
	require.Len(t, result.Errors, 0)
	require.Equal(t, "", result.Extra)

	c.inner = mockPrecheckItem{err: errors.New("mock error")}
	result = c.Check(ctx)
	require.Equal(t, StateFailure, result.State)
	require.Equal(t, "", result.Extra)
	require.Len(t, result.Errors, 1)
	require.Equal(t, "mock error", result.Errors[0].ShortErr)
	require.Equal(t, StateFailure, result.Errors[0].Severity)

	// lightning prechecker returns fail
	lightningMsg := "TiKV stores (1) contains more than 1000 empty regions respectively, which will greatly affect the import speed and success rate"
	c.inner = mockPrecheckItem{msg: lightningMsg}
	result = c.Check(ctx)
	require.Equal(t, StateWarning, result.State)
	require.Equal(t, "", result.Extra)
	require.Len(t, result.Errors, 1)
	require.Equal(t, lightningMsg, result.Errors[0].ShortErr)
	require.Equal(t, StateWarning, result.Errors[0].Severity)

	// lightning prechecker returns warning or success
	lightningMsg = "Cluster doesn't have too many empty regions"
	c.inner = mockPrecheckItem{msg: lightningMsg, pass: true}
	result = c.Check(ctx)
	require.Equal(t, StateSuccess, result.State)
	require.Equal(t, "", result.Extra)
	require.Len(t, result.Errors, 0)
}

// nolint:dupl
func TestRegionUnbalanced(t *testing.T) {
	ctx := context.Background()
	c := &LightningRegionDistributionChecker{inner: mockPrecheckItem{pass: true}}
	result := c.Check(ctx)
	require.Equal(t, StateSuccess, result.State)
	require.Len(t, result.Errors, 0)
	require.Equal(t, "", result.Extra)

	c.inner = mockPrecheckItem{err: errors.New("mock error")}
	result = c.Check(ctx)
	require.Equal(t, StateFailure, result.State)
	require.Equal(t, "", result.Extra)
	require.Len(t, result.Errors, 1)
	require.Equal(t, "mock error", result.Errors[0].ShortErr)
	require.Equal(t, StateFailure, result.Errors[0].Severity)

	// lightning prechecker returns fail
	lightningMsg := "Region distribution is unbalanced, the ratio of the regions count of the store(2) with least regions(500) to the store(1) with most regions(5000) is 0.1, but we expect it must not be less than 0.5"
	c.inner = mockPrecheckItem{msg: lightningMsg}
	result = c.Check(ctx)
	require.Equal(t, StateWarning, result.State)
	require.Equal(t, "", result.Extra)
	require.Len(t, result.Errors, 1)
	require.Equal(t, lightningMsg, result.Errors[0].ShortErr)
	require.Equal(t, StateWarning, result.Errors[0].Severity)

	// lightning prechecker returns warning or success
	lightningMsg = "Cluster region distribution is balanced"
	c.inner = mockPrecheckItem{msg: lightningMsg, pass: true}
	result = c.Check(ctx)
	require.Equal(t, StateSuccess, result.State)
	require.Equal(t, "", result.Extra)
	require.Len(t, result.Errors, 0)
}

func TestClusterVersion(t *testing.T) {
	ctx := context.Background()
	c := &LightningClusterVersionChecker{inner: mockPrecheckItem{pass: true}}
	result := c.Check(ctx)
	require.Equal(t, StateSuccess, result.State)
	require.Len(t, result.Errors, 0)
	require.Equal(t, "", result.Extra)

	c.inner = mockPrecheckItem{err: errors.New("mock error")}
	result = c.Check(ctx)
	require.Equal(t, StateFailure, result.State)
	require.Equal(t, "", result.Extra)
	require.Len(t, result.Errors, 1)
	require.Equal(t, "mock error", result.Errors[0].ShortErr)
	require.Equal(t, StateFailure, result.Errors[0].Severity)

	lightningMsg := "Cluster version check failed: TiNB version too old, required to be in [2.3.5, 3.0.0), found '2.1.0': [BR:Common:ErrVersionMismatch]version mismatch"
	c.inner = mockPrecheckItem{msg: lightningMsg}
	result = c.Check(ctx)
	require.Equal(t, StateFailure, result.State)
	require.Equal(t, "", result.Extra)
	require.Len(t, result.Errors, 1)
	require.Equal(t, lightningMsg, result.Errors[0].ShortErr)
	require.Equal(t, StateFailure, result.Errors[0].Severity)
}

func TestTableEmpty(t *testing.T) {
	ctx := context.Background()
	c := &LightningTableEmptyChecker{inner: mockPrecheckItem{pass: true}}
	result := c.Check(ctx)
	require.Equal(t, StateSuccess, result.State)
	require.Len(t, result.Errors, 0)
	require.Equal(t, "", result.Extra)

	c.inner = mockPrecheckItem{err: errors.New("mock error")}
	result = c.Check(ctx)
	require.Equal(t, StateFailure, result.State)
	require.Equal(t, "", result.Extra)
	require.Len(t, result.Errors, 1)
	require.Equal(t, "mock error", result.Errors[0].ShortErr)
	require.Equal(t, StateFailure, result.Errors[0].Severity)

	lightningMsg := "table(s) [test.t1] are not empty"
	c.inner = mockPrecheckItem{msg: lightningMsg}
	result = c.Check(ctx)
	require.Equal(t, StateFailure, result.State)
	require.Equal(t, "", result.Extra)
	require.Len(t, result.Errors, 1)
	require.Equal(t, lightningMsg, result.Errors[0].ShortErr)
	require.Equal(t, StateFailure, result.Errors[0].Severity)
}
