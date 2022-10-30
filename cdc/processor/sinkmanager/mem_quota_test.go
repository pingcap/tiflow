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

package sinkmanager

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestMemQuotaTryAcquire(t *testing.T) {
	t.Parallel()

	m := newMemQuota(model.DefaultChangeFeedID("1"), 100)
	require.True(t, m.tryAcquire(50))
	require.True(t, m.tryAcquire(50))
	require.False(t, m.tryAcquire(1))
}

func TestMemQuotaRefund(t *testing.T) {
	t.Parallel()

	m := newMemQuota(model.DefaultChangeFeedID("1"), 100)
	require.True(t, m.tryAcquire(50))
	require.True(t, m.tryAcquire(50))
	m.refund(50)
	require.True(t, m.tryAcquire(1))
	require.True(t, m.tryAcquire(49))
}

func TestMemQuotaHasAvailable(t *testing.T) {
	t.Parallel()

	m := newMemQuota(model.DefaultChangeFeedID("1"), 100)
	require.True(t, m.hasAvailable(100))
	require.True(t, m.tryAcquire(100))
	require.False(t, m.hasAvailable(1))
}

func TestMemQuotaRecordAndRelease(t *testing.T) {
	t.Parallel()

	m := newMemQuota(model.DefaultChangeFeedID("1"), 300)
	require.True(t, m.tryAcquire(100))
	m.record(1, model.NewResolvedTs(100), 100)
	require.True(t, m.tryAcquire(100))
	m.record(1, model.NewResolvedTs(200), 100)
	require.True(t, m.tryAcquire(100))
	m.record(1, model.NewResolvedTs(300), 100)
	require.False(t, m.tryAcquire(1))
	require.False(t, m.hasAvailable(1))
	// release the memory of resolvedTs 100
	m.release(1, model.NewResolvedTs(101))
	require.True(t, m.hasAvailable(100))
	// release the memory of resolvedTs 200
	m.release(1, model.NewResolvedTs(201))
	require.True(t, m.hasAvailable(200))
	// release the memory of resolvedTs 300
	m.release(1, model.NewResolvedTs(301))
	require.True(t, m.hasAvailable(300))
	// release the memory of resolvedTs 300 again
	m.release(1, model.NewResolvedTs(301))
	require.True(t, m.hasAvailable(300))
}

func TestMemQuotaRecordAndReleaseWithBatchID(t *testing.T) {
	t.Parallel()

	m := newMemQuota(model.DefaultChangeFeedID("1"), 300)
	require.True(t, m.tryAcquire(100))
	resolvedTs := model.ResolvedTs{
		Mode:    model.BatchResolvedMode,
		Ts:      100,
		BatchID: 1,
	}
	m.record(1, resolvedTs, 100)
	resolvedTs = model.ResolvedTs{
		Mode:    model.BatchResolvedMode,
		Ts:      100,
		BatchID: 2,
	}
	require.True(t, m.tryAcquire(100))
	m.record(1, resolvedTs, 100)
	resolvedTs = model.ResolvedTs{
		Mode:    model.BatchResolvedMode,
		Ts:      100,
		BatchID: 3,
	}
	require.True(t, m.tryAcquire(100))
	m.record(1, resolvedTs, 100)
	require.False(t, m.tryAcquire(1))
	require.False(t, m.hasAvailable(1))

	// release the memory of resolvedTs 100 batchID 2
	resolvedTs = model.ResolvedTs{
		Mode:    model.BatchResolvedMode,
		Ts:      100,
		BatchID: 2,
	}
	m.release(1, resolvedTs)
	require.True(t, m.hasAvailable(200))
	// release the memory of resolvedTs 101
	m.release(1, model.NewResolvedTs(101))
	require.True(t, m.hasAvailable(300))
}
