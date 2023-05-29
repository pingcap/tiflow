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

package memquota

import (
	"context"
	"sync"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestMemQuotaTryAcquire(t *testing.T) {
	t.Parallel()

	m := NewMemQuota(model.DefaultChangeFeedID("1"), 100, "")
	defer m.Close()

	require.True(t, m.TryAcquire(50))
	require.True(t, m.TryAcquire(50))
	require.False(t, m.TryAcquire(1))
}

func TestMemQuotaForceAcquire(t *testing.T) {
	t.Parallel()

	m := NewMemQuota(model.DefaultChangeFeedID("1"), 100, "")
	defer m.Close()

	require.True(t, m.TryAcquire(100))
	require.False(t, m.TryAcquire(1))
	m.ForceAcquire(1)
	require.Equal(t, uint64(101), m.GetUsedBytes())
}

func TestMemQuotaBlockAcquire(t *testing.T) {
	t.Parallel()

	m := NewMemQuota(model.DefaultChangeFeedID("1"), 100, "")
	defer m.Close()
	err := m.BlockAcquire(100)
	require.NoError(t, err)
	m.Record(1, model.NewResolvedTs(1), 50)
	m.Record(1, model.NewResolvedTs(2), 50)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := m.BlockAcquire(50)
		require.NoError(t, err)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := m.BlockAcquire(50)
		require.NoError(t, err)
	}()
	m.Release(1, model.NewResolvedTs(1))
	m.Release(1, model.NewResolvedTs(2))
	wg.Wait()
}

func TestMemQuotaClose(t *testing.T) {
	t.Parallel()

	m := NewMemQuota(model.DefaultChangeFeedID("1"), 100, "")

	err := m.BlockAcquire(100)
	require.NoError(t, err)
	m.Record(1, model.NewResolvedTs(2), 100)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := m.BlockAcquire(50)
		if err != nil {
			require.ErrorIs(t, err, context.Canceled)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := m.BlockAcquire(50)
		if err != nil {
			require.ErrorIs(t, err, context.Canceled)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := m.BlockAcquire(50)
		if err != nil {
			require.ErrorIs(t, err, context.Canceled)
		}
	}()

	// Randomly release some memory.
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.Release(1, model.NewResolvedTs(2))
	}()
	m.Close()
	wg.Wait()
}

func TestMemQuotaRefund(t *testing.T) {
	t.Parallel()

	m := NewMemQuota(model.DefaultChangeFeedID("1"), 100, "")
	defer m.Close()

	require.True(t, m.TryAcquire(50))
	require.True(t, m.TryAcquire(50))
	m.Refund(50)
	require.True(t, m.TryAcquire(1))
	require.True(t, m.TryAcquire(49))

	// Test notify the blocked acquire.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := m.BlockAcquire(50)
		require.NoError(t, err)
	}()
	m.Refund(50)
	wg.Wait()
}

func TestMemQuotaHasAvailable(t *testing.T) {
	t.Parallel()

	m := NewMemQuota(model.DefaultChangeFeedID("1"), 100, "")
	defer m.Close()

	require.True(t, m.hasAvailable(100))
	require.True(t, m.TryAcquire(100))
	require.False(t, m.hasAvailable(1))
}

func TestMemQuotaRecordAndRelease(t *testing.T) {
	t.Parallel()

	m := NewMemQuota(model.DefaultChangeFeedID("1"), 300, "")
	defer m.Close()
	m.AddTable(1)

	require.True(t, m.TryAcquire(100))
	m.Record(1, model.NewResolvedTs(100), 100)
	require.True(t, m.TryAcquire(100))
	m.Record(1, model.NewResolvedTs(200), 100)
	require.True(t, m.TryAcquire(100))
	m.Record(1, model.NewResolvedTs(300), 100)
	require.False(t, m.TryAcquire(1))
	require.False(t, m.hasAvailable(1))
	// release the memory of resolvedTs 100
	m.Release(1, model.NewResolvedTs(101))
	require.True(t, m.hasAvailable(100))
	// release the memory of resolvedTs 200
	m.Release(1, model.NewResolvedTs(201))
	require.True(t, m.hasAvailable(200))
	// release the memory of resolvedTs 300
	m.Release(1, model.NewResolvedTs(301))
	require.True(t, m.hasAvailable(300))
	// release the memory of resolvedTs 300 again
	m.Release(1, model.NewResolvedTs(301))
	require.True(t, m.hasAvailable(300))
}

func TestMemQuotaRecordAndReleaseWithBatchID(t *testing.T) {
	t.Parallel()

	m := NewMemQuota(model.DefaultChangeFeedID("1"), 300, "")
	defer m.Close()
	m.AddTable(1)

	require.True(t, m.TryAcquire(100))
	resolvedTs := model.ResolvedTs{
		Mode:    model.BatchResolvedMode,
		Ts:      100,
		BatchID: 1,
	}
	m.Record(1, resolvedTs, 100)
	resolvedTs = model.ResolvedTs{
		Mode:    model.BatchResolvedMode,
		Ts:      100,
		BatchID: 2,
	}
	require.True(t, m.TryAcquire(100))
	m.Record(1, resolvedTs, 100)
	resolvedTs = model.ResolvedTs{
		Mode:    model.BatchResolvedMode,
		Ts:      100,
		BatchID: 3,
	}
	require.True(t, m.TryAcquire(100))
	m.Record(1, resolvedTs, 100)
	require.False(t, m.TryAcquire(1))
	require.False(t, m.hasAvailable(1))

	// release the memory of resolvedTs 100 batchID 2
	resolvedTs = model.ResolvedTs{
		Mode:    model.BatchResolvedMode,
		Ts:      100,
		BatchID: 2,
	}
	m.Release(1, resolvedTs)
	require.True(t, m.hasAvailable(200))
	// release the memory of resolvedTs 101
	m.Release(1, model.NewResolvedTs(101))
	require.True(t, m.hasAvailable(300))
}

func TestMemQuotaRecordAndClean(t *testing.T) {
	t.Parallel()

	m := NewMemQuota(model.DefaultChangeFeedID("1"), 300, "")
	defer m.Close()
	m.AddTable(1)

	require.True(t, m.TryAcquire(100))
	m.Record(1, model.NewResolvedTs(100), 100)
	require.True(t, m.TryAcquire(100))
	m.Record(1, model.NewResolvedTs(200), 100)
	require.True(t, m.TryAcquire(100))
	m.Record(1, model.NewResolvedTs(300), 100)
	require.False(t, m.TryAcquire(1))
	require.False(t, m.hasAvailable(1))

	// clean the all memory.
	cleanedBytes := m.ClearTable(1)
	require.Equal(t, uint64(300), cleanedBytes)
	require.True(t, m.hasAvailable(100))

	cleanedBytes = m.RemoveTable(1)
	require.Equal(t, uint64(0), cleanedBytes)
}
