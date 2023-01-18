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
	"sync"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestMemQuotaTryAcquire(t *testing.T) {
	t.Parallel()

	m := newMemQuota(model.DefaultChangeFeedID("1"), 100, "")
	defer m.close()

	require.True(t, m.tryAcquire(50))
	require.True(t, m.tryAcquire(50))
	require.False(t, m.tryAcquire(1))
}

func TestMemQuotaForceAcquire(t *testing.T) {
	t.Parallel()

	m := newMemQuota(model.DefaultChangeFeedID("1"), 100, "")
	defer m.close()

	require.True(t, m.tryAcquire(100))
	require.False(t, m.tryAcquire(1))
	m.forceAcquire(1)
	require.Equal(t, uint64(101), m.getUsedBytes())
}

func TestMemQuotaBlockAcquire(t *testing.T) {
	t.Parallel()

	m := newMemQuota(model.DefaultChangeFeedID("1"), 100, "")
	defer m.close()
	err := m.blockAcquire(100)
	require.NoError(t, err)
	m.record(1, model.NewResolvedTs(1), 50)
	m.record(1, model.NewResolvedTs(2), 50)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := m.blockAcquire(50)
		require.NoError(t, err)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := m.blockAcquire(50)
		require.NoError(t, err)
	}()
	m.release(1, model.NewResolvedTs(1))
	m.release(1, model.NewResolvedTs(2))
	wg.Wait()
}

func TestMemQuotaClose(t *testing.T) {
	t.Parallel()

	m := newMemQuota(model.DefaultChangeFeedID("1"), 100, "")

	err := m.blockAcquire(100)
	require.NoError(t, err)
	m.record(1, model.NewResolvedTs(2), 100)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := m.blockAcquire(50)
		if err != nil {
			require.ErrorIs(t, err, cerrors.ErrFlowControllerAborted)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := m.blockAcquire(50)
		if err != nil {
			require.ErrorIs(t, err, cerrors.ErrFlowControllerAborted)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := m.blockAcquire(50)
		if err != nil {
			require.ErrorIs(t, err, cerrors.ErrFlowControllerAborted)
		}
	}()

	// Randomly release some memory.
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.release(1, model.NewResolvedTs(2))
	}()
	m.close()
	wg.Wait()
}

func TestMemQuotaRefund(t *testing.T) {
	t.Parallel()

	m := newMemQuota(model.DefaultChangeFeedID("1"), 100, "")
	defer m.close()

	require.True(t, m.tryAcquire(50))
	require.True(t, m.tryAcquire(50))
	m.refund(50)
	require.True(t, m.tryAcquire(1))
	require.True(t, m.tryAcquire(49))

	// Test notify the blocked acquire.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := m.blockAcquire(50)
		require.NoError(t, err)
	}()
	m.refund(50)
	wg.Wait()
}

func TestMemQuotaHasAvailable(t *testing.T) {
	t.Parallel()

	m := newMemQuota(model.DefaultChangeFeedID("1"), 100, "")
	defer m.close()

	require.True(t, m.hasAvailable(100))
	require.True(t, m.tryAcquire(100))
	require.False(t, m.hasAvailable(1))
}

func TestMemQuotaRecordAndRelease(t *testing.T) {
	t.Parallel()

	m := newMemQuota(model.DefaultChangeFeedID("1"), 300, "")
	defer m.close()
	m.addTable(1)

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

	m := newMemQuota(model.DefaultChangeFeedID("1"), 300, "")
	defer m.close()
	m.addTable(1)

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

func TestMemQuotaRecordAndClean(t *testing.T) {
	t.Parallel()

	m := newMemQuota(model.DefaultChangeFeedID("1"), 300, "")
	defer m.close()
	m.addTable(1)

	require.True(t, m.tryAcquire(100))
	m.record(1, model.NewResolvedTs(100), 100)
	require.True(t, m.tryAcquire(100))
	m.record(1, model.NewResolvedTs(200), 100)
	require.True(t, m.tryAcquire(100))
	m.record(1, model.NewResolvedTs(300), 100)
	require.False(t, m.tryAcquire(1))
	require.False(t, m.hasAvailable(1))

	// clean the all memory.
	cleanedBytes := m.clean(1)
	require.Equal(t, uint64(300), cleanedBytes)
	require.True(t, m.hasAvailable(100))
}
