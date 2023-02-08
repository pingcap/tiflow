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
	"sync"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
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
<<<<<<< HEAD:cdc/processor/sinkmanager/mem_quota_test.go
	m.record(1, model.NewResolvedTs(1), 50)
	m.record(1, model.NewResolvedTs(2), 50)
=======

	span := spanz.TableIDToComparableSpan(1)
	m.Record(span, model.NewResolvedTs(1), 50)
	m.Record(span, model.NewResolvedTs(2), 50)
>>>>>>> ae12f82ade (*(ticdc): fix some major problems about pull-based-sink (#8179)):cdc/processor/memquota/mem_quota_test.go

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
<<<<<<< HEAD:cdc/processor/sinkmanager/mem_quota_test.go
	m.release(1, model.NewResolvedTs(1))
	m.release(1, model.NewResolvedTs(2))
=======
	m.Release(span, model.NewResolvedTs(1))
	m.Release(span, model.NewResolvedTs(2))
>>>>>>> ae12f82ade (*(ticdc): fix some major problems about pull-based-sink (#8179)):cdc/processor/memquota/mem_quota_test.go
	wg.Wait()
}

func TestMemQuotaClose(t *testing.T) {
	t.Parallel()

	m := NewMemQuota(model.DefaultChangeFeedID("1"), 100, "")

	err := m.BlockAcquire(100)
	require.NoError(t, err)
<<<<<<< HEAD:cdc/processor/sinkmanager/mem_quota_test.go
	m.record(1, model.NewResolvedTs(2), 100)
=======
	span := spanz.TableIDToComparableSpan(1)
	m.Record(span, model.NewResolvedTs(2), 100)
>>>>>>> ae12f82ade (*(ticdc): fix some major problems about pull-based-sink (#8179)):cdc/processor/memquota/mem_quota_test.go

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := m.BlockAcquire(50)
		if err != nil {
			require.ErrorIs(t, err, cerrors.ErrFlowControllerAborted)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := m.BlockAcquire(50)
		if err != nil {
			require.ErrorIs(t, err, cerrors.ErrFlowControllerAborted)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := m.BlockAcquire(50)
		if err != nil {
			require.ErrorIs(t, err, cerrors.ErrFlowControllerAborted)
		}
	}()

	// Randomly release some memory.
	wg.Add(1)
	go func() {
		defer wg.Done()
<<<<<<< HEAD:cdc/processor/sinkmanager/mem_quota_test.go
		m.release(1, model.NewResolvedTs(2))
=======
		m.Release(span, model.NewResolvedTs(2))
>>>>>>> ae12f82ade (*(ticdc): fix some major problems about pull-based-sink (#8179)):cdc/processor/memquota/mem_quota_test.go
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

<<<<<<< HEAD:cdc/processor/sinkmanager/mem_quota_test.go
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
=======
	m := NewMemQuota(model.DefaultChangeFeedID("1"), 300, "")
	defer m.Close()
	span := spanz.TableIDToComparableSpan(1)
	m.AddTable(span)

	require.True(t, m.TryAcquire(100))
	m.Record(span, model.NewResolvedTs(100), 100)
	require.True(t, m.TryAcquire(100))
	m.Record(span, model.NewResolvedTs(200), 100)
	require.True(t, m.TryAcquire(100))
	m.Record(span, model.NewResolvedTs(300), 100)
	require.False(t, m.TryAcquire(1))
	require.False(t, m.hasAvailable(1))
	// release the memory of resolvedTs 100
	m.Release(span, model.NewResolvedTs(101))
	require.True(t, m.hasAvailable(100))
	// release the memory of resolvedTs 200
	m.Release(span, model.NewResolvedTs(201))
	require.True(t, m.hasAvailable(200))
	// release the memory of resolvedTs 300
	m.Release(span, model.NewResolvedTs(301))
	require.True(t, m.hasAvailable(300))
	// release the memory of resolvedTs 300 again
	m.Release(span, model.NewResolvedTs(301))
>>>>>>> ae12f82ade (*(ticdc): fix some major problems about pull-based-sink (#8179)):cdc/processor/memquota/mem_quota_test.go
	require.True(t, m.hasAvailable(300))
}

func TestMemQuotaRecordAndReleaseWithBatchID(t *testing.T) {
	t.Parallel()

<<<<<<< HEAD:cdc/processor/sinkmanager/mem_quota_test.go
	m := newMemQuota(model.DefaultChangeFeedID("1"), 300, "")
	defer m.close()
	m.addTable(1)
=======
	m := NewMemQuota(model.DefaultChangeFeedID("1"), 300, "")
	defer m.Close()
	span := spanz.TableIDToComparableSpan(1)
	m.AddTable(span)
>>>>>>> ae12f82ade (*(ticdc): fix some major problems about pull-based-sink (#8179)):cdc/processor/memquota/mem_quota_test.go

	require.True(t, m.TryAcquire(100))
	resolvedTs := model.ResolvedTs{
		Mode:    model.BatchResolvedMode,
		Ts:      100,
		BatchID: 1,
	}
<<<<<<< HEAD:cdc/processor/sinkmanager/mem_quota_test.go
	m.record(1, resolvedTs, 100)
=======
	m.Record(span, resolvedTs, 100)
>>>>>>> ae12f82ade (*(ticdc): fix some major problems about pull-based-sink (#8179)):cdc/processor/memquota/mem_quota_test.go
	resolvedTs = model.ResolvedTs{
		Mode:    model.BatchResolvedMode,
		Ts:      100,
		BatchID: 2,
	}
<<<<<<< HEAD:cdc/processor/sinkmanager/mem_quota_test.go
	require.True(t, m.tryAcquire(100))
	m.record(1, resolvedTs, 100)
=======
	require.True(t, m.TryAcquire(100))
	m.Record(span, resolvedTs, 100)
>>>>>>> ae12f82ade (*(ticdc): fix some major problems about pull-based-sink (#8179)):cdc/processor/memquota/mem_quota_test.go
	resolvedTs = model.ResolvedTs{
		Mode:    model.BatchResolvedMode,
		Ts:      100,
		BatchID: 3,
	}
<<<<<<< HEAD:cdc/processor/sinkmanager/mem_quota_test.go
	require.True(t, m.tryAcquire(100))
	m.record(1, resolvedTs, 100)
	require.False(t, m.tryAcquire(1))
=======
	require.True(t, m.TryAcquire(100))
	m.Record(span, resolvedTs, 100)
	require.False(t, m.TryAcquire(1))
>>>>>>> ae12f82ade (*(ticdc): fix some major problems about pull-based-sink (#8179)):cdc/processor/memquota/mem_quota_test.go
	require.False(t, m.hasAvailable(1))

	// release the memory of resolvedTs 100 batchID 2
	resolvedTs = model.ResolvedTs{
		Mode:    model.BatchResolvedMode,
		Ts:      100,
		BatchID: 2,
	}
<<<<<<< HEAD:cdc/processor/sinkmanager/mem_quota_test.go
	m.release(1, resolvedTs)
	require.True(t, m.hasAvailable(200))
	// release the memory of resolvedTs 101
	m.release(1, model.NewResolvedTs(101))
=======
	m.Release(span, resolvedTs)
	require.True(t, m.hasAvailable(200))
	// release the memory of resolvedTs 101
	m.Release(span, model.NewResolvedTs(101))
>>>>>>> ae12f82ade (*(ticdc): fix some major problems about pull-based-sink (#8179)):cdc/processor/memquota/mem_quota_test.go
	require.True(t, m.hasAvailable(300))
}

func TestMemQuotaRecordAndClean(t *testing.T) {
	t.Parallel()

<<<<<<< HEAD:cdc/processor/sinkmanager/mem_quota_test.go
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
=======
	m := NewMemQuota(model.DefaultChangeFeedID("1"), 300, "")
	defer m.Close()
	span := spanz.TableIDToComparableSpan(1)
	m.AddTable(span)

	require.True(t, m.TryAcquire(100))
	m.Record(span, model.NewResolvedTs(100), 100)
	require.True(t, m.TryAcquire(100))
	m.Record(span, model.NewResolvedTs(200), 100)
	require.True(t, m.TryAcquire(100))
	m.Record(span, model.NewResolvedTs(300), 100)
	require.False(t, m.TryAcquire(1))
	require.False(t, m.hasAvailable(1))

	// clean the all memory.
	cleanedBytes := m.Clean(span)
>>>>>>> ae12f82ade (*(ticdc): fix some major problems about pull-based-sink (#8179)):cdc/processor/memquota/mem_quota_test.go
	require.Equal(t, uint64(300), cleanedBytes)
	require.True(t, m.hasAvailable(100))
}
