// Copyright 2021 PingCAP, Inc.
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

package gc

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestUpdateGCSafePoint(t *testing.T) {
	t.Parallel()

	mockPDClient := &MockPDClient{}
	pdClock := pdutil.NewClock4Test()
	gcManager := NewManager(etcd.GcServiceIDForTest(),
		mockPDClient, pdClock).(*gcManager)
	ctx := cdcContext.NewBackendContext4Test(true)

	startTs := oracle.GoTimeToTS(time.Now())
	mockPDClient.UpdateServiceGCSafePointFunc = func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
		require.Equal(t, startTs, safePoint)
		require.Equal(t, gcManager.gcTTL, ttl)
		require.Equal(t, etcd.GcServiceIDForTest(), serviceID)
		return 0, nil
	}
	err := gcManager.TryUpdateGCSafePoint(ctx, startTs, false /* forceUpdate */)
	require.Nil(t, err)

	// gcManager must not update frequent.
	gcManager.lastUpdatedTime = time.Now()
	startTs++
	err = gcManager.TryUpdateGCSafePoint(ctx, startTs, false /* forceUpdate */)
	require.Nil(t, err)

	// Assume that the gc safe point updated gcSafepointUpdateInterval ago.
	gcManager.lastUpdatedTime = time.Now().Add(-gcSafepointUpdateInterval)
	startTs++
	mockPDClient.UpdateServiceGCSafePointFunc = func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
		require.Equal(t, startTs, safePoint)
		require.Equal(t, gcManager.gcTTL, ttl)
		require.Equal(t, etcd.GcServiceIDForTest(), serviceID)
		return 0, nil
	}
	err = gcManager.TryUpdateGCSafePoint(ctx, startTs, false /* forceUpdate */)
	require.Nil(t, err)

	// Force update
	startTs++
	ch := make(chan struct{}, 1)
	mockPDClient.UpdateServiceGCSafePointFunc = func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
		require.Equal(t, startTs, safePoint)
		require.Equal(t, gcManager.gcTTL, ttl)
		require.Equal(t, etcd.GcServiceIDForTest(), serviceID)
		ch <- struct{}{}
		return 0, nil
	}
	err = gcManager.TryUpdateGCSafePoint(ctx, startTs, true /* forceUpdate */)
	require.Nil(t, err)
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	case <-ch:
	}
}

func TestCheckStaleCheckpointTs(t *testing.T) {
	t.Parallel()

	mockPDClient := &MockPDClient{}
	pdClock := pdutil.NewClock4Test()
	gcManager := NewManager(etcd.GcServiceIDForTest(),
		mockPDClient, pdClock).(*gcManager)
	ctx := context.Background()

	time.Sleep(1 * time.Second)

	cfID := model.DefaultChangeFeedID("cfID")
	err := gcManager.CheckStaleCheckpointTs(ctx, cfID, oracle.GoTimeToTS(time.Now()))
	require.Nil(t, err)

	gcManager.lastSafePointTs = 20
	err = gcManager.CheckStaleCheckpointTs(ctx, cfID, 10)
	require.True(t, cerror.ErrSnapshotLostByGC.Equal(errors.Cause(err)))
	require.True(t, cerror.IsChangefeedGCFastFailError(err))
}
