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

	"github.com/pingcap/tiflow/pkg/pdtime"

	"github.com/pingcap/errors"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestUpdateGCSafePoint(t *testing.T) {
	defer testleak.AfterTest(t)()
	mockPDClient := &MockPDClient{}
	gcManager := NewManager(mockPDClient).(*gcManager)
	ctx := cdcContext.NewBackendContext4Test(true)

	startTs := oracle.GoTimeToTS(time.Now())
	mockPDClient.UpdateServiceGCSafePointFunc = func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
		require.Equal(t, safePoint, startTs)
		require.Equal(t, ttl, gcManager.gcTTL)
		require.Equal(t, serviceID, CDCServiceSafePointID)
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
		require.Equal(t, safePoint, startTs)
		require.Equal(t, ttl, gcManager.gcTTL)
		require.Equal(t, serviceID, CDCServiceSafePointID)
		return 0, nil
	}
	err = gcManager.TryUpdateGCSafePoint(ctx, startTs, false /* forceUpdate */)
	require.Nil(t, err)

	// Force update
	startTs++
	ch := make(chan struct{}, 1)
	mockPDClient.UpdateServiceGCSafePointFunc = func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
		require.Equal(t, safePoint, startTs)
		require.Equal(t, ttl, gcManager.gcTTL)
		require.Equal(t, serviceID, CDCServiceSafePointID)
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
	defer testleak.AfterTest(t)()
	mockPDClient := &MockPDClient{}
	gcManager := NewManager(mockPDClient).(*gcManager)
	gcManager.isTiCDCBlockGC = true
	ctx := context.Background()

	clock, err := pdtime.NewClock(context.Background(), mockPDClient)
	require.Nil(t, err)

	go clock.Run(ctx)
	time.Sleep(1 * time.Second)
	defer clock.Stop()

	cCtx := cdcContext.NewContext(ctx, &cdcContext.GlobalVars{
		PDClock: clock,
	})

	err = gcManager.CheckStaleCheckpointTs(cCtx, "cfID", 10)
	require.True(t, cerror.ErrGCTTLExceeded.Equal(errors.Cause(err)))
	require.True(t, cerror.ChangefeedFastFailError(err))

	err = gcManager.CheckStaleCheckpointTs(cCtx, "cfID", oracle.GoTimeToTS(time.Now()))
	require.Nil(t, err)

	gcManager.isTiCDCBlockGC = false
	gcManager.lastSafePointTs = 20
	err = gcManager.CheckStaleCheckpointTs(cCtx, "cfID", 10)
	require.True(t, cerror.ErrSnapshotLostByGC.Equal(errors.Cause(err)))
	require.True(t, cerror.ChangefeedFastFailError(err))
}
