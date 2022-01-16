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

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/store/tikv/oracle"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

func Test(t *testing.T) {
	check.TestingT(t)
}

var _ = check.Suite(&gcManagerSuite{})

type gcManagerSuite struct {
}

func (s *gcManagerSuite) TestUpdateGCSafePoint(c *check.C) {
	defer testleak.AfterTest(c)()
	mockPDClient := &MockPDClient{}
	gcManager := NewManager(mockPDClient).(*gcManager)
	ctx := cdcContext.NewBackendContext4Test(true)

	startTs := oracle.GoTimeToTS(time.Now())
	mockPDClient.UpdateServiceGCSafePointFunc = func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
		c.Assert(safePoint, check.Equals, startTs)
		c.Assert(ttl, check.Equals, gcManager.gcTTL)
		c.Assert(serviceID, check.Equals, CDCServiceSafePointID)
		return 0, nil
	}
	err := gcManager.TryUpdateGCSafePoint(ctx, startTs, false /* forceUpdate */)
	c.Assert(err, check.IsNil)

	// gcManager must not update frequent.
	gcManager.lastUpdatedTime = time.Now()
	startTs++
	err = gcManager.TryUpdateGCSafePoint(ctx, startTs, false /* forceUpdate */)
	c.Assert(err, check.IsNil)

	// Assume that the gc safe point updated gcSafepointUpdateInterval ago.
	gcManager.lastUpdatedTime = time.Now().Add(-gcSafepointUpdateInterval)
	startTs++
	mockPDClient.UpdateServiceGCSafePointFunc = func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
		c.Assert(safePoint, check.Equals, startTs)
		c.Assert(ttl, check.Equals, gcManager.gcTTL)
		c.Assert(serviceID, check.Equals, CDCServiceSafePointID)
		return 0, nil
	}
	err = gcManager.TryUpdateGCSafePoint(ctx, startTs, false /* forceUpdate */)
	c.Assert(err, check.IsNil)

	// Force update
	startTs++
	ch := make(chan struct{}, 1)
	mockPDClient.UpdateServiceGCSafePointFunc = func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
		c.Assert(safePoint, check.Equals, startTs)
		c.Assert(ttl, check.Equals, gcManager.gcTTL)
		c.Assert(serviceID, check.Equals, CDCServiceSafePointID)
		ch <- struct{}{}
		return 0, nil
	}
	err = gcManager.TryUpdateGCSafePoint(ctx, startTs, true /* forceUpdate */)
	c.Assert(err, check.IsNil)
	select {
	case <-time.After(5 * time.Second):
		c.Fatal("timeout")
	case <-ch:
	}
}

func (s *gcManagerSuite) TestCheckStaleCheckpointTs(c *check.C) {
	defer testleak.AfterTest(c)()
	mockPDClient := &MockPDClient{}
	gcManager := NewManager(mockPDClient).(*gcManager)
	gcManager.isTiCDCBlockGC = true
	ctx := context.Background()

	TimeAcquirer := pdtime.NewTimeAcquirer(mockPDClient)
	go TimeAcquirer.Run(ctx)
	time.Sleep(1 * time.Second)
	defer TimeAcquirer.Stop()

	cCtx := cdcContext.NewContext(ctx, &cdcContext.GlobalVars{
		TimeAcquirer: TimeAcquirer,
	})

	err := gcManager.CheckStaleCheckpointTs(cCtx, "cfID", 10)
	c.Assert(cerror.ErrGCTTLExceeded.Equal(errors.Cause(err)), check.IsTrue)
	c.Assert(cerror.ChangefeedFastFailError(err), check.IsTrue)

	err = gcManager.CheckStaleCheckpointTs(cCtx, "cfID", oracle.GoTimeToTS(time.Now()))
	c.Assert(err, check.IsNil)

	gcManager.isTiCDCBlockGC = false
	gcManager.lastSafePointTs = 20
	err = gcManager.CheckStaleCheckpointTs(cCtx, "cfID", 10)
	c.Assert(cerror.ErrSnapshotLostByGC.Equal(errors.Cause(err)), check.IsTrue)
	c.Assert(cerror.ChangefeedFastFailError(err), check.IsTrue)
}
