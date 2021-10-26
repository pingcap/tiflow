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

package owner

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"github.com/pingcap/tidb/store/tikv/oracle"
	pd "github.com/tikv/pd/client"
)

var _ = check.Suite(&gcManagerSuite{})

type gcManagerSuite struct {
}

type mockPDClient struct {
	pd.Client
	updateServiceGCSafePointFunc func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error)
}

func (m *mockPDClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	return m.updateServiceGCSafePointFunc(ctx, serviceID, ttl, safePoint)
}

func (m *mockPDClient) GetTS(ctx context.Context) (int64, int64, error) {
	return oracle.GetPhysical(time.Now()), 0, nil
}

func (s *gcManagerSuite) TestUpdateGCSafePoint(c *check.C) {
	defer testleak.AfterTest(c)()
	gcManager := newGCManager()
	ctx := cdcContext.NewBackendContext4Test(true)
	mockPDClient := &mockPDClient{}
	ctx.GlobalVars().PDClient = mockPDClient
	state := model.NewGlobalState().(*model.GlobalReactorState)
	tester := orchestrator.NewReactorStateTester(c, state, nil)

	// no changefeed, the gc safe point should be max uint64
	mockPDClient.updateServiceGCSafePointFunc = func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
		c.Assert(safePoint, check.Equals, uint64(math.MaxUint64))
		return 0, nil
	}
	err := gcManager.updateGCSafePoint(ctx, state)
	c.Assert(err, check.IsNil)
	// add a stopped changefeed
	changefeedID1 := "changefeed-test1"
	changefeedID2 := "changefeed-test2"
	tester.MustUpdate(fmt.Sprintf("/tidb/cdc/changefeed/info/%s", changefeedID1), []byte(`{"config":{"cyclic-replication":{}},"state":"failed"}`))
	tester.MustApplyPatches()
	state.Changefeeds[changefeedID1].PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		return &model.ChangeFeedStatus{CheckpointTs: 1}, true, nil
	})
	tester.MustApplyPatches()
	err = gcManager.updateGCSafePoint(ctx, state)
	c.Assert(err, check.IsNil)

	// switch the state of changefeed to normal
	state.Changefeeds[changefeedID1].PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		info.State = model.StateNormal
		return info, true, nil
	})
	tester.MustApplyPatches()
	// the gc safe point should be updated to 1(checkpoint Ts of changefeed-test1)
	mockPDClient.updateServiceGCSafePointFunc = func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
		c.Assert(serviceID, check.Equals, cdcServiceSafePointID)
		c.Assert(ttl, check.Equals, gcManager.gcTTL)
		c.Assert(safePoint, check.Equals, uint64(1))
		return 0, nil
	}
	err = gcManager.updateGCSafePoint(ctx, state)
	c.Assert(err, check.IsNil)

	// add another changefeed
	tester.MustUpdate(fmt.Sprintf("/tidb/cdc/changefeed/info/%s", changefeedID2), []byte(`{"config":{"cyclic-replication":{}},"state":"normal"}`))
	tester.MustApplyPatches()
	state.Changefeeds[changefeedID1].PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		return &model.ChangeFeedStatus{CheckpointTs: 20}, true, nil
	})
	state.Changefeeds[changefeedID2].PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		return &model.ChangeFeedStatus{CheckpointTs: 30}, true, nil
	})
	tester.MustApplyPatches()
	// the gc safe point should not be updated, because it was recently updated
	mockPDClient.updateServiceGCSafePointFunc = func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
		c.Errorf("should not update gc safe point")
		return 0, nil
	}
	err = gcManager.updateGCSafePoint(ctx, state)
	c.Assert(err, check.IsNil)

	// assume that the gc safe point updated one hour ago
	gcManager.lastUpdatedTime = time.Now().Add(-time.Hour)

	// the gc safe point should be updated to 1(checkpoint Ts of changefeed-test1)
	mockPDClient.updateServiceGCSafePointFunc = func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
		c.Assert(serviceID, check.Equals, cdcServiceSafePointID)
		c.Assert(ttl, check.Equals, gcManager.gcTTL)
		c.Assert(safePoint, check.Equals, uint64(20))
		return 0, nil
	}
	err = gcManager.updateGCSafePoint(ctx, state)
	c.Assert(err, check.IsNil)
}

func (s *gcManagerSuite) TestTimeFromPD(c *check.C) {
	defer testleak.AfterTest(c)()
	gcManager := newGCManager()
	ctx := cdcContext.NewBackendContext4Test(true)
	mockPDClient := &mockPDClient{}
	ctx.GlobalVars().PDClient = mockPDClient
	t1, err := gcManager.currentTimeFromPDCached(ctx)
	c.Assert(err, check.IsNil)
	c.Assert(t1, check.Equals, gcManager.pdPhysicalTimeCache)

	time.Sleep(50 * time.Millisecond)
	// should return cached time
	t2, err := gcManager.currentTimeFromPDCached(ctx)
	c.Assert(err, check.IsNil)
	c.Assert(t2, check.Equals, gcManager.pdPhysicalTimeCache)
	c.Assert(t2, check.Equals, t1)

	time.Sleep(50 * time.Millisecond)
	// assume that the gc safe point updated one hour ago
	gcManager.lastUpdatedPdTime = time.Now().Add(-time.Hour)
	t3, err := gcManager.currentTimeFromPDCached(ctx)
	c.Assert(err, check.IsNil)
	c.Assert(t3, check.Equals, gcManager.pdPhysicalTimeCache)
	// should return new time
	c.Assert(t3, check.Not(check.Equals), t2)
}

func (s *gcManagerSuite) TestCheckStaleCheckpointTs(c *check.C) {
	defer testleak.AfterTest(c)()
	gcManager := newGCManager()
	gcManager.isTiCDCBlockGC = true
	ctx := cdcContext.NewBackendContext4Test(true)
	mockPDClient := &mockPDClient{}
	ctx.GlobalVars().PDClient = mockPDClient
	err := gcManager.checkStaleCheckpointTs(ctx, "cfID", 10)
	c.Assert(cerror.ErrGCTTLExceeded.Equal(errors.Cause(err)), check.IsTrue)
	c.Assert(cerror.ChangefeedFastFailError(err), check.IsTrue)

	err = gcManager.checkStaleCheckpointTs(ctx, "cfID", oracle.ComposeTS(oracle.GetPhysical(time.Now()), 0))
	c.Assert(err, check.IsNil)

	gcManager.isTiCDCBlockGC = false
	gcManager.lastSafePointTs = 20
	err = gcManager.checkStaleCheckpointTs(ctx, "cfID", 10)
	c.Assert(cerror.ErrSnapshotLostByGC.Equal(errors.Cause(err)), check.IsTrue)
	c.Assert(cerror.ChangefeedFastFailError(err), check.IsTrue)
}
