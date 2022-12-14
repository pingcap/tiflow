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

package kv

import (
	"context"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"github.com/stretchr/testify/require"
)

type regionWorkerSuite struct{}

var _ = check.Suite(&regionWorkerSuite{})

func (s *regionWorkerSuite) TestRegionStateManager(c *check.C) {
	defer testleak.AfterTest(c)()
	rsm := newRegionStateManager(4)

	regionID := uint64(1000)
	_, ok := rsm.getState(regionID)
	c.Assert(ok, check.IsFalse)

	rsm.setState(regionID, &regionFeedState{requestID: 2})
	state, ok := rsm.getState(regionID)
	c.Assert(ok, check.IsTrue)
	c.Assert(state.requestID, check.Equals, uint64(2))
}

func (s *regionWorkerSuite) TestRegionStateManagerThreadSafe(c *check.C) {
	defer testleak.AfterTest(c)()
	rsm := newRegionStateManager(4)
	regionCount := 100
	regionIDs := make([]uint64, regionCount)
	for i := 0; i < regionCount; i++ {
		regionID := uint64(1000 + i)
		regionIDs[i] = regionID
		rsm.setState(regionID, &regionFeedState{requestID: uint64(i + 1), lastResolvedTs: uint64(1000)})
	}

	var wg sync.WaitGroup
	concurrency := 20
	wg.Add(concurrency * 2)
	for j := 0; j < concurrency; j++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				idx := rand.Intn(regionCount)
				regionID := regionIDs[idx]
				s, ok := rsm.getState(regionID)
				c.Assert(ok, check.IsTrue)
				s.lock.RLock()
				c.Assert(s.requestID, check.Equals, uint64(idx+1))
				s.lock.RUnlock()
			}
		}()
	}
	for j := 0; j < concurrency; j++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				// simulate write less than read
				if i%5 != 0 {
					continue
				}
				regionID := regionIDs[rand.Intn(regionCount)]
				s, ok := rsm.getState(regionID)
				c.Assert(ok, check.IsTrue)
				s.lock.Lock()
				s.lastResolvedTs += 10
				s.lock.Unlock()
				rsm.setState(regionID, s)
			}
		}()
	}
	wg.Wait()

	totalResolvedTs := uint64(0)
	for _, regionID := range regionIDs {
		s, ok := rsm.getState(regionID)
		c.Assert(ok, check.IsTrue)
		c.Assert(s.lastResolvedTs, check.Greater, uint64(1000))
		totalResolvedTs += s.lastResolvedTs
	}
	// 100 regions, initial resolved ts 1000;
	// 2000 * resolved ts forward, increased by 10 each time, routine number is `concurrency`.
	c.Assert(totalResolvedTs, check.Equals, uint64(100*1000+2000*10*concurrency))
}

func (s *regionWorkerSuite) TestRegionStateManagerBucket(c *check.C) {
	defer testleak.AfterTest(c)()
	rsm := newRegionStateManager(-1)
	c.Assert(rsm.bucket, check.GreaterEqual, minRegionStateBucket)
	c.Assert(rsm.bucket, check.LessEqual, maxRegionStateBucket)

	bucket := rsm.bucket * 2
	rsm = newRegionStateManager(bucket)
	c.Assert(rsm.bucket, check.Equals, bucket)
}

func (s *regionWorkerSuite) TestRegionWorkerPoolSize(c *check.C) {
	defer testleak.AfterTest(c)()

	conf := config.GetDefaultServerConfig()
	conf.KVClient.WorkerPoolSize = 0
	config.StoreGlobalServerConfig(conf)
	size := getWorkerPoolSize()
	min := func(a, b int) int {
		if a < b {
			return a
		}
		return b
	}
	c.Assert(size, check.Equals, min(runtime.NumCPU()*2, maxWorkerPoolSize))

	conf.KVClient.WorkerPoolSize = 5
	size = getWorkerPoolSize()
	c.Assert(size, check.Equals, 5)

	conf.KVClient.WorkerPoolSize = maxWorkerPoolSize + 1
	size = getWorkerPoolSize()
	c.Assert(size, check.Equals, maxWorkerPoolSize)
}

func TestRegionWokerHandleEventEntryEventOutOfOrder(t *testing.T) {
	// For UPDATE SQL, its prewrite event has both value and old value.
	// It is possible that TiDB prewrites multiple times for the same row when
	// there are other transactions it conflicts with. For this case,
	// if the value is not "short", only the first prewrite contains the value.
	//
	// TiKV may output events for the UPDATE SQL as following:
	//
	// TiDB: [Prwrite1]    [Prewrite2]      [Commit]
	//       v             v                v                                   Time
	// ---------------------------------------------------------------------------->
	//         ^            ^    ^           ^     ^       ^     ^          ^     ^
	// TiKV:   [Scan Start] [Send Prewrite2] [Send Commit] [Send Prewrite1] [Send Init]
	// TiCDC:                    [Recv Prewrite2]  [Recv Commit] [Recv Prewrite1] [Recv Init]

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventCh := make(chan model.RegionFeedEvent, 2)
	s := createFakeEventFeedSession(ctx)
	s.enableOldValue = true
	s.eventCh = eventCh
	span := regionspan.Span{}.Hack()
	state := newRegionFeedState(newSingleRegionInfo(
		tikv.RegionVerID{},
		regionspan.ToComparableSpan(span),
		0, &tikv.RPCContext{}), 0)
	state.start()
	worker := newRegionWorker(s, "")
	worker.initMetrics(ctx)
	require.Equal(t, 2, cap(worker.outputCh))

	// Receive prewrite2 with empty value.
	events := &cdcpb.Event_Entries_{
		Entries: &cdcpb.Event_Entries{
			Entries: []*cdcpb.Event_Row{{
				StartTs:  1,
				Type:     cdcpb.Event_PREWRITE,
				OpType:   cdcpb.Event_Row_PUT,
				Key:      []byte("key"),
				Value:    nil,
				OldValue: []byte("oldvalue"),
			}},
		},
	}
	err := worker.handleEventEntry(ctx, events, state)
	require.Nil(t, err)

	// Receive commit.
	events = &cdcpb.Event_Entries_{
		Entries: &cdcpb.Event_Entries{
			Entries: []*cdcpb.Event_Row{{
				StartTs:  1,
				CommitTs: 2,
				Type:     cdcpb.Event_COMMIT,
				OpType:   cdcpb.Event_Row_PUT,
				Key:      []byte("key"),
			}},
		},
	}
	err = worker.handleEventEntry(context.Background(), events, state)
	require.Nil(t, err)

	// Must not output event.
	var event model.RegionFeedEvent
	var ok bool
	select {
	case event, ok = <-eventCh:
	default:
	}
	require.Falsef(t, ok, "%v", event)
	require.EqualValuesf(t, model.RegionFeedEvent{}, event, "%v", event)

	// Receive prewrite1 with actual value.
	events = &cdcpb.Event_Entries_{
		Entries: &cdcpb.Event_Entries{
			Entries: []*cdcpb.Event_Row{{
				StartTs:  1,
				Type:     cdcpb.Event_PREWRITE,
				OpType:   cdcpb.Event_Row_PUT,
				Key:      []byte("key"),
				Value:    []byte("value"),
				OldValue: []byte("oldvalue"),
			}},
		},
	}
	err = worker.handleEventEntry(ctx, events, state)
	require.Nil(t, err)

	// Must not output event.
	select {
	case event, ok = <-eventCh:
	default:
	}
	require.Falsef(t, ok, "%v", event)
	require.EqualValuesf(t, model.RegionFeedEvent{}, event, "%v", event)

	// Receive prewrite1 with actual value.
	events = &cdcpb.Event_Entries_{
		Entries: &cdcpb.Event_Entries{
			Entries: []*cdcpb.Event_Row{
				{
					Type: cdcpb.Event_INITIALIZED,
				},
			},
		},
	}
	err = worker.handleEventEntry(ctx, events, state)
	require.Nil(t, err)

	// Must output event.
	select {
	case event, ok = <-eventCh:
	default:
	}
	require.Truef(t, ok, "%v", event)
	require.EqualValuesf(t, model.RegionFeedEvent{
		RegionID: 0,
		Val: &model.RawKVEntry{
			OpType:   model.OpTypePut,
			Key:      []byte("key"),
			Value:    []byte("value"),
			StartTs:  1,
			CRTs:     2,
			RegionID: 0,
			OldValue: []byte("oldvalue"),
		},
	}, event, "%v", event)
}
