// Copyright 2023 PingCAP, Inc.
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
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/tiflow/cdc/kv/regionlock"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func newSharedClientForTestSharedRegionWorker() *SharedClient {
	// sharedRegionWorker only requires `SharedClient.onRegionFail`.
	cfg := &config.ServerConfig{Debug: &config.DebugConfig{Puller: &config.PullerConfig{LogRegionDetails: false}}}
	return NewSharedClient(model.ChangeFeedID{}, cfg, false, nil, nil, nil, nil, nil)
}

// For UPDATE SQL, its prewrite event has both value and old value.
// It is possible that TiDB prewrites multiple times for the same row when
// there are other transactions it conflicts with. For this case,
// if the value is not "short", only the first prewrite contains the value.
//
// TiKV may output events for the UPDATE SQL as following:
//
// TiDB: [Prwrite1]    [Prewrite2]      [Commit]
//
//	v             v                v                                   Time
//
// ---------------------------------------------------------------------------->
//
//	^            ^    ^           ^     ^       ^     ^          ^     ^
//
// TiKV:   [Scan Start] [Send Prewrite2] [Send Commit] [Send Prewrite1] [Send Init]
// TiCDC:                    [Recv Prewrite2]  [Recv Commit] [Recv Prewrite1] [Recv Init]
func TestSharedRegionWokerHandleEventEntryEventOutOfOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := newSharedClientForTestSharedRegionWorker()
	defer client.Close()

	worker := newSharedRegionWorker(client)
	eventCh := make(chan MultiplexingEvent, 2)

	span := spanz.ToSpan([]byte{}, spanz.UpperBoundKey)
	sri := newSingleRegionInfo(tikv.RegionVerID{}, span, &tikv.RPCContext{})
	sri.requestedTable = &requestedTable{subscriptionID: SubscriptionID(1), eventCh: eventCh}
	sri.lockedRange = &regionlock.LockedRange{}
	state := newRegionFeedState(sri, 1)
	state.start()

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
	select {
	case <-eventCh:
		require.True(t, false, "shouldn't get an event")
	case <-time.NewTimer(100 * time.Millisecond).C:
	}

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
	case <-eventCh:
		require.True(t, false, "shouldn't get an event")
	case <-time.NewTimer(100 * time.Millisecond).C:
	}

	// Receive initialized.
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
	case event := <-eventCh:
		require.Equal(t, uint64(2), event.Val.CRTs)
		require.Equal(t, uint64(1), event.Val.StartTs)
		require.Equal(t, "value", string(event.Val.Value))
		require.Equal(t, "oldvalue", string(event.Val.OldValue))
	case <-time.NewTimer(100 * time.Millisecond).C:
		require.True(t, false, "must get an event")
	}
}

func TestSharedRegionWorkerHandleResolvedTs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := newSharedClientForTestSharedRegionWorker()
	defer client.Close()

	worker := newSharedRegionWorker(client)
	eventCh := make(chan MultiplexingEvent, 2)

	s1 := newRegionFeedState(singleRegionInfo{verID: tikv.NewRegionVerID(1, 1, 1)}, 1)
	s1.sri.requestedTable = client.newRequestedTable(1, tablepb.Span{}, 0, eventCh)
	s1.sri.lockedRange = &regionlock.LockedRange{}
	s1.setInitialized()
	s1.updateResolvedTs(9)

	s2 := newRegionFeedState(singleRegionInfo{verID: tikv.NewRegionVerID(2, 2, 2)}, 2)
	s2.sri.requestedTable = client.newRequestedTable(2, tablepb.Span{}, 0, eventCh)
	s2.sri.lockedRange = &regionlock.LockedRange{}
	s2.setInitialized()
	s2.updateResolvedTs(11)

	s3 := newRegionFeedState(singleRegionInfo{verID: tikv.NewRegionVerID(3, 3, 3)}, 3)
	s3.sri.requestedTable = client.newRequestedTable(3, tablepb.Span{}, 0, eventCh)
	s3.sri.lockedRange = &regionlock.LockedRange{}
	s3.updateResolvedTs(8)

	worker.handleResolvedTs(ctx, resolvedTsBatch{ts: 10, regions: []*regionFeedState{s1, s2, s3}})
	require.Equal(t, uint64(10), s1.getLastResolvedTs())
	require.Equal(t, uint64(11), s2.getLastResolvedTs())
	require.Equal(t, uint64(8), s3.getLastResolvedTs())

	select {
	case event := <-eventCh:
		require.Equal(t, uint64(0), event.RegionID)
		require.Equal(t, uint64(10), event.Resolved.ResolvedTs)
		require.Equal(t, 1, len(event.Resolved.Spans))
		require.Equal(t, uint64(1), event.Resolved.Spans[0].Region)
	case <-time.NewTimer(100 * time.Millisecond).C:
		require.True(t, false, "must get an event")
	}
}
