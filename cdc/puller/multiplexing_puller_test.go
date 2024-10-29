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

package puller

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
)

func newMultiplexingPullerForTest(outputCh chan<- *model.RawKVEntry) *MultiplexingPuller {
	cfg := &config.ServerConfig{Debug: &config.DebugConfig{Puller: &config.PullerConfig{LogRegionDetails: false}}}
	client := kv.NewSharedClient(model.ChangeFeedID{}, cfg, false, nil, nil, nil, nil, nil)
	consume := func(ctx context.Context, e *model.RawKVEntry, _ []tablepb.Span, _ model.ShouldSplitKVEntry) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case outputCh <- e:
			return nil
		}
	}
	return NewMultiplexingPuller(
		model.ChangeFeedID{},
		client,
		pdutil.NewClock4Test(),
		consume,
		1, func(tablepb.Span, int) int { return 0 }, 1,
	)
}

func TestMultiplexingPullerResolvedForward(t *testing.T) {
	outputCh := make(chan *model.RawKVEntry, 16)
	puller := newMultiplexingPullerForTest(outputCh)
	defer puller.client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		puller.run(ctx, false)
	}()

	events := []model.RegionFeedEvent{
		{
			Resolved: &model.ResolvedSpans{
				Spans: []model.RegionComparableSpan{{
					Span: spanz.ToSpan([]byte("t_a"), []byte("t_c")),
				}}, ResolvedTs: uint64(1001),
			},
		},
		{
			Resolved: &model.ResolvedSpans{
				Spans: []model.RegionComparableSpan{{
					Span: spanz.ToSpan([]byte("t_c"), []byte("t_d")),
				}}, ResolvedTs: uint64(1002),
			},
		},
		{
			Resolved: &model.ResolvedSpans{
				Spans: []model.RegionComparableSpan{{
					Span: spanz.ToSpan([]byte("t_d"), []byte("t_e")),
				}}, ResolvedTs: uint64(1000),
			},
		},
	}

	spans := []tablepb.Span{spanz.ToSpan([]byte("t_a"), []byte("t_e"))}
	spans[0].TableID = 1
	shouldSplitKVEntry := func(raw *model.RawKVEntry) bool {
		return false
	}
	puller.subscribe(spans, 996, "test", shouldSplitKVEntry)
	subID := puller.subscriptions.n.GetV(spans[0]).subID
	for _, event := range events {
		puller.inputChs[0] <- kv.MultiplexingEvent{RegionFeedEvent: event, SubscriptionID: subID}
	}

	select {
	case ev := <-outputCh:
		require.Equal(t, model.OpTypeResolved, ev.OpType)
		require.Equal(t, uint64(1000), ev.CRTs)
	case <-time.NewTimer(100 * time.Millisecond).C:
		require.True(t, false, "must get an event")
	}
	cancel()
	wg.Wait()
}
