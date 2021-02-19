// Copyright 2020 PingCAP, Inc.
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
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/txnutil"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/tikv"
	pd "github.com/tikv/pd/client"
)

type pullerSuite struct {
}

var _ = check.Suite(&pullerSuite{})

type mockPdClientForPullerTest struct {
	pd.Client
	clusterID uint64
}

func (mc *mockPdClientForPullerTest) GetClusterID(ctx context.Context) uint64 {
	return mc.clusterID
}

type mockCDCKVClient struct {
	expectations chan *model.RegionFeedEvent
}

type mockInjectedPuller struct {
	Puller
	cli *mockCDCKVClient
}

func newMockCDCKVClient(
	ctx context.Context,
	pd pd.Client,
	kvStorage tikv.Storage,
	credential *security.Credential,
) kv.CDCKVClient {
	return &mockCDCKVClient{
		expectations: make(chan *model.RegionFeedEvent, 1024),
	}
}

func (mc *mockCDCKVClient) EventFeed(
	ctx context.Context,
	span regionspan.ComparableSpan,
	ts uint64,
	enableOldValue bool,
	enableRegionWorker bool,
	lockResolver txnutil.LockResolver,
	isPullerInit kv.PullerInitialization,
	eventCh chan<- *model.RegionFeedEvent,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev := <-mc.expectations:
			if ev == nil {
				return nil
			}
			eventCh <- ev
		}
	}
}

func (mc *mockCDCKVClient) Close() error {
	close(mc.expectations)
	if len(mc.expectations) > 0 {
		buf := bytes.NewBufferString("mockCDCKVClient: not all expectations were satisfied! Still waiting\n")
		for e := range mc.expectations {
			_, _ = buf.WriteString(fmt.Sprintf("%s", e.GetValue()))
		}
		return errors.New(buf.String())
	}
	return nil
}

func (mc *mockCDCKVClient) Returns(ev *model.RegionFeedEvent) {
	mc.expectations <- ev
}

func (s *pullerSuite) newPullerForTest(
	c *check.C,
	spans []regionspan.Span,
	checkpointTs uint64,
) (*mockInjectedPuller, context.CancelFunc, *sync.WaitGroup, tidbkv.Storage) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	store, err := mockstore.NewMockStore()
	c.Assert(err, check.IsNil)
	enableOldValue := true
	backupNewCDCKVClient := kv.NewCDCKVClient
	kv.NewCDCKVClient = newMockCDCKVClient
	defer func() {
		kv.NewCDCKVClient = backupNewCDCKVClient
	}()
	pdCli := &mockPdClientForPullerTest{clusterID: uint64(1)}
	plr := NewPuller(ctx, pdCli, nil /* credential */, store, checkpointTs, spans, nil /* limitter */, enableOldValue, false /*enableRegionWorker*/)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := plr.Run(ctx)
		if err != nil {
			c.Assert(errors.Cause(err), check.Equals, context.Canceled)
		}
	}()
	c.Assert(err, check.IsNil)
	mockPlr := &mockInjectedPuller{
		Puller: plr,
		cli:    plr.(*pullerImpl).kvCli.(*mockCDCKVClient),
	}
	return mockPlr, cancel, &wg, store
}

func (s *pullerSuite) TestPullerResolvedForward(c *check.C) {
	defer testleak.AfterTest(c)()
	spans := []regionspan.Span{
		{Start: []byte("t_a"), End: []byte("t_e")},
	}
	checkpointTs := uint64(996)
	plr, cancel, wg, store := s.newPullerForTest(c, spans, checkpointTs)

	plr.cli.Returns(&model.RegionFeedEvent{
		Resolved: &model.ResolvedSpan{
			Span:       regionspan.ToComparableSpan(regionspan.Span{Start: []byte("t_a"), End: []byte("t_c")}),
			ResolvedTs: uint64(1001),
		},
	})
	plr.cli.Returns(&model.RegionFeedEvent{
		Resolved: &model.ResolvedSpan{
			Span:       regionspan.ToComparableSpan(regionspan.Span{Start: []byte("t_c"), End: []byte("t_d")}),
			ResolvedTs: uint64(1002),
		},
	})
	plr.cli.Returns(&model.RegionFeedEvent{
		Resolved: &model.ResolvedSpan{
			Span:       regionspan.ToComparableSpan(regionspan.Span{Start: []byte("t_d"), End: []byte("t_e")}),
			ResolvedTs: uint64(1000),
		},
	})
	ev := <-plr.Output()
	c.Assert(ev.OpType, check.Equals, model.OpTypeResolved)
	c.Assert(ev.CRTs, check.Equals, uint64(1000))
	c.Assert(plr.IsInitialized(), check.IsTrue)
	err := retry.Run(time.Millisecond*10, 10, func() error {
		ts := plr.GetResolvedTs()
		if ts != uint64(1000) {
			return errors.Errorf("resolved ts %d of puller does not forward to 1000", ts)
		}
		return nil
	})
	c.Assert(err, check.IsNil)

	store.Close()
	cancel()
	wg.Wait()
}

func (s *pullerSuite) TestPullerRawKV(c *check.C) {
	defer testleak.AfterTest(c)()
	spans := []regionspan.Span{
		{Start: []byte("c"), End: []byte("e")},
	}
	checkpointTs := uint64(996)
	plr, cancel, wg, store := s.newPullerForTest(c, spans, checkpointTs)

	// key not in expected region spans, will be ignored
	plr.cli.Returns(&model.RegionFeedEvent{
		Val: &model.RawKVEntry{
			OpType: model.OpTypePut,
			Key:    []byte("a"),
			Value:  []byte("test-value"),
			CRTs:   uint64(1002),
		},
	})
	plr.cli.Returns(&model.RegionFeedEvent{
		Val: &model.RawKVEntry{
			OpType: model.OpTypePut,
			Key:    []byte("d"),
			Value:  []byte("test-value"),
			CRTs:   uint64(1003),
		},
	})
	ev := <-plr.Output()
	c.Assert(ev.OpType, check.Equals, model.OpTypePut)
	c.Assert(ev.Key, check.DeepEquals, []byte("d"))

	store.Close()
	cancel()
	wg.Wait()
}
