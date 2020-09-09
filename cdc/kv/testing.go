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

package kv

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/txnutil"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

var genValueID int

func genValue() []byte {
	genValueID++
	return []byte("value_" + strconv.Itoa(genValueID))
}

type eventChecker struct {
	t       require.TestingT
	eventCh chan *model.RegionFeedEvent
	closeCh chan struct{}

	vals        []*model.RawKVEntry
	checkpoints []*model.ResolvedSpan
}

func valInSlice(val *model.RawKVEntry, vals []*model.RawKVEntry) bool {
	for _, v := range vals {
		if val.CRTs == v.CRTs && bytes.Equal(val.Key, v.Key) {
			return true
		}
	}
	return false
}

func newEventChecker(t require.TestingT) *eventChecker {
	ec := &eventChecker{
		t:       t,
		eventCh: make(chan *model.RegionFeedEvent),
		closeCh: make(chan struct{}),
	}

	go func() {
		for {
			select {
			case e := <-ec.eventCh:
				log.Debug("get event", zap.Reflect("event", e))
				if e.Val != nil {
					// check if the value event break the checkpoint guarantee
					for _, cp := range ec.checkpoints {
						if !regionspan.KeyInSpan(e.Val.Key, cp.Span) ||
							e.Val.CRTs > cp.ResolvedTs {
							continue
						}

						if !valInSlice(e.Val, ec.vals) {
							require.FailNowf(t, "unexpected value event", "value: %+v checkpoint: %+v", e.Val, cp)
						}
					}

					ec.vals = append(ec.vals, e.Val)
				} else {
					ec.checkpoints = append(ec.checkpoints, e.Resolved)
				}
			case <-ec.closeCh:
				return
			}
		}
	}()

	return ec
}

// stop the checker
func (ec *eventChecker) stop() {
	close(ec.closeCh)
}

// CreateStorage creates a tikv Storage instance.
func CreateStorage(pdAddr string) (storage kv.Storage, err error) {
	tiPath := fmt.Sprintf("tikv://%s?disableGC=true", pdAddr)
	err = store.Register("tikv", tikv.Driver{})
	if err != nil && !strings.Contains(err.Error(), "already registered") {
		return
	}
	storage, err = store.New(tiPath)
	return
}

func mustGetTimestamp(t require.TestingT, storage kv.Storage) uint64 {
	ts, err := storage.GetOracle().GetTimestamp(context.Background())
	require.NoError(t, err)

	return ts
}

func mustGetValue(t require.TestingT, eventCh <-chan *model.RegionFeedEvent, value []byte) {
	timeout := time.After(time.Second * 20)

	for {
		select {
		case e := <-eventCh:
			if e.Val != nil && bytes.Equal(e.Val.Value, value) {
				return
			}
		case <-timeout:
			require.FailNowf(t, "timeout to get value", "value: %v", value)
		}
	}
}

type mockPullerInit struct{}

func (*mockPullerInit) IsInitialized() bool {
	return true
}

// TestSplit try split on every region, and test can get value event from
// every region after split.
func TestSplit(t require.TestingT, pdCli pd.Client, storage kv.Storage) {
	cli, err := NewCDCClient(context.Background(), pdCli, storage.(tikv.Storage), &security.Credential{})
	require.NoError(t, err)
	defer cli.Close()

	eventCh := make(chan *model.RegionFeedEvent, 1<<20)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startTS := mustGetTimestamp(t, storage)

	lockresolver := txnutil.NewLockerResolver(storage.(tikv.Storage))
	isPullInit := &mockPullerInit{}
	go func() {
		err := cli.EventFeed(ctx, regionspan.ComparableSpan{Start: nil, End: nil}, startTS, false, lockresolver, isPullInit, eventCh)
		require.Equal(t, err, context.Canceled)
	}()

	preRegions, _, err := pdCli.ScanRegions(context.Background(), nil, nil, 10000)
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		regions := preRegions
		// In second loop try split every region.
		if i == 1 {
			splitStore, ok := storage.(kv.SplittableStore)
			require.True(t, ok)
			for _, r := range preRegions {
				splitKey := r.GetStartKey()
				if len(splitKey) == 0 {
					splitKey = []byte{0}
				} else {
					splitKey = append(splitKey, 0)
				}
				splitKeys := [][]byte{splitKey}
				_, err := splitStore.SplitRegions(context.Background(), splitKeys, false)
				require.NoError(t, err)
			}

			time.Sleep(time.Second * 3)

			var afterRegions []*metapb.Region
			afterRegions, _, err = pdCli.ScanRegions(context.Background(), nil, nil, 10000)
			require.NoError(t, err)
			require.Greater(t, len(afterRegions), len(preRegions))

			regions = afterRegions
		}

		// Put a key on every region and check we can get the event.
		for _, r := range regions {
			key := r.GetStartKey()
			if len(key) == 0 {
				key = []byte{0}
			}
			value := genValue()

			var tx kv.Transaction
			tx, err = storage.Begin()
			require.NoError(t, err)
			err = tx.Set(key, value)
			require.NoError(t, err)
			err = tx.Commit(ctx)
			require.NoError(t, err)

			mustGetValue(t, eventCh, value)
		}
	}
}

func mustSetKey(t require.TestingT, storage kv.Storage, key []byte, value []byte) {
	tx, err := storage.Begin()
	require.NoError(t, err)
	err = tx.Set(key, value)
	require.NoError(t, err)
	err = tx.Commit(context.Background())
	require.NoError(t, err)
}

func mustDeleteKey(t require.TestingT, storage kv.Storage, key []byte) {
	tx, err := storage.Begin()
	require.NoError(t, err)
	err = tx.Delete(key)
	require.NoError(t, err)
	err = tx.Commit(context.Background())
	require.NoError(t, err)
}

// TestGetKVSimple test simple KV operations
func TestGetKVSimple(t require.TestingT, pdCli pd.Client, storage kv.Storage) {
	cli, err := NewCDCClient(context.Background(), pdCli, storage.(tikv.Storage), &security.Credential{})
	require.NoError(t, err)
	defer cli.Close()

	checker := newEventChecker(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startTS := mustGetTimestamp(t, storage)
	lockresolver := txnutil.NewLockerResolver(storage.(tikv.Storage))
	isPullInit := &mockPullerInit{}
	go func() {
		err := cli.EventFeed(ctx, regionspan.ComparableSpan{Start: nil, End: nil}, startTS, false, lockresolver, isPullInit, checker.eventCh)
		require.Equal(t, err, context.Canceled)
	}()

	key := []byte("s1")
	value := []byte("s1v")

	// set
	mustSetKey(t, storage, key, value)

	// delete
	mustDeleteKey(t, storage, key)

	// set again
	mustSetKey(t, storage, key, value)

	for i := 0; i < 2; i++ {
		// start a new EventFeed with the startTS before the kv operations should also get the same events.
		// This can test the initialize case.
		if i == 1 {
			checker = newEventChecker(t)
			go func() {
				err := cli.EventFeed(ctx, regionspan.ComparableSpan{Start: nil, End: nil}, startTS, false, lockresolver, isPullInit, checker.eventCh)
				require.Equal(t, err, context.Canceled)
			}()
		}

		time.Sleep(5 * time.Second)
		checker.stop()

		// filter the unrelated keys event.
		var vals []*model.RawKVEntry
		for _, v := range checker.vals {
			if bytes.Equal(v.Key, key) {
				vals = append(vals, v)
			}
		}
		checker.vals = vals

		// check we can get the events.
		require.Len(t, checker.vals, 3)
		require.Equal(t, checker.vals[0].OpType, model.OpTypePut)
		require.Equal(t, checker.vals[0].Key, key)
		require.Equal(t, checker.vals[0].Value, value)

		require.Equal(t, checker.vals[1].OpType, model.OpTypeDelete)
		require.Equal(t, checker.vals[1].Key, key)

		require.Equal(t, checker.vals[2].OpType, model.OpTypePut)
		require.Equal(t, checker.vals[2].Key, key)
		require.Equal(t, checker.vals[2].Value, value)
	}
}
