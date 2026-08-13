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

package keyspan

import (
	"bytes"
	"unsafe"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/tikv/client-go/v2/tikv"
)

// RegionCache is a simplified interface of tikv.RegionCache.
// It is useful to restrict RegionCache usage and mocking in tests.
type RegionCache interface {
	// LoadRegionsInKeyRange loads regions in [startKey,endKey].
	LoadRegionsInKeyRange(
		bo *tikv.Backoffer, startKey, endKey []byte,
	) (regions []*tikv.Region, err error)
}

// mockCache mocks tikv.RegionCache.
type mockCache struct {
	regions *spanz.BtreeMap[uint64]
}

// NewMockRegionCache returns a new MockCache.
func NewMockRegionCache() *mockCache { return &mockCache{regions: spanz.NewBtreeMap[uint64]()} }

func (m *mockCache) LoadRegionsInKeyRange(
	bo *tikv.Backoffer, startKey, endKey []byte,
) (regions []*tikv.Region, err error) {
	m.regions.Ascend(func(loc tablepb.Span, id uint64) bool {
		if bytes.Compare(loc.StartKey, endKey) >= 0 ||
			bytes.Compare(loc.EndKey, startKey) <= 0 {
			return true
		}
		region := &tikv.Region{}
		meta := &metapb.Region{
			Id:       id,
			StartKey: loc.StartKey,
			EndKey:   loc.EndKey,
		}

		// region.meta is not exported, so we use unsafe to set it for testing.
		regionPtr := (*struct {
			meta *metapb.Region
		})(unsafe.Pointer(region))
		regionPtr.meta = meta

		regions = append(regions, region)
		return true
	})
	return
}

// NewReconcilerForTests returns a Reconciler.
func NewReconcilerForTests(
	cache RegionCache, config *config.ChangefeedSchedulerConfig,
) *Reconciler {
	return &Reconciler{
		tableSpans: make(map[int64]splittedSpans),
		config:     config,
		splitter:   []splitter{newRegionCountSplitter(model.ChangeFeedID{}, cache, config.RegionPerSpan)},
	}
}
