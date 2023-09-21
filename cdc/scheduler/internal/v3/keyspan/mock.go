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

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/tikv/client-go/v2/tikv"
)

// RegionCache is a simplified interface of tikv.RegionCache.
// It is useful to restrict RegionCache usage and mocking in tests.
type RegionCache interface {
	// ListRegionIDsInKeyRange lists ids of regions in [startKey,endKey].
	ListRegionIDsInKeyRange(
		bo *tikv.Backoffer, startKey, endKey []byte,
	) (regionIDs []uint64, err error)
	// LocateRegionByID searches for the region with ID.
	LocateRegionByID(bo *tikv.Backoffer, regionID uint64) (*tikv.KeyLocation, error)
}

// mockCache mocks tikv.RegionCache.
type mockCache struct {
	regions *spanz.BtreeMap[uint64]
}

// NewMockRegionCache returns a new MockCache.
func NewMockRegionCache() *mockCache { return &mockCache{regions: spanz.NewBtreeMap[uint64]()} }

// ListRegionIDsInKeyRange lists ids of regions in [startKey,endKey].
func (m *mockCache) ListRegionIDsInKeyRange(
	bo *tikv.Backoffer, startKey, endKey []byte,
) (regionIDs []uint64, err error) {
	m.regions.Ascend(func(loc tablepb.Span, id uint64) bool {
		if bytes.Compare(loc.StartKey, endKey) >= 0 ||
			bytes.Compare(loc.EndKey, startKey) <= 0 {
			return true
		}
		regionIDs = append(regionIDs, id)
		return true
	})
	return
}

// LocateRegionByID searches for the region with ID.
func (m *mockCache) LocateRegionByID(
	bo *tikv.Backoffer, regionID uint64,
) (loc *tikv.KeyLocation, err error) {
	m.regions.Ascend(func(span tablepb.Span, id uint64) bool {
		if id != regionID {
			return true
		}
		loc = &tikv.KeyLocation{
			StartKey: span.StartKey,
			EndKey:   span.EndKey,
		}
		return false
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
