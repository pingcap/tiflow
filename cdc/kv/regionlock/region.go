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

package regionlock

import (
	"sort"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/spanz"
)

// CheckRegionsLeftCover checks whether the regions cover the left part of given span
func CheckRegionsLeftCover(regions []*metapb.Region, span tablepb.Span) bool {
	subRegions := CutRegionsLeftCoverSpan(regions, span)
	return len(regions) > 0 && len(subRegions) == len(regions)
}

// CutRegionsLeftCoverSpan cuts regions at the position which doesn't cover span
// or is incontinuous with the previous one.
func CutRegionsLeftCoverSpan(regions []*metapb.Region, span tablepb.Span) []*metapb.Region {
	if len(regions) == 0 {
		return nil
	}
	sort.Slice(regions, func(i, j int) bool {
		return spanz.StartCompare(regions[i].StartKey, regions[j].StartKey) == -1
	})
	if spanz.StartCompare(regions[0].StartKey, span.StartKey) == 1 {
		return nil
	}

	nextStart := regions[0].StartKey
	for i, region := range regions {
		// incontinuous regions
		if spanz.StartCompare(nextStart, region.StartKey) != 0 {
			return regions[:i]
		}
		nextStart = region.EndKey
	}
	return regions
}
