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

package regionspan

import (
	"sort"

	"github.com/pingcap/kvproto/pkg/metapb"
)

// CheckRegionsLeftCover checks whether the regions cover the left part of given span
func CheckRegionsLeftCover(regions []*metapb.Region, span ComparableSpan) bool {
	if len(regions) == 0 {
		return false
	}
	sort.Slice(regions, func(i, j int) bool {
		return StartCompare(regions[i].StartKey, regions[j].StartKey) == -1
	})

	if StartCompare(regions[0].StartKey, span.Start) == 1 {
		return false
	}

	nextStart := regions[0].StartKey
	for _, region := range regions {
		// incontinuous regions
		if StartCompare(nextStart, region.StartKey) != 0 {
			return false
		}
		nextStart = region.EndKey
	}
	return true
}
