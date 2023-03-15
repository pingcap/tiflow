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

package keyspan

import (
	"bytes"
	"context"
	"math"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

type regionCountSplitter struct {
	changefeedID model.ChangeFeedID
	regionCache  RegionCache
}

func newRegionCountSplitter(
	changefeedID model.ChangeFeedID, regionCache RegionCache,
) *regionCountSplitter {
	return &regionCountSplitter{
		changefeedID: changefeedID,
		regionCache:  regionCache,
	}
}

func (m *regionCountSplitter) split(
	ctx context.Context, span tablepb.Span, totalCaptures int,
	config *config.ChangefeedSchedulerConfig,
) []tablepb.Span {
	bo := tikv.NewBackoffer(ctx, 500)
	regions, err := m.regionCache.ListRegionIDsInKeyRange(bo, span.StartKey, span.EndKey)
	if err != nil {
		log.Warn("schedulerv3: list regions failed, skip split span",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Error(err))
		return []tablepb.Span{span}
	}
	if len(regions) <= config.RegionThreshold || totalCaptures == 0 {
		return []tablepb.Span{span}
	}

	totalRegions := len(regions)
	if totalRegions == 0 {
		totalCaptures = 1
	}
	stepper := newEvenlySplitStepper(totalCaptures, totalRegions)
	spans := make([]tablepb.Span, 0, stepper.SpanCount())
	start, end := 0, stepper.Step()
	for {
		startRegion, err := m.regionCache.LocateRegionByID(bo, regions[start])
		if err != nil {
			log.Warn("schedulerv3: get regions failed, skip split span",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.Error(err))
			return []tablepb.Span{span}
		}
		endRegion, err := m.regionCache.LocateRegionByID(bo, regions[end-1])
		if err != nil {
			log.Warn("schedulerv3: get regions failed, skip split span",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.Error(err))
			return []tablepb.Span{span}
		}
		if len(spans) > 0 &&
			bytes.Compare(spans[len(spans)-1].EndKey, startRegion.StartKey) > 0 {
			log.Warn("schedulerv3: list region out of order detected",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.Stringer("lastSpan", &spans[len(spans)-1]),
				zap.Stringer("region", startRegion))
			return []tablepb.Span{span}
		}
		spans = append(spans, tablepb.Span{
			TableID:  span.TableID,
			StartKey: startRegion.StartKey,
			EndKey:   endRegion.EndKey,
		})

		if end == len(regions) {
			break
		}
		start = end
		step := stepper.Step()
		if end+step < len(regions) {
			end = end + step
		} else {
			end = len(regions)
		}
	}
	// Make sure spans does not exceed [startKey, endKey).
	spans[0].StartKey = span.StartKey
	spans[len(spans)-1].EndKey = span.EndKey
	return spans
}

type evenlySplitStepper struct {
	spanCount          int
	regionPerSpan      int
	extraRegionPerSpan int
	remain             int
}

func newEvenlySplitStepper(totalCaptures int, totalRegion int) evenlySplitStepper {
	extraRegionPerSpan := 0
	regionPerSpan, remain := totalRegion/totalCaptures, totalRegion%totalCaptures
	if regionPerSpan == 0 {
		regionPerSpan = 1
		extraRegionPerSpan = 0
		totalCaptures = totalRegion
	} else if remain != 0 {
		// Evenly distributes the remaining regions.
		extraRegionPerSpan = int(math.Ceil(float64(remain) / float64(totalCaptures)))
	}
	return evenlySplitStepper{
		regionPerSpan:      regionPerSpan,
		spanCount:          totalCaptures,
		extraRegionPerSpan: extraRegionPerSpan,
		remain:             remain,
	}
}

func (e *evenlySplitStepper) SpanCount() int {
	return e.spanCount
}

func (e *evenlySplitStepper) Step() int {
	if e.remain <= 0 {
		return e.regionPerSpan
	}
	e.remain = e.remain - e.extraRegionPerSpan
	return e.regionPerSpan + e.extraRegionPerSpan
}
