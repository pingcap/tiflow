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
	"context"
	"encoding/hex"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"go.uber.org/zap"
)

const regionWrittenKeyBase = 1

type writeSplitter struct {
	changefeedID model.ChangeFeedID
	pdAPIClient  pdutil.PDAPIClient
}

func newWriteSplitter(
	changefeedID model.ChangeFeedID, pdAPIClient pdutil.PDAPIClient,
) *writeSplitter {
	return &writeSplitter{
		changefeedID: changefeedID,
		pdAPIClient:  pdAPIClient,
	}
}

func (m *writeSplitter) split(
	ctx context.Context, span tablepb.Span, totalCaptures int,
	config *config.ChangefeedSchedulerConfig,
) []tablepb.Span {
	if config.WriteKeyThreshold == 0 {
		return nil
	}
	regions, err := m.pdAPIClient.ScanRegions(ctx, span)
	if err != nil {
		// Skip split.
		log.Warn("schedulerv3: scan regions failed, skip split span",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.String("span", span.String()),
			zap.Error(err))
		return nil
	}

	pages := totalCaptures
	if len(regions)/spanRegionLimit > pages {
		pages = len(regions) / spanRegionLimit
	}

	if pages <= 1 {
		log.Warn("schedulerv3: only one capture and the regions number less than"+
			" the maxSpanRegionLimit, skip split span",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.String("span", span.String()),
			zap.Error(err))
		return []tablepb.Span{span}
	}

	info := splitRegionsByWrittenKeys(span.TableID,
		regions,
		config.WriteKeyThreshold,
		pages,
		spanRegionLimit)

	log.Info("schedulerv3: split span by written keys",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID),
		zap.String("span", span.String()),
		zap.Ints("counts", info.Counts),
		zap.Ints("weights", info.Weights),
		zap.Int("spans", len(info.Spans)),
		zap.Int("totalCaptures", totalCaptures),
		zap.Int("writeKeyThreshold", config.WriteKeyThreshold),
		zap.Int("spanRegionLimit", spanRegionLimit))
	return info.Spans
}

type splitRegionsInfo struct {
	Counts  []int
	Weights []int
	Spans   []tablepb.Span
}

// splitRegionsByWrittenKeys returns a slice of regions that evenly split the range by write keys.
// pages is the number of splits to make, actually it is the number of captures.
func splitRegionsByWrittenKeys(
	tableID model.TableID, regions []pdutil.RegionInfo,
	writeKeyThreshold int, pages int, spanRegionLimit int,
) *splitRegionsInfo {
	decodeKey := func(hexkey string) []byte {
		key, _ := hex.DecodeString(hexkey)
		return key
	}
	totalWriteNormalized := uint64(0)
	totalWrite := totalWriteNormalized
	for i := range regions {
		totalWrite += regions[i].WrittenKeys
		// Override 0 to 1 to reflect the baseline cost of a region.
		// Also, it makes split evenly when there is no write.
		regions[i].WrittenKeys += regionWrittenKeyBase
		totalWriteNormalized += regions[i].WrittenKeys
	}
	if totalWrite < uint64(writeKeyThreshold) {
		return &splitRegionsInfo{
			Counts:  []int{len(regions)},
			Weights: []int{int(totalWriteNormalized)},
			Spans: []tablepb.Span{{
				TableID:  tableID,
				StartKey: tablepb.Key(decodeKey(regions[0].StartKey)),
				EndKey:   tablepb.Key(decodeKey(regions[len(regions)-1].EndKey)),
			}},
		}
	}

	writtenKeysPerPage := totalWriteNormalized / uint64(pages)
	counts := make([]int, 0, pages)
	weights := make([]int, 0, pages)
	spans := make([]tablepb.Span, 0, pages)
	accWrittenKeys, pageWrittenKeys := uint64(0), uint64(0)
	pageStartIdx, pageLastIdx := 0, 0
	pageRegionsCount := 0
	// split the table into pages-1 spans, each span has writtenKeysPerPage written keys.
	for i := 1; i < pages; i++ {
		for idx := pageStartIdx; idx < len(regions); idx++ {
			restPages := pages - i
			restRegions := len(regions) - idx
			pageLastIdx = idx
			currentWrittenKeys := regions[idx].WrittenKeys
			// If there is at least one region, and the rest regions can't fill the rest pages or
			// the accWrittenKeys plus currentWrittenKeys is larger than writtenKeysPerPage,
			// then use the region from pageStartIdx to idx-1 to as a span and start a new page.
			if (idx > pageStartIdx) &&
				((restPages >= restRegions) ||
					(accWrittenKeys+currentWrittenKeys > writtenKeysPerPage) ||
					pageRegionsCount >= spanRegionLimit) {
				spans = append(spans, tablepb.Span{
					TableID:  tableID,
					StartKey: tablepb.Key(decodeKey(regions[pageStartIdx].StartKey)),
					EndKey:   tablepb.Key(decodeKey(regions[idx-1].EndKey)),
				})
				counts = append(counts, idx-pageStartIdx)
				weights = append(weights, int(pageWrittenKeys))
				pageWrittenKeys = 0
				pageStartIdx = idx
				accWrittenKeys = 0
				pageRegionsCount = 0
				break
			}
			pageWrittenKeys += currentWrittenKeys
			accWrittenKeys += currentWrittenKeys
			pageRegionsCount++
		}
	}

	// The last span contains the rest regions.
	spans = append(spans, tablepb.Span{
		TableID:  tableID,
		StartKey: tablepb.Key(decodeKey(regions[pageLastIdx].StartKey)),
		EndKey:   tablepb.Key(decodeKey(regions[len(regions)-1].EndKey)),
	})
	counts = append(counts, len(regions)-pageLastIdx)
	pageWrittenKeys = 0
	for idx := pageLastIdx; idx < len(regions); idx++ {
		pageWrittenKeys += regions[idx].WrittenKeys
	}
	weights = append(weights, int(pageWrittenKeys))

	return &splitRegionsInfo{
		Counts:  counts,
		Weights: weights,
		Spans:   spans,
	}
}
