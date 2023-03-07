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
		return nil
	}
	if totalCaptures <= 1 {
		return []tablepb.Span{span}
	}
	info := splitRegionsByWrittenKeys(span.TableID, regions, config.WriteKeyThreshold, totalCaptures)
	log.Info("schedulerv3: split span by written keys",
		zap.Ints("counts", info.Counts),
		zap.Ints("weights", info.Weights),
		zap.String("span", span.String()))
	return info.Spans
}

type splitRegionsInfo struct {
	Counts  []int
	Weights []int
	Spans   []tablepb.Span
}

// splitRegionsByWrittenKeys returns a slice of regions that evenly split the range by write keys.
func splitRegionsByWrittenKeys(
	tableID model.TableID, regions []pdutil.RegionInfo, writeKeyThreshold int, pages int,
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
		// Also it makes split evenly when there is no write throughtput.
		regions[i].WrittenKeys++
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
	for i := 1; i < pages; i++ {
		for idx := pageStartIdx; idx < len(regions); idx++ {
			restPages := pages - i
			restRegions := len(regions) - idx
			pageLastIdx = idx
			currentWrittenKeys := regions[idx].WrittenKeys
			if (idx > pageStartIdx) &&
				((restPages >= restRegions) ||
					(2*accWrittenKeys+currentWrittenKeys > 2*uint64(i)*writtenKeysPerPage)) {
				spans = append(spans, tablepb.Span{
					TableID:  tableID,
					StartKey: tablepb.Key(decodeKey(regions[pageStartIdx].StartKey)),
					EndKey:   tablepb.Key(decodeKey(regions[idx-1].EndKey)),
				})
				counts = append(counts, idx-pageStartIdx)
				weights = append(weights, int(pageWrittenKeys))
				pageWrittenKeys = 0
				pageStartIdx = idx
				break
			}
			pageWrittenKeys += currentWrittenKeys
			accWrittenKeys += currentWrittenKeys
		}
	}
	// Always end with the last region.
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
