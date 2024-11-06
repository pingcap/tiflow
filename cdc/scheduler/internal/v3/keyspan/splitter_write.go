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
	"github.com/pingcap/tiflow/pkg/pdutil"
	"go.uber.org/zap"
)

const regionWrittenKeyBase = 1

type writeSplitter struct {
	changefeedID      model.ChangeFeedID
	pdAPIClient       pdutil.PDAPIClient
	writeKeyThreshold int
}

type splitRegionsInfo struct {
	RegionCounts []int
	Weights      []uint64
	WriteKeys    []uint64
	Spans        []tablepb.Span
}

func newWriteSplitter(
	changefeedID model.ChangeFeedID,
	pdAPIClient pdutil.PDAPIClient,
	writeKeyThreshold int,
) *writeSplitter {
	return &writeSplitter{
		changefeedID:      changefeedID,
		pdAPIClient:       pdAPIClient,
		writeKeyThreshold: writeKeyThreshold,
	}
}

func (m *writeSplitter) split(
	ctx context.Context,
	span tablepb.Span,
	captureNum int,
) []tablepb.Span {
	if m.writeKeyThreshold == 0 {
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

	spansNum := getSpansNumber(len(regions), captureNum)
	if spansNum <= 1 {
		log.Warn("schedulerv3: only one capture and the regions number less than"+
			" the maxSpanRegionLimit, skip split span",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.String("span", span.String()),
			zap.Error(err))
		return []tablepb.Span{span}
	}

	splitInfo := m.splitRegionsByWrittenKeysV1(span.TableID, regions, spansNum)
	log.Info("schedulerv3: split span by written keys",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID),
		zap.String("span", span.String()),
		zap.Ints("perSpanRegionCounts", splitInfo.RegionCounts),
		zap.Uint64s("weights", splitInfo.Weights),
		zap.Int("spans", len(splitInfo.Spans)),
		zap.Int("totalCaptures", captureNum),
		zap.Int("writeKeyThreshold", m.writeKeyThreshold),
		zap.Int("spanRegionLimit", spanRegionLimit))

	return splitInfo.Spans
}

// splitRegionsByWrittenKeysV1 tries to split the regions into at least `baseSpansNum` spans,
// each span has approximately the same write weight.
// The algorithm is:
//  1. Sum the written keys of all regions, and normalize the written keys of each region by
//     adding baseline weights (regionWrittenKeyBase) to each region's written keys. Which takes
//     the region number into account.
//  2. Calculate the writeLimitPerSpan.
//  3. Split the table into spans:
//     3.1 If the total write is less than writeKeyThreshold, don't need to split the regions.
//     3.2 If the restSpans count is one, and the restWeight is less than writeLimitPerSpan,
//     we will use the rest regions as the last span. If the restWeight is larger than writeLimitPerSpan,
//     then we need to add more restSpans (restWeight / writeLimitPerSpan) to split the rest regions.
//     3.3 If the restRegions is less than equal to restSpans, then every region will be a span.
//     3.4 If the spanWriteWeight is larger than writeLimitPerSpan or the regionCount is larger
//     than spanRegionLimit, then use the region range from spanStartIndex to i to as a span.
//  4. Return the split result.
func (m *writeSplitter) splitRegionsByWrittenKeysV1(
	tableID model.TableID,
	regions []pdutil.RegionInfo,
	baseSpansNum int,
) *splitRegionsInfo {
	decodeKey := func(hexkey string) []byte {
		key, _ := hex.DecodeString(hexkey)
		return key
	}

	totalWrite, totalWriteNormalized := uint64(0), uint64(0)
	for i := range regions {
		totalWrite += regions[i].WrittenKeys
		regions[i].WrittenKeys += regionWrittenKeyBase
		totalWriteNormalized += regions[i].WrittenKeys
	}

	// 1. If the total write is less than writeKeyThreshold
	// don't need to split the regions
	if totalWrite < uint64(m.writeKeyThreshold) {
		return &splitRegionsInfo{
			RegionCounts: []int{len(regions)},
			Weights:      []uint64{totalWriteNormalized},
			Spans: []tablepb.Span{{
				TableID:  tableID,
				StartKey: tablepb.Key(decodeKey(regions[0].StartKey)),
				EndKey:   tablepb.Key(decodeKey(regions[len(regions)-1].EndKey)),
			}},
		}
	}

	// 2. Calculate the writeLimitPerSpan, if one span's write is larger that
	// this number, we should create a new span.
	writeLimitPerSpan := totalWriteNormalized / uint64(baseSpansNum)

	// The result of this method
	var (
		regionCounts = make([]int, 0, baseSpansNum)
		writeKeys    = make([]uint64, 0, baseSpansNum)
		weights      = make([]uint64, 0, baseSpansNum)
		spans        = make([]tablepb.Span, 0, baseSpansNum)
	)

	// Temp variables used in the loop
	var (
		spanWriteWeight = uint64(0)
		spanStartIndex  = 0
		restSpans       = baseSpansNum
		regionCount     = 0
		restWeight      = int64(totalWriteNormalized)
	)

	// 3. Split the table into spans, each span has approximately
	// `writeWeightPerSpan` weight or `spanRegionLimit` regions.
	for i := 0; i < len(regions); i++ {
		restRegions := len(regions) - i
		regionCount++
		spanWriteWeight += regions[i].WrittenKeys
		// If the restSpans count is one, and the restWeight is less than equal to writeLimitPerSpan,
		// we will use the rest regions as the last span. If the restWeight is larger than writeLimitPerSpan,
		// then we need to add more restSpans (restWeight / writeLimitPerSpan) + 1 to split the rest regions.
		if restSpans == 1 {
			if restWeight <= int64(writeLimitPerSpan) {
				spans = append(spans, tablepb.Span{
					TableID:  tableID,
					StartKey: tablepb.Key(decodeKey(regions[spanStartIndex].StartKey)),
					EndKey:   tablepb.Key(decodeKey(regions[len(regions)-1].EndKey)),
				})

				lastSpanRegionCount := len(regions) - spanStartIndex
				lastSpanWriteWeight := uint64(0)
				lastSpanWriteKey := uint64(0)
				for j := spanStartIndex; j < len(regions); j++ {
					lastSpanWriteKey += regions[j].WrittenKeys
					lastSpanWriteWeight += regions[j].WrittenKeys
				}
				regionCounts = append(regionCounts, lastSpanRegionCount)
				weights = append(weights, lastSpanWriteWeight)
				writeKeys = append(writeKeys, lastSpanWriteKey)
				spanStartIndex = len(regions)
				break
			}
			// If the restWeight is larger than writeLimitPerSpan,
			// then we need to update the restSpans.
			restSpans = int(restWeight)/int(writeLimitPerSpan) + 1
		}

		// If the restRegions is less than equal to restSpans,
		// then every region will be a span.
		if restRegions <= restSpans {
			spans = append(spans, tablepb.Span{
				TableID:  tableID,
				StartKey: tablepb.Key(decodeKey(regions[spanStartIndex].StartKey)),
				EndKey:   tablepb.Key(decodeKey(regions[i].EndKey)),
			})
			regionCounts = append(regionCounts, regionCount)
			weights = append(weights, spanWriteWeight)

			// reset the temp variables to start a new span
			restSpans--
			restWeight -= int64(spanWriteWeight)
			spanWriteWeight = 0
			regionCount = 0
			spanStartIndex = i + 1
			continue
		}

		// If the spanWriteWeight is larger than writeLimitPerSpan or the regionCount
		// is larger than spanRegionLimit, then use the region range from
		// spanStartIndex to i to as a span.
		if spanWriteWeight > writeLimitPerSpan || regionCount >= spanRegionLimit {
			spans = append(spans, tablepb.Span{
				TableID:  tableID,
				StartKey: tablepb.Key(decodeKey(regions[spanStartIndex].StartKey)),
				EndKey:   tablepb.Key(decodeKey(regions[i].EndKey)),
			})
			regionCounts = append(regionCounts, regionCount)
			weights = append(weights, spanWriteWeight)
			// reset the temp variables to start a new span
			restSpans--
			restWeight -= int64(spanWriteWeight)
			spanWriteWeight = 0
			regionCount = 0
			spanStartIndex = i + 1
		}
	}
	// All regions should be processed and append to spans
	if spanStartIndex != len(regions) {
		spans = append(spans, tablepb.Span{
			TableID:  tableID,
			StartKey: tablepb.Key(decodeKey(regions[spanStartIndex].StartKey)),
			EndKey:   tablepb.Key(decodeKey(regions[len(regions)-1].EndKey)),
		})
		lastSpanRegionCount := len(regions) - spanStartIndex
		lastSpanWriteWeight := uint64(0)
		lastSpanWriteKey := uint64(0)
		for j := spanStartIndex; j < len(regions); j++ {
			lastSpanWriteKey += regions[j].WrittenKeys
			lastSpanWriteWeight += regions[j].WrittenKeys
		}
		regionCounts = append(regionCounts, lastSpanRegionCount)
		weights = append(weights, lastSpanWriteWeight)
		writeKeys = append(writeKeys, lastSpanWriteKey)
		log.Warn("some regions are added to the last span, it should not appear",
			zap.Int("spanStartIndex", spanStartIndex),
			zap.Int("regionsLength", len(regions)),
			zap.Int("restSpans", restSpans),
			zap.Int64("restWeight", restWeight),
			zap.Any("prevSpan", spans[len(spans)-2]),
			zap.Any("lastSpan", spans[len(spans)-1]),
		)
	}
	return &splitRegionsInfo{
		RegionCounts: regionCounts,
		Weights:      weights,
		WriteKeys:    writeKeys,
		Spans:        spans,
	}
}
