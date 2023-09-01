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
	Weights      []int
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

	splitInfo := m.splitRegionsByWrittenKeys(span.TableID, regions, spansNum)
	log.Info("schedulerv3: split span by written keys",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID),
		zap.String("span", span.String()),
		zap.Ints("perSpanRegionCounts", splitInfo.RegionCounts),
		zap.Ints("weights", splitInfo.Weights),
		zap.Int("spans", len(splitInfo.Spans)),
		zap.Int("totalCaptures", captureNum),
		zap.Int("writeKeyThreshold", m.writeKeyThreshold),
		zap.Int("spanRegionLimit", spanRegionLimit))

	return splitInfo.Spans
}

// splitRegionsByWrittenKeys returns a slice of regions that evenly split the range by write keys.
// spansNum is the number of splits to make.
func (m *writeSplitter) splitRegionsByWrittenKeys(
	tableID model.TableID,
	regions []pdutil.RegionInfo,
	spansNum int,
) *splitRegionsInfo {
	decodeKey := func(hexkey string) []byte {
		key, _ := hex.DecodeString(hexkey)
		return key
	}
	totalWrite, totalWriteNormalized := uint64(0), uint64(0)
	for i := range regions {
		totalWrite += regions[i].WrittenKeys
		// Override 0 to 1 to reflect the baseline cost of a region.
		// Also, it makes split evenly when there is no write.
		regions[i].WrittenKeys += regionWrittenKeyBase
		totalWriteNormalized += regions[i].WrittenKeys
	}
	// 1. If the total write is less than writeKeyThreshold
	// don't need to split the regions
	if totalWrite < uint64(m.writeKeyThreshold) {
		return &splitRegionsInfo{
			RegionCounts: []int{len(regions)},
			Weights:      []int{int(totalWriteNormalized)},
			Spans: []tablepb.Span{{
				TableID:  tableID,
				StartKey: tablepb.Key(decodeKey(regions[0].StartKey)),
				EndKey:   tablepb.Key(decodeKey(regions[len(regions)-1].EndKey)),
			}},
		}
	}
	// 2. Calculate the writeLimitPerSpan, if one span's write is larger that
	// this number, we should create a new span.
	writeLimitPerSpan := totalWriteNormalized / uint64(spansNum)

	// spans infos.
	var (
		regionCounts = make([]int, 0, spansNum)
		weights      = make([]int, 0, spansNum)
		spans        = make([]tablepb.Span, 0, spansNum)
	)

	// 3. Split the table into pages-1 spans, each span has writeWeightPerSpan weight.
	spanWriteWeight := uint64(0)
	// A span contains regions: [spanStartIndex, spanEndIndex-1]
	spanStartIndex, spanEndIndex := 0, 0
	restSpans := spansNum
	for i := spanStartIndex; i < len(regions); i++ {
		restRegions := len(regions) - i
		spanEndIndex = i
		// If the restSpans count is 1 or the restRegions is less than equal to restSpans,
		// stop split. And we will use the rest regions as the last span.
		if restSpans == 1 {
			break
		}
		regionWriteWeight := regions[i].WrittenKeys
		log.Info("schedulerv3: split span by written keys", zap.Any("regionWriteWeight", regionWriteWeight), zap.Any("writeLimitPerSpan", writeLimitPerSpan))
		// If the spanWriteWeight plus regionWriteWeight is larger than writeLimitPerSpan,
		// then use the region from spanStartIndex to i-1 to as a span and
		// start a new span.
		if i > spanStartIndex && (restSpans >= restRegions ||
			(spanWriteWeight+regionWriteWeight > writeLimitPerSpan)) {
			spans = append(spans, tablepb.Span{
				TableID:  tableID,
				StartKey: tablepb.Key(decodeKey(regions[spanStartIndex].StartKey)),
				EndKey:   tablepb.Key(decodeKey(regions[i-1].EndKey)),
			})
			regionCounts = append(regionCounts, i-spanStartIndex)
			weights = append(weights, int(spanWriteWeight))
			restSpans--
			log.Info("schedulerv3: split span by written keys",
				zap.Any("regionCounts", regionCounts),
				zap.Any("spanWriteWeight", spanWriteWeight),
				zap.Any("start", spanStartIndex),
				zap.Any("idx", i))
			spanWriteWeight = 0
			// update the spanStartIndex to i to start a new span.
			spanStartIndex = i
		} else {
			spanWriteWeight += regionWriteWeight
		}
	}

	// 4. The last span contains the rest regions.
	spans = append(spans, tablepb.Span{
		TableID:  tableID,
		StartKey: tablepb.Key(decodeKey(regions[spanEndIndex].StartKey)),
		EndKey:   tablepb.Key(decodeKey(regions[len(regions)-1].EndKey)),
	})
	regionCounts = append(regionCounts, len(regions)-spanEndIndex)
	spanWriteWeight = 0
	for i := spanEndIndex; i < len(regions); i++ {
		spanWriteWeight += regions[i].WrittenKeys
	}
	weights = append(weights, int(spanWriteWeight))
	log.Info("schedulerv3: split span by written keys", zap.Any("regionCounts", regionCounts), zap.Any("weights", weights), zap.Any("spans", spans))
	return &splitRegionsInfo{
		RegionCounts: regionCounts,
		Weights:      weights,
		Spans:        spans,
	}
}
