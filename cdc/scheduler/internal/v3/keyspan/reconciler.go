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
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/compat"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/replication"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

type splitSpans struct {
	byAddTable bool
	spans      []tablepb.Span
}

// Reconciler reconciles span and table mapping, make sure spans are in
// a desired state and covers all table ranges.
type Reconciler struct {
	tableSpans map[model.TableID]splitSpans
	spanCache  []tablepb.Span

	regionCache      RegionCache
	changefeedID     model.ChangeFeedID
	maxRegionPerSpan int
}

// NewReconciler returns a Reconciler.
func NewReconciler(
	changefeedID model.ChangeFeedID, regionCache RegionCache,
	maxRegionPerSpan int,
) *Reconciler {
	return &Reconciler{
		tableSpans:       make(map[int64]splitSpans),
		regionCache:      regionCache,
		changefeedID:     changefeedID,
		maxRegionPerSpan: maxRegionPerSpan,
	}
}

// Reconcile spans that need to be replicated based on current cluster status.
// It handles following cases:
// 1. Changefeed initialization
// 2. Owner switch.
// 3. Owner switch after some captures fail.
// 4. Add table by DDL.
// 5. Drop table by DDL.
// 6. Some captures fail, does NOT affect spans.
func (m *Reconciler) Reconcile(
	ctx context.Context,
	currentTables *replication.TableRanges,
	replications *spanz.Map[*replication.ReplicationSet],
	compat *compat.Compat,
) []tablepb.Span {
	tablesLenEqual := currentTables.Len() == len(m.tableSpans)
	allTablesFound := true
	updateCache := false
	currentTables.Iter(func(tableID model.TableID, tableStart, tableEnd tablepb.Span) bool {
		if _, ok := m.tableSpans[tableID]; !ok {
			// Find a new table.
			allTablesFound = false
			updateCache = true
		}

		// Reconcile spans from current replications.
		coveredSpans, holes := replications.FindHoles(tableStart, tableEnd)
		if len(coveredSpans) == 0 {
			// No such spans in replications.
			if _, ok := m.tableSpans[tableID]; ok {
				// And we have seen such spans before, it means these spans are
				// not yet be scheduled due to basic scheduler's batch add task
				// rate limit.
				return true
			}
			// And we have not seen such spans before, maybe:
			// 1. it's a table being added when starting a changefeed
			//    or after owner switch.
			// 4. it's a new table being created by DDL when a changefeed is running.
			tableSpan := spanz.TableIDToComparableSpan(tableID)
			spans := []tablepb.Span{tableSpan}
			if compat.CheckSpanReplicationEnabled() {
				spans = m.splitSpan(ctx, tableSpan)
			}
			m.tableSpans[tableID] = splitSpans{
				byAddTable: true,
				spans:      spans,
			}
			updateCache = true
		} else if len(holes) != 0 {
			// There are some holes in the table span, maybe:
			if spans, ok := m.tableSpans[tableID]; ok && spans.byAddTable {
				// These spans are split by reconciler add table. It may be
				// still in progress because of basic scheduler rate limit.
				return true
			}
			// 3. owner switch after some captures failed.
			log.Info("schedulerv3: detect owner switch after captures fail",
				zap.String("changefeed", m.changefeedID.ID),
				zap.String("namespace", m.changefeedID.Namespace),
				zap.Int64("tableID", tableID),
				zap.Int("holes", len(holes)),
				zap.Stringer("spanStart", &tableStart),
				zap.Stringer("spanEnd", &tableEnd),
				zap.Stringer("foundStart", &coveredSpans[0]),
				zap.Stringer("foundEnd", &coveredSpans[len(coveredSpans)-1]))
			spans := make([]tablepb.Span, 0, len(coveredSpans)+len(holes))
			spans = append(spans, coveredSpans...)
			for _, s := range holes {
				spans = append(spans, tablepb.Span{
					TableID:  tableID,
					StartKey: s.StartKey,
					EndKey:   s.EndKey,
				})
				// TODO: maybe we should split holes too.
			}
			m.tableSpans[tableID] = splitSpans{
				byAddTable: false,
				spans:      spans,
			}
			updateCache = true
		} else {
			// Found and no hole, maybe:
			// 2. owner switch and no capture fails.
			ss := m.tableSpans[tableID]
			ss.byAddTable = false
			ss.spans = ss.spans[:0]
			ss.spans = append(ss.spans, coveredSpans...)
			m.tableSpans[tableID] = ss
		}
		return true
	})

	// 4. Drop table by DDL.
	// For most of the time, remove tables are unlikely to happen.
	//
	// Fast path for check whether two sets are identical:
	// If the length of currentTables and tableSpan are equal,
	// and for all tables in currentTables have a record in tableSpan.
	if !tablesLenEqual || !allTablesFound {
		// The two sets are not identical. We need to find removed tables.
		// Build a tableID hash set to improve performance.
		currentTableSet := make(map[model.TableID]struct{}, currentTables.Len())
		currentTables.Iter(func(tableID model.TableID, _, _ tablepb.Span) bool {
			currentTableSet[tableID] = struct{}{}
			return true
		})
		for tableID := range m.tableSpans {
			_, ok := currentTableSet[tableID]
			if !ok {
				// Found dropped table.
				delete(m.tableSpans, tableID)
				updateCache = true
			}
		}
	}

	if updateCache {
		m.spanCache = make([]tablepb.Span, 0)
		for _, ss := range m.tableSpans {
			m.spanCache = append(m.spanCache, ss.spans...)
		}
	}
	return m.spanCache
}

func (m *Reconciler) splitSpan(ctx context.Context, span tablepb.Span) []tablepb.Span {
	bo := tikv.NewBackoffer(ctx, 500)
	regions, err := m.regionCache.ListRegionIDsInKeyRange(bo, span.StartKey, span.EndKey)
	if err != nil {
		log.Warn("schedulerv3: list regions failed, skip split span",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Error(err))
		return []tablepb.Span{span}
	}
	if len(regions) <= m.maxRegionPerSpan {
		return []tablepb.Span{span}
	}

	spans := make([]tablepb.Span, 0, len(regions)/m.maxRegionPerSpan+1)
	start, end := 0, m.maxRegionPerSpan
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
		if end+m.maxRegionPerSpan < len(regions) {
			end = end + m.maxRegionPerSpan
		} else {
			end = len(regions)
		}
	}
	// Make sure spans does not exceed [startKey, endKey).
	spans[0].StartKey = span.StartKey
	spans[len(spans)-1].EndKey = span.EndKey
	return spans
}
