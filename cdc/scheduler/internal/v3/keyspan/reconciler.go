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

// Reconciler reconciles span and table mapping, make sure spans are in
// a desired state and covers all table ranges.
type Reconciler struct {
	tableSpans map[model.TableID][]tablepb.Span
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
		tableSpans:       make(map[int64][]tablepb.Span),
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
	currentTables []model.TableID,
	replications *spanz.Map[*replication.ReplicationSet],
	compat *compat.Compat,
) []tablepb.Span {
	tablesLenEqual := len(currentTables) == len(m.tableSpans)
	allTablesFound := true
	updateCache := false
	for _, tableID := range currentTables {
		if _, ok := m.tableSpans[tableID]; !ok {
			// Find a new table.
			allTablesFound = false
			updateCache = true
		}

		// Reconcile spans from current replications.
		tableStart, tableEnd := spanz.TableIDToComparableRange(tableID)
		coveredSpans, holes := replications.FindHoles(tableStart, tableEnd)
		if len(coveredSpans) == 0 {
			// No such spans in replications.
			if _, ok := m.tableSpans[tableID]; ok {
				// We have seen such spans before, it is impossible.
				log.Panic("schedulerv3: impossible spans",
					zap.String("changefeed", m.changefeedID.ID),
					zap.String("namespace", m.changefeedID.Namespace),
					zap.Int64("tableID", tableID),
					zap.Stringer("spanStart", &tableStart),
					zap.Stringer("spanEnd", &tableEnd))
			}
			// And we have not seen such spans before, maybe:
			// 1. it's a table being added when starting a changefeed
			//    or after owner switch.
			// 4. it's a new table being created by DDL when a changefeed is running.
			tableSpan := spanz.TableIDToComparableSpan(tableID)
			if compat.CheckSpanReplicationEnabled() {
				m.tableSpans[tableID] = m.splitSpan(ctx, tableSpan)
			} else {
				// Do not split table if span replication is not enabled.
				m.tableSpans[tableID] = []tablepb.Span{tableSpan}
			}
			updateCache = true
		} else if len(holes) != 0 {
			// There are some holes in the table span, maybe:
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
			for i := range holes {
				holes[i].TableID = tableID
				m.tableSpans[tableID] = append(coveredSpans, holes[i])
				// TODO: maybe we should split holes too.
			}
			updateCache = true
		} else {
			// Found and no hole, maybe:
			// 2. owner switch and no capture fails.
			m.tableSpans[tableID] = coveredSpans
		}
	}

	// 4. Drop table by DDL.
	// For most of the time, remove tables are unlikely to happen.
	//
	// Fast path for check whether two sets are identical:
	// If the length of currentTables and tableSpan are equal,
	// and for all tables in currentTables have a record in tableSpan.
	if !tablesLenEqual || !allTablesFound {
		// The two sets are not identical. We need to find removed tables.
		// Build a tableID hash set to improve performance.
		currentTableSet := make(map[model.TableID]struct{}, len(currentTables))
		for _, tableID := range currentTables {
			currentTableSet[tableID] = struct{}{}
		}
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
		for _, spans := range m.tableSpans {
			m.spanCache = append(m.spanCache, spans...)
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
