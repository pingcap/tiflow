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
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/compat"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/member"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/replication"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/pingcap/tiflow/pkg/upstream"
	"go.uber.org/zap"
)

const (
	// spanRegionLimit is the maximum number of regions a span can cover.
	spanRegionLimit = 50000
	// baseSpanNumberCoefficient is the base coefficient that use to
	// multiply the number of captures to get the number of spans.
	baseSpanNumberCoefficient = 3
)

type splitter interface {
	split(
		ctx context.Context, span tablepb.Span, totalCaptures int,
	) []tablepb.Span
}

type splittedSpans struct {
	byAddTable bool
	spans      []tablepb.Span
}

// Reconciler reconciles span and table mapping, make sure spans are in
// a desired state and covers all table ranges.
type Reconciler struct {
	tableSpans map[model.TableID]splittedSpans
	spanCache  []tablepb.Span

	changefeedID model.ChangeFeedID
	config       *config.ChangefeedSchedulerConfig

	splitter []splitter
}

// NewReconciler returns a Reconciler.
func NewReconciler(
	changefeedID model.ChangeFeedID,
	up *upstream.Upstream,
	config *config.ChangefeedSchedulerConfig,
) (*Reconciler, error) {
	pdapi, err := pdutil.NewPDAPIClient(up.PDClient, up.SecurityConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Reconciler{
		tableSpans:   make(map[int64]splittedSpans),
		changefeedID: changefeedID,
		config:       config,
		splitter: []splitter{
			// write splitter has the highest priority.
			newWriteSplitter(changefeedID, pdapi, config.WriteKeyThreshold),
			newRegionCountSplitter(changefeedID, up.RegionCache, config.RegionThreshold),
		},
	}, nil
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
	replications *spanz.BtreeMap[*replication.ReplicationSet],
	aliveCaptures map[model.CaptureID]*member.CaptureStatus,
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
				for _, splitter := range m.splitter {
					spans = splitter.split(ctx, tableSpan, len(aliveCaptures))
					if len(spans) > 1 {
						break
					}
				}
			}
			m.tableSpans[tableID] = splittedSpans{
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
				zap.String("spanStart", tableStart.String()),
				zap.String("spanEnd", tableEnd.String()),
				zap.String("foundStart", coveredSpans[0].String()),
				zap.String("foundEnd", coveredSpans[len(coveredSpans)-1].String()))
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
			m.tableSpans[tableID] = splittedSpans{
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

const maxSpanNumber = 100

func getSpansNumber(regionNum, captureNum int) int {
	coefficient := captureNum - 1
	if baseSpanNumberCoefficient > coefficient {
		coefficient = baseSpanNumberCoefficient
	}
	spanNum := 1
	if regionNum > 1 {
		// spanNumber = max(captureNum * coefficient, totalRegions / spanRegionLimit)
		spanNum = captureNum * coefficient
		if regionNum/spanRegionLimit > spanNum {
			spanNum = regionNum / spanRegionLimit
		}
	}
	if spanNum > maxSpanNumber {
		spanNum = maxSpanNumber
	}
	return spanNum
}
