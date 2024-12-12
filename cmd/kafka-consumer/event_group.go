// Copyright 2024 PingCAP, Inc.
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

package main

import (
	"sort"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"go.uber.org/zap"
)

// EventsGroup could store change event message.
type eventsGroup struct {
	partition int32
	tableID   int64

	events        []*model.RowChangedEvent
	highWatermark uint64
}

// NewEventsGroup will create new event group.
func NewEventsGroup(partition int32, tableID int64) *eventsGroup {
	return &eventsGroup{
		partition: partition,
		tableID:   tableID,
		events:    make([]*model.RowChangedEvent, 0),
	}
}

// Append will append an event to event groups.
func (g *eventsGroup) Append(row *model.RowChangedEvent, offset kafka.Offset) {
	g.events = append(g.events, row)
	if row.CommitTs > g.highWatermark {
		g.highWatermark = row.CommitTs
	}
	log.Info("DML event received",
		zap.Int32("partition", g.partition),
		zap.Any("offset", offset),
		zap.Uint64("commitTs", row.CommitTs),
		zap.Uint64("highWatermark", g.highWatermark),
		zap.Int64("physicalTableID", row.GetTableID()),
		zap.String("schema", row.TableInfo.GetSchemaName()),
		zap.String("table", row.TableInfo.GetTableName()),
		zap.Any("columns", row.Columns), zap.Any("preColumns", row.PreColumns))
}

// Resolve will get events where CommitTs is less than resolveTs.
func (g *eventsGroup) Resolve(resolve uint64, protocol config.Protocol) []*model.RowChangedEvent {
	switch protocol {
	case config.ProtocolCanalJSON:
		sort.Slice(g.events, func(i, j int) bool {
			return g.events[i].CommitTs < g.events[j].CommitTs
		})
	default:
		if !sort.SliceIsSorted(g.events, func(i, j int) bool {
			return g.events[i].CommitTs < g.events[j].CommitTs
		}) {
			log.Warn("events are not sorted", zap.Int32("partition", g.partition),
				zap.Int64("tableID", g.tableID), zap.Int("eventCount", len(g.events)))
		}
	}

	i := sort.Search(len(g.events), func(i int) bool {
		return g.events[i].CommitTs > resolve
	})

	result := g.events[:i]
	g.events = g.events[i:]

	if len(result) != 0 && len(g.events) != 0 {
		log.Warn("not all events resolved",
			zap.Int32("partition", g.partition), zap.Int64("tableID", g.tableID),
			zap.Int("resolved", len(result)), zap.Int("remained", len(g.events)),
			zap.Uint64("resolveTs", resolve), zap.Uint64("firstCommitTs", g.events[0].CommitTs))
	}

	return result
}
