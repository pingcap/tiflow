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
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
	"sort"
)

// EventsGroup could store change event message.
type eventsGroup struct {
	events        []*model.RowChangedEvent
	highWatermark uint64
}

// NewEventsGroup will create new event group.
func NewEventsGroup() *eventsGroup {
	return &eventsGroup{
		events: make([]*model.RowChangedEvent, 0),
	}
}

// Append will append an event to event groups.
func (g *eventsGroup) Append(e *model.RowChangedEvent) {
	g.events = append(g.events, e)
	g.highWatermark = e.CommitTs
}

// Resolve will get events where CommitTs is less than resolveTs.
func (g *eventsGroup) Resolve(resolve uint64) []*model.RowChangedEvent {
	i := sort.Search(len(g.events), func(i int) bool {
		return g.events[i].CommitTs > resolve
	})

	result := g.events[:i]
	g.events = g.events[i:]

	if len(g.events) != 0 {
		log.Warn("not all events resolved",
			zap.Int("length", len(g.events)), zap.Uint64("firstCommitTs", g.events[0].CommitTs))
	}

	return result
}
