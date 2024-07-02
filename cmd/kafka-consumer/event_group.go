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

	"github.com/pingcap/tiflow/cdc/model"
)

// EventsGroup could store change event message.
type eventsGroup struct {
	events []*model.RowChangedEvent
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
}

// Resolve will get events where CommitTs is less than resolveTs.
func (g *eventsGroup) Resolve(resolveTs uint64) []*model.RowChangedEvent {
	sort.Slice(g.events, func(i, j int) bool {
		return g.events[i].CommitTs < g.events[j].CommitTs
	})

	i := sort.Search(len(g.events), func(i int) bool {
		return g.events[i].CommitTs > resolveTs
	})

	result := g.events[:i]
	g.events = g.events[i:]
	return result
}
