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

const (
	defaultMaxBufferedBytes = 100 * 1024 * 1024
	defaultMaxBufferedCount = 500
)

// EventsGroup could store change event message.
type eventsGroup struct {
	events []*model.RowChangedEvent
	bytes  int
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
	g.bytes += e.ApproximateBytes()
}

// ShouldFlushEvents return true if buffered events too much, to reduce memory usage.
func (g *eventsGroup) ShouldFlushEvents() bool {
	return g.bytes >= defaultMaxBufferedBytes || len(g.events) >= defaultMaxBufferedCount
}

// Resolve will get events where CommitTs is less than resolveTs.
func (g *eventsGroup) Resolve(resolveTs uint64) *eventsGroup {
	sort.Slice(g.events, func(i, j int) bool {
		return g.events[i].CommitTs < g.events[j].CommitTs
	})

	i := sort.Search(len(g.events), func(i int) bool {
		return g.events[i].CommitTs > resolveTs
	})

	result := &eventsGroup{
		events: g.events[:i],
	}
	var bytes int
	for _, e := range result.events {
		bytes += e.ApproximateBytes()
	}
	result.bytes = bytes

	g.events = g.events[i:]
	g.bytes = g.bytes - bytes

	return result
}
