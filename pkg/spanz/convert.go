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

package spanz

import (
	"sort"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/regionspan"
)

// ArrayToSpan converts an array of TableID to an array of Span.
func ArrayToSpan(in []model.TableID) []tablepb.Span {
	out := make([]tablepb.Span, 0, len(in))
	for _, tableID := range in {
		out = append(out, tablepb.Span{TableID: tableID})
	}
	return out
}

// TableIDToComparableSpan converts a TableID to a Span whose
// StartKey and EndKey are encoded in Comparable format.
func TableIDToComparableSpan(tableID model.TableID) tablepb.Span {
	tableSpan := regionspan.ToComparableSpan(regionspan.GetTableSpan(tableID))
	return tablepb.Span{
		TableID:  tableID,
		StartKey: tableSpan.Start,
		EndKey:   tableSpan.End,
	}
}

// TableIDToComparableRange returns a range of a table,
// start and end are encoded in Comparable format.
func TableIDToComparableRange(tableID model.TableID) (start, end tablepb.Span) {
	tableSpan := TableIDToComparableSpan(tableID)
	start = tableSpan
	start.EndKey = nil
	end = tableSpan
	end.StartKey = tableSpan.EndKey
	end.EndKey = nil
	return
}

type sortableSpans []tablepb.Span

func (a sortableSpans) Len() int           { return len(a) }
func (a sortableSpans) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a sortableSpans) Less(i, j int) bool { return a[i].Less(&a[j]) }

// Sort sorts a slice of Span.
func Sort(spans []tablepb.Span) {
	sort.Sort(sortableSpans(spans))
}
