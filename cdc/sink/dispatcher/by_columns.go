// Copyright 2021 PingCAP, Inc.
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

package dispatcher

import (
	"strings"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/hash"
)

type columnsDispatcher struct {
	partitionNum  int32
	targetColumns map[string]struct{}
	hasher        *hash.PositionInertia
}

func getTargetColumns(columnNames string) map[string]struct{} {
	targetColumns := make(map[string]struct{})
	names := strings.Split(columnNames, ",")
	for _, column := range names {
		column = strings.Trim(column, " ")
		targetColumns[column] = struct{}{}
	}

	return targetColumns
}

func newColumnsDispatcher(partitionNum int32, s string) *columnsDispatcher {
	// s should be a string in the form like "[column-1,column-2,..., column-n]"
	// we cannot know whether the specified columns exist or not, just assume that all target columns exists.
	columnNames := strings.Trim(s, "[]")
	targetColumns := getTargetColumns(columnNames)
	return &columnsDispatcher{
		partitionNum:  partitionNum,
		targetColumns: targetColumns,
		hasher:        hash.NewPositionInertia(),
	}
}

// Dispatch the row to the target which can be indicated by a number.
// If no target column found, just send it to 0.
// If user set, for table A, by column "a, b, c", but one of the selected column does not exist in A, shall we report the error, or just ignore it.
// todo (Ling Jin): discuss this with other stakeholder.
func (r *columnsDispatcher) Dispatch(row *model.RowChangedEvent) int32 {
	r.hasher.Reset()
	for _, col := range row.Columns {
		if _, ok := r.targetColumns[col.Name]; ok {
			r.hasher.Write([]byte(model.ColumnValueString(col.Value)))
		}
	}

	return int32(r.hasher.Sum32() % uint32(r.partitionNum))
}
