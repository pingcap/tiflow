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

package splitter

import (
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tiflow/sync_diff_inspector/utils"
	"go.uber.org/zap"
)

// indexFields wraps the column info for the user config "index-fields".
type indexFields struct {
	cols      []*model.ColumnInfo
	tableInfo *model.TableInfo
	empty     bool
}

func indexFieldsFromConfigString(strFields string, tableInfo *model.TableInfo) (*indexFields, error) {
	if len(strFields) == 0 {
		// Empty option
		return &indexFields{empty: true}, nil
	}

	if tableInfo == nil {
		log.Panic("parsing index fields with empty tableInfo",
			zap.String("index-fields", strFields))
	}

	splitFieldArr := strings.Split(strFields, ",")
	for i := range splitFieldArr {
		splitFieldArr[i] = strings.TrimSpace(splitFieldArr[i])
	}

	fields, err := GetSplitFields(tableInfo, splitFieldArr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Sort the columns to help with comparison.
	sortColsInPlace(fields)

	return &indexFields{
		cols:      fields,
		tableInfo: tableInfo,
	}, nil
}

func (f *indexFields) MatchesIndex(index *model.IndexInfo) bool {
	if f.empty {
		// Default config matches all.
		return true
	}

	// Sanity checks.
	if index == nil {
		log.Panic("matching with empty index")
	}
	if len(f.cols) == 0 {
		log.Panic("unexpected cols with length 0")
	}

	if len(index.Columns) != len(f.cols) {
		// We need an exact match.
		// Lengths not matching eliminates the possibility.
		return false
	}

	indexCols := utils.GetColumnsFromIndex(index, f.tableInfo)
	// Sort for comparison
	sortColsInPlace(indexCols)

	for i := 0; i < len(indexCols); i++ {
		if f.cols[i].ID != indexCols[i].ID {
			return false
		}
	}

	return true
}

func (f *indexFields) Cols() []*model.ColumnInfo {
	return f.cols
}

// IsEmpty returns true if the struct represents an empty
// user-configured "index-fields" option.
func (f *indexFields) IsEmpty() bool {
	return f.empty
}

func sortColsInPlace(cols []*model.ColumnInfo) {
	sort.SliceStable(cols, func(i, j int) bool {
		return cols[i].ID < cols[j].ID
	})
}
