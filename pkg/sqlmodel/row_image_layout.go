// Copyright 2026 PingCAP, Inc.
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

package sqlmodel

import "github.com/pingcap/tidb/pkg/meta/model"

// RowImageLayout describes how a binlog row image maps to source table columns.
type RowImageLayout struct {
	columns                     []*model.ColumnInfo
	visibleColumns              []*model.ColumnInfo
	visibleOffsetByColumnOffset []int
	writableColumns             []*model.ColumnInfo
}

// NewRowImageLayout creates a RowImageLayout from source and target TableInfo.
func NewRowImageLayout(source, target *model.TableInfo) RowImageLayout {
	return NewRowImageLayoutFromColumns(source.Columns, target.Columns)
}

// NewRowImageLayoutFromColumns creates a RowImageLayout from source and target columns.
func NewRowImageLayoutFromColumns(sourceColumns, targetColumns []*model.ColumnInfo) RowImageLayout {
	visibleColumns := VisibleColumns(sourceColumns)
	visibleOffsetByColumnOffset := make([]int, len(sourceColumns))
	for i := range visibleOffsetByColumnOffset {
		visibleOffsetByColumnOffset[i] = -1
	}
	for i, column := range visibleColumns {
		visibleOffsetByColumnOffset[column.Offset] = i
	}

	layout := RowImageLayout{
		columns:                     sourceColumns,
		visibleColumns:              visibleColumns,
		visibleOffsetByColumnOffset: visibleOffsetByColumnOffset,
	}
	if targetColumns != nil {
		layout.writableColumns = writableSourceColumns(visibleColumns, targetColumns)
	}
	return layout
}

// VisibleColumns returns the visible columns from the given table columns.
func VisibleColumns(columns []*model.ColumnInfo) []*model.ColumnInfo {
	ret := make([]*model.ColumnInfo, 0, len(columns))
	for _, col := range columns {
		if !col.Hidden {
			ret = append(ret, col)
		}
	}
	return ret
}

// VisibleColumnCount returns the number of visible columns.
func VisibleColumnCount(columns []*model.ColumnInfo) int {
	count := 0
	for _, col := range columns {
		if !col.Hidden {
			count++
		}
	}
	return count
}

// VisibleColumns returns the visible source columns in the row image.
func (l RowImageLayout) VisibleColumns() []*model.ColumnInfo {
	return l.visibleColumns
}

// VisibleColumnCount returns the visible source column count in the row image.
func (l RowImageLayout) VisibleColumnCount() int {
	return len(l.visibleColumns)
}

// WritableColumns returns visible source columns that can be written to the target table.
func (l RowImageLayout) WritableColumns() []*model.ColumnInfo {
	return l.writableColumns
}

// FullValues expands a visible-only row image to the source table column layout.
func (l RowImageLayout) FullValues(row []any) ([]any, bool) {
	if len(row) == len(l.columns) {
		return row, true
	}
	if len(row) != len(l.visibleColumns) {
		return nil, false
	}

	fullValues := make([]any, len(l.columns))
	for i, col := range l.visibleColumns {
		fullValues[col.Offset] = row[i]
	}
	return fullValues, true
}

// SourceColumnCountForVisibleColumnCount returns the source-column prefix width
// that contains exactly columnCount visible columns.
func (l RowImageLayout) SourceColumnCountForVisibleColumnCount(columnCount int) (int, bool) {
	visibleCount := 0
	sourceColumnCount := len(l.columns)
	for i, col := range l.columns {
		if col.Hidden {
			continue
		}
		visibleCount++
		if visibleCount > columnCount {
			sourceColumnCount = i
			break
		}
	}
	if visibleCount < columnCount {
		return 0, false
	}
	return sourceColumnCount, true
}

func (l RowImageLayout) isFullValues(values []any) bool {
	return len(values) == len(l.columns)
}

func (l RowImageLayout) columnsForValues(values []any) []*model.ColumnInfo {
	if l.isFullValues(values) {
		return l.columns
	}
	return l.visibleColumns
}

func (l RowImageLayout) columnsAndValuesByIndex(
	indexInfo *model.IndexInfo,
	values []any,
) ([]*model.ColumnInfo, []any) {
	cols := make([]*model.ColumnInfo, 0, len(indexInfo.Columns))
	vals := make([]any, 0, len(indexInfo.Columns))
	for _, column := range indexInfo.Columns {
		offset := l.valueOffset(column.Offset, values)
		cols = append(cols, l.columns[column.Offset])
		vals = append(vals, values[offset])
	}
	return cols, vals
}

func (l RowImageLayout) valuesByIndex(indexInfo *model.IndexInfo, values []any) []any {
	ret := make([]any, 0, len(indexInfo.Columns))
	if values == nil {
		return ret
	}
	for _, column := range indexInfo.Columns {
		offset := l.valueOffset(column.Offset, values)
		ret = append(ret, values[offset])
	}
	return ret
}

func (l RowImageLayout) valueOffset(columnOffset int, values []any) int {
	if l.isFullValues(values) {
		return columnOffset
	}
	return l.visibleOffsetByColumnOffset[columnOffset]
}

func (l RowImageLayout) valueByOffset(columnOffset int, values []any) any {
	return values[l.valueOffset(columnOffset, values)]
}
