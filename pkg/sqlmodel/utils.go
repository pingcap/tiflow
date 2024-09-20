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

package sqlmodel

import (
	"fmt"
	"strings"

	timodel "github.com/pingcap/tidb/pkg/meta/model"
)

func getColsAndValuesOfIdx(
	columns []*timodel.ColumnInfo,
	indexColumns *timodel.IndexInfo,
	data []interface{},
) ([]*timodel.ColumnInfo, []interface{}) {
	cols := make([]*timodel.ColumnInfo, 0, len(indexColumns.Columns))
	values := make([]interface{}, 0, len(indexColumns.Columns))
	for _, col := range indexColumns.Columns {
		cols = append(cols, columns[col.Offset])
		values = append(values, data[col.Offset])
	}

	return cols, values
}

// valuesHolder gens values holder like (?,?,?).
func valuesHolder(n int) string {
	var builder strings.Builder
	builder.Grow((n-1)*2 + 3)
	builder.WriteByte('(')
	for i := 0; i < n; i++ {
		if i > 0 {
			builder.WriteString(",")
		}
		builder.WriteString("?")
	}
	builder.WriteByte(')')
	return builder.String()
}

// generatedColumnsNameSet returns a set of generated columns' name.
func generatedColumnsNameSet(columns []*timodel.ColumnInfo) map[string]struct{} {
	m := make(map[string]struct{})
	for _, col := range columns {
		if col.IsGenerated() {
			m[col.Name.L] = struct{}{}
		}
	}
	return m
}

// ColValAsStr convert column value as string
func ColValAsStr(v interface{}) string {
	switch dv := v.(type) {
	case []byte:
		return string(dv)
	case string:
		return dv
	}
	return fmt.Sprintf("%v", v)
}
