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

package testutil

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

// HiddenColumnName returns the first hidden column name in the table info.
func HiddenColumnName(t testing.TB, ti *model.TableInfo) string {
	t.Helper()

	for _, col := range ti.Columns {
		if col.Hidden {
			return col.Name.L
		}
	}
	require.FailNow(t, "hidden column not found")
	return ""
}

// ExpressionIndexColumnName returns the generated column name for the single-column expression index.
func ExpressionIndexColumnName(t testing.TB, ti *model.TableInfo, indexName string) string {
	t.Helper()

	for _, idx := range ti.Indices {
		if idx.Name.L == indexName {
			require.Len(t, idx.Columns, 1)
			return idx.Columns[0].Name.L
		}
	}
	require.FailNowf(t, "index not found", "index %q not found", indexName)
	return ""
}

// ReorderColumnsByName reorders table info columns by name and refreshes offsets.
func ReorderColumnsByName(t testing.TB, ti *model.TableInfo, names ...string) {
	t.Helper()
	require.Len(t, names, len(ti.Columns))

	colsByName := make(map[string]*model.ColumnInfo, len(ti.Columns))
	for _, col := range ti.Columns {
		colsByName[col.Name.L] = col
	}

	for i, name := range names {
		col := colsByName[name]
		require.NotNilf(t, col, "column %q not found", name)
		ti.Columns[i] = col
		col.Offset = i
	}

	for _, idx := range ti.Indices {
		for _, idxCol := range idx.Columns {
			col := colsByName[idxCol.Name.L]
			require.NotNilf(t, col, "index column %q not found", idxCol.Name.L)
			idxCol.Offset = col.Offset
		}
	}
}
