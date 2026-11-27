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

import (
	"testing"

	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/util/testutil"
	"github.com/stretchr/testify/require"
)

func TestRowImageLayoutWithInterleavedHiddenColumn(t *testing.T) {
	t.Parallel()

	tableInfo := mockTableInfo(t, "CREATE TABLE t ("+
		"id INT PRIMARY KEY, a INT, b INT, UNIQUE KEY idx_expr ((a + b)))")
	hidden := testutil.HiddenColumnName(t, tableInfo)
	testutil.ReorderColumnsByName(t, tableInfo, "id", "a", hidden, "b")

	layout := NewRowImageLayout(tableInfo, tableInfo)
	require.Equal(t, 3, layout.VisibleColumnCount())
	require.Equal(t, []string{"id", "a", "b"}, columnNames(layout.VisibleColumns()))
	require.Equal(t, 2, layout.valueOffset(3, []any{1, 2, 3}))
	require.Equal(t, 3, layout.valueByOffset(3, []any{1, 2, 3}))

	fullValues, ok := layout.FullValues([]any{1, 2, 3})
	require.True(t, ok)
	require.Equal(t, []any{1, 2, nil, 3}, fullValues)

	sourceColumnCount, ok := layout.SourceColumnCountForVisibleColumnCount(2)
	require.True(t, ok)
	require.Equal(t, 3, sourceColumnCount)

	sourceColumnCount, ok = layout.SourceColumnCountForVisibleColumnCount(3)
	require.True(t, ok)
	require.Equal(t, len(tableInfo.Columns), sourceColumnCount)

	_, ok = layout.SourceColumnCountForVisibleColumnCount(4)
	require.False(t, ok)
}

func columnNames(columns []*timodel.ColumnInfo) []string {
	names := make([]string, 0, len(columns))
	for _, column := range columns {
		names = append(names, column.Name.L)
	}
	return names
}
