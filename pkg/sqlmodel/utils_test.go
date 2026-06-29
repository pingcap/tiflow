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
	"testing"

	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestValuesHolder(t *testing.T) {
	t.Parallel()

	require.Equal(t, "()", valuesHolder(0))
	require.Equal(t, "(?)", valuesHolder(1))
	require.Equal(t, "(?,?)", valuesHolder(2))
}

func TestValidatorGenColData(t *testing.T) {
	res := ColValAsStr(1)
	require.Equal(t, "1", res)
	res = ColValAsStr(1.2)
	require.Equal(t, "1.2", res)
	res = ColValAsStr("abc")
	require.Equal(t, "abc", res)
	res = ColValAsStr([]byte{'\x01', '\x02', '\x03'})
	require.Equal(t, "\x01\x02\x03", res)
	res = ColValAsStr(decimal.NewFromInt(222123123))
	require.Equal(t, "222123123", res)
}

func expressionIndexColumnName(t *testing.T, ti *timodel.TableInfo, indexName string) string {
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

func reorderColumnsByName(t *testing.T, ti *timodel.TableInfo, names ...string) {
	t.Helper()
	require.Len(t, names, len(ti.Columns))

	colsByName := make(map[string]*timodel.ColumnInfo, len(ti.Columns))
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
