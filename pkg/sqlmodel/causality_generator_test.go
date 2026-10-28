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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlmodel

import (
	"sync"
	"testing"

	timodel "github.com/pingcap/tidb/pkg/meta/model"
	cdcmodel "github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestCausalityKeyGeneratorIsolation(t *testing.T) {
	schemas := []string{
		"CREATE TABLE t(id INT PRIMARY KEY,s VARCHAR(64), UNIQUE KEY prefix(s(4)))",
		"CREATE TABLE t(id INT,s VARCHAR(64))",
		"CREATE TABLE t(id INT PRIMARY KEY,s VARCHAR(64),UNIQUE KEY expr ((lower(s))))",
	}
	table := &cdcmodel.TableName{Schema: "db", Table: "t"}
	for _, schema := range schemas {
		ti := mockTableInfo(t, schema)
		handle := GetWhereHandle(ti, ti)
		wantRow := NewRowChange(table, nil, []any{1, "AbCdEF"}, []any{2, "ABCdef"}, ti, nil, nil)
		want := wantRow.CausalityKeys() // different handle; shared handle below is cold
		rows := make([]*RowChange, 32)
		for i := range rows {
			rows[i] = NewRowChange(table, nil, []any{1, "AbCdEF"}, []any{2, "ABCdef"}, ti, nil, nil)
			rows[i].SetWhereHandle(handle)
		}
		var wg sync.WaitGroup
		for i := range rows {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				g := NewCausalityKeyGenerator(utils.NewSessionCtx(map[string]string{"time_zone": "UTC"}))
				for j := 0; j < 20; j++ {
					require.Equal(t, want, g.Keys(rows[i]))
					g.Reset()
					require.Empty(t, g.handles)
				}
				require.Same(t, handle, rows[i].whereHandle)
				require.Same(t, utils.ZeroSessionCtx, rows[i].tiSessionCtx)
			}(i)
		}
		wg.Wait()
	}
}

// Compare preparation with the unchanged serial generator for routed FK keys,
// nullable keys, prefix/collation conversion and changing schema handles. The
// generator must not modify the RowChange's evaluation state.
func TestCausalityKeyGeneratorForeignKeysAndSchemaChange(t *testing.T) {
	parent := mockTableInfo(t, "CREATE TABLE p(id INT PRIMARY KEY)")
	source := &cdcmodel.TableName{Schema: "DB", Table: "Child"}
	canonical := &cdcmodel.TableName{Schema: "db", Table: "child"}
	ctx := utils.NewSessionCtx(map[string]string{"time_zone": "+08:00", "sql_mode": "STRICT_ALL_TABLES"})
	g := NewCausalityKeyGenerator(ctx)
	for _, schema := range []string{
		"CREATE TABLE c(id INT PRIMARY KEY, p INT, s VARCHAR(64), UNIQUE KEY u(s(3))) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci",
		"CREATE TABLE c(id INT PRIMARY KEY, p INT, s VARCHAR(64), UNIQUE KEY u((lower(s))))",
	} {
		ti := mockTableInfo(t, schema)
		handle := GetWhereHandle(ti, ti)
		for _, value := range []any{nil, "AbCdEF", "中文字符"} {
			row := NewRowChange(source, nil, []any{1, 10, value}, []any{2, nil, value}, ti, nil, ctx)
			row.SetWhereHandle(handle)
			row.SetCausalityKeySourceTable(canonical)
			row.SetForeignKeyRelations([]ForeignKeyCausalityRelation{{ParentTable: "db.p", ParentColumns: []*timodel.ColumnInfo{parent.Columns[0]}, ChildColumnIdx: []int{1}}})
			want := row.CausalityKeys()
			require.Equal(t, want, g.Keys(row))
			require.Contains(t, want, "10.id.db.p")
			require.Same(t, ctx, row.tiSessionCtx)
			require.Same(t, handle, row.whereHandle)
			g.Reset()
			require.Empty(t, g.handles)
		}
	}
}
