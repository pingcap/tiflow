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

package filter

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

// cmd: go test -benchmem -run=^$ -bench ^BenchmarkSkipDML$ github.com/pingcap/tiflow/pkg/filter
// goos: maxOS 12.3.1
// goarch: arm64
// cpu: Apple M1 Pro
// BenchmarkSkipDML/insert-1
// BenchmarkSkipDML/insert-1-10              990166              1151 ns/op             768 B/op         24 allocs/op
// BenchmarkSkipDML/insert-2
// BenchmarkSkipDML/insert-2-10             1000000              1187 ns/op             768 B/op         24 allocs/op
// BenchmarkSkipDML/update
// BenchmarkSkipDML/update-10                698208              1637 ns/op            1480 B/op         43 allocs/op
// BenchmarkSkipDML/delete
// BenchmarkSkipDML/delete-10               1000000              1112 ns/op             768 B/op         24 allocs/op
func BenchmarkSkipDML(b *testing.B) {
	t := &testing.T{}
	helper := newTestHelper(t)

	defer helper.close()
	helper.getTk().MustExec("use test;")
	ddl := "create table test.student(id int primary key, name char(50), age int, gender char(10))"
	tableInfo := helper.execDDL(ddl)
	cfg := &config.FilterConfig{
		EventFilters: []*config.EventFilterRule{
			{
				Matcher:                  []string{"test.student"},
				IgnoreInsertValueExpr:    "name = 'Will'",
				IgnoreDeleteValueExpr:    "age >= 32",
				IgnoreUpdateOldValueExpr: "gender = 'female'",
				IgnoreUpdateNewValueExpr: "age > 28",
			},
		},
	}

	sessCtx := utils.NewSessionCtx(map[string]string{
		"time_zone": "UTC",
	})
	f, err := newExprFilter("", cfg)
	require.Nil(b, err)

	type innerCase struct {
		name      string
		schema    string
		table     string
		updateDDl string
		// set preColumns to non nil to indicate this case is for update
		preColumns []*model.Column
		// set columns to non nil to indicate this case is for insert
		// set columns to nil to indicate this case is for delete
		columns []*model.Column
		preRow  []interface{}
		row     []interface{}
		ignore  bool
	}

	cases := []innerCase{
		{ // insert
			name:   "insert-1",
			schema: "test",
			table:  "student",
			columns: []*model.Column{
				{Name: "none"},
			},
			row:    []interface{}{999, "Will", 39, "male"},
			ignore: true,
		},
		{ // insert
			name:   "insert-2",
			schema: "test",
			table:  "student",
			// we execute updateDDl to update the table info
			updateDDl: "ALTER TABLE student ADD COLUMN mather char(50)",
			columns: []*model.Column{
				{Name: "none"},
			},
			row:    []interface{}{999, "Will", 39, "male"},
			ignore: true,
		},
		{ // update
			name:   "update",
			schema: "test",
			table:  "student",
			preColumns: []*model.Column{
				{Name: "none"},
			},
			preRow: []interface{}{876, "Li", 45, "female"},
			columns: []*model.Column{
				{Name: "none"},
			},
			row:    []interface{}{1, "Dongmen", 20, "male"},
			ignore: true,
		},
		{ // delete
			name:   "delete",
			schema: "test",
			table:  "student",
			preColumns: []*model.Column{
				{Name: "none"},
			},
			preRow: []interface{}{876, "Li", 45, "female"},
			ignore: true,
		},
	}

	for _, c := range cases {
		rowDatums, err := utils.AdjustBinaryProtocolForDatum(sessCtx, c.row, tableInfo.Columns)
		require.Nil(t, err)
		preRowDatums, err := utils.AdjustBinaryProtocolForDatum(sessCtx, c.preRow, tableInfo.Columns)
		require.Nil(t, err)
		tableInfo := model.BuildTableInfo(c.schema, c.table, c.columns, nil)
		row := &model.RowChangedEvent{
			TableInfo:  tableInfo,
			Columns:    model.Columns2ColumnDatas(c.columns, tableInfo),
			PreColumns: model.Columns2ColumnDatas(c.preColumns, tableInfo),
		}
		rawRow := model.RowChangedDatums{
			RowDatums:    rowDatums,
			PreRowDatums: preRowDatums,
		}
		b.Run(c.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				ignore, err := f.shouldSkipDML(row, rawRow, tableInfo)
				require.Equal(b, c.ignore, ignore)
				require.Nil(b, err)
			}
		})
	}
}
