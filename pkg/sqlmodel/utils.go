package sqlmodel

import (
	"strings"

	timodel "github.com/pingcap/tidb/parser/model"
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

func isGenerated(columns []*timodel.ColumnInfo, name timodel.CIStr) bool {
	for _, col := range columns {
		if col.Name.L == name.L {
			return col.IsGenerated()
		}
	}
	return false
}
