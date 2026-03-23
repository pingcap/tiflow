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

package conn

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/dbutil"
)

// GetTables returns names of all non-view tables in the specified schema.
// It is compatible with upstreams like PolarDB-X, whose SHOW FULL TABLES
// may return extra columns after the standard table name and table type.
func GetTables(ctx context.Context, db dbutil.QueryExecutor, schemaName string) ([]string, error) {
	query := fmt.Sprintf("SHOW FULL TABLES IN `%s` WHERE Table_Type != 'VIEW';", escapeName(schemaName))
	return queryTables(ctx, db, query)
}

func queryTables(ctx context.Context, db dbutil.QueryExecutor, query string) ([]string, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(columns) < 2 {
		return nil, errors.Errorf("unexpected SHOW FULL TABLES result column count %d, expected at least 2", len(columns))
	}

	tables := make([]string, 0, 8)
	for rows.Next() {
		var table, tableType sql.NullString
		values := make([]any, len(columns))
		values[0] = &table
		values[1] = &tableType
		for i := 2; i < len(values); i++ {
			values[i] = new(sql.RawBytes)
		}

		if err := rows.Scan(values...); err != nil {
			return nil, errors.Trace(err)
		}
		if !table.Valid || !tableType.Valid {
			continue
		}

		tables = append(tables, table.String)
	}

	return tables, errors.Trace(rows.Err())
}

func escapeName(name string) string {
	return strings.ReplaceAll(name, "`", "``")
}
