// Copyright 2021 PingCAP, Inc.
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

package dbconn

import (
	"fmt"

	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

// GetTableCreateSQL gets table create sql by 'show create table schema.table'.
func GetTableCreateSQL(tctx *tcontext.Context, conn *DBConn, tableID string) (sql string, err error) {
	querySQL := fmt.Sprintf("SHOW CREATE TABLE %s", tableID)
	var table, createStr string

	rows, err := conn.QuerySQL(tctx, querySQL)
	if err != nil {
		return "", terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}

	defer rows.Close()
	if rows.Next() {
		if scanErr := rows.Scan(&table, &createStr); scanErr != nil {
			return "", terror.DBErrorAdapt(scanErr, terror.ErrDBDriverError)
		}
	} else {
		return "", terror.ErrSyncerDownstreamTableNotFound.Generate(tableID)
	}

	if err = rows.Close(); err != nil {
		return "", terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError)
	}
	return createStr, nil
}
