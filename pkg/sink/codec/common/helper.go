// Copyright 2023 PingCAP, Inc.
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

package common

import (
	"context"
	"database/sql"
	"fmt"
	"math"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// ColumnsHolder read columns from sql.Rows
type ColumnsHolder struct {
	Values        []interface{}
	ValuePointers []interface{}
	Types         []*sql.ColumnType
}

func newColumnHolder(rows *sql.Rows) (*ColumnsHolder, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, errors.Trace(err)
	}

	values := make([]interface{}, len(columnTypes))
	valuePointers := make([]interface{}, len(columnTypes))
	for i := range values {
		valuePointers[i] = &values[i]
	}

	return &ColumnsHolder{
		Values:        values,
		ValuePointers: valuePointers,
		Types:         columnTypes,
	}, nil
}

// Length return the column count
func (h *ColumnsHolder) Length() int {
	return len(h.Values)
}

// MustQueryTimezone query the timezone from the upstream database
func MustQueryTimezone(ctx context.Context, db *sql.DB) string {
	conn, err := db.Conn(ctx)
	if err != nil {
		log.Panic("establish connection to the upstream tidb failed", zap.Error(err))
	}
	defer conn.Close()

	var timezone string
	query := "SELECT @@global.time_zone"
	err = conn.QueryRowContext(ctx, query).Scan(&timezone)
	if err != nil {
		log.Panic("query timezone failed", zap.Error(err))
	}

	log.Info("query global timezone from the upstream tidb",
		zap.Any("timezone", timezone))
	return timezone
}

// MustSnapshotQuery query the db by the snapshot read with the given commitTs
func MustSnapshotQuery(
	ctx context.Context, db *sql.DB, commitTs uint64, schema, table string, conditions map[string]interface{},
) *ColumnsHolder {
	conn, err := db.Conn(ctx)
	if err != nil {
		log.Panic("establish connection to the upstream tidb failed",
			zap.String("schema", schema), zap.String("table", table),
			zap.Uint64("commitTs", commitTs), zap.Error(err))
	}
	defer conn.Close()

	// 1. set snapshot read
	query := fmt.Sprintf("set @@tidb_snapshot=%d", commitTs)
	_, err = conn.ExecContext(ctx, query)
	if err != nil {
		mysqlErr, ok := errors.Cause(err).(*mysql.MySQLError)
		if ok {
			// Error 8055 (HY000): snapshot is older than GC safe point
			if mysqlErr.Number == 8055 {
				log.Error("set snapshot read failed, since snapshot is older than GC safe point")
			}
		}

		log.Panic("set snapshot read failed",
			zap.String("query", query),
			zap.String("schema", schema), zap.String("table", table),
			zap.Uint64("commitTs", commitTs), zap.Error(err))
	}

	// 2. query the whole row
	query = fmt.Sprintf("select * from %s.%s where ", schema, table)
	var whereClause string
	for name, value := range conditions {
		if whereClause != "" {
			whereClause += " and "
		}
		whereClause += fmt.Sprintf("%s = %v", name, value)
	}
	query += whereClause

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		log.Panic("query row failed",
			zap.String("query", query),
			zap.String("schema", schema), zap.String("table", table),
			zap.Uint64("commitTs", commitTs), zap.Error(err))
	}
	defer rows.Close()

	holder, err := newColumnHolder(rows)
	if err != nil {
		log.Panic("obtain the columns holder failed",
			zap.String("query", query),
			zap.String("schema", schema), zap.String("table", table),
			zap.Uint64("commitTs", commitTs), zap.Error(err))
	}
	for rows.Next() {
		err = rows.Scan(holder.ValuePointers...)
		if err != nil {
			log.Panic("scan row failed",
				zap.String("query", query),
				zap.String("schema", schema), zap.String("table", table),
				zap.Uint64("commitTs", commitTs), zap.Error(err))
		}
	}
	return holder
}

// MustBinaryLiteralToInt convert bytes into uint64,
// by follow https://github.com/pingcap/tidb/blob/e3417913f58cdd5a136259b902bf177eaf3aa637/types/binary_literal.go#L105
func MustBinaryLiteralToInt(bytes []byte) uint64 {
	bytes = trimLeadingZeroBytes(bytes)
	length := len(bytes)

	if length > 8 {
		log.Panic("invalid bit value found", zap.ByteString("value", bytes))
		return math.MaxUint64
	}

	if length == 0 {
		return 0
	}

	// Note: the byte-order is BigEndian.
	val := uint64(bytes[0])
	for i := 1; i < length; i++ {
		val = (val << 8) | uint64(bytes[i])
	}
	return val
}

func trimLeadingZeroBytes(bytes []byte) []byte {
	if len(bytes) == 0 {
		return bytes
	}
	pos, posMax := 0, len(bytes)-1
	for ; pos < posMax; pos++ {
		if bytes[pos] != 0 {
			break
		}
	}
	return bytes[pos:]
}
