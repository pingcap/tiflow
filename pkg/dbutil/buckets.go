// Copyright 2025 PingCAP, Inc.
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

package dbutil

import (
	"context"
	"database/sql"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	tidbdbutil "github.com/pingcap/tidb/pkg/util/dbutil"
	"go.uber.org/zap"
)

type Bucket = tidbdbutil.Bucket
type QueryExecutor = tidbdbutil.QueryExecutor

// GetBucketsInfo selects from stats_buckets in TiDB.
func GetBucketsInfo(ctx context.Context, db QueryExecutor, schema, table string, tableInfo *model.TableInfo) (map[string][]Bucket, error) {
	buckets := make(map[string][]Bucket)

	indices := tidbdbutil.FindAllIndex(tableInfo)
	indexMap := make(map[int64]string, len(indices))
	for _, idx := range indices {
		indexMap[idx.ID] = idx.Name.O
	}
	columnMap := make(map[int64]string, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		columnMap[col.ID] = col.Name.O
	}

	query := `SELECT is_index, hist_id, bucket_id, count, lower_bound, upper_bound
FROM mysql.stats_buckets
WHERE table_id IN (
	SELECT tidb_table_id FROM information_schema.tables WHERE table_schema = ? AND table_name = ?
	UNION ALL
	SELECT tidb_partition_id FROM information_schema.partitions WHERE table_schema = ? AND table_name = ?
)
ORDER BY is_index, hist_id, bucket_id`

	log.Debug("GetBucketsInfo", zap.String("sql", query), zap.String("schema", schema), zap.String("table", table))

	rows, err := db.QueryContext(ctx, query, schema, table, schema, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	for rows.Next() {
		var isIndex, histID, bucketID, count sql.NullInt64
		var lowerBound, upperBound sql.NullString
		if err := rows.Scan(&isIndex, &histID, &bucketID, &count, &lowerBound, &upperBound); err != nil {
			return nil, errors.Trace(err)
		}

		b := Bucket{
			Count:      count.Int64,
			LowerBound: lowerBound.String,
			UpperBound: upperBound.String,
		}

		var key string
		if isIndex.Int64 == 1 {
			key = indexMap[histID.Int64]
		} else {
			key = columnMap[histID.Int64]
		}

		buckets[key] = append(buckets[key], b)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}

	for _, index := range indices {
		if index.Name.O != "PRIMARY" {
			continue
		}
		_, ok := buckets[index.Name.O]
		if !ok && len(index.Columns) == 1 {
			if _, ok := buckets[index.Columns[0].Name.O]; !ok {
				log.Warn("GetBucketsInfo: No primary key buckets found, returning empty buckets",
					zap.String("schema", schema),
					zap.String("table", table),
					zap.String("primary_key_column", index.Columns[0].Name.O))
				return buckets, nil
			}
			buckets[index.Name.O] = buckets[index.Columns[0].Name.O]
			delete(buckets, index.Columns[0].Name.O)
		}
	}

	return buckets, nil
}
