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
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	tmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	tidbdbutil "github.com/pingcap/tidb/pkg/util/dbutil"
	"go.uber.org/zap"
)

type (
	Bucket        = tidbdbutil.Bucket
	QueryExecutor = tidbdbutil.QueryExecutor
)

// GetBucketsInfo selects from stats_buckets in TiDB.
func GetBucketsInfo(ctx context.Context, db QueryExecutor, schema, table string, tableInfo *model.TableInfo) (map[string][]Bucket, error) {
	buckets := make(map[string][]Bucket)
	missingIndexHistIDs := make(map[int64]struct{})
	missingColumnHistIDs := make(map[int64]struct{})

	indices := tidbdbutil.FindAllIndex(tableInfo)
	// Pre-build lightweight maps for indices: index ID -> index name / column types.
	indexColumnTypesMap := make(map[int64][]byte, len(indices))
	indexNameMap := make(map[int64]string, len(indices))
	for _, idx := range indices {
		indexNameMap[idx.ID] = idx.Name.O

		// Build column types array for this index.
		idxColumnTypes := make([]byte, 0, len(idx.Columns))
		for _, idxCol := range idx.Columns {
			for _, col := range tableInfo.Columns {
				if col.Name.L == idxCol.Name.L {
					idxColumnTypes = append(idxColumnTypes, col.GetType())
					break
				}
			}
		}
		indexColumnTypesMap[idx.ID] = idxColumnTypes
	}

	// Pre-build lightweight maps for columns: column ID -> name / FieldType.
	columnTypeMap := make(map[int64]*types.FieldType, len(tableInfo.Columns))
	columnNameMap := make(map[int64]string, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		columnNameMap[col.ID] = col.Name.O
		columnTypeMap[col.ID] = &col.FieldType
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
		var lowerBoundBytes, upperBoundBytes []byte

		if err := rows.Scan(&isIndex, &histID, &bucketID, &count, &lowerBoundBytes, &upperBoundBytes); err != nil {
			return nil, errors.Trace(err)
		}

		var key string
		var lowerBoundStr, upperBoundStr string
		var decodeErr error

		if isIndex.Int64 == 1 {
			// Index bucket (is_index = 1).
			idxColumnTypes, ok := indexColumnTypesMap[histID.Int64]
			indexName := indexNameMap[histID.Int64]
			if !ok {
				if _, seen := missingIndexHistIDs[histID.Int64]; !seen {
					// If cannot determine key, skip this record.
					log.Warn("skipping index bucket with unknown key",
						zap.Int64("histID", histID.Int64))
					missingIndexHistIDs[histID.Int64] = struct{}{}
				}
				continue
			}

			key = indexName

			// Decode index bound using pre-computed column types.
			lowerBoundStr, decodeErr = decodeIndexBound(lowerBoundBytes, idxColumnTypes)
			if decodeErr != nil {
				log.Warn("Failed to decode lower_bound for index",
					zap.Error(decodeErr),
					zap.Int64("histID", histID.Int64))
				lowerBoundStr = fmt.Sprintf("0x%x", lowerBoundBytes)
			}

			upperBoundStr, decodeErr = decodeIndexBound(upperBoundBytes, idxColumnTypes)
			if decodeErr != nil {
				log.Warn("Failed to decode upper_bound for index",
					zap.Error(decodeErr),
					zap.Int64("histID", histID.Int64))
				upperBoundStr = fmt.Sprintf("0x%x", upperBoundBytes)
			}
		} else {
			// Column bucket (is_index = 0).
			columnName, ok := columnNameMap[histID.Int64]
			columnTypes := columnTypeMap[histID.Int64]
			if !ok {
				if _, seen := missingColumnHistIDs[histID.Int64]; !seen {
					// If cannot determine key, skip this record.
					log.Warn("skipping column bucket with unknown key",
						zap.Int64("histID", histID.Int64))
					missingColumnHistIDs[histID.Int64] = struct{}{}
				}
				continue
			}

			key = columnName

			lowerBoundStr, decodeErr = decodeColumnBound(lowerBoundBytes, columnTypes)
			if decodeErr != nil {
				log.Warn("Failed to decode lower_bound for column",
					zap.Error(decodeErr),
					zap.Int64("histID", histID.Int64))
				lowerBoundStr = fmt.Sprintf("0x%x", lowerBoundBytes)
			}

			upperBoundStr, decodeErr = decodeColumnBound(upperBoundBytes, columnTypes)
			if decodeErr != nil {
				log.Warn("Failed to decode upper_bound for column",
					zap.Error(decodeErr),
					zap.Int64("histID", histID.Int64))
				upperBoundStr = fmt.Sprintf("0x%x", upperBoundBytes)
			}
		}

		b := Bucket{
			Count:      count.Int64,
			LowerBound: lowerBoundStr,
			UpperBound: upperBoundStr,
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
					zap.String("primaryKeyColumn", index.Columns[0].Name.O))
				return buckets, nil
			}
			buckets[index.Name.O] = buckets[index.Columns[0].Name.O]
			delete(buckets, index.Columns[0].Name.O)
		}
	}

	return buckets, nil
}

// decodeColumnBound decodes column bound values (is_index=0) using FieldType only.
func decodeColumnBound(boundBytes []byte, originalFt *types.FieldType) (string, error) {
	if len(boundBytes) == 0 {
		return "", nil
	}

	ctx := statistics.UTCWithAllowInvalidDateCtx

	// Here we directly create KindBytes type Datum, then convert to TypeBlob.
	blobDatum := types.NewBytesDatum(boundBytes)

	// First convert to TypeBlob (simulate GetDatum behavior).
	blobFt := types.NewFieldType(tmysql.TypeBlob)
	blobFt.SetCharset(charset.CharsetBin)
	blobFt.SetCollate(charset.CollationBin)
	blobTypeDatum, err := blobDatum.ConvertTo(ctx, blobFt)
	if err != nil {
		return "", errors.Trace(err)
	}

	// For string types, need special handling (because may store collate key).
	// Reference: pkg/statistics/handle/storage/read.go
	// First convert to TypeBlob type (bypass charset/collation check), then convert back to original type.
	var targetFt *types.FieldType
	if types.EvalType(originalFt.GetType()) == types.ETString &&
		originalFt.GetType() != tmysql.TypeEnum &&
		originalFt.GetType() != tmysql.TypeSet {
		targetFt = types.NewFieldType(tmysql.TypeBlob)
		targetFt.SetCharset(charset.CharsetBin)
		targetFt.SetCollate(charset.CollationBin)
	} else {
		targetFt = originalFt
	}

	// Use convertBoundFromBlob logic (reference: pkg/statistics/handle/storage/read.go:918-946).
	var convertedDatum types.Datum
	if originalFt.GetType() == tmysql.TypeBit {
		// BIT type special handling (reference: convertBoundFromBlob).
		// Simplified: first convert to TypeBlob, then convert back to BIT.
		convertedDatum, err = blobTypeDatum.ConvertTo(ctx, targetFt)
		if err != nil {
			return "", errors.Trace(err)
		}
		// Then convert back to BIT type.
		convertedDatum, err = convertedDatum.ConvertTo(ctx, originalFt)
		if err != nil {
			return "", errors.Trace(err)
		}
	} else {
		// Other types: convert from TypeBlob to target type.
		convertedDatum, err = blobTypeDatum.ConvertTo(ctx, targetFt)
		if err != nil {
			return "", errors.Trace(err)
		}
		// If string type, need to convert back to original type (restore correct charset/collation).
		if types.EvalType(originalFt.GetType()) == types.ETString &&
			originalFt.GetType() != tmysql.TypeEnum &&
			originalFt.GetType() != tmysql.TypeSet {
			convertedDatum, err = convertedDatum.ConvertTo(ctx, originalFt)
			if err != nil {
				return "", errors.Trace(err)
			}
		}
	}

	return convertedDatum.ToString()
}

// decodeIndexBound decodes index bound values (is_index=1) using pre-computed column types.
func decodeIndexBound(boundBytes []byte, idxColumnTypes []byte) (string, error) {
	if len(boundBytes) == 0 {
		return "", nil
	}

	// If cannot get column types, use default value (single column index).
	if len(idxColumnTypes) == 0 {
		idxColumnTypes = []byte{tmysql.TypeVarchar}
	}

	// Decode encoded values.
	numCols := len(idxColumnTypes)
	if numCols == 0 {
		numCols = 1
	}

	decodedVals, remained, err := codec.DecodeRange(
		boundBytes,
		numCols,
		idxColumnTypes,
		time.UTC,
	)
	if err != nil {
		return "", errors.Trace(err)
	}

	if len(remained) > 0 {
		decodedVals = append(decodedVals, types.NewBytesDatum(remained))
	}

	return types.DatumsToString(decodedVals, true)
}
