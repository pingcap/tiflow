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

package splitter

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
	"github.com/pingcap/tiflow/sync_diff_inspector/progress"
	"github.com/pingcap/tiflow/sync_diff_inspector/source/common"
	"github.com/pingcap/tiflow/sync_diff_inspector/utils"
	"go.uber.org/zap"
)

const limitBatchSize = 128

// LimitIterator is the iterator with limit
type LimitIterator struct {
	table *common.TableDiff

	queryTmpl string
	chunkSize int64
	indexID   int64

	resultCh chan iteratorResult
	cancel   context.CancelFunc
	db       *sql.DB

	progressID   string
	columnOffset map[string]int

	indexColumnNames []ast.CIStr
	logger           *zap.Logger
}

// NewLimitIterator return a new iterator
func NewLimitIterator(
	ctx context.Context,
	progressID string,
	table *common.TableDiff,
	dbConn *sql.DB,
	candidate *IndexCandidate,
) (*LimitIterator, error) {
	return NewLimitIteratorWithCheckpoint(ctx, progressID, table, dbConn, nil, candidate)
}

// NewLimitIteratorWithCheckpoint return a new iterator
func NewLimitIteratorWithCheckpoint(
	ctx context.Context,
	progressID string,
	table *common.TableDiff,
	dbConn *sql.DB,
	startRange *RangeInfo,
	candidate *IndexCandidate,
) (*LimitIterator, error) {
	logger := log.L().With(
		zap.String("db", table.Schema),
		zap.String("table", table.Table),
	)
	indexColumns := candidate.Columns
	logger.Debug("limit select index", zap.String("index", candidate.Index.Name.O))

	columnOffset := make(map[string]int, len(indexColumns))
	for i, indexColumn := range indexColumns {
		columnOffset[indexColumn.Name.O] = i
	}
	indexColumnNames := utils.GetColumnNames(indexColumns)

	tagChunk := chunk.NewChunkRangeOffset(columnOffset, table.Info)
	tagChunk.IndexColumnNames = indexColumnNames

	unfinished := startRange == nil
	beginBucketID := 0
	if startRange != nil {
		bounds := startRange.ChunkRange.Bounds
		if len(bounds) != len(indexColumns) {
			logger.Warn("checkpoint node columns are not equal to selected index columns, skip checkpoint.")
			unfinished = true
		} else {
			tagChunk = chunk.NewChunkRange(table.Info)
			tagChunk.IndexColumnNames = indexColumnNames
			for _, bound := range bounds {
				unfinished = unfinished || bound.HasUpper
				tagChunk.Update(bound.Column, bound.Upper, "", bound.HasUpper, false)
			}
			beginBucketID = startRange.ChunkRange.Index.BucketIndexRight + 1
			logger.Debug("limit splitter resume from checkpoint",
				zap.Int("bucket", beginBucketID))
		}
	}

	totalCount, err := getRowCount(ctx, dbConn, table.Schema, table.Table, "", nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	chunkSize := table.ChunkSize
	if chunkSize <= 0 {
		chunkSize = utils.CalculateChunkSize(totalCount)
	}
	totalChunks := max(int((totalCount+chunkSize-1)/chunkSize), 1)
	logger.Info("get chunk size and count for table",
		zap.Int64("total rows", totalCount),
		zap.Int64("chunk size", chunkSize),
		zap.Int("chunk count", totalChunks),
		zap.Int("finished chunks", beginBucketID),
	)

	remainChunks := max(totalChunks-beginBucketID, 0)

	lctx, cancel := context.WithCancel(ctx)
	queryTmpl := generateBoundQueryTemplate(indexColumns, table, chunkSize, candidate.Index.Name.O)

	limitIterator := &LimitIterator{
		table,

		queryTmpl,
		chunkSize,
		candidate.Index.ID,

		make(chan iteratorResult, DefaultChannelBuffer),
		cancel,
		dbConn,

		progressID,
		columnOffset,

		indexColumnNames,
		logger,
	}

	progress.StartTable(progressID, remainChunks, true)
	if !unfinished {
		close(limitIterator.resultCh)
	} else {
		go limitIterator.produceChunks(lctx, tagChunk, beginBucketID)
	}

	return limitIterator, nil
}

// Close close the iterator
func (lmt *LimitIterator) Close() {
	lmt.cancel()
}

// Next return the next chunk
func (lmt *LimitIterator) Next() (*chunk.Range, error) {
	result, ok := <-lmt.resultCh
	if !ok {
		return nil, nil
	}
	if result.err != nil {
		return nil, errors.Trace(result.err)
	}
	if result.chunk != nil {
		failpoint.Inject("print-chunk-info", func() {
			lowerBounds := make([]string, len(result.chunk.Bounds))
			upperBounds := make([]string, len(result.chunk.Bounds))
			for i, bound := range result.chunk.Bounds {
				lowerBounds[i] = bound.Lower
				upperBounds[i] = bound.Upper
			}
			lmt.logger.Info("failpoint print-chunk-info injected (limit splitter)",
				zap.Strings("lowerBounds", lowerBounds),
				zap.Strings("upperBounds", upperBounds),
				zap.String("indexCode", result.chunk.Index.ToString()))
		})
	}
	return result.chunk, nil
}

// GetIndexID get the current index id
func (lmt *LimitIterator) GetIndexID() int64 {
	return lmt.indexID
}

func (lmt *LimitIterator) produceChunks(ctx context.Context, tagChunk *chunk.Range, bucketID int) {
	queryRange := lmt.chunkSize * int64(limitBatchSize)
	for {
		where, args := tagChunk.ToString(lmt.table.Collation)
		query := fmt.Sprintf(lmt.queryTmpl, where)
		bounds, err := lmt.batchGetBounds(ctx, query, append(args, queryRange)...)
		if err != nil {
			select {
			case <-ctx.Done():
			case lmt.resultCh <- iteratorResult{err: errors.Trace(err)}:
			}
			return
		}

		ignoreLastN := 0
		failpoint.Inject("ignore-last-n-chunk-in-bucket", func(v failpoint.Value) {
			ignoreLastN = v.(int)
			lmt.logger.Info("failpoint ignore-last-n-chunk-in-bucket injected (limit splitter)", zap.Int("n", ignoreLastN))
			if ignoreLastN > 0 && len(bounds) > ignoreLastN {
				bounds = bounds[:len(bounds)-ignoreLastN]
			}
		})

		lmt.logger.Debug("limit iterator fetched bounds",
			zap.Int("count", len(bounds)),
			zap.Int64("query-range", queryRange),
			zap.Int("bucket", bucketID))

		if len(bounds) == 0 {
			// Send tagChunk as the last chunk
			chunk.InitChunk(tagChunk, chunk.Limit, bucketID, bucketID, lmt.table.Collation, lmt.table.Range)
			lmt.logger.Debug("limit iterator finished",
				zap.Int("bucket", bucketID))
			select {
			case <-ctx.Done():
			case lmt.resultCh <- iteratorResult{chunk: tagChunk}:
			}
			close(lmt.resultCh)
			return
		}

		for _, dataMap := range bounds {
			chunkRange := tagChunk
			newTagChunk := chunk.NewChunkRangeOffset(lmt.columnOffset, lmt.table.Info)
			newTagChunk.IndexColumnNames = lmt.indexColumnNames
			for column, data := range dataMap {
				newTagChunk.Update(column, string(data.Data), "", !data.IsNull, false)
				chunkRange.Update(column, "", string(data.Data), false, !data.IsNull)
			}

			chunk.InitChunk(chunkRange, chunk.Limit, bucketID, bucketID, lmt.table.Collation, lmt.table.Range)
			if ignoreLastN > 0 {
				chunkRange.Index.ChunkCnt = chunkRange.Index.ChunkIndex + 1 + ignoreLastN
			}
			bucketID++
			select {
			case <-ctx.Done():
				return
			case lmt.resultCh <- iteratorResult{chunk: chunkRange}:
			}
			tagChunk = newTagChunk
		}
	}
}

func (lmt *LimitIterator) batchGetBounds(
	ctx context.Context, query string, args ...any,
) ([]map[string]*dbutil.ColumnData, error) {
	lmt.logger.Debug("limit iterator query bounds",
		zap.String("sql", query),
		zap.Any("args", args))
	rows, err := lmt.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dataMaps := make([]map[string]*dbutil.ColumnData, 0)
	for rows.Next() {
		dataMap, err := dbutil.ScanRow(rows)
		if err != nil {
			return nil, err
		}
		dataMaps = append(dataMaps, dataMap)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	lmt.logger.Debug("limit iterator query bounds done",
		zap.Int("count", len(dataMaps)))
	return dataMaps, nil
}

func generateBoundQueryTemplate(
	indexColumns []*model.ColumnInfo,
	table *common.TableDiff,
	chunkSize int64, indexName string,
) string {
	fields := make([]string, 0, len(indexColumns))
	for _, columnInfo := range indexColumns {
		fields = append(fields, dbutil.ColumnName(columnInfo.Name.O))
	}
	columns := strings.Join(fields, ", ")
	tableName := dbutil.TableName(table.Schema, table.Table)
	indexHint := fmt.Sprintf("/*+ USE_INDEX(%s, %s) */",
		tableName, dbutil.ColumnName(indexName))

	return fmt.Sprintf(
		"SELECT %s FROM (SELECT %s %s, ROW_NUMBER() OVER (ORDER BY %s) AS rn FROM %s WHERE %%s LIMIT ?) AS t WHERE MOD(rn, %d) = 0",
		columns,
		indexHint,
		columns,
		columns,
		tableName,
		chunkSize,
	)
}
