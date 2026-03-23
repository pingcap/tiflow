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

const defaultLimitBatchSize int64 = 32

// LimitIterator is the iterator with limit
type LimitIterator struct {
	table     *common.TableDiff
	tagChunk  *chunk.Range
	queryTmpl string

	queryRange int64

	indexID          int64
	indexColumnNames []ast.CIStr

	chunksCh chan *chunk.Range
	errCh    chan error
	cancel   context.CancelFunc
	dbConn   *sql.DB

	progressID   string
	columnOffset map[string]int

	logger *zap.Logger
}

// NewLimitIterator return a new iterator
func NewLimitIterator(ctx context.Context, progressID string, table *common.TableDiff, dbConn *sql.DB) (*LimitIterator, error) {
	return NewLimitIteratorWithCheckpoint(ctx, progressID, table, dbConn, nil)
}

// NewLimitIteratorWithCheckpoint return a new iterator
func NewLimitIteratorWithCheckpoint(
	ctx context.Context,
	progressID string,
	table *common.TableDiff,
	dbConn *sql.DB,
	startRange *RangeInfo,
) (*LimitIterator, error) {
	logger := log.L().With(
		zap.String("db", table.Schema),
		zap.String("table", table.Table),
	)

	indices, err := utils.GetBetterIndex(ctx, dbConn, table.Schema, table.Table, table.Info)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var (
		indexColumns []*model.ColumnInfo
		indexID      int64
		indexName    string

		tagChunk *chunk.Range
		chunksCh = make(chan *chunk.Range, DefaultChannelBuffer)
		errCh    = make(chan error)

		columnOffset  = make(map[string]int)
		undone        = startRange == nil
		beginBucketID int
	)

	for _, index := range indices {
		if index == nil {
			continue
		}
		if startRange != nil && startRange.IndexID != index.ID {
			continue
		}
		logger.Debug("Limit select index", zap.String("index", index.Name.O))

		indexColumns = utils.GetColumnsFromIndex(index, table.Info)

		if len(indexColumns) < len(index.Columns) {
			// some column in index is ignored.
			log.Debug("indexColumns empty, try next index")
			indexColumns = nil
			continue
		}

		indexID = index.ID
		indexName = index.Name.O
		for i, indexColumn := range indexColumns {
			columnOffset[indexColumn.Name.O] = i
		}

		if startRange != nil {
			tagChunk = chunk.NewChunkRange(table.Info)
			bounds := startRange.ChunkRange.Bounds
			if len(bounds) != len(indexColumns) {
				logger.Warn("checkpoint node columns are not equal to selected index columns, skip checkpoint.")
				break
			}

			for _, bound := range bounds {
				undone = undone || bound.HasUpper
				tagChunk.Update(bound.Column, bound.Upper, "", bound.HasUpper, false)
			}

			beginBucketID = startRange.ChunkRange.Index.BucketIndexRight + 1

		} else {
			tagChunk = chunk.NewChunkRangeOffset(columnOffset, table.Info)
		}

		break
	}

	if indexColumns == nil {
		return nil, errors.NotFoundf("not found index")
	}

	chunkSize := table.ChunkSize
	if chunkSize <= 0 {
		cnt, err := getRowCount(ctx, dbConn, table.Schema, table.Table, "", nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(table.Info.Indices) != 0 {
			chunkSize = utils.CalculateChunkSize(cnt)
		} else {
			// no index
			// will use table scan
			// so we use one chunk
			chunkSize = cnt
		}
	}
	logger.Info("get chunk size and count for table",
		zap.Int64("chunk size", chunkSize),
		zap.Int("finished chunks", beginBucketID),
	)

	lctx, cancel := context.WithCancel(ctx)
	queryTmpl := generateBoundQueryTemplate(indexColumns, table, chunkSize, indexName)

	batchSize := defaultLimitBatchSize
	if table.CheckThreadCount > 0 {
		batchSize = int64(table.CheckThreadCount * 2)
	}

	limitIterator := &LimitIterator{
		table,
		tagChunk,
		queryTmpl,

		chunkSize * batchSize,

		indexID,
		utils.GetColumnNames(indexColumns),

		chunksCh,
		errCh,

		cancel,
		dbConn,

		progressID,
		columnOffset,

		logger,
	}

	progress.StartTable(progressID, 0, false)
	if !undone {
		// this table is finished.
		close(chunksCh)
	} else {
		go limitIterator.produceChunks(lctx, beginBucketID)
	}

	return limitIterator, nil
}

// Close close the iterator
func (lmt *LimitIterator) Close() {
	lmt.cancel()
}

// Next return the next chunk
func (lmt *LimitIterator) Next() (*chunk.Range, error) {
	select {
	case err := <-lmt.errCh:
		return nil, errors.Trace(err)
	case c, ok := <-lmt.chunksCh:
		if !ok && c == nil {
			return nil, nil
		}
		if c != nil {
			failpoint.Inject("print-chunk-info", func() {
				lowerBounds := make([]string, len(c.Bounds))
				upperBounds := make([]string, len(c.Bounds))
				for i, bound := range c.Bounds {
					lowerBounds[i] = bound.Lower
					upperBounds[i] = bound.Upper
				}
				lmt.logger.Info("failpoint print-chunk-info injected (limit splitter)",
					zap.Strings("lowerBounds", lowerBounds),
					zap.Strings("upperBounds", upperBounds),
					zap.String("indexCode", c.Index.ToString()))
			})
		}
		return c, nil
	}
}

// GetIndexID get the current index id
func (lmt *LimitIterator) GetIndexID() int64 {
	return lmt.indexID
}

func (lmt *LimitIterator) produceChunks(ctx context.Context, bucketID int) {
	for {
		where, args := lmt.tagChunk.ToString(lmt.table.Collation)
		query := fmt.Sprintf(lmt.queryTmpl, where)
		bounds, err := lmt.batchGetBounds(ctx, query, append(args, lmt.queryRange)...)
		if err != nil {
			select {
			case <-ctx.Done():
			case lmt.errCh <- errors.Trace(err):
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
			zap.Int64("query-range", lmt.queryRange),
			zap.Int("bucket", bucketID))

		chunkRange := lmt.tagChunk
		lmt.tagChunk = nil
		if len(bounds) == 0 {
			// there is no row in result set
			chunk.InitChunk(chunkRange, chunk.Limit, bucketID, bucketID, lmt.table.Collation, lmt.table.Range)
			progress.UpdateTotal(lmt.progressID, 1, true)
			select {
			case <-ctx.Done():
			case lmt.chunksCh <- chunkRange:
			}
			close(lmt.chunksCh)
			return
		}

		for _, dataMap := range bounds {
			chunkRange := lmt.tagChunk
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
			progress.UpdateTotal(lmt.progressID, 1, false)
			select {
			case <-ctx.Done():
				return
			case lmt.chunksCh <- chunkRange:
			}
			lmt.tagChunk = newTagChunk
		}
	}
}

func (lmt *LimitIterator) batchGetBounds(
	ctx context.Context, query string, args ...any,
) ([]map[string]*dbutil.ColumnData, error) {
	rows, err := lmt.dbConn.QueryContext(ctx, query, args...)
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
	chunkSize int64,
	indexName string,
) string {
	fields := make([]string, 0, len(indexColumns))
	for _, columnInfo := range indexColumns {
		fields = append(fields, dbutil.ColumnName(columnInfo.Name.O))
	}
	columns := strings.Join(fields, ", ")
	orderBy := utils.BuildOrderByClause(indexColumns, table.Collation)
	tableName := dbutil.TableName(table.Schema, table.Table)
	indexHint := fmt.Sprintf("/*+ USE_INDEX(%s, %s) */",
		tableName, dbutil.ColumnName(indexName))

	return fmt.Sprintf(
		"SELECT %s FROM (SELECT %s %s, ROW_NUMBER() OVER (ORDER BY %s) AS rn FROM %s WHERE %%s LIMIT ?) AS t WHERE MOD(rn, %d) = 0",
		columns,
		indexHint,
		columns,
		orderBy,
		tableName,
		chunkSize,
	)
}
