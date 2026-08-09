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
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
	"github.com/pingcap/tiflow/sync_diff_inspector/progress"
	"github.com/pingcap/tiflow/sync_diff_inspector/source/common"
	"github.com/pingcap/tiflow/sync_diff_inspector/utils"
	"go.uber.org/zap"
)

// LimitIterator is the iterator with limit
type LimitIterator struct {
	table     *common.TableDiff
	tagChunk  *chunk.Range
	queryTmpl string

	indexID int64

	chunksCh chan *chunk.Range
	errCh    chan error
	cancel   context.CancelFunc
	dbConn   *sql.DB

	progressID   string
	columnOffset map[string]int
	// estChunkCount is an estimated chunk count derived from table statistics
	// (never a COUNT(*) scan). It seeds the progress bar; 0 means "unknown".
	estChunkCount int
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
	indices, err := utils.GetBetterIndex(ctx, dbConn, table.Schema, table.Table, table.Info)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var indexColumns []*model.ColumnInfo
	var tagChunk *chunk.Range
	columnOffset := make(map[string]int)
	chunksCh := make(chan *chunk.Range, DefaultChannelBuffer)
	errCh := make(chan error)
	undone := startRange == nil
	beginBucketID := 0
	var indexID int64
	for _, index := range indices {
		if index == nil {
			continue
		}
		if startRange != nil && startRange.IndexID != index.ID {
			continue
		}
		log.Debug("Limit select index", zap.String("index", index.Name.O))

		indexColumns = utils.GetColumnsFromIndex(index, table.Info)

		if len(indexColumns) < len(index.Columns) {
			// some column in index is ignored.
			log.Debug("indexColumns empty, try next index")
			indexColumns = nil
			continue
		}

		indexID = index.ID
		for i, indexColumn := range indexColumns {
			columnOffset[indexColumn.Name.O] = i
		}

		if startRange != nil {
			tagChunk = chunk.NewChunkRange(table.Info)
			bounds := startRange.ChunkRange.Bounds
			if len(bounds) != len(indexColumns) {
				log.Warn("checkpoint node columns are not equal to selected index columns, skip checkpoint.")
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

	tagChunk.IndexColumnNames = utils.GetColumnNames(indexColumns)

	// estRows is an estimated row count read from table statistics, never a
	// COUNT(*) full-table scan. It is only used to seed the progress bar with an
	// initial chunk total; the real total is corrected as chunks are produced.
	// For checkpoint resume (startRange != nil) we cannot cheaply estimate the
	// rows remaining behind the WHERE clause, so we skip the estimate and let the
	// progress bar grow purely dynamically.
	estRows := int64(0)
	if undone && startRange == nil {
		if v, ok := getEstimatedRowCount(ctx, dbConn, table.Schema, table.Table); ok {
			estRows = v
		}
	}

	chunkSize := table.ChunkSize
	if chunkSize <= 0 {
		// estRows == 0 falls back to the default chunk size (50000).
		chunkSize = utils.CalculateChunkSize(estRows)
	}
	log.Info("get chunk size for table", zap.Int64("chunk size", chunkSize),
		zap.Int64("estimated rows", estRows),
		zap.String("db", table.Schema), zap.String("table", table.Table))

	estChunkCount := 0
	if estRows > 0 {
		estChunkCount = int((estRows + chunkSize - 1) / chunkSize)
	}

	lctx, cancel := context.WithCancel(ctx)
	queryTmpl := generateLimitQueryTemplate(indexColumns, table, chunkSize)

	limitIterator := &LimitIterator{
		table,
		tagChunk,
		queryTmpl,

		indexID,

		chunksCh,
		errCh,

		cancel,
		dbConn,

		progressID,
		columnOffset,
		estChunkCount,
	}

	if progressID != "" {
		progress.StartTable(progressID, estChunkCount, false)
	}
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
				log.Info("failpoint print-chunk-info injected (limit splitter)",
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

// Len returns an estimated chunk count for this iterator (0 when unknown).
// The value comes from table statistics, not a COUNT(*) scan, so it is only an
// estimate; callers use it to seed a progress bar, not for correctness.
func (lmt *LimitIterator) Len() int {
	return lmt.estChunkCount
}

func (lmt *LimitIterator) produceChunks(ctx context.Context, bucketID int) {
	// produced counts chunks actually emitted; curTotal tracks the progress-bar
	// denominator. We keep curTotal >= produced so the bar never overflows, and
	// on completion we correct it to the exact produced count and freeze it.
	produced := 0
	curTotal := lmt.estChunkCount
	bumpTotal := func() {
		produced++
		if lmt.progressID != "" && produced > curTotal {
			progress.UpdateTotal(lmt.progressID, produced-curTotal, false)
			curTotal = produced
		}
	}
	defer func() {
		if lmt.progressID != "" {
			// Correct the estimate to the exact count and stop further updates.
			progress.UpdateTotal(lmt.progressID, produced-curTotal, true)
		}
		close(lmt.chunksCh)
	}()
	for {
		where, args := lmt.tagChunk.ToString(lmt.table.Collation)
		query := fmt.Sprintf(lmt.queryTmpl, where)
		dataMap, err := lmt.getLimitRow(ctx, query, args)
		if err != nil {
			select {
			case <-ctx.Done():
			case lmt.errCh <- errors.Trace(err):
			}
			return
		}

		chunkRange := lmt.tagChunk
		lmt.tagChunk = nil
		if dataMap == nil {
			// there is no row in result set
			chunk.InitChunk(chunkRange, chunk.Limit, bucketID, bucketID, lmt.table.Collation, lmt.table.Range)
			select {
			case <-ctx.Done():
			case lmt.chunksCh <- chunkRange:
			}
			return
		}

		newTagChunk := chunk.NewChunkRangeOffset(lmt.columnOffset, lmt.table.Info)
		newTagChunk.IndexColumnNames = chunkRange.IndexColumnNames
		for column, data := range dataMap {
			newTagChunk.Update(column, string(data.Data), "", !data.IsNull, false)
			chunkRange.Update(column, "", string(data.Data), false, !data.IsNull)
		}

		chunk.InitChunk(chunkRange, chunk.Limit, bucketID, bucketID, lmt.table.Collation, lmt.table.Range)
		bucketID++
		bumpTotal()
		select {
		case <-ctx.Done():
			return
		case lmt.chunksCh <- chunkRange:
		}
		lmt.tagChunk = newTagChunk

		failpoint.Inject("check-one-chunk", func() {
			log.Info("failpoint check-one-chunk injected, stop producing new chunks.")
			failpoint.Return()
		})
	}
}

func (lmt *LimitIterator) getLimitRow(ctx context.Context, query string, args []interface{}) (map[string]*dbutil.ColumnData, error) {
	rows, err := lmt.dbConn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	dataMap, err := dbutil.ScanRow(rows)
	if err != nil {
		return nil, err
	}
	return dataMap, nil
}

// getEstimatedRowCount returns an estimated row count from table statistics
// (information_schema.tables.TABLE_ROWS) without scanning the table. The boolean
// is false when no usable estimate is available (query error, missing row, or a
// NULL/non-positive value), in which case callers fall back to a dynamic total.
func getEstimatedRowCount(ctx context.Context, db *sql.DB, schemaName, tableName string) (int64, bool) {
	failpoint.Inject("getEstimatedRowCount", func(val failpoint.Value) {
		if v, ok := val.(int); ok {
			failpoint.Return(int64(v), v > 0)
		}
	})
	query := "SELECT TABLE_ROWS FROM information_schema.tables WHERE table_schema = ? AND table_name = ?"
	var estRows sql.NullInt64
	if err := db.QueryRowContext(ctx, query, schemaName, tableName).Scan(&estRows); err != nil {
		log.Warn("failed to get estimated row count, progress bar will grow dynamically",
			zap.String("db", schemaName), zap.String("table", tableName), zap.Error(err))
		return 0, false
	}
	if !estRows.Valid || estRows.Int64 <= 0 {
		return 0, false
	}
	return estRows.Int64, true
}

func generateLimitQueryTemplate(indexColumns []*model.ColumnInfo, table *common.TableDiff, chunkSize int64) string {
	fields := make([]string, 0, len(indexColumns))
	for _, columnInfo := range indexColumns {
		fields = append(fields, dbutil.ColumnName(columnInfo.Name.O))
	}
	columns := strings.Join(fields, ", ")
	orderBy := utils.BuildOrderByClause(indexColumns, table.Collation)
	tableName := dbutil.TableName(table.Schema, table.Table)

	return fmt.Sprintf("SELECT %s FROM %s WHERE %%s ORDER BY %s LIMIT %d,1",
		columns, tableName, orderBy, chunkSize)
}
