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
			tagChunk = chunk.NewChunkRange()
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
			tagChunk = chunk.NewChunkRangeOffset(columnOffset)
		}

		break
	}

	if indexColumns == nil {
		return nil, errors.NotFoundf("not found index")
	}

	chunkSize := table.ChunkSize
	if chunkSize <= 0 {
		cnt, err := dbutil.GetRowCount(ctx, dbConn, table.Schema, table.Table, "", nil)
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
	log.Info("get chunk size for table", zap.Int64("chunk size", chunkSize),
		zap.String("db", table.Schema), zap.String("table", table.Table))

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
			progress.UpdateTotal(lmt.progressID, 1, true)
			select {
			case <-ctx.Done():
			case lmt.chunksCh <- chunkRange:
			}
			close(lmt.chunksCh)
			return
		}

		newTagChunk := chunk.NewChunkRangeOffset(lmt.columnOffset)
		for column, data := range dataMap {
			newTagChunk.Update(column, string(data.Data), "", !data.IsNull, false)
			chunkRange.Update(column, "", string(data.Data), false, !data.IsNull)
		}

		chunk.InitChunk(chunkRange, chunk.Limit, bucketID, bucketID, lmt.table.Collation, lmt.table.Range)
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

func generateLimitQueryTemplate(indexColumns []*model.ColumnInfo, table *common.TableDiff, chunkSize int64) string {
	fields := make([]string, 0, len(indexColumns))
	for _, columnInfo := range indexColumns {
		fields = append(fields, dbutil.ColumnName(columnInfo.Name.O))
	}
	columns := strings.Join(fields, ", ")

	// TODO: the limit splitter has not been used yet.
	// once it is used, need to add `collation` after `ORDER BY`.
	return fmt.Sprintf("SELECT %s FROM %s WHERE %%s ORDER BY %s LIMIT %d,1", columns, dbutil.TableName(table.Schema, table.Table), columns, chunkSize)
}
