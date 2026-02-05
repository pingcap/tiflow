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

// RandomIterator is used to random iterate a table
type RandomIterator struct {
	table        *common.TableDiff
	chunkSize    int64
	chunkBase    *chunk.Range
	splitColumns []string
	randomValues [][]string
	nextChunk    uint

	dbConn *sql.DB
}

// a wrapper for get row count to integrate failpoint.
func getRowCount(ctx context.Context, db dbutil.QueryExecutor, schemaName string, tableName string, where string, args []any) (int64, error) {
	failpoint.Inject("getRowCount", func(val failpoint.Value) {
		if count, ok := val.(int); ok {
			failpoint.Return(int64(count), nil)
		}
	})
	return dbutil.GetRowCount(ctx, db, schemaName, tableName, where, args)
}

// NewRandomIterator return a new iterator
func NewRandomIterator(ctx context.Context, progressID string, table *common.TableDiff, dbConn *sql.DB) (*RandomIterator, error) {
	return NewRandomIteratorWithCheckpoint(ctx, progressID, table, dbConn, nil)
}

// NewRandomIteratorWithCheckpoint return a new iterator with checkpoint
func NewRandomIteratorWithCheckpoint(
	ctx context.Context,
	progressID string,
	table *common.TableDiff,
	dbConn *sql.DB,
	startRange *RangeInfo,
) (*RandomIterator, error) {
	// get the chunk count by data count and chunk size
	var splitFieldArr []string
	if len(table.Fields) != 0 {
		splitFieldArr = strings.Split(table.Fields, ",")
	}

	for i := range splitFieldArr {
		splitFieldArr[i] = strings.TrimSpace(splitFieldArr[i])
	}

	fields, err := GetSplitFields(table.Info, splitFieldArr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	chunkRange := chunk.NewChunkRange(table.Info)

	// Below logic is modified from BucketIterator
	// It's used to find the index which can match the split fields in RandomIterator.
	fieldNames := utils.GetColumnNames(fields)
	indices := dbutil.FindAllIndex(table.Info)
NEXTINDEX:
	for _, index := range indices {
		if index == nil {
			continue
		}
		if startRange != nil && startRange.IndexID != index.ID {
			continue
		}

		indexColumns := utils.GetColumnsFromIndex(index, table.Info)

		if len(indexColumns) < len(index.Columns) {
			// some column in index is ignored.
			continue
		}

		if !utils.IsIndexMatchingColumns(index, fieldNames) {
			// We are enforcing user configured "index-fields" settings.
			continue
		}

		// skip the index that has expression column
		for _, col := range indexColumns {
			if col.Hidden {
				continue NEXTINDEX
			}
		}

		// Found the index, store column names
		chunkRange.IndexColumnNames = utils.GetColumnNames(indexColumns)
		break
	}

	beginIndex := 0
	bucketChunkCnt := 0
	chunkCnt := 0
	var chunkSize int64 = 0
	if startRange != nil {
		c := startRange.GetChunk()
		if c.IsLastChunkForTable() {
			return &RandomIterator{
				table:     table,
				chunkSize: 0,
				chunkBase: nil,
				nextChunk: 0,
				dbConn:    dbConn,
			}, nil
		}
		// The sequences in `chunk.Range.Bounds` should be equivalent.
		for _, bound := range c.Bounds {
			chunkRange.Update(bound.Column, bound.Upper, "", true, false)
		}

		// Recover the chunkIndex. Let it be next to the checkpoint node.
		beginIndex = c.Index.ChunkIndex + 1
		bucketChunkCnt = c.Index.ChunkCnt
		// For chunk splitted by random splitter, the checkpoint chunk records the tableCnt.
		chunkCnt = bucketChunkCnt - beginIndex
	} else {
		cnt, err := getRowCount(ctx, dbConn, table.Schema, table.Table, table.Range, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}

		chunkSize = table.ChunkSize
		// We can use config file to fix chunkSize,
		// otherwise chunkSize is 0.
		if chunkSize <= 0 {
			if len(table.Info.Indices) != 0 {
				chunkSize = utils.CalculateChunkSize(cnt)
			} else {
				// no index
				// will use table scan
				// so we use one chunk
				// plus 1 to avoid chunkSize is 0
				// while chunkCnt = (2cnt)/(cnt+1) <= 1
				chunkSize = cnt + 1
			}
		}
		log.Info("get chunk size for table", zap.Int64("chunk size", chunkSize),
			zap.String("db", table.Schema), zap.String("table", table.Table))

		// When cnt is 0, chunkCnt should be also 0.
		// When cnt is in [1, chunkSize], chunkCnt should be 1.
		chunkCnt = int((cnt + chunkSize - 1) / chunkSize)
		log.Info("split range by random", zap.Int64("row count", cnt), zap.Int("split chunk num", chunkCnt))
		bucketChunkCnt = chunkCnt
	}

	randomValues, err := getRandomSplitValues(ctx, dbConn, chunkRange, chunkCnt, table.Schema, table.Table, fields, table.Range, table.Collation)
	if err != nil {
		return nil, errors.Trace(err)
	}
	chunkCount := len(randomValues) + 1
	chunkRange.Index = &chunk.CID{
		BucketIndexLeft:  0,
		BucketIndexRight: 0,
		ChunkIndex:       beginIndex,
		ChunkCnt:         bucketChunkCnt,
	}
	chunkRange.Type = chunk.Random

	failpoint.Inject("ignore-last-n-chunk-in-bucket", func(v failpoint.Value) {
		log.Info("failpoint ignore-last-n-chunk-in-bucket injected (random splitter)", zap.Int("n", v.(int)))
		if chunkCount <= 1+v.(int) {
			failpoint.Return(nil, nil)
		}
		chunkCount -= v.(int)
		if len(randomValues) >= chunkCount {
			randomValues = randomValues[:chunkCount-1]
		}
	})

	progress.StartTable(progressID, chunkCount, true)
	splitColumns := make([]string, 0, len(fields))
	for _, field := range fields {
		splitColumns = append(splitColumns, field.Name.O)
	}
	return &RandomIterator{
		table:        table,
		chunkSize:    chunkSize,
		chunkBase:    chunkRange,
		splitColumns: splitColumns,
		randomValues: randomValues,
		nextChunk:    0,
		dbConn:       dbConn,
	}, nil
}

func (s *RandomIterator) buildChunk(chunkIndex int) *chunk.Range {
	c := s.chunkBase.Clone()
	if len(s.randomValues) > 0 {
		for i, column := range s.splitColumns {
			switch {
			case chunkIndex == 0:
				c.Update(column, "", s.randomValues[0][i], false, true)
			case chunkIndex == len(s.randomValues):
				c.Update(column, s.randomValues[chunkIndex-1][i], "", true, false)
			default:
				c.Update(column, s.randomValues[chunkIndex-1][i], s.randomValues[chunkIndex][i], true, true)
			}
		}
	}

	conditions, args := c.ToString(s.table.Collation)
	c.Where = fmt.Sprintf("((%s) AND (%s))", conditions, s.table.Range)
	c.Args = args
	c.Index.ChunkIndex += chunkIndex
	return c
}

// Next get the next chunk
func (s *RandomIterator) Next() (*chunk.Range, error) {
	if s.chunkBase == nil {
		return nil, nil
	}
	if uint(len(s.randomValues)+1) <= s.nextChunk {
		return nil, nil
	}
	c := s.buildChunk(int(s.nextChunk))
	s.nextChunk = s.nextChunk + 1
	failpoint.Inject("print-chunk-info", func() {
		lowerBounds := make([]string, len(c.Bounds))
		upperBounds := make([]string, len(c.Bounds))
		for i, bound := range c.Bounds {
			lowerBounds[i] = bound.Lower
			upperBounds[i] = bound.Upper
		}
		log.Info("failpoint print-chunk-info injected (random splitter)", zap.Strings("lowerBounds", lowerBounds), zap.Strings("upperBounds", upperBounds), zap.String("indexCode", c.Index.ToString()))
	})
	return c, nil
}

// Close close the iterator
func (s *RandomIterator) Close() {
}

// GetSplitFields returns fields to split chunks, order by pk, uk, index, columns.
func GetSplitFields(table *model.TableInfo, splitFields []string) ([]*model.ColumnInfo, error) {
	colsMap := make(map[string]*model.ColumnInfo)

	splitCols := make([]*model.ColumnInfo, 0, 2)
	for _, splitField := range splitFields {
		col := dbutil.FindColumnByName(table.Columns, splitField)
		if col == nil {
			return nil, errors.NotFoundf("column %s in table %s", splitField, table.Name)
		}
		splitCols = append(splitCols, col)
	}

	if len(splitCols) != 0 {
		return splitCols, nil
	}

	for _, col := range table.Columns {
		colsMap[col.Name.O] = col
	}
	indices := dbutil.FindAllIndex(table)
	if len(indices) != 0 {
	NEXTINDEX:
		for _, idx := range indices {
			cols := make([]*model.ColumnInfo, 0, len(table.Columns))
			for _, icol := range idx.Columns {
				col := colsMap[icol.Name.O]
				if col.Hidden {
					continue NEXTINDEX
				}
				cols = append(cols, col)
			}
			return cols, nil
		}
	}

	for _, col := range table.Columns {
		if !col.Hidden {
			return []*model.ColumnInfo{col}, nil
		}
	}
	return nil, errors.NotFoundf("not found column")
}

// splitRangeByRandom splits a chunk to multiple chunks by random
// Notice: If the `count <= 1`, it will skip splitting and return `chunk` as a slice directly.
func splitRangeByRandom(ctx context.Context, db *sql.DB, chunk *chunk.Range, count int, schema string, table string, columns []*model.ColumnInfo, limits, collation string) (chunks []*chunk.Range, err error) {
	if count <= 1 {
		chunks = append(chunks, chunk)
		return chunks, nil
	}

	randomValues, err := getRandomSplitValues(ctx, db, chunk, count, schema, table, columns, limits, collation)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for i := 0; i <= len(randomValues); i++ {
		newChunk := chunk.Copy()

		for j, column := range columns {
			if i == 0 {
				if len(randomValues) == 0 {
					// randomValues is empty, so chunks will append chunk itself.
					break
				}
				newChunk.Update(column.Name.O, "", randomValues[i][j], false, true)
			} else if i == len(randomValues) {
				newChunk.Update(column.Name.O, randomValues[i-1][j], "", true, false)
			} else {
				newChunk.Update(column.Name.O, randomValues[i-1][j], randomValues[i][j], true, true)
			}
		}
		chunks = append(chunks, newChunk)
	}
	log.Debug("split range by random", zap.Stringer("origin chunk", chunk), zap.Int("split num", len(chunks)))
	return chunks, nil
}

func getRandomSplitValues(ctx context.Context, db *sql.DB, chunk *chunk.Range, count int, schema string, table string, columns []*model.ColumnInfo, limits, collation string) ([][]string, error) {
	if count <= 1 {
		return nil, nil
	}

	chunkLimits, args := chunk.ToString(collation)
	limitRange := fmt.Sprintf("(%s) AND (%s)", chunkLimits, limits)

	randomValues, err := utils.GetRandomValues(ctx, db, schema, table, columns, count-1, limitRange, args, collation)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Debug("get split values by random", zap.Stringer("chunk", chunk), zap.Int("random values num", len(randomValues)))
	return randomValues, nil
}
