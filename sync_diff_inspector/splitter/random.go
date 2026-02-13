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
	table     *common.TableDiff
	chunkSize int64
	chunks    []*chunk.Range
	nextChunk uint

	IndexID int64

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
func NewRandomIterator(ctx context.Context, progressID string, table *common.TableDiff, dbConn *sql.DB, candidate *IndexCandidate) (*RandomIterator, error) {
	return NewRandomIteratorWithCheckpoint(ctx, progressID, table, dbConn, nil, candidate)
}

// NewRandomIteratorWithCheckpoint return a new iterator with checkpoint
func NewRandomIteratorWithCheckpoint(
	ctx context.Context,
	progressID string,
	table *common.TableDiff,
	dbConn *sql.DB,
	startRange *RangeInfo,
	candidate *IndexCandidate,
) (*RandomIterator, error) {
	fields := candidate.Columns
	chunkRange := chunk.NewChunkRange(table.Info)
	if candidate.Index != nil && candidate.Index.ID >= 0 {
		chunkRange.IndexColumnNames = utils.GetColumnNames(fields)
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
				chunks:    nil,
				nextChunk: 0,
				IndexID:   candidate.Index.ID,
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

	chunks, err := splitRangeByRandom(ctx, dbConn, chunkRange, chunkCnt, table.Schema, table.Table, fields, table.Range, table.Collation)
	if err != nil {
		return nil, errors.Trace(err)
	}
	chunk.InitChunks(chunks, chunk.Random, 0, 0, beginIndex, table.Collation, table.Range, bucketChunkCnt)

	failpoint.Inject("ignore-last-n-chunk-in-bucket", func(v failpoint.Value) {
		log.Info("failpoint ignore-last-n-chunk-in-bucket injected (random splitter)", zap.Int("n", v.(int)))
		if len(chunks) <= 1+v.(int) {
			failpoint.Return(nil, nil)
		}
		chunks = chunks[:(len(chunks) - v.(int))]
	})

	progress.StartTable(progressID, len(chunks), true)
	return &RandomIterator{
		table:     table,
		chunkSize: chunkSize,
		chunks:    chunks,
		nextChunk: 0,
		dbConn:    dbConn,
		IndexID:   candidate.Index.ID,
	}, nil
}

// Next get the next chunk
func (s *RandomIterator) Next() (*chunk.Range, error) {
	if uint(len(s.chunks)) <= s.nextChunk {
		return nil, nil
	}
	c := s.chunks[s.nextChunk]
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

// GetIndexID returns the index ID
func (s *RandomIterator) GetIndexID() int64 {
	return s.IndexID
}

// Close close the iterator
func (s *RandomIterator) Close() {
}

// splitRangeByRandom splits a chunk to multiple chunks by random
// Notice: If the `count <= 1`, it will skip splitting and return `chunk` as a slice directly.
func splitRangeByRandom(ctx context.Context, db *sql.DB, chunk *chunk.Range, count int, schema string, table string, columns []*model.ColumnInfo, limits, collation string) (chunks []*chunk.Range, err error) {
	if count <= 1 {
		chunks = append(chunks, chunk)
		return chunks, nil
	}

	chunkLimits, args := chunk.ToString(collation)
	limitRange := fmt.Sprintf("(%s) AND (%s)", chunkLimits, limits)

	randomValues, err := utils.GetRandomValues(ctx, db, schema, table, columns, count-1, limitRange, args, collation)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Debug("get split values by random", zap.Stringer("chunk", chunk), zap.Int("random values num", len(randomValues)))
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
