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

// DefaultChannelBuffer is the default size for channel buffer
const DefaultChannelBuffer = 1024

// BucketIterator is struct for bucket iterator
type BucketIterator struct {
	ctx    context.Context
	cancel context.CancelFunc

	buckets      []dbutil.Bucket
	table        *common.TableDiff
	indexColumns []*model.ColumnInfo

	chunkPool *utils.WorkerPool

	chunkSize int64
	chunks    []*chunk.Range
	nextChunk uint

	chunksCh   chan []*chunk.Range
	errCh      chan error
	indexID    int64
	progressID string

	dbConn *sql.DB
}

// NewBucketIterator return a new iterator
func NewBucketIterator(ctx context.Context, progressID string, table *common.TableDiff, dbConn *sql.DB) (*BucketIterator, error) {
	return NewBucketIteratorWithCheckpoint(ctx, progressID, table, dbConn, nil, utils.NewWorkerPool(1, "bucketIter"))
}

// NewBucketIteratorWithCheckpoint return a new iterator
func NewBucketIteratorWithCheckpoint(
	ctx context.Context,
	progressID string,
	table *common.TableDiff,
	dbConn *sql.DB,
	startRange *RangeInfo,
	bucketSpliterPool *utils.WorkerPool,
) (*BucketIterator, error) {
	if !utils.IsRangeTrivial(table.Range) {
		return nil, errors.Errorf(
			"BucketIterator does not support user configured Range. Range: %s",
			table.Range)
	}

	ctx, cancel := context.WithCancel(ctx)
	bs := &BucketIterator{
		ctx:    ctx,
		cancel: cancel,

		table:     table,
		chunkPool: bucketSpliterPool,
		chunkSize: table.ChunkSize,
		chunksCh:  make(chan []*chunk.Range, DefaultChannelBuffer),
		errCh:     make(chan error, 1),
		dbConn:    dbConn,

		progressID: progressID,
	}

	if err := bs.init(ctx, startRange); err != nil {
		return nil, errors.Trace(err)
	}

	// Let the progress bar begins to record the table.
	progress.StartTable(bs.progressID, 0, false)
	go bs.produceChunks(startRange)

	return bs, nil
}

// GetIndexID return the index id
func (s *BucketIterator) GetIndexID() int64 {
	return s.indexID
}

// Next return the next chunk
func (s *BucketIterator) Next() (*chunk.Range, error) {
	var ok bool
	if uint(len(s.chunks)) <= s.nextChunk {
		select {
		case err := <-s.errCh:
			return nil, errors.Trace(err)
		case s.chunks, ok = <-s.chunksCh:
			if !ok && s.chunks == nil {
				log.Info("close chunks channel for table",
					zap.String("schema", s.table.Schema), zap.String("table", s.table.Table))
				return nil, nil
			}
		}
		s.nextChunk = 0
		failpoint.Inject("ignore-last-n-chunk-in-bucket", func(v failpoint.Value) {
			log.Info("failpoint ignore-last-n-chunk-in-bucket injected (bucket splitter)", zap.Int("n", v.(int)))
			if len(s.chunks) <= 1+v.(int) {
				failpoint.Return(nil, nil)
			}
			s.chunks = s.chunks[:(len(s.chunks) - v.(int))]
		})
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
		log.Info("failpoint print-chunk-info injected (bucket splitter)", zap.Strings("lowerBounds", lowerBounds), zap.Strings("upperBounds", upperBounds), zap.String("indexCode", c.Index.ToString()))
	})
	return c, nil
}

func (s *BucketIterator) init(ctx context.Context, startRange *RangeInfo) error {
	fields, err := indexFieldsFromConfigString(s.table.Fields, s.table.Info)
	if err != nil {
		return err
	}

	s.nextChunk = 0
	buckets, err := dbutil.GetBucketsInfo(ctx, s.dbConn, s.table.Schema, s.table.Table, s.table.Info)
	if err != nil {
		return errors.Trace(err)
	}

	var indices []*model.IndexInfo
	if fields.IsEmpty() {
		indices, err = utils.GetBetterIndex(context.Background(), s.dbConn, s.table.Schema, s.table.Table, s.table.Info)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		// There are user configured "index-fields", so we will try to match from all indices.
		indices = dbutil.FindAllIndex(s.table.Info)
	}

NEXTINDEX:
	for _, index := range indices {
		if index == nil {
			continue
		}
		if startRange != nil && startRange.IndexID != index.ID {
			continue
		}

		indexColumns := utils.GetColumnsFromIndex(index, s.table.Info)

		if len(indexColumns) < len(index.Columns) {
			// some column in index is ignored.
			continue
		}

		if !fields.MatchesIndex(index) {
			// We are enforcing user configured "index-fields" settings.
			continue
		}

		// skip the index that has expression column
		for _, col := range indexColumns {
			if col.Hidden {
				continue NEXTINDEX
			}
		}

		bucket, ok := buckets[index.Name.O]
		if !ok {
			// We found an index matching the "index-fields", but no bucket is found
			// for that index. Returning an error here will make the caller retry with
			// the random splitter.
			return errors.NotFoundf("index %s in buckets info", index.Name.O)
		}
		log.Debug("buckets for index", zap.String("index", index.Name.O), zap.Reflect("buckets", buckets))

		s.buckets = bucket
		s.indexColumns = indexColumns
		s.indexID = index.ID
		break
	}

	if s.buckets == nil || s.indexColumns == nil {
		return errors.NotFoundf("no index to split buckets")
	}

	// Notice: `cnt` is only an estimated value
	cnt := s.buckets[len(s.buckets)-1].Count
	// We can use config file to fix chunkSize,
	// otherwise chunkSize is 0.
	if s.chunkSize <= 0 {
		s.chunkSize = utils.CalculateChunkSize(cnt)
	}

	log.Info("get chunk size for table", zap.Int64("chunk size", s.chunkSize),
		zap.String("db", s.table.Schema), zap.String("table", s.table.Table))
	return nil
}

// Close closes the iterator
func (s *BucketIterator) Close() {
	s.cancel()
	s.chunkPool.WaitFinished()
}

func (s *BucketIterator) splitChunkForBucket(
	firstBucketID, lastBucketID, beginIndex int,
	bucketChunkCnt, splitChunkCnt int,
	chunkRange *chunk.Range,
) {
	s.chunkPool.Apply(func() {
		chunks, err := splitRangeByRandom(s.ctx, s.dbConn, chunkRange, splitChunkCnt, s.table.Schema, s.table.Table, s.indexColumns, s.table.Range, s.table.Collation)
		if err != nil {
			select {
			case <-s.ctx.Done():
			case s.errCh <- errors.Trace(err):
			}
			return
		}
		chunk.InitChunks(chunks, chunk.Bucket, firstBucketID, lastBucketID, beginIndex, s.table.Collation, s.table.Range, bucketChunkCnt)
		progress.UpdateTotal(s.progressID, len(chunks), false)
		s.chunksCh <- chunks
	})
}

func (s *BucketIterator) produceChunks(startRange *RangeInfo) {
	defer func() {
		s.chunkPool.WaitFinished()
		close(s.chunksCh)
		progress.UpdateTotal(s.progressID, 0, true)
	}()
	var (
		lowerValues, upperValues []string
		latestCount              int64
		err                      error
	)
	firstBucket := 0
	if startRange != nil {
		c := startRange.GetChunk()
		if c.IsLastChunkForTable() {
			// the last checkpoint range is the last chunk so return
			return
		}
		// init values for the next bucket
		firstBucket = c.Index.BucketIndexRight + 1
		// Note: Since this chunk is not the last one,
		//       its bucketID is less than len(s.buckets)
		if c.Index.BucketIndexRight >= len(s.buckets) {
			select {
			case <-s.ctx.Done():
			case s.errCh <- errors.New("Wrong Bucket: Bucket index of the checkpoint node is larger than buckets' size"):
			}
			return
		}
		latestCount = s.buckets[c.Index.BucketIndexRight].Count
		nextUpperValues, err := dbutil.AnalyzeValuesFromBuckets(s.buckets[c.Index.BucketIndexRight].UpperBound, s.indexColumns)
		if err != nil {
			select {
			case <-s.ctx.Done():
			case s.errCh <- errors.Trace(err):
			}
			return
		}
		lowerValues = nextUpperValues

		// build left chunks for this bucket
		leftCnt := c.Index.ChunkCnt - c.Index.ChunkIndex - 1
		if leftCnt > 0 {
			chunkRange := chunk.NewChunkRange()

			for i, column := range s.indexColumns {
				chunkRange.Update(column.Name.O, "", nextUpperValues[i], false, true)
			}

			for _, bound := range c.Bounds {
				chunkRange.Update(bound.Column, bound.Upper, "", true, false)
			}

			s.splitChunkForBucket(c.Index.BucketIndexLeft, c.Index.BucketIndexRight, c.Index.ChunkIndex+1, c.Index.ChunkCnt, leftCnt, chunkRange)
		}
	}
	halfChunkSize := s.chunkSize >> 1
	// `firstBucket` is the first bucket of one chunk.
	// It is equivalent to `BucketLeftIndex` of the chunk's ID.
	for i := firstBucket; i < len(s.buckets); i++ {
		count := s.buckets[i].Count - latestCount
		if count < s.chunkSize {
			// merge more buckets into one chunk
			continue
		}

		upperValues, err = dbutil.AnalyzeValuesFromBuckets(s.buckets[i].UpperBound, s.indexColumns)
		if err != nil {
			select {
			case <-s.ctx.Done():
			case s.errCh <- errors.Trace(err):
			}
			return
		}

		chunkRange := chunk.NewChunkRange()
		for j, column := range s.indexColumns {
			var lowerValue, upperValue string
			if len(lowerValues) > 0 {
				lowerValue = lowerValues[j]
			}
			if len(upperValues) > 0 {
				upperValue = upperValues[j]
			}
			chunkRange.Update(column.Name.O, lowerValue, upperValue, len(lowerValues) > 0, len(upperValues) > 0)
		}

		// `splitRangeByRandom` will skip when chunkCnt <= 1
		// assume the number of the selected buckets is `x`
		// if x >= 2                              ->  chunkCnt = 1
		// if x = 1                               ->  chunkCnt = (count + halfChunkSize) / chunkSize
		// count >= chunkSize
		if i == firstBucket {
			//
			chunkCnt := int((count + halfChunkSize) / s.chunkSize)
			s.splitChunkForBucket(firstBucket, i, 0, chunkCnt, chunkCnt, chunkRange)
		} else {
			// use multi-buckets so chunkCnt = 1
			s.splitChunkForBucket(firstBucket, i, 0, 1, 1, chunkRange)
		}

		latestCount = s.buckets[i].Count
		lowerValues = upperValues
		firstBucket = i + 1

		failpoint.Inject("check-one-bucket", func() {
			log.Info("failpoint check-one-bucket injected, stop producing new chunks.")
			failpoint.Return()
		})
	}

	// merge the rest keys into one chunk
	chunkRange := chunk.NewChunkRange()
	if len(lowerValues) > 0 {
		for j, column := range s.indexColumns {
			chunkRange.Update(column.Name.O, lowerValues[j], "", true, false)
		}
	}
	// When the table is much less than chunkSize,
	// it will return a chunk include the whole table.
	s.splitChunkForBucket(firstBucket, len(s.buckets), 0, 1, 1, chunkRange)
}
