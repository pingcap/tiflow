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

package source

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
	"github.com/pingcap/tiflow/sync_diff_inspector/progress"
	"github.com/pingcap/tiflow/sync_diff_inspector/source/common"
	"github.com/pingcap/tiflow/sync_diff_inspector/splitter"
	"github.com/pingcap/tiflow/sync_diff_inspector/utils"
)

// ChunksIterator is used for single mysql/tidb source.
type ChunksIterator struct {
	ctx    context.Context
	cancel context.CancelFunc

	ID            *chunk.CID
	tableAnalyzer TableAnalyzer

	TableDiffs       []*common.TableDiff
	chunksCh         chan *splitter.RangeInfo
	errCh            chan error
	splitThreadCount int

	pool *utils.WorkerPool
}

// NewChunksIterator returns a new iterator
func NewChunksIterator(
	ctx context.Context,
	analyzer TableAnalyzer,
	tableDiffs []*common.TableDiff,
	startRange *splitter.RangeInfo,
	splitThreadCount int,
) (*ChunksIterator, error) {
	ctxx, cancel := context.WithCancel(ctx)
	iter := &ChunksIterator{
		ctx:    ctxx,
		cancel: cancel,

		splitThreadCount: splitThreadCount,
		tableAnalyzer:    analyzer,
		TableDiffs:       tableDiffs,

		// reserve 30 capacity for each goroutine on average
		chunksCh: make(chan *splitter.RangeInfo, 30*splitThreadCount),
		errCh:    make(chan error, len(tableDiffs)),
		pool:     utils.NewWorkerPool(uint(splitThreadCount), "chunks producer"),
	}
	go iter.produceChunks(startRange)
	return iter, nil
}

func (t *ChunksIterator) produceChunks(startRange *splitter.RangeInfo) {
	defer func() {
		t.pool.WaitFinished()
		close(t.chunksCh)
	}()

	nextTableIndex := 0

	// If chunkRange
	if startRange != nil {
		curIndex := startRange.GetTableIndex()
		curTable := t.TableDiffs[curIndex]
		nextTableIndex = curIndex + 1

		// if this chunk is empty, data-check for this table should be skipped
		if startRange.ChunkRange.Type != chunk.Empty {
			t.pool.Apply(func() {
				chunkIter, err := t.tableAnalyzer.AnalyzeSplitter(t.ctx, curTable, startRange)
				if err != nil {
					t.errCh <- errors.Trace(err)
					return
				}
				defer chunkIter.Close()
				for {
					c, err := chunkIter.Next()
					if err != nil {
						t.errCh <- errors.Trace(err)
						return
					}
					if c == nil {
						break
					}
					c.Index.TableIndex = curIndex
					select {
					case <-t.ctx.Done():
						log.Info("Stop do produce chunks by context done")
						return
					case t.chunksCh <- &splitter.RangeInfo{
						ChunkRange: c,
						IndexID:    getCurTableIndexID(chunkIter),
						ProgressID: dbutil.TableName(curTable.Schema, curTable.Table),
					}:
					}
				}
			})
		}
	}

	for ; nextTableIndex < len(t.TableDiffs); nextTableIndex++ {
		curTableIndex := nextTableIndex
		// skip data-check, but still need to send a empty chunk to make checkpoint continuous
		if t.TableDiffs[curTableIndex].IgnoreDataCheck || !common.AllTableExist(t.TableDiffs[curTableIndex].TableLack) {
			t.pool.Apply(func() {
				table := t.TableDiffs[curTableIndex]
				progressID := dbutil.TableName(table.Schema, table.Table)
				progress.StartTable(progressID, 1, true)
				select {
				case <-t.ctx.Done():
					log.Info("Stop do produce chunks by context done")
					return
				case t.chunksCh <- &splitter.RangeInfo{
					ChunkRange: &chunk.Range{
						Index: &chunk.CID{
							TableIndex: curTableIndex,
						},
						Type:    chunk.Empty,
						IsFirst: true,
						IsLast:  true,
					},
					ProgressID: progressID,
				}:
				}
			})
			continue
		}

		t.pool.Apply(func() {
			table := t.TableDiffs[curTableIndex]
			chunkIter, err := t.tableAnalyzer.AnalyzeSplitter(t.ctx, table, nil)
			if err != nil {
				t.errCh <- errors.Trace(err)
				return
			}
			defer chunkIter.Close()
			for {
				c, err := chunkIter.Next()
				if err != nil {
					t.errCh <- errors.Trace(err)
					return
				}
				if c == nil {
					break
				}
				c.Index.TableIndex = curTableIndex
				select {
				case <-t.ctx.Done():
					log.Info("Stop do produce chunks by context done")
					return
				case t.chunksCh <- &splitter.RangeInfo{
					ChunkRange: c,
					IndexID:    getCurTableIndexID(chunkIter),
					ProgressID: dbutil.TableName(table.Schema, table.Table),
				}:
				}
			}
		})
	}
}

// Next returns the next chunk
func (t *ChunksIterator) Next(ctx context.Context) (*splitter.RangeInfo, error) {
	select {
	case <-ctx.Done():
		return nil, nil
	case r, ok := <-t.chunksCh:
		if !ok && r == nil {
			return nil, nil
		}
		return r, nil
	case err := <-t.errCh:
		return nil, errors.Trace(err)
	}
}

// Close closes the iterator
func (t *ChunksIterator) Close() {
	t.cancel()
	t.pool.WaitFinished()
}

// TODO: getCurTableIndexID only used for binary search, should be optimized later.
func getCurTableIndexID(tableIter splitter.ChunkIterator) int64 {
	if bt, ok := tableIter.(*splitter.BucketIterator); ok {
		return bt.GetIndexID()
	}
	return 0
}
