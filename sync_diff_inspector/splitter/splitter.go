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
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	tiflowdbutil "github.com/pingcap/tiflow/pkg/dbutil"
	"github.com/pingcap/tiflow/sync_diff_inspector/checkpoints"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
	"github.com/pingcap/tiflow/sync_diff_inspector/source/common"
	"github.com/pingcap/tiflow/sync_diff_inspector/utils"
	"go.uber.org/zap"
)

const (
	// SplitThreshold is the threshold for splitting
	SplitThreshold = 1000
)

// ChunkIterator generate next chunk for only one table lazily.
type ChunkIterator interface {
	// Next seeks the next chunk, return nil if seeks to end.
	Next() (*chunk.Range, error)

	// Close close the current iterator.
	Close()

	GetIndexID() int64
}

type iteratorResult struct {
	chunk *chunk.Range
	err   error
}

// RangeInfo represents the unit of a process chunk.
// It's the only entrance of checkpoint.
type RangeInfo struct {
	ChunkRange *chunk.Range `json:"chunk-range"`
	// for bucket checkpoint
	IndexID int64 `json:"index-id"`

	ProgressID string `json:"progress-id"`
}

// GetTableIndex returns the index of table diffs.
// IMPORTANT!!!
// We need to keep the tables order during checkpoint.
// So we should have to save the config info to checkpoint file too
func (r *RangeInfo) GetTableIndex() int { return r.ChunkRange.Index.TableIndex }

// GetBucketIndexLeft returns the BucketIndexLeft
func (r *RangeInfo) GetBucketIndexLeft() int { return r.ChunkRange.Index.BucketIndexLeft }

// GetBucketIndexRight returns the BucketIndexRight
func (r *RangeInfo) GetBucketIndexRight() int { return r.ChunkRange.Index.BucketIndexRight }

// GetChunkIndex returns the ChunkIndex
func (r *RangeInfo) GetChunkIndex() int { return r.ChunkRange.Index.ChunkIndex }

// GetChunk returns the chunk
func (r *RangeInfo) GetChunk() *chunk.Range {
	return r.ChunkRange
}

// Copy returns a copy of RangeInfo
func (r *RangeInfo) Copy() *RangeInfo {
	return &RangeInfo{
		ChunkRange: r.ChunkRange.Clone(),
		IndexID:    r.IndexID,
		ProgressID: r.ProgressID,
	}
}

// Update updates the current RangeInfo
func (r *RangeInfo) Update(column, lower, upper string, updateLower, updateUpper bool, collation, limits string) {
	r.ChunkRange.Update(column, lower, upper, updateLower, updateUpper)
	conditions, args := r.ChunkRange.ToString(collation)
	r.ChunkRange.Where = fmt.Sprintf("((%s) AND (%s))", conditions, limits)
	r.ChunkRange.Args = args
}

// ToNode converts RangeInfo to node
func (r *RangeInfo) ToNode() *checkpoints.Node {
	return &checkpoints.Node{
		ChunkRange: r.ChunkRange,
		IndexID:    r.IndexID,
	}
}

// FromNode converts the Node into RangeInfo
func FromNode(n *checkpoints.Node) *RangeInfo {
	return &RangeInfo{
		ChunkRange: n.ChunkRange,
		IndexID:    n.IndexID,
	}
}

// IndexCandidate represents an index that can be used to split data.
type IndexCandidate struct {
	Index       *model.IndexInfo
	Columns     []*model.ColumnInfo
	selectivity float64
}

// ChooseSplitType chooses a split type and index candidate.
func ChooseSplitType(
	ctx context.Context,
	db *sql.DB,
	table *common.TableDiff,
	startRange *RangeInfo,
) (chunk.Type, *IndexCandidate, error) {
	logger := log.L().With(
		zap.String("table", dbutil.TableName(table.Schema, table.Table)))

	// If we have checkpoint info, use it directly.
	if startRange != nil {
		candidate, err := pickCandidateFromCheckpoint(
			ctx, table, startRange, table.Fields, db)
		logger.Debug("choose splitter from checkpoint",
			zap.String("type", startRange.ChunkRange.Type.String()))
		return startRange.ChunkRange.Type, candidate, err
	}

	candidates, err := BuildIndexCandidates(
		ctx, db, table, table.Fields)
	if err != nil {
		return chunk.Others, nil, errors.Trace(err)
	}

	logger.Debug("build index candidates",
		zap.Int("count", len(candidates)),
		zap.String("fields", table.Fields))

	// If not TiDB or no candidate, use random splitter.
	isTiDB, err := dbutil.IsTiDB(ctx, db)
	if err != nil || !isTiDB || len(candidates) == 0 {
		if err != nil {
			logger.Warn("failed to detect database, fallback to random splitter", zap.Error(err))
		}
		logger.Debug("choose random splitter",
			zap.Bool("is_tidb", isTiDB),
			zap.Int("candidates", len(candidates)))
		if len(candidates) > 0 {
			return chunk.Random, candidates[0], nil
		}
		candidate, err := BuildFakeCandidateForRandom(ctx, db, table, table.Fields)
		return chunk.Random, candidate, err
	}

	// Try to use bucket splitter first.
	if utils.IsRangeTrivial(table.Range) {
		buckets, err := tiflowdbutil.GetBucketsInfo(
			ctx, db, table.Schema, table.Table, table.Info)
		if err == nil {
			for _, candidate := range candidates {
				if _, ok := buckets[candidate.Index.Name.O]; ok {
					logger.Debug("choose bucket splitter",
						zap.String("index", candidate.Index.Name.O))
					return chunk.Bucket, candidate, nil
				}
			}
		}
	}

	logger.Debug("bucket splitter not available, try other splitter")

	if table.UseLimitIterator {
		logger.Debug("choose limit splitter due to user config",
			zap.String("range", table.Range))
		return chunk.Limit, candidates[0], nil
	}

	logger.Debug("choose random splitter due to non-trivial range")
	return chunk.Random, candidates[0], nil
}

// BuildFakeCandidateForRandom builds a fake index candidate for Random splitter.
// It's used when there is no real index can be used, either no index exists or
// user has specified some fields that no index can cover. The index returned is
// not a real index in database, but just used for splitting data.
func BuildFakeCandidateForRandom(
	ctx context.Context,
	dbConn *sql.DB,
	table *common.TableDiff,
	strFields string,
) (*IndexCandidate, error) {
	fakeCandidate := &IndexCandidate{
		Index:   &model.IndexInfo{ID: -1},
		Columns: make([]*model.ColumnInfo, 0),
	}

	if len(strFields) == 0 {
		for _, col := range table.Info.Columns {
			if !col.Hidden {
				fakeCandidate.Columns = append(fakeCandidate.Columns, col)
				return fakeCandidate, nil
			}
		}
	}

	for _, str := range strings.Split(strFields, ",") {
		colName := strings.TrimSpace(str)
		col := dbutil.FindColumnByName(table.Info.Columns, colName)
		if col == nil {
			return nil, errors.NotFoundf("column %s in %s.%s",
				colName, table.Schema, table.Table)
		}
		fakeCandidate.Columns = append(fakeCandidate.Columns, col)
	}
	return fakeCandidate, nil
}

// pickCandidateFromCheckpoint finds the index candidate from checkpoint info.
func pickCandidateFromCheckpoint(
	ctx context.Context,
	table *common.TableDiff,
	startRange *RangeInfo,
	strFields string,
	db *sql.DB,
) (*IndexCandidate, error) {
	index := table.Info.FindIndexByID(startRange.IndexID)
	if index != nil {
		return &IndexCandidate{
			Index:   index,
			Columns: utils.GetColumnsFromIndex(index, table.Info),
		}, nil
	}

	// It's possible that no index can be found when using Random splitter.
	// But for other splitters, we must find the index.
	if startRange.ChunkRange.Type == chunk.Random {
		return BuildFakeCandidateForRandom(ctx, db, table, strFields)
	}
	return nil, errors.New("can't find index from checkpoint")
}

// BuildIndexCandidates builds index candidates according to user specified fields.
// If no fields specified, return all indices, sorted by uniqueness and cardinality.
// Otherwise, return indices that can cover all specified fields.
func BuildIndexCandidates(
	ctx context.Context,
	dbConn *sql.DB,
	table *common.TableDiff,
	strFields string,
) ([]*IndexCandidate, error) {
	userIndexFields, err := indexFieldsFromConfigString(strFields, table.Info)
	if err != nil {
		return nil, errors.Trace(err)
	}

	candidates := make([]*IndexCandidate, 0)
	for _, index := range dbutil.FindAllIndex(table.Info) {
		if isValidIndex(index, table.Info) && userIndexFields.MatchesIndex(index) {
			candidates = append(candidates, &IndexCandidate{
				Index:   index,
				Columns: utils.GetColumnsFromIndex(index, table.Info),
			})
		}
	}

	isTiDB, err := dbutil.IsTiDB(ctx, dbConn)
	if err != nil || !isTiDB {
		return candidates, nil
	}
	if len(candidates) <= 1 {
		return candidates, nil
	}
	for _, candidate := range candidates {
		if candidate.Index.Unique || candidate.Index.Primary {
			return candidates, nil
		}
	}

	// No PK/UK, sort by cardinality
	for _, candidate := range candidates {
		candidate.selectivity, err = utils.GetSelectivity(
			ctx, dbConn, table.Schema, table.Table, candidate.Columns[0].Name.O, table.Info)
		if err != nil {
			return candidates, errors.Trace(err)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].selectivity > candidates[j].selectivity
	})
	return candidates, nil
}

func isValidIndex(index *model.IndexInfo, tableInfo *model.TableInfo) bool {
	if index == nil {
		return false
	}
	for _, idxCol := range index.Columns {
		if idxCol.Offset < 0 || idxCol.Offset >= len(tableInfo.Columns) {
			return false
		}
		col := tableInfo.Columns[idxCol.Offset]
		if col.Hidden {
			return false
		}
	}
	return true
}
