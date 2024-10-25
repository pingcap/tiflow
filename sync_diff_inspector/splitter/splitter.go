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
	"fmt"

	"github.com/pingcap/tiflow/sync_diff_inspector/checkpoints"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
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
