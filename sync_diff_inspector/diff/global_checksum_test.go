// Copyright 2024 PingCAP, Inc.
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

package diff

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tiflow/sync_diff_inspector/checkpoints"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
	"github.com/pingcap/tiflow/sync_diff_inspector/source"
	"github.com/pingcap/tiflow/sync_diff_inspector/source/common"
	"github.com/pingcap/tiflow/sync_diff_inspector/splitter"
	"github.com/stretchr/testify/require"
)

type mockChecksumIterator struct {
	chunks []*chunk.Range
	next   int
}

func (m *mockChecksumIterator) Next() (*chunk.Range, error) {
	if m.next >= len(m.chunks) {
		return nil, nil
	}
	c := m.chunks[m.next]
	m.next++
	return c.Clone(), nil
}

func (m *mockChecksumIterator) Close() {}

type mockChecksumSource struct {
	iterator splitter.ChunkIterator

	startRange *splitter.RangeInfo
	infos      map[int]*source.ChecksumInfo
	delays     map[int]time.Duration
}

func (m *mockChecksumSource) GetChecksumOnlyIteratorWithStart(
	_ context.Context,
	_ int,
	startRange *splitter.RangeInfo,
) (splitter.ChunkIterator, error) {
	if startRange != nil {
		m.startRange = startRange.Copy()
	} else {
		m.startRange = nil
	}
	return m.iterator, nil
}

func (m *mockChecksumSource) GetTableAnalyzer() source.TableAnalyzer {
	return nil
}

func (m *mockChecksumSource) GetRangeIterator(context.Context, *splitter.RangeInfo, source.TableAnalyzer, int) (source.RangeIterator, error) {
	return nil, nil
}

func (m *mockChecksumSource) GetCountAndMD5(ctx context.Context, tableRange *splitter.RangeInfo) *source.ChecksumInfo {
	idx := tableRange.ChunkRange.Index.ChunkIndex
	if delay, ok := m.delays[idx]; ok && delay > 0 {
		select {
		case <-ctx.Done():
			return &source.ChecksumInfo{Err: ctx.Err()}
		case <-time.After(delay):
		}
	}
	info, ok := m.infos[idx]
	if !ok {
		return &source.ChecksumInfo{Err: fmt.Errorf("missing checksum info for chunk %d", idx)}
	}
	ret := *info
	return &ret
}

func (m *mockChecksumSource) GetCountForLackTable(context.Context, *splitter.RangeInfo) int64 {
	return 0
}

func (m *mockChecksumSource) GetRowsIterator(context.Context, *splitter.RangeInfo) (source.RowDataIterator, error) {
	return nil, nil
}

func (m *mockChecksumSource) GenerateFixSQL(source.DMLType, map[string]*dbutil.ColumnData, map[string]*dbutil.ColumnData, int) string {
	return ""
}

func (m *mockChecksumSource) GetTables() []*common.TableDiff {
	return nil
}

func (m *mockChecksumSource) GetSourceStructInfo(context.Context, int) ([]*model.TableInfo, error) {
	return nil, nil
}

func (m *mockChecksumSource) GetDB() *sql.DB {
	return nil
}

func (m *mockChecksumSource) GetSnapshot() string {
	return ""
}

func (m *mockChecksumSource) Close() {}

// GetChecksumOnlyIterator adapts the mock to checksumOnlyIteratorSource.
func (m *mockChecksumSource) GetChecksumOnlyIterator(
	ctx context.Context,
	tableIndex int,
	startRange *splitter.RangeInfo,
) (splitter.ChunkIterator, error) {
	return m.GetChecksumOnlyIteratorWithStart(ctx, tableIndex, startRange)
}

func newChecksumChunk(idx int) *chunk.Range {
	return &chunk.Range{
		Index: &chunk.CID{
			ChunkIndex: idx,
			ChunkCnt:   2,
		},
	}
}

func TestGetSourceGlobalChecksumKeepsCheckpointOrder(t *testing.T) {
	df := &Diff{checkThreadCount: 2}
	state := &checkpoints.ChecksumSourceState{}

	src := &mockChecksumSource{
		iterator: &mockChecksumIterator{
			chunks: []*chunk.Range{newChecksumChunk(0), newChecksumChunk(1)},
		},
		infos: map[int]*source.ChecksumInfo{
			0: {Count: 5, Checksum: 7},
			1: {Count: 11, Checksum: 13},
		},
		delays: map[int]time.Duration{
			0: 40 * time.Millisecond,
		},
	}

	count, checksum, err := df.getSourceGlobalChecksum(context.Background(), src, 0, state, "")
	require.NoError(t, err)
	require.Equal(t, int64(16), count)
	require.Equal(t, uint64(10), checksum)

	require.True(t, state.Done)
	require.Equal(t, int64(16), state.Count)
	require.Equal(t, uint64(10), state.Checksum)
	require.NotNil(t, state.LastRange)
	require.Equal(t, 1, state.LastRange.Index.ChunkIndex)
}

func TestGetSourceGlobalChecksumResumeFromLastRange(t *testing.T) {
	lastRange := newChecksumChunk(3)
	state := &checkpoints.ChecksumSourceState{
		LastRange: lastRange,
		Count:     9,
		Checksum:  12,
	}

	src := &mockChecksumSource{
		iterator: &mockChecksumIterator{},
		infos:    map[int]*source.ChecksumInfo{},
	}
	df := &Diff{checkThreadCount: 1}

	count, checksum, err := df.getSourceGlobalChecksum(context.Background(), src, 0, state, "")
	require.NoError(t, err)
	require.Equal(t, int64(9), count)
	require.Equal(t, uint64(12), checksum)
	require.True(t, state.Done)

	require.NotNil(t, src.startRange)
	require.NotNil(t, src.startRange.ChunkRange)
	require.Equal(t, 3, src.startRange.ChunkRange.Index.ChunkIndex)
	require.NotSame(t, state.LastRange, src.startRange.ChunkRange)
}
