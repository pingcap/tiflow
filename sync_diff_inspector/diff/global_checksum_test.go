// Copyright 2026 PingCAP, Inc.
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
	"io"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tiflow/sync_diff_inspector/checkpoints"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
	"github.com/pingcap/tiflow/sync_diff_inspector/config"
	"github.com/pingcap/tiflow/sync_diff_inspector/progress"
	"github.com/pingcap/tiflow/sync_diff_inspector/report"
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
	iterator  *mockChecksumIterator
	iterators map[int]*mockChecksumIterator

	startRange   *splitter.RangeInfo
	tables       []*common.TableDiff
	infos        map[int]*source.ChecksumInfo
	infosByTable map[int]map[int]*source.ChecksumInfo
	// barriers maps chunk index to a channel that must be closed before
	// GetCountAndMD5 returns for that chunk. Used for deterministic ordering.
	barriers map[int]<-chan struct{}
	// onCalled is invoked with the chunk index when GetCountAndMD5 starts.
	onCalled         func(idx int)
	getIteratorCalls []int
}

func (m *mockChecksumSource) getIterator(
	tableIndex int,
	startRange *splitter.RangeInfo,
) (*mockChecksumIterator, int, error) {
	if startRange != nil {
		m.startRange = startRange.Copy()
	} else {
		m.startRange = nil
	}
	m.getIteratorCalls = append(m.getIteratorCalls, tableIndex)
	if m.iterators != nil {
		iter, ok := m.iterators[tableIndex]
		if !ok {
			return nil, 0, fmt.Errorf("missing iterator for table %d", tableIndex)
		}
		return iter, len(iter.chunks), nil
	}
	return m.iterator, len(m.iterator.chunks), nil
}

func (m *mockChecksumSource) GetTableAnalyzer() source.TableAnalyzer {
	return nil
}

func (m *mockChecksumSource) GetRangeIterator(context.Context, *splitter.RangeInfo, source.TableAnalyzer, int) (source.RangeIterator, error) {
	return nil, nil
}

func (m *mockChecksumSource) GetCountAndMD5(ctx context.Context, tableRange *splitter.RangeInfo) *source.ChecksumInfo {
	tableIdx := tableRange.ChunkRange.Index.TableIndex
	idx := tableRange.ChunkRange.Index.ChunkIndex
	if m.onCalled != nil {
		m.onCalled(idx)
	}
	if ch, ok := m.barriers[idx]; ok {
		select {
		case <-ctx.Done():
			return &source.ChecksumInfo{Err: ctx.Err()}
		case <-ch:
		}
	}
	infos := m.infos
	if m.infosByTable != nil {
		var ok bool
		infos, ok = m.infosByTable[tableIdx]
		if !ok {
			return &source.ChecksumInfo{Err: fmt.Errorf("missing checksum infos for table %d", tableIdx)}
		}
	}
	info, ok := infos[idx]
	if !ok {
		return &source.ChecksumInfo{Err: fmt.Errorf("missing checksum info for table %d chunk %d", tableIdx, idx)}
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
	return m.tables
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

func (m *mockChecksumSource) GetGlobalChecksumIterator(
	_ context.Context,
	tableIndex int,
	startRange *splitter.RangeInfo,
) (splitter.ChunkIterator, int, error) {
	return m.getIterator(tableIndex, startRange)
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

	// Use a channel barrier to deterministically force chunk 0 to complete
	// after chunk 1, without relying on wall-clock timing.
	chunk1Started := make(chan struct{})
	src := &mockChecksumSource{
		iterator: &mockChecksumIterator{
			chunks: []*chunk.Range{newChecksumChunk(0), newChecksumChunk(1)},
		},
		infos: map[int]*source.ChecksumInfo{
			0: {Count: 5, Checksum: 7},
			1: {Count: 11, Checksum: 13},
		},
		barriers: map[int]<-chan struct{}{
			0: chunk1Started, // chunk 0 waits until chunk 1 has started
		},
		onCalled: func(idx int) {
			if idx == 1 {
				close(chunk1Started)
			}
		},
	}

	iter, _, err := src.GetGlobalChecksumIterator(context.Background(), 0, nil)
	require.NoError(t, err)

	count, checksum, err := df.getSourceGlobalChecksum(context.Background(), src, 0, iter, state, "")
	require.NoError(t, err)
	require.Equal(t, int64(16), count)
	require.Equal(t, uint64(10), checksum)

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

	iter, _, err := src.GetGlobalChecksumIterator(context.Background(), 0, &splitter.RangeInfo{ChunkRange: lastRange.Clone()})
	require.NoError(t, err)

	count, checksum, err := df.getSourceGlobalChecksum(context.Background(), src, 0, iter, state, "")
	require.NoError(t, err)
	require.Equal(t, int64(9), count)
	require.Equal(t, uint64(12), checksum)

	require.NotNil(t, src.startRange)
	require.NotNil(t, src.startRange.ChunkRange)
	require.Equal(t, 3, src.startRange.ChunkRange.Index.ChunkIndex)
	require.NotSame(t, state.LastRange, src.startRange.ChunkRange)
}

func TestGlobalChecksumErrorCheckpointStopsAtCurrentTable(t *testing.T) {
	checkpointDir := t.TempDir()
	tableDiffs := []*common.TableDiff{
		{
			Schema: "test",
			Table:  "tbl0",
		},
		{
			Schema: "test",
			Table:  "tbl1",
		},
	}
	taskCfg := &config.TaskConfig{
		OutputDir: checkpointDir,
		FixDir:    filepath.Join(checkpointDir, "fix"),
	}

	errSrc := &mockChecksumSource{
		iterators: map[int]*mockChecksumIterator{
			0: {
				chunks: []*chunk.Range{
					{
						Index: &chunk.CID{
							TableIndex: 0,
							ChunkIndex: 0,
							ChunkCnt:   1,
						},
					},
				},
			},
			1: {
				chunks: []*chunk.Range{
					{
						Index: &chunk.CID{
							TableIndex: 1,
							ChunkIndex: 0,
							ChunkCnt:   1,
						},
					},
				},
			},
		},
		tables: tableDiffs,
		infosByTable: map[int]map[int]*source.ChecksumInfo{
			0: {
				0: {Err: fmt.Errorf("transient checksum error")},
			},
			1: {
				0: {Count: 8, Checksum: 11},
			},
		},
	}
	okSrc := &mockChecksumSource{
		iterators: map[int]*mockChecksumIterator{
			0: {
				chunks: []*chunk.Range{
					{
						Index: &chunk.CID{
							TableIndex: 0,
							ChunkIndex: 0,
							ChunkCnt:   1,
						},
					},
				},
			},
			1: {
				chunks: []*chunk.Range{
					{
						Index: &chunk.CID{
							TableIndex: 1,
							ChunkIndex: 0,
							ChunkCnt:   1,
						},
					},
				},
			},
		},
		tables: tableDiffs,
		infosByTable: map[int]map[int]*source.ChecksumInfo{
			0: {
				0: {Count: 5, Checksum: 7},
			},
			1: {
				0: {Count: 8, Checksum: 11},
			},
		},
	}

	df := &Diff{
		upstream:         errSrc,
		downstream:       okSrc,
		workSource:       okSrc,
		checkThreadCount: 1,
		cp:               new(checkpoints.Checkpoint),
		report:           report.NewReport(taskCfg),
		CheckpointDir:    checkpointDir,
	}
	df.report.Init(tableDiffs, nil, nil)
	progress.Init(len(tableDiffs), 0)
	progress.SetOutput(io.Discard)

	err := df.equalByGlobalChecksum(context.Background())
	require.NoError(t, err)

	state, reportInfo, err := df.cp.LoadChecksumState(filepath.Join(checkpointDir, checkpointFile))
	require.NoError(t, err)
	require.NotNil(t, state)
	require.NotNil(t, reportInfo)
	require.Equal(t, 0, state.TableIndex)

	result := reportInfo.TableResults["test"]["tbl0"]
	require.NotNil(t, result)
	require.True(t, result.DataEqual)
	require.Empty(t, result.ChunkMap)
	require.Zero(t, result.UpCount)
	require.Zero(t, result.DownCount)
	result = reportInfo.TableResults["test"]["tbl1"]
	require.NotNil(t, result)
	require.True(t, result.DataEqual)
	require.Empty(t, result.ChunkMap)
	require.Zero(t, result.UpCount)
	require.Zero(t, result.DownCount)
	require.Equal(t, []int{0}, errSrc.getIteratorCalls)
	require.Equal(t, []int{0}, okSrc.getIteratorCalls)

	progress.Close()
}
