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

package chunk

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChunkUpdate(t *testing.T) {
	chunk := &Range{
		Bounds: []*Bound{
			{
				Column:   "a",
				Lower:    "1",
				Upper:    "2",
				HasLower: true,
				HasUpper: true,
			}, {
				Column:   "b",
				Lower:    "3",
				Upper:    "4",
				HasLower: true,
				HasUpper: true,
			},
		},
	}

	testCases := []struct {
		boundArgs  []string
		expectStr  string
		expectArgs []interface{}
	}{
		{
			[]string{"a", "5", "6"},
			"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
			[]interface{}{"5", "5", "3", "6", "6", "4"},
		}, {
			[]string{"b", "5", "6"},
			"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
			[]interface{}{"1", "1", "5", "2", "2", "6"},
		}, {
			[]string{"c", "7", "8"},
			"((`a` > ?) OR (`a` = ? AND `b` > ?) OR (`a` = ? AND `b` = ? AND `c` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` < ?) OR (`a` = ? AND `b` = ? AND `c` <= ?))",
			[]interface{}{"1", "1", "3", "1", "3", "7", "2", "2", "4", "2", "4", "8"},
		},
	}

	for _, cs := range testCases {
		newChunk := chunk.CopyAndUpdate(cs.boundArgs[0], cs.boundArgs[1], cs.boundArgs[2], true, true)
		conditions, args := newChunk.ToString("")
		require.Equal(t, conditions, cs.expectStr)
		require.Equal(t, args, cs.expectArgs)
	}

	// the origin chunk is not changed
	conditions, args := chunk.ToString("")
	require.Equal(t, conditions, "((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))")
	expectArgs := []interface{}{"1", "1", "3", "2", "2", "4"}
	require.Equal(t, args, expectArgs)

	// test chunk update build by offset
	columnOffset := map[string]int{
		"a": 1,
		"b": 0,
	}
	chunkRange := NewChunkRangeOffset(columnOffset)
	chunkRange.Update("a", "1", "2", true, true)
	chunkRange.Update("b", "3", "4", true, true)
	require.Equal(t, chunkRange.ToMeta(), "range in sequence: (3,1) < (b,a) <= (4,2)")
}

func TestChunkToString(t *testing.T) {
	// lower & upper
	chunk := &Range{
		Bounds: []*Bound{
			{
				Column:   "a",
				Lower:    "1",
				Upper:    "2",
				HasLower: true,
				HasUpper: true,
			}, {
				Column:   "b",
				Lower:    "3",
				Upper:    "4",
				HasLower: true,
				HasUpper: true,
			}, {
				Column:   "c",
				Lower:    "5",
				Upper:    "6",
				HasLower: true,
				HasUpper: true,
			},
		},
	}

	conditions, args := chunk.ToString("")
	require.Equal(t, conditions, "((`a` > ?) OR (`a` = ? AND `b` > ?) OR (`a` = ? AND `b` = ? AND `c` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` < ?) OR (`a` = ? AND `b` = ? AND `c` <= ?))")
	expectArgs := []string{"1", "1", "3", "1", "3", "5", "2", "2", "4", "2", "4", "6"}
	for i, arg := range args {
		require.Equal(t, arg, expectArgs[i])
	}

	conditions, args = chunk.ToString("latin1")
	require.Equal(t, conditions, "((`a` COLLATE 'latin1' > ?) OR (`a` COLLATE 'latin1' = ? AND `b` COLLATE 'latin1' > ?) OR (`a` COLLATE 'latin1' = ? AND `b` COLLATE 'latin1' = ? AND `c` COLLATE 'latin1' > ?)) AND ((`a` COLLATE 'latin1' < ?) OR (`a` COLLATE 'latin1' = ? AND `b` COLLATE 'latin1' < ?) OR (`a` COLLATE 'latin1' = ? AND `b` COLLATE 'latin1' = ? AND `c` COLLATE 'latin1' <= ?))")
	expectArgs = []string{"1", "1", "3", "1", "3", "5", "2", "2", "4", "2", "4", "6"}
	for i, arg := range args {
		require.Equal(t, arg, expectArgs[i])
	}

	require.Equal(t, chunk.String(), `{"index":null,"type":0,"bounds":[{"column":"a","lower":"1","upper":"2","has-lower":true,"has-upper":true},{"column":"b","lower":"3","upper":"4","has-lower":true,"has-upper":true},{"column":"c","lower":"5","upper":"6","has-lower":true,"has-upper":true}],"is-first":false,"is-last":false,"where":"","args":null}`)
	require.Equal(t, chunk.ToMeta(), "range in sequence: (1,3,5) < (a,b,c) <= (2,4,6)")

	// upper
	chunk = &Range{
		Bounds: []*Bound{
			{
				Column:   "a",
				Lower:    "1",
				Upper:    "2",
				HasLower: false,
				HasUpper: true,
			}, {
				Column:   "b",
				Lower:    "3",
				Upper:    "4",
				HasLower: false,
				HasUpper: true,
			}, {
				Column:   "c",
				Lower:    "5",
				Upper:    "6",
				HasLower: false,
				HasUpper: true,
			},
		},
	}

	conditions, args = chunk.ToString("latin1")
	require.Equal(t, conditions, "(`a` COLLATE 'latin1' < ?) OR (`a` COLLATE 'latin1' = ? AND `b` COLLATE 'latin1' < ?) OR (`a` COLLATE 'latin1' = ? AND `b` COLLATE 'latin1' = ? AND `c` COLLATE 'latin1' <= ?)")
	expectArgs = []string{"2", "2", "4", "2", "4", "6"}
	for i, arg := range args {
		require.Equal(t, arg, expectArgs[i])
	}

	require.Equal(t, chunk.String(), `{"index":null,"type":0,"bounds":[{"column":"a","lower":"1","upper":"2","has-lower":false,"has-upper":true},{"column":"b","lower":"3","upper":"4","has-lower":false,"has-upper":true},{"column":"c","lower":"5","upper":"6","has-lower":false,"has-upper":true}],"is-first":false,"is-last":false,"where":"","args":null}`)
	require.Equal(t, chunk.ToMeta(), "range in sequence: (a,b,c) <= (2,4,6)")

	// lower
	chunk = &Range{
		Bounds: []*Bound{
			{
				Column:   "a",
				Lower:    "1",
				Upper:    "2",
				HasLower: true,
				HasUpper: false,
			}, {
				Column:   "b",
				Lower:    "3",
				Upper:    "4",
				HasLower: true,
				HasUpper: false,
			}, {
				Column:   "c",
				Lower:    "5",
				Upper:    "6",
				HasLower: true,
				HasUpper: false,
			},
		},
	}

	conditions, args = chunk.ToString("")
	require.Equal(t, conditions, "(`a` > ?) OR (`a` = ? AND `b` > ?) OR (`a` = ? AND `b` = ? AND `c` > ?)")
	expectArgs = []string{"1", "1", "3", "1", "3", "5"}
	for i, arg := range args {
		require.Equal(t, arg, expectArgs[i])
	}

	conditions, args = chunk.ToString("latin1")
	require.Equal(t, conditions, "(`a` COLLATE 'latin1' > ?) OR (`a` COLLATE 'latin1' = ? AND `b` COLLATE 'latin1' > ?) OR (`a` COLLATE 'latin1' = ? AND `b` COLLATE 'latin1' = ? AND `c` COLLATE 'latin1' > ?)")
	expectArgs = []string{"1", "1", "3", "1", "3", "5"}
	for i, arg := range args {
		require.Equal(t, arg, expectArgs[i])
	}

	require.Equal(t, chunk.String(), `{"index":null,"type":0,"bounds":[{"column":"a","lower":"1","upper":"2","has-lower":true,"has-upper":false},{"column":"b","lower":"3","upper":"4","has-lower":true,"has-upper":false},{"column":"c","lower":"5","upper":"6","has-lower":true,"has-upper":false}],"is-first":false,"is-last":false,"where":"","args":null}`)
	require.Equal(t, chunk.ToMeta(), "range in sequence: (1,3,5) < (a,b,c)")

	// none
	chunk = &Range{
		Bounds: []*Bound{
			{
				Column:   "a",
				Lower:    "1",
				Upper:    "2",
				HasLower: false,
				HasUpper: false,
			}, {
				Column:   "b",
				Lower:    "3",
				Upper:    "4",
				HasLower: false,
				HasUpper: false,
			}, {
				Column:   "c",
				Lower:    "5",
				Upper:    "6",
				HasLower: false,
				HasUpper: false,
			},
		},
	}
	conditions, args = chunk.ToString("")
	require.Equal(t, conditions, "TRUE")
	expectArgs = []string{}
	for i, arg := range args {
		require.Equal(t, arg, expectArgs[i])
	}
	require.Equal(t, chunk.String(), `{"index":null,"type":0,"bounds":[{"column":"a","lower":"1","upper":"2","has-lower":false,"has-upper":false},{"column":"b","lower":"3","upper":"4","has-lower":false,"has-upper":false},{"column":"c","lower":"5","upper":"6","has-lower":false,"has-upper":false}],"is-first":false,"is-last":false,"where":"","args":null}`)
	require.Equal(t, chunk.ToMeta(), "range in sequence: Full")

	// same & lower & upper
	chunk = &Range{
		Bounds: []*Bound{
			{
				Column:   "a",
				Lower:    "1",
				Upper:    "1",
				HasLower: true,
				HasUpper: true,
			}, {
				Column:   "b",
				Lower:    "3",
				Upper:    "4",
				HasLower: true,
				HasUpper: true,
			}, {
				Column:   "c",
				Lower:    "5",
				Upper:    "5",
				HasLower: true,
				HasUpper: true,
			},
		},
	}

	conditions, args = chunk.ToString("")
	require.Equal(t, conditions, "(`a` = ?) AND ((`b` > ?) OR (`b` = ? AND `c` > ?)) AND ((`b` < ?) OR (`b` = ? AND `c` <= ?))")
	expectArgs = []string{"1", "3", "3", "5", "4", "4", "5"}
	for i, arg := range args {
		require.Equal(t, arg, expectArgs[i])
	}

	conditions, args = chunk.ToString("latin1")
	require.Equal(t, conditions, "(`a` COLLATE 'latin1' = ?) AND ((`b` COLLATE 'latin1' > ?) OR (`b` COLLATE 'latin1' = ? AND `c` COLLATE 'latin1' > ?)) AND ((`b` COLLATE 'latin1' < ?) OR (`b` COLLATE 'latin1' = ? AND `c` COLLATE 'latin1' <= ?))")
	expectArgs = []string{"1", "3", "3", "5", "4", "4", "5"}
	for i, arg := range args {
		require.Equal(t, arg, expectArgs[i])
	}

	require.Equal(t, chunk.String(), `{"index":null,"type":0,"bounds":[{"column":"a","lower":"1","upper":"1","has-lower":true,"has-upper":true},{"column":"b","lower":"3","upper":"4","has-lower":true,"has-upper":true},{"column":"c","lower":"5","upper":"5","has-lower":true,"has-upper":true}],"is-first":false,"is-last":false,"where":"","args":null}`)
	require.Equal(t, chunk.ToMeta(), "range in sequence: (1,3,5) < (a,b,c) <= (1,4,5)")

	// same & upper
	chunk = &Range{
		Bounds: []*Bound{
			{
				Column:   "a",
				Lower:    "2",
				Upper:    "2",
				HasLower: false,
				HasUpper: true,
			}, {
				Column:   "b",
				Lower:    "3",
				Upper:    "4",
				HasLower: false,
				HasUpper: true,
			}, {
				Column:   "c",
				Lower:    "5",
				Upper:    "6",
				HasLower: false,
				HasUpper: true,
			},
		},
	}

	conditions, args = chunk.ToString("latin1")
	require.Equal(t, conditions, "(`a` COLLATE 'latin1' < ?) OR (`a` COLLATE 'latin1' = ? AND `b` COLLATE 'latin1' < ?) OR (`a` COLLATE 'latin1' = ? AND `b` COLLATE 'latin1' = ? AND `c` COLLATE 'latin1' <= ?)")
	expectArgs = []string{"2", "2", "4", "2", "4", "6"}
	for i, arg := range args {
		require.Equal(t, arg, expectArgs[i])
	}

	require.Equal(t, chunk.String(), `{"index":null,"type":0,"bounds":[{"column":"a","lower":"2","upper":"2","has-lower":false,"has-upper":true},{"column":"b","lower":"3","upper":"4","has-lower":false,"has-upper":true},{"column":"c","lower":"5","upper":"6","has-lower":false,"has-upper":true}],"is-first":false,"is-last":false,"where":"","args":null}`)
	require.Equal(t, chunk.ToMeta(), "range in sequence: (a,b,c) <= (2,4,6)")

	// same & lower
	chunk = &Range{
		Bounds: []*Bound{
			{
				Column:   "a",
				Lower:    "1",
				Upper:    "1",
				HasLower: true,
				HasUpper: false,
			}, {
				Column:   "b",
				Lower:    "3",
				Upper:    "4",
				HasLower: true,
				HasUpper: false,
			}, {
				Column:   "c",
				Lower:    "5",
				Upper:    "6",
				HasLower: true,
				HasUpper: false,
			},
		},
	}

	conditions, args = chunk.ToString("")
	require.Equal(t, conditions, "(`a` > ?) OR (`a` = ? AND `b` > ?) OR (`a` = ? AND `b` = ? AND `c` > ?)")
	expectArgs = []string{"1", "1", "3", "1", "3", "5"}
	for i, arg := range args {
		require.Equal(t, arg, expectArgs[i])
	}

	conditions, args = chunk.ToString("latin1")
	require.Equal(t, conditions, "(`a` COLLATE 'latin1' > ?) OR (`a` COLLATE 'latin1' = ? AND `b` COLLATE 'latin1' > ?) OR (`a` COLLATE 'latin1' = ? AND `b` COLLATE 'latin1' = ? AND `c` COLLATE 'latin1' > ?)")
	expectArgs = []string{"1", "1", "3", "1", "3", "5"}
	for i, arg := range args {
		require.Equal(t, arg, expectArgs[i])
	}

	require.Equal(t, chunk.String(), `{"index":null,"type":0,"bounds":[{"column":"a","lower":"1","upper":"1","has-lower":true,"has-upper":false},{"column":"b","lower":"3","upper":"4","has-lower":true,"has-upper":false},{"column":"c","lower":"5","upper":"6","has-lower":true,"has-upper":false}],"is-first":false,"is-last":false,"where":"","args":null}`)
	require.Equal(t, chunk.ToMeta(), "range in sequence: (1,3,5) < (a,b,c)")

	// same & none
	chunk = &Range{
		Bounds: []*Bound{
			{
				Column:   "a",
				Lower:    "1",
				Upper:    "1",
				HasLower: false,
				HasUpper: false,
			}, {
				Column:   "b",
				Lower:    "3",
				Upper:    "4",
				HasLower: false,
				HasUpper: false,
			}, {
				Column:   "c",
				Lower:    "5",
				Upper:    "6",
				HasLower: false,
				HasUpper: false,
			},
		},
	}
	conditions, args = chunk.ToString("")
	require.Equal(t, conditions, "TRUE")
	expectArgs = []string{}
	for i, arg := range args {
		require.Equal(t, arg, expectArgs[i])
	}
	require.Equal(t, chunk.String(), `{"index":null,"type":0,"bounds":[{"column":"a","lower":"1","upper":"1","has-lower":false,"has-upper":false},{"column":"b","lower":"3","upper":"4","has-lower":false,"has-upper":false},{"column":"c","lower":"5","upper":"6","has-lower":false,"has-upper":false}],"is-first":false,"is-last":false,"where":"","args":null}`)
	require.Equal(t, chunk.ToMeta(), "range in sequence: Full")

	// all equal
	chunk = &Range{
		Bounds: []*Bound{
			{
				Column:   "a",
				Lower:    "1",
				Upper:    "1",
				HasLower: true,
				HasUpper: true,
			}, {
				Column:   "b",
				Lower:    "3",
				Upper:    "3",
				HasLower: true,
				HasUpper: true,
			}, {
				Column:   "c",
				Lower:    "6",
				Upper:    "6",
				HasLower: true,
				HasUpper: true,
			},
		},
	}
	conditions, args = chunk.ToString("")
	require.Equal(t, conditions, "FALSE")
	expectArgs = []string{}
	for i, arg := range args {
		require.Equal(t, arg, expectArgs[i])
	}
	require.Equal(t, chunk.String(), `{"index":null,"type":0,"bounds":[{"column":"a","lower":"1","upper":"1","has-lower":true,"has-upper":true},{"column":"b","lower":"3","upper":"3","has-lower":true,"has-upper":true},{"column":"c","lower":"6","upper":"6","has-lower":true,"has-upper":true}],"is-first":false,"is-last":false,"where":"","args":null}`)
	require.Equal(t, chunk.ToMeta(), "range in sequence: (1,3,6) < (a,b,c) <= (1,3,6)")

}

func TestChunkInit(t *testing.T) {
	chunks := []*Range{
		{
			Bounds: []*Bound{
				{
					Column:   "a",
					Lower:    "1",
					Upper:    "2",
					HasLower: true,
					HasUpper: true,
				}, {
					Column:   "b",
					Lower:    "3",
					Upper:    "4",
					HasLower: true,
					HasUpper: true,
				}, {
					Column:   "c",
					Lower:    "5",
					Upper:    "6",
					HasLower: true,
					HasUpper: true,
				},
			},
		}, {
			Bounds: []*Bound{
				{
					Column:   "a",
					Lower:    "2",
					Upper:    "3",
					HasLower: true,
					HasUpper: true,
				}, {
					Column:   "b",
					Lower:    "4",
					Upper:    "5",
					HasLower: true,
					HasUpper: true,
				}, {
					Column:   "c",
					Lower:    "6",
					Upper:    "7",
					HasLower: true,
					HasUpper: true,
				},
			},
		},
	}

	InitChunks(chunks, Others, 1, 1, 0, "[123]", "[sdfds fsd fd gd]", 1)
	require.Equal(t, chunks[0].Where, "((((`a` COLLATE '[123]' > ?) OR (`a` COLLATE '[123]' = ? AND `b` COLLATE '[123]' > ?) OR (`a` COLLATE '[123]' = ? AND `b` COLLATE '[123]' = ? AND `c` COLLATE '[123]' > ?)) AND ((`a` COLLATE '[123]' < ?) OR (`a` COLLATE '[123]' = ? AND `b` COLLATE '[123]' < ?) OR (`a` COLLATE '[123]' = ? AND `b` COLLATE '[123]' = ? AND `c` COLLATE '[123]' <= ?))) AND ([sdfds fsd fd gd]))")
	require.Equal(t, chunks[0].Args, []interface{}{"1", "1", "3", "1", "3", "5", "2", "2", "4", "2", "4", "6"})
	require.Equal(t, chunks[0].Type, Others)
	InitChunk(chunks[1], Others, 2, 2, "[456]", "[dsfsdf]")
	require.Equal(t, chunks[1].Where, "((((`a` COLLATE '[456]' > ?) OR (`a` COLLATE '[456]' = ? AND `b` COLLATE '[456]' > ?) OR (`a` COLLATE '[456]' = ? AND `b` COLLATE '[456]' = ? AND `c` COLLATE '[456]' > ?)) AND ((`a` COLLATE '[456]' < ?) OR (`a` COLLATE '[456]' = ? AND `b` COLLATE '[456]' < ?) OR (`a` COLLATE '[456]' = ? AND `b` COLLATE '[456]' = ? AND `c` COLLATE '[456]' <= ?))) AND ([dsfsdf]))")
	require.Equal(t, chunks[1].Args, []interface{}{"2", "2", "4", "2", "4", "6", "3", "3", "5", "3", "5", "7"})
	require.Equal(t, chunks[1].Type, Others)
}

func TestChunkCopyAndUpdate(t *testing.T) {
	chunk := NewChunkRange()
	chunk.Update("a", "1", "2", true, true)
	chunk.Update("a", "2", "3", true, true)
	chunk.Update("a", "324", "5435", false, false)
	chunk.Update("b", "4", "5", true, false)
	chunk.Update("b", "8", "9", false, true)
	chunk.Update("c", "6", "7", false, true)
	chunk.Update("c", "10", "11", true, false)

	conditions, args := chunk.ToString("")
	require.Equal(t, conditions, "((`a` > ?) OR (`a` = ? AND `b` > ?) OR (`a` = ? AND `b` = ? AND `c` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` < ?) OR (`a` = ? AND `b` = ? AND `c` <= ?))")
	require.Equal(t, args, []interface{}{"2", "2", "4", "2", "4", "10", "3", "3", "9", "3", "9", "7"})

	chunk2 := chunk.CopyAndUpdate("a", "4", "6", true, true)
	conditions, args = chunk2.ToString("")
	require.Equal(t, conditions, "((`a` > ?) OR (`a` = ? AND `b` > ?) OR (`a` = ? AND `b` = ? AND `c` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` < ?) OR (`a` = ? AND `b` = ? AND `c` <= ?))")
	require.Equal(t, args, []interface{}{"4", "4", "4", "4", "4", "10", "6", "6", "9", "6", "9", "7"})
	_, args = chunk.ToString("")
	// `Copy` use the same []string
	require.Equal(t, args, []interface{}{"2", "2", "4", "2", "4", "10", "3", "3", "9", "3", "9", "7"})

	InitChunk(chunk, Others, 2, 2, "[324]", "[543]")
	chunk3 := chunk.Clone()
	chunk3.Update("a", "2", "3", true, true)
	require.Equal(t, chunk3.Where, "((((`a` COLLATE '[324]' > ?) OR (`a` COLLATE '[324]' = ? AND `b` COLLATE '[324]' > ?) OR (`a` COLLATE '[324]' = ? AND `b` COLLATE '[324]' = ? AND `c` COLLATE '[324]' > ?)) AND ((`a` COLLATE '[324]' < ?) OR (`a` COLLATE '[324]' = ? AND `b` COLLATE '[324]' < ?) OR (`a` COLLATE '[324]' = ? AND `b` COLLATE '[324]' = ? AND `c` COLLATE '[324]' <= ?))) AND ([543]))")
	require.Equal(t, chunk3.Args, []interface{}{"2", "2", "4", "2", "4", "10", "3", "3", "9", "3", "9", "7"})
	require.Equal(t, chunk3.Type, Others)
}

func TestChunkID(t *testing.T) {
	chunkIDBase := &ChunkID{
		TableIndex:       2,
		BucketIndexLeft:  2,
		BucketIndexRight: 2,
		ChunkIndex:       2,
		ChunkCnt:         4,
	}

	str := chunkIDBase.ToString()
	require.Equal(t, str, "2:2-2:2:4")
	chunkIDtmp := &ChunkID{}
	chunkIDtmp.FromString(str)
	require.Equal(t, chunkIDBase.Compare(chunkIDtmp), 0)

	chunkIDSmalls := []*ChunkID{
		{
			TableIndex:       1,
			BucketIndexLeft:  3,
			BucketIndexRight: 3,
			ChunkIndex:       4,
			ChunkCnt:         5,
		}, {
			TableIndex:       2,
			BucketIndexLeft:  1,
			BucketIndexRight: 1,
			ChunkIndex:       3,
			ChunkCnt:         5,
		}, {
			TableIndex:       2,
			BucketIndexLeft:  2,
			BucketIndexRight: 2,
			ChunkIndex:       1,
			ChunkCnt:         4,
		},
	}

	stringRes := []string{
		"1:3-3:4:5",
		"2:1-1:3:5",
		"2:2-2:1:4",
	}

	for i, chunkIDSmall := range chunkIDSmalls {
		require.Equal(t, chunkIDBase.Compare(chunkIDSmall), 1)
		str = chunkIDSmall.ToString()
		require.Equal(t, str, stringRes[i])
		chunkIDtmp = &ChunkID{}
		chunkIDtmp.FromString(str)
		require.Equal(t, chunkIDSmall.Compare(chunkIDtmp), 0)
	}

	chunkIDLarges := []*ChunkID{
		{
			TableIndex:       3,
			BucketIndexLeft:  1,
			BucketIndexRight: 1,
			ChunkIndex:       2,
			ChunkCnt:         3,
		}, {
			TableIndex:       2,
			BucketIndexLeft:  3,
			BucketIndexRight: 3,
			ChunkIndex:       1,
			ChunkCnt:         3,
		}, {
			TableIndex:       2,
			BucketIndexLeft:  2,
			BucketIndexRight: 2,
			ChunkIndex:       3,
			ChunkCnt:         4,
		},
	}

	stringRes = []string{
		"3:1-1:2:3",
		"2:3-3:1:3",
		"2:2-2:3:4",
	}

	for i, chunkIDLarge := range chunkIDLarges {
		require.Equal(t, chunkIDBase.Compare(chunkIDLarge), -1)
		str = chunkIDLarge.ToString()
		require.Equal(t, str, stringRes[i])
		chunkIDtmp = &ChunkID{}
		chunkIDtmp.FromString(str)
		require.Equal(t, chunkIDLarge.Compare(chunkIDtmp), 0)
	}

}

func TestChunkIndex(t *testing.T) {
	chunkRange := NewChunkRange()
	chunkRange.Index.ChunkIndex = 0
	chunkRange.Index.ChunkCnt = 3
	require.True(t, chunkRange.IsFirstChunkForBucket())
	require.False(t, chunkRange.IsLastChunkForBucket())
	chunkRange.Index.ChunkIndex = 2
	require.False(t, chunkRange.IsFirstChunkForBucket())
	require.True(t, chunkRange.IsLastChunkForBucket())

	chunkRange.Bounds = []*Bound{
		{
			Lower:    "1",
			HasLower: true,
		}, {
			Lower:    "2",
			HasLower: true,
		},
	}
	require.True(t, chunkRange.IsLastChunkForTable())
	require.False(t, chunkRange.IsFirstChunkForTable())
	chunkRange.Bounds = []*Bound{
		{
			Upper:    "1",
			HasUpper: true,
		}, {
			Upper:    "2",
			HasUpper: true,
		},
	}
	require.False(t, chunkRange.IsLastChunkForTable())
	require.True(t, chunkRange.IsFirstChunkForTable())
	chunkRange.Bounds = []*Bound{
		{
			Upper:    "1",
			HasUpper: true,
		}, {
			Lower:    "2",
			HasLower: true,
		},
	}
	require.False(t, chunkRange.IsLastChunkForTable())
	require.False(t, chunkRange.IsFirstChunkForTable())
}
