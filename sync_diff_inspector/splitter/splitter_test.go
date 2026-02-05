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
	"database/sql/driver"
	"fmt"
	"sort"
	"strconv"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	ttypes "github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
	"github.com/pingcap/tiflow/sync_diff_inspector/source/common"
	"github.com/pingcap/tiflow/sync_diff_inspector/utils"
	"github.com/stretchr/testify/require"
)

type chunkResult struct {
	chunkStr string
	args     []any
}

func buildCandidateForTest(t *testing.T, tableDiff *common.TableDiff) *IndexCandidate {
	t.Helper()
	fields, err := indexFieldsFromConfigString(tableDiff.Fields, tableDiff.Info)
	require.NoError(t, err)

	candidates := make([]*IndexCandidate, 0)
	for _, index := range dbutil.FindAllIndex(tableDiff.Info) {
		if isValidIndex(index, tableDiff.Info) && fields.MatchesIndex(index) {
			candidates = append(candidates, &IndexCandidate{
				Index:   index,
				Columns: utils.GetColumnsFromIndex(index, tableDiff.Info),
			})
		}
	}
	if len(candidates) > 0 {
		return candidates[0]
	}
	candidate, err := BuildFakeCandidateForRandom(context.Background(), nil, tableDiff, tableDiff.Fields)
	require.NoError(t, err)
	return candidate
}

func TestSplitRangeByRandom(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	testCases := []struct {
		createTableSQL string
		splitCount     int
		originChunk    *chunk.Range
		randomValues   [][]string
		expectResult   []chunkResult
	}{
		{
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))",
			3,
			chunk.NewChunkRange(nil).CopyAndUpdate("a", "0", "10", true, true).CopyAndUpdate("b", "a", "z", true, true),
			[][]string{
				{"5", "7"},
				{"g", "n"},
			},
			[]chunkResult{
				{
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"0", "0", "a", "5", "5", "g"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"5", "5", "g", "7", "7", "n"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"7", "7", "n", "10", "10", "z"},
				},
			},
		},
		{
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`b`, `a`))",
			3,
			chunk.NewChunkRange(nil).CopyAndUpdate("b", "a", "z", true, true).CopyAndUpdate("a", "0", "10", true, true),
			[][]string{
				{"g", "n"},
				{"5", "7"},
			},
			[]chunkResult{
				{
					"((`b` > ?) OR (`b` = ? AND `a` > ?)) AND ((`b` < ?) OR (`b` = ? AND `a` <= ?))",
					[]interface{}{"a", "a", "0", "g", "g", "5"},
				}, {
					"((`b` > ?) OR (`b` = ? AND `a` > ?)) AND ((`b` < ?) OR (`b` = ? AND `a` <= ?))",
					[]interface{}{"g", "g", "5", "n", "n", "7"},
				}, {
					"((`b` > ?) OR (`b` = ? AND `a` > ?)) AND ((`b` < ?) OR (`b` = ? AND `a` <= ?))",
					[]interface{}{"n", "n", "7", "z", "z", "10"},
				},
			},
		},
		{
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`b`))",
			3,
			chunk.NewChunkRange(nil).CopyAndUpdate("b", "a", "z", true, true),
			[][]string{
				{"g", "n"},
			},
			[]chunkResult{
				{
					"((`b` > ?)) AND ((`b` <= ?))",
					[]interface{}{"a", "g"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]interface{}{"g", "n"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]interface{}{"n", "z"},
				},
			},
		},
		{
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`b`))",
			2,
			chunk.NewChunkRange(nil).CopyAndUpdate("b", "a", "z", true, true),
			[][]string{
				{"g"},
			},
			[]chunkResult{
				{
					"((`b` > ?)) AND ((`b` <= ?))",
					[]interface{}{"a", "g"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]interface{}{"g", "z"},
				},
			},
		},
		{
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`b`))",
			3,
			chunk.NewChunkRange(nil).CopyAndUpdate("b", "a", "z", true, true),
			[][]string{
				{},
			},
			[]chunkResult{
				{
					"((`b` > ?)) AND ((`b` <= ?))",
					[]interface{}{"a", "z"},
				},
			},
		},
	}

	for _, testCase := range testCases {
		tableInfo, err := utils.GetTableInfoBySQL(testCase.createTableSQL, parser.New())
		require.NoError(t, err)

		tableDiff := &common.TableDiff{
			Schema: "test",
			Table:  "test",
			Info:   tableInfo,
		}
		splitCols := buildCandidateForTest(t, tableDiff).Columns
		createFakeResultForRandomSplit(t, mock, 0, testCase.randomValues)
		chunks, err := splitRangeByRandom(context.Background(), db, testCase.originChunk, testCase.splitCount, "test", "test", splitCols, "", "")
		require.NoError(t, err)
		for j, chunk := range chunks {
			chunkStr, args := chunk.ToString("")
			require.Equal(t, chunkStr, testCase.expectResult[j].chunkStr)
			require.Equal(t, args, testCase.expectResult[j].args)
		}
	}
}

func TestRandomSpliter(t *testing.T) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	testCases := []struct {
		createTableSQL string
		count          int
		fields         string
		IgnoreColumns  []string
		randomValues   [][]string
		expectResult   []chunkResult
	}{
		{
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))",
			10,
			"",
			nil,
			[][]string{
				{"1", "2", "3", "4", "5"},
				{"a", "b", "c", "d", "e"},
			},
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]interface{}{"1", "1", "a"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"1", "1", "a", "2", "2", "b"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"2", "2", "b", "3", "3", "c"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"3", "3", "c", "4", "4", "d"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"4", "4", "d", "5", "5", "e"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]interface{}{"5", "5", "e"},
				},
			},
		}, {
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`b`))",
			10,
			"",
			nil,
			[][]string{
				{"a", "b", "c", "d", "e"},
			},
			[]chunkResult{
				{
					"(`b` <= ?)",
					[]interface{}{"a"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]interface{}{"a", "b"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]interface{}{"b", "c"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]interface{}{"c", "d"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]interface{}{"d", "e"},
				}, {
					"(`b` > ?)",
					[]interface{}{"e"},
				},
			},
		}, {
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float)",
			10,
			"b,c",
			nil,
			[][]string{
				{"a", "b", "c", "d", "e"},
				{"1.1", "2.2", "3.3", "4.4", "5.5"},
			},
			[]chunkResult{
				{
					"(`b` < ?) OR (`b` = ? AND `c` <= ?)",
					[]interface{}{"a", "a", "1.1"},
				}, {
					"((`b` > ?) OR (`b` = ? AND `c` > ?)) AND ((`b` < ?) OR (`b` = ? AND `c` <= ?))",
					[]interface{}{"a", "a", "1.1", "b", "b", "2.2"},
				}, {
					"((`b` > ?) OR (`b` = ? AND `c` > ?)) AND ((`b` < ?) OR (`b` = ? AND `c` <= ?))",
					[]interface{}{"b", "b", "2.2", "c", "c", "3.3"},
				}, {
					"((`b` > ?) OR (`b` = ? AND `c` > ?)) AND ((`b` < ?) OR (`b` = ? AND `c` <= ?))",
					[]interface{}{"c", "c", "3.3", "d", "d", "4.4"},
				}, {
					"((`b` > ?) OR (`b` = ? AND `c` > ?)) AND ((`b` < ?) OR (`b` = ? AND `c` <= ?))",
					[]interface{}{"d", "d", "4.4", "e", "e", "5.5"},
				}, {
					"(`b` > ?) OR (`b` = ? AND `c` > ?)",
					[]interface{}{"e", "e", "5.5"},
				},
			},
		}, {
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float)",
			10,
			"",
			[]string{"a"},
			[][]string{
				{"a", "b", "c", "d", "e"},
			},
			[]chunkResult{
				{
					"(`b` <= ?)",
					[]interface{}{"a"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]interface{}{"a", "b"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]interface{}{"b", "c"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]interface{}{"c", "d"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]interface{}{"d", "e"},
				}, {
					"(`b` > ?)",
					[]interface{}{"e"},
				},
			},
		}, {
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float)",
			10,
			"",
			nil,
			[][]string{
				{"1", "2", "3", "4", "5"},
			},
			[]chunkResult{
				{
					"(`a` <= ?)",
					[]interface{}{"1"},
				}, {
					"((`a` > ?)) AND ((`a` <= ?))",
					[]interface{}{"1", "2"},
				}, {
					"((`a` > ?)) AND ((`a` <= ?))",
					[]interface{}{"2", "3"},
				}, {
					"((`a` > ?)) AND ((`a` <= ?))",
					[]interface{}{"3", "4"},
				}, {
					"((`a` > ?)) AND ((`a` <= ?))",
					[]interface{}{"4", "5"},
				}, {
					"(`a` > ?)",
					[]interface{}{"5"},
				},
			},
		},
	}

	for _, testCase := range testCases {
		tableInfo, err := utils.GetTableInfoBySQL(testCase.createTableSQL, parser.New())
		require.NoError(t, err)

		info, needUnifiedTimeStamp := utils.ResetColumns(tableInfo, testCase.IgnoreColumns)
		tableDiff := &common.TableDiff{
			Schema:              "test",
			Table:               "test",
			Info:                info,
			IgnoreColumns:       testCase.IgnoreColumns,
			NeedUnifiedTimeZone: needUnifiedTimeStamp,
			Fields:              testCase.fields,
			ChunkSize:           5,
		}

		createFakeResultForRandomSplit(t, mock, testCase.count, testCase.randomValues)

		iter, err := NewRandomIterator(ctx, "", tableDiff, db, buildCandidateForTest(t, tableDiff))
		require.NoError(t, err)

		j := 0
		for {
			chunk, err := iter.Next()
			require.NoError(t, err)
			if chunk == nil {
				break
			}
			chunkStr, args := chunk.ToString("")
			require.Equal(t, chunkStr, testCase.expectResult[j].chunkStr)
			require.Equal(t, args, testCase.expectResult[j].args)
			j = j + 1
		}
	}

	// Test Checkpoint
	stopJ := 3
	tableInfo, err := utils.GetTableInfoBySQL(testCases[0].createTableSQL, parser.New())
	require.NoError(t, err)

	tableDiff := &common.TableDiff{
		Schema: "test",
		Table:  "test",
		Info:   tableInfo,
		// IgnoreColumns: []string{"c"},
		// Fields:        "a,b",
		ChunkSize: 5,
	}

	createFakeResultForRandomSplit(t, mock, testCases[0].count, testCases[0].randomValues)

	iter, err := NewRandomIterator(ctx, "", tableDiff, db, buildCandidateForTest(t, tableDiff))
	require.NoError(t, err)

	var chunk *chunk.Range
	for j := 0; j < stopJ; j++ {
		chunk, err = iter.Next()
		require.NoError(t, err)
	}

	bounds1 := chunk.Bounds
	chunkID1 := chunk.Index

	rangeInfo := &RangeInfo{
		ChunkRange: chunk,
	}

	createFakeResultForRandomSplit(t, mock, testCases[0].count, testCases[0].randomValues)

	iter, err = NewRandomIteratorWithCheckpoint(ctx, "", tableDiff, db, rangeInfo, buildCandidateForTest(t, tableDiff))
	require.NoError(t, err)

	chunk, err = iter.Next()
	require.NoError(t, err)

	for i, bound := range chunk.Bounds {
		require.Equal(t, bounds1[i].Upper, bound.Lower)
	}

	require.Equal(t, chunk.Index.ChunkCnt, chunkID1.ChunkCnt)
	require.Equal(t, chunk.Index.ChunkIndex, chunkID1.ChunkIndex+1)
}

func createFakeResultForRandomSplit(t *testing.T, mock sqlmock.Sqlmock, count int, randomValues [][]string) {
	createFakeResultForCount(t, count)
	if randomValues == nil {
		return
	}
	// generate fake result for get random value for column a
	columns := []string{"a", "b", "c", "d", "e", "f"}
	rowsNames := make([]string, 0, len(randomValues))
	for i := 0; i < len(randomValues); i++ {
		rowsNames = append(rowsNames, columns[i])
	}
	randomRows := sqlmock.NewRows(rowsNames)
	for i := 0; i < len(randomValues[0]); i++ {
		row := make([]driver.Value, 0, len(randomValues))
		for j := 0; j < len(randomValues); j++ {
			row = append(row, randomValues[j][i])
		}
		randomRows.AddRow(row...)
	}
	mock.ExpectQuery("ORDER BY rand_value").WillReturnRows(randomRows)
}

func TestBucketSpliter(t *testing.T) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	createTableSQL := "create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo, err := utils.GetTableInfoBySQL(createTableSQL, parser.New())
	require.NoError(t, err)

	testCases := []struct {
		chunkSize     int64
		aRandomValues []interface{}
		bRandomValues []interface{}
		expectResult  []chunkResult
	}{
		{
			// chunk size less than the count of bucket 64, and the bucket's count 64 >= 32, so will split by random in every bucket
			32,
			[]interface{}{32, 32 * 3, 32 * 5, 32 * 7, 32 * 9},
			[]interface{}{6, 6 * 3, 6 * 5, 6 * 7, 6 * 9},
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]interface{}{"32", "32", "6"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"32", "32", "6", "63", "63", "11"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"63", "63", "11", "96", "96", "18"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"96", "96", "18", "127", "127", "23"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"127", "127", "23", "160", "160", "30"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"160", "160", "30", "191", "191", "35"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"191", "191", "35", "224", "224", "42"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"224", "224", "42", "255", "255", "47"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"255", "255", "47", "288", "288", "54"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"288", "288", "54", "319", "319", "59"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]interface{}{"319", "319", "59"},
				},
			},
		}, {
			// chunk size less than the count of bucket 64, but 64 is  less than 2*50, so will not split every bucket
			50,
			nil,
			nil,
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]interface{}{"63", "63", "11"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"63", "63", "11", "127", "127", "23"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"127", "127", "23", "191", "191", "35"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"191", "191", "35", "255", "255", "47"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"255", "255", "47", "319", "319", "59"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]interface{}{"319", "319", "59"},
				},
			},
		}, {
			// chunk size is equal to the count of bucket 64, so every becket will generate a chunk
			64,
			nil,
			nil,
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]interface{}{"63", "63", "11"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"63", "63", "11", "127", "127", "23"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"127", "127", "23", "191", "191", "35"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"191", "191", "35", "255", "255", "47"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"255", "255", "47", "319", "319", "59"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]interface{}{"319", "319", "59"},
				},
			},
		}, {
			// chunk size is greater than the count of bucket 64, will combine two bucket into chunk
			127,
			nil,
			nil,
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]interface{}{"127", "127", "23"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"127", "127", "23", "255", "255", "47"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]interface{}{"255", "255", "47"},
				},
			},
		}, {
			// chunk size is equal to the double count of bucket 64, will combine two bucket into one chunk
			128,
			nil,
			nil,
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]interface{}{"127", "127", "23"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"127", "127", "23", "255", "255", "47"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]interface{}{"255", "255", "47"},
				},
			},
		}, {
			// chunk size is greater than the double count of bucket 64, will combine three bucket into one chunk
			129,
			nil,
			nil,
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]interface{}{"191", "191", "35"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]interface{}{"191", "191", "35"},
				},
			},
		}, {
			// chunk size is greater than the total count, only generate one chunk
			400,
			nil,
			nil,
			[]chunkResult{
				{
					"TRUE",
					nil,
				},
			},
		},
	}

	tableDiff := &common.TableDiff{
		Schema: "test",
		Table:  "test",
		Info:   tableInfo,
	}

	for i, testCase := range testCases {
		fmt.Printf("%d", i)
		createFakeResultForBucketSplit(mock, testCase.aRandomValues, testCase.bRandomValues)
		tableDiff.ChunkSize = testCase.chunkSize
		iter, err := NewBucketIteratorForTest(ctx, "", tableDiff, db, buildCandidateForTest(t, tableDiff))
		require.NoError(t, err)
		defer iter.Close()

		obtainChunks := make([]chunkResult, 0, len(testCase.expectResult))
		nextBeginBucket := 0
		for {
			chunk, err := iter.Next()
			require.NoError(t, err)
			if chunk == nil {
				break
			}
			chunkStr, _ := chunk.ToString("")
			if nextBeginBucket == 0 {
				require.Equal(t, chunk.Index.BucketIndexLeft, 0)
			} else {
				require.Equal(t, chunk.Index.BucketIndexLeft, nextBeginBucket)
			}
			if chunk.Index.ChunkIndex+1 == chunk.Index.ChunkCnt {
				nextBeginBucket = chunk.Index.BucketIndexRight + 1
			}
			obtainChunks = append(obtainChunks, chunkResult{chunkStr, chunk.Args})

		}
		sort.Slice(obtainChunks, func(i, j int) bool {
			totalIndex := len(obtainChunks[i].args)
			if totalIndex > len(obtainChunks[j].args) {
				totalIndex = len(obtainChunks[j].args)
			}
			for index := 0; index < totalIndex; index++ {
				a1, _ := strconv.Atoi(obtainChunks[i].args[index].(string))
				a2, _ := strconv.Atoi(obtainChunks[j].args[index].(string))
				if a1 < a2 {
					return true
				} else if a1 > a2 {
					return false
				}
			}
			if len(obtainChunks[i].args) == len(obtainChunks[j].args) {
				// hack way for test case 6
				return len(obtainChunks[i].chunkStr) > len(obtainChunks[j].chunkStr)
			}
			return len(obtainChunks[i].args) < len(obtainChunks[j].args)
		})
		// we expect chunk count is same after we generate chunk concurrently
		require.Equal(t, len(obtainChunks), len(testCase.expectResult))
		for i, e := range testCase.expectResult {
			require.Equal(t, obtainChunks[i].args, e.args)
			require.Equal(t, obtainChunks[i].chunkStr, e.chunkStr)
		}
	}

	// Test Checkpoint
	stopJ := 3
	createFakeResultForBucketSplit(mock, testCases[0].aRandomValues, testCases[0].bRandomValues)
	tableDiff.ChunkSize = testCases[0].chunkSize
	iter, err := NewBucketIteratorForTest(ctx, "", tableDiff, db, buildCandidateForTest(t, tableDiff))
	require.NoError(t, err)
	j := 0
	var chunk *chunk.Range
	for ; j < stopJ; j++ {
		chunk, err = iter.Next()
		require.NoError(t, err)
	}
	for {
		c, err := iter.Next()
		require.NoError(t, err)
		if c == nil {
			break
		}
	}
	iter.Close()

	bounds1 := chunk.Bounds

	rangeInfo := &RangeInfo{
		ChunkRange: chunk,
		IndexID:    iter.GetIndexID(),
	}

	// drop the origin db since we cannot ensure order of mock string after we concurrent produce chunks.
	db, mock, err = sqlmock.New()
	require.NoError(t, err)
	createFakeResultForBucketSplit(mock, nil, nil)
	testfailpoint.Enable(t, "github.com/pingcap/tiflow/sync_diff_inspector/splitter/getRowCount", "return(64)")
	startBucket := min(chunk.Index.BucketIndexRight+1, len(testCases[0].aRandomValues))
	createFakeResultForRandom(
		mock,
		testCases[0].aRandomValues[startBucket:],
		testCases[0].bRandomValues[startBucket:],
	)
	iter, err = NewBucketIterator(ctx, "", tableDiff, db, rangeInfo, utils.NewWorkerPool(1, "bucketIter"), buildCandidateForTest(t, tableDiff))
	require.NoError(t, err)
	chunk, err = iter.Next()
	require.NoError(t, err)

	for i, bound := range chunk.Bounds {
		require.Equal(t, bounds1[i].Upper, bound.Lower)
	}
	for {
		c, err := iter.Next()
		require.NoError(t, err)
		if c == nil {
			break
		}
	}
	iter.Close()

	// Mock column a is ignored.
	ignoredInfo, _ := utils.ResetColumns(tableInfo.Clone(), []string{"a"})
	ignoredTable := *tableDiff
	ignoredTable.Info = ignoredInfo
	createFakeResultForBucketSplit(mock, testCases[0].aRandomValues, testCases[0].bRandomValues)
	_, err = NewBucketIteratorForTest(ctx, "", &ignoredTable, db, buildCandidateForTest(t, &ignoredTable))
	require.Error(t, err)
}

func createFakeResultForBucketSplit(mock sqlmock.Sqlmock, aRandomValues, bRandomValues []interface{}) {
	/*
		+---------+------------+-------------+----------+-----------+-------+---------+-------------+-------------+
		| Db_name | Table_name | Column_name | Is_index | Bucket_id | Count | Repeats | Lower_Bound | Upper_Bound |
		+---------+------------+-------------+----------+-----------+-------+---------+-------------+-------------+
		| test    | test       | PRIMARY     |        1 |         0 |    64 |       1 | (0, 0)      | (63, 11)    |
		| test    | test       | PRIMARY     |        1 |         1 |   128 |       1 | (64, 12)    | (127, 23)   |
		| test    | test       | PRIMARY     |        1 |         2 |   192 |       1 | (128, 24)   | (191, 35)   |
		| test    | test       | PRIMARY     |        1 |         3 |   256 |       1 | (192, 36)   | (255, 47)   |
		| test    | test       | PRIMARY     |        1 |         4 |   320 |       1 | (256, 48)   | (319, 59)   |
		+---------+------------+-------------+----------+-----------+-------+---------+-------------+-------------+
	*/

	// Mock query with subquery to get all table_ids (main table + partitions) at once
	statsRows := sqlmock.NewRows([]string{"is_index", "hist_id", "bucket_id", "count", "lower_bound", "upper_bound"})
	for i := 0; i < 5; i++ {
		// Encode index bounds as real encoded keys: PRIMARY(a, b) where both a and b are integers.
		lowerA, lowerB := i*64, i*12
		upperA, upperB := (i+1)*64-1, (i+1)*12-1

		lowerDatums := []ttypes.Datum{ttypes.NewIntDatum(int64(lowerA)), ttypes.NewStringDatum(fmt.Sprintf("%d", lowerB))}
		upperDatums := []ttypes.Datum{ttypes.NewIntDatum(int64(upperA)), ttypes.NewStringDatum(fmt.Sprintf("%d", upperB))}

		lowerEncoded, _ := codec.EncodeKey(time.UTC, nil, lowerDatums...)
		upperEncoded, _ := codec.EncodeKey(time.UTC, nil, upperDatums...)

		statsRows.AddRow(1, 1, i, (i+1)*64, lowerEncoded, upperEncoded)
	}
	mock.ExpectQuery("SELECT is_index, hist_id, bucket_id, count, lower_bound, upper_bound FROM mysql.stats_buckets WHERE table_id IN \\(\\s*SELECT tidb_table_id FROM information_schema.tables WHERE table_schema = \\? AND table_name = \\? UNION ALL SELECT tidb_partition_id FROM information_schema.partitions WHERE table_schema = \\? AND table_name = \\?\\s*\\)[\\s\\S]*ORDER BY is_index, hist_id, bucket_id").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(statsRows)

	createFakeResultForRandom(mock, aRandomValues, bRandomValues)
}

func createFakeResultForCount(t *testing.T, count int) {
	if count > 0 {
		// generate fake result for get the row count of this table
		testfailpoint.Enable(t,
			"github.com/pingcap/tiflow/sync_diff_inspector/splitter/getRowCount",
			fmt.Sprintf("return(%d)", count),
		)
	}
}

func createFakeResultForRandom(mock sqlmock.Sqlmock, aRandomValues, bRandomValues []interface{}) {
	for i := 0; i < len(aRandomValues); i++ {
		aRandomRows := sqlmock.NewRows([]string{"a", "b"})
		aRandomRows.AddRow(aRandomValues[i], bRandomValues[i])
		mock.ExpectQuery("ORDER BY rand_value").WillReturnRows(aRandomRows)
	}
}

func TestLimitSpliter(t *testing.T) {
	ctx := context.Background()

	createTableSQL := "create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo, err := utils.GetTableInfoBySQL(createTableSQL, parser.New())
	require.NoError(t, err)

	testCases := []struct {
		limitAValues []string
		limitBValues []string
		expectResult []chunkResult
	}{
		{
			[]string{"1000", "2000", "3000", "4000"},
			[]string{"a", "b", "c", "d"},
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]interface{}{"1000", "1000", "a"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"1000", "1000", "a", "2000", "2000", "b"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"2000", "2000", "b", "3000", "3000", "c"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]interface{}{"3000", "3000", "c", "4000", "4000", "d"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]interface{}{"4000", "4000", "d"},
				},
			},
		},
	}

	tableDiff := &common.TableDiff{
		Schema:    "test",
		Table:     "test",
		Info:      tableInfo,
		ChunkSize: 1000,
	}

	for _, testCase := range testCases {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		createFakeResultForLimitSplit(mock, testCase.limitAValues, testCase.limitBValues, true)

		iter, err := NewLimitIterator(ctx, "", tableDiff, db, buildCandidateForTest(t, tableDiff))
		require.NoError(t, err)

		j := 0
		for {
			chunk, err := iter.Next()
			require.NoError(t, err)
			if chunk == nil {
				break
			}
			chunkStr, args := chunk.ToString("")
			require.Equal(t, chunkStr, testCase.expectResult[j].chunkStr)
			require.Equal(t, args, testCase.expectResult[j].args)
			j = j + 1
		}
	}

	db2, mock2, err := sqlmock.New()
	require.NoError(t, err)
	defer db2.Close()

	// Test Checkpoint
	stopJ := 2
	createFakeResultForLimitSplit(mock2, testCases[0].limitAValues[:stopJ], testCases[0].limitBValues[:stopJ], true)
	iter, err := NewLimitIterator(ctx, "", tableDiff, db2, buildCandidateForTest(t, tableDiff))
	require.NoError(t, err)
	j := 0
	var chunk *chunk.Range
	for ; j < stopJ; j++ {
		chunk, err = iter.Next()
		require.NoError(t, err)
	}
	bounds1 := chunk.Bounds

	rangeInfo := &RangeInfo{
		ChunkRange: chunk,
		IndexID:    iter.GetIndexID(),
	}

	db3, mock3, err := sqlmock.New()
	require.NoError(t, err)
	defer db3.Close()

	createFakeResultForLimitSplit(mock3, testCases[0].limitAValues[stopJ:], testCases[0].limitBValues[stopJ:], true)
	iter, err = NewLimitIteratorWithCheckpoint(ctx, "", tableDiff, db3, rangeInfo, buildCandidateForTest(t, tableDiff))
	require.NoError(t, err)
	chunk, err = iter.Next()
	require.NoError(t, err)

	for i, bound := range chunk.Bounds {
		require.Equal(t, bounds1[i].Upper, bound.Lower)
	}
}

func createFakeResultForLimitSplit(mock sqlmock.Sqlmock, aValues []string, bValues []string, needEnd bool) {
	mock.ExpectQuery("SELECT COUNT.*").WillReturnRows(sqlmock.NewRows([]string{"cnt"}).AddRow(len(aValues)))
	for start := 0; start < len(aValues); start += defaultLimitBatchSize {
		end := start + defaultLimitBatchSize
		if end > len(aValues) {
			end = len(aValues)
		}
		limitRows := sqlmock.NewRows([]string{"a", "b"})
		for i := start; i < end; i++ {
			limitRows.AddRow(aValues[i], bValues[i])
		}
		mock.ExpectQuery("SELECT `a`,.*").WillReturnRows(limitRows)
	}

	if needEnd {
		mock.ExpectQuery("SELECT `a`,.*").WillReturnRows(sqlmock.NewRows([]string{"a", "b"}))
	}
}

func TestRangeInfo(t *testing.T) {
	rangeInfo := &RangeInfo{
		ChunkRange: chunk.NewChunkRange(chunk.GenFakeTableInfo("a")),
		IndexID:    2,
		ProgressID: "324312",
	}
	rangeInfo.Update("a", "1", "2", true, true, "[23]", "[sdg]")
	rangeInfo.ChunkRange.Index.TableIndex = 1
	chunkRange := rangeInfo.GetChunk()
	require.Equal(t, chunkRange.Where, "((((`a` COLLATE '[23]' > ?)) AND ((`a` COLLATE '[23]' <= ?))) AND ([sdg]))")
	require.Equal(t, chunkRange.Args, []interface{}{"1", "2"})

	require.Equal(t, rangeInfo.GetTableIndex(), 1)

	rangeInfo2 := FromNode(rangeInfo.ToNode())

	chunkRange = rangeInfo2.GetChunk()
	require.Equal(t, chunkRange.Where, "((((`a` COLLATE '[23]' > ?)) AND ((`a` COLLATE '[23]' <= ?))) AND ([sdg]))")
	require.Equal(t, chunkRange.Args, []interface{}{"1", "2"})

	require.Equal(t, rangeInfo2.GetTableIndex(), 1)
}

func TestChunkSize(t *testing.T) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	createTableSQL := "create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo, err := utils.GetTableInfoBySQL(createTableSQL, parser.New())
	require.NoError(t, err)

	tableDiff := &common.TableDiff{
		Schema:    "test",
		Table:     "test",
		Info:      tableInfo,
		ChunkSize: 0,
	}

	// test bucket splitter chunksize
	// Mock query with subquery to get all table_ids (main table + partitions) at once
	statsRows := sqlmock.NewRows([]string{"is_index", "hist_id", "bucket_id", "count", "lower_bound", "upper_bound"})
	// Notice, use wrong Bound to kill bucket producer
	statsRows.AddRow(1, 1, 0, 1000000000, "(1, 2, wrong!)", "(2, 3, wrong!)")
	mock.ExpectQuery("SELECT is_index, hist_id, bucket_id, count, lower_bound, upper_bound FROM mysql.stats_buckets WHERE table_id IN \\(\\s*SELECT tidb_table_id FROM information_schema.tables WHERE table_schema = \\? AND table_name = \\? UNION ALL SELECT tidb_partition_id FROM information_schema.partitions WHERE table_schema = \\? AND table_name = \\?\\s*\\)[\\s\\S]*ORDER BY is_index, hist_id, bucket_id").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(statsRows)

	bucketIter, err := NewBucketIteratorForTest(ctx, "", tableDiff, db, buildCandidateForTest(t, tableDiff))
	require.NoError(t, err)
	require.Equal(t, bucketIter.chunkSize, int64(100000))

	createFakeResultForBucketSplit(mock, nil, nil)
	bucketIter, err = NewBucketIteratorForTest(ctx, "", tableDiff, db, buildCandidateForTest(t, tableDiff))
	require.NoError(t, err)
	require.Equal(t, bucketIter.chunkSize, int64(50000))

	// test random splitter chunksize
	// chunkNum is only 1, so don't need randomValues
	createFakeResultForRandomSplit(t, mock, 1000, nil)
	randomIter, err := NewRandomIterator(ctx, "", tableDiff, db, buildCandidateForTest(t, tableDiff))
	require.NoError(t, err)
	require.Equal(t, randomIter.chunkSize, int64(50000))

	createFakeResultForRandomSplit(t, mock, 1000000000, [][]string{
		{"1", "2", "3", "4", "5"},
		{"a", "b", "c", "d", "e"},
	})
	randomIter, err = NewRandomIterator(ctx, "", tableDiff, db, buildCandidateForTest(t, tableDiff))
	require.NoError(t, err)
	require.Equal(t, randomIter.chunkSize, int64(100000))

	createTableSQL = "create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime)"
	tableInfo, err = utils.GetTableInfoBySQL(createTableSQL, parser.New())
	require.NoError(t, err)

	tableDiffNoIndex := &common.TableDiff{
		Schema:    "test",
		Table:     "test",
		Info:      tableInfo,
		ChunkSize: 0,
	}
	// no index
	createFakeResultForRandomSplit(t, mock, 1000, nil)
	randomIter, err = NewRandomIterator(ctx, "", tableDiffNoIndex, db, buildCandidateForTest(t, tableDiffNoIndex))
	require.NoError(t, err)
	require.Equal(t, randomIter.chunkSize, int64(1001))

	// test limit splitter chunksize
	mock.ExpectQuery("SELECT `a`.*ROW_NUMBER\\(\\) OVER .*MOD\\(rn, 50000\\) = 0 ORDER BY rn").
		WillReturnRows(sqlmock.NewRows([]string{"a", "b"}))
	_, err = NewLimitIterator(ctx, "", tableDiff, db, buildCandidateForTest(t, tableDiff))
	require.NoError(t, err)
}

func TestBucketSpliterHint(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	ctx := context.Background()

	testCases := []struct {
		tableSQL      string
		indexCount    int
		expectColumns []ast.CIStr
	}{
		{
			"create table `test`.`test`(`a` int, `b` int, `c` int, primary key(`a`, `b`), unique key i1(`c`))",
			0,
			[]ast.CIStr{ast.NewCIStr("a"), ast.NewCIStr("b")},
		},
		{
			"create table `test`.`test`(`a` int, `b` int, `c` int, unique key i1(`c`))",
			0,
			[]ast.CIStr{ast.NewCIStr("c")},
		},
		{
			"create table `test`.`test`(`a` int, `b` int, `c` int, key i2(`b`))",
			1,
			[]ast.CIStr{ast.NewCIStr("b")},
		},
	}

	for _, tc := range testCases {
		tableInfo, err := utils.GetTableInfoBySQL(tc.tableSQL, parser.New())
		require.NoError(t, err)

		tableDiff := &common.TableDiff{
			Schema: "test",
			Table:  "test",
			Info:   tableInfo,
		}

		createFakeResultForBucketIterator(mock, tc.indexCount)

		iter, err := NewBucketIterator(ctx, "", tableDiff, db, nil, utils.NewWorkerPool(1, "bucketIter"), buildCandidateForTest(t, tableDiff))
		require.NoError(t, err)
		chunk, err := iter.Next()
		require.NoError(t, err)
		require.Equal(t, tc.expectColumns, chunk.IndexColumnNames)
	}
}

func TestRandomSpliterHint(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	ctx := context.Background()

	testCases := []struct {
		tableSQL      string
		expectColumns []ast.CIStr
	}{
		{
			"create table `test`.`test`(`a` int, `b` int, `c` int, primary key(`a`, `b`), unique key i1(`c`))",
			[]ast.CIStr{ast.NewCIStr("a"), ast.NewCIStr("b")},
		},
		{
			"create table `test`.`test`(`a` int, `b` int, `c` int, unique key i1(`c`), key i2(`b`))",
			[]ast.CIStr{ast.NewCIStr("c")},
		},
		{
			"create table `test`.`test`(`a` int, `b` int, `c` int, key i2(`b`))",
			[]ast.CIStr{ast.NewCIStr("b")},
		},
		{
			"create table `test`.`test`(`a` int, `b` int, `c` int, primary key(`b`, `a`), unique key i1(`c`))",
			[]ast.CIStr{ast.NewCIStr("b"), ast.NewCIStr("a")},
		},
		{
			"create table `test`.`test`(`a` int, `b` int, `c` int)",
			nil,
		},
	}

	testfailpoint.Enable(t, "github.com/pingcap/tiflow/sync_diff_inspector/splitter/getRowCount", "return(320)")

	for _, tc := range testCases {
		tableInfo, err := utils.GetTableInfoBySQL(tc.tableSQL, parser.New())
		require.NoError(t, err)

		for _, tableRange := range []string{"", "c > 100"} {
			tableDiff := &common.TableDiff{
				Schema: "test",
				Table:  "test",
				Info:   tableInfo,
				Range:  tableRange,
			}

			mock.ExpectQuery("SELECT COUNT.*").WillReturnRows(
				sqlmock.NewRows([]string{"cnt"}).AddRow(320),
			)
			iter, err := NewRandomIteratorWithCheckpoint(ctx, "", tableDiff, db, nil, buildCandidateForTest(t, tableDiff))
			require.NoError(t, err)
			chunk, err := iter.Next()
			require.NoError(t, err)
			require.Equal(t, tc.expectColumns, chunk.IndexColumnNames)
		}
	}
}

func createFakeResultForBucketIterator(mock sqlmock.Sqlmock, indexCount int) {
	/*
		+---------+----------+-----------+-------+-------------+-------------+
		| hist_id | is_index | bucket_id | count | lower_bound | upper_bound |
		+---------+----------+-----------+-------+-------------+-------------+
		| 1       |        1 |         0 |    64 | (0, 0)      | (63, 11)    |
		| 1       |        1 |         1 |   128 | (64, 12)    | (127, 23)   |
		| 1       |        1 |         2 |   192 | (128, 24)   | (191, 35)   |
		| 1       |        1 |         3 |   256 | (192, 36)   | (255, 47)   |
		| 1       |        1 |         4 |   320 | (256, 48)   | (319, 59)   |
		+---------+----------+-----------+-------+-------------+-------------+
	*/
	// Mock query with subquery to get all table_ids (main table + partitions) at once
	statsRows := sqlmock.NewRows([]string{"is_index", "hist_id", "bucket_id", "count", "lower_bound", "upper_bound"})
	for i := range []string{"PRIMARY", "i1", "i2", "i3", "i4"} {
		histID := int64(i + 1) // hist_id starts from 1
		for j := 0; j < 5; j++ {
			statsRows.AddRow(1, histID, j, (j+1)*64, fmt.Sprintf("(%d, %d)", j*64, j*12), fmt.Sprintf("(%d, %d)", (j+1)*64-1, (j+1)*12-1))
		}
	}
	mock.ExpectQuery("SELECT is_index, hist_id, bucket_id, count, lower_bound, upper_bound FROM mysql.stats_buckets WHERE table_id IN \\(\\s*SELECT tidb_table_id FROM information_schema.tables WHERE table_schema = \\? AND table_name = \\? UNION ALL SELECT tidb_partition_id FROM information_schema.partitions WHERE table_schema = \\? AND table_name = \\?\\s*\\)[\\s\\S]*ORDER BY is_index, hist_id, bucket_id").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(statsRows)

	for range indexCount {
		mock.ExpectQuery("SELECT COUNT\\(DISTINCT *").WillReturnRows(sqlmock.NewRows([]string{"SEL"}).AddRow("5"))
	}
}
