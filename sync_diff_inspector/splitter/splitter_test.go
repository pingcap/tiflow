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
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
	"github.com/pingcap/tiflow/sync_diff_inspector/source/common"
	"github.com/pingcap/tiflow/sync_diff_inspector/utils"
	"github.com/stretchr/testify/require"
)

type chunkResult struct {
	chunkStr string
	args     []interface{}
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

		splitCols, err := GetSplitFields(tableInfo, nil)
		require.NoError(t, err)
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

		iter, err := NewRandomIterator(ctx, "", tableDiff, db)
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

	iter, err := NewRandomIterator(ctx, "", tableDiff, db)
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

	iter, err = NewRandomIteratorWithCheckpoint(ctx, "", tableDiff, db, rangeInfo)
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

func createFakeResultForCount(t *testing.T, count int) {
	if count > 0 {
		testfailpoint.Enable(t,
			"github.com/pingcap/tiflow/sync_diff_inspector/splitter/getRowCount",
			fmt.Sprintf("return(%d)", count),
		)
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

		createFakeResultForLimitSplit(t, mock, testCase.limitAValues, testCase.limitBValues, true)

		iter, err := NewLimitIterator(ctx, "", tableDiff, db)
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
	createFakeResultForLimitSplit(t, mock2, testCases[0].limitAValues[:stopJ], testCases[0].limitBValues[:stopJ], true)
	iter, err := NewLimitIterator(ctx, "", tableDiff, db2)
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

	createFakeResultForLimitSplit(t, mock3, testCases[0].limitAValues[stopJ:], testCases[0].limitBValues[stopJ:], true)
	iter, err = NewLimitIteratorWithCheckpoint(ctx, "", tableDiff, db3, rangeInfo)
	require.NoError(t, err)
	chunk, err = iter.Next()
	require.NoError(t, err)

	for i, bound := range chunk.Bounds {
		require.Equal(t, bounds1[i].Upper, bound.Lower)
	}
}

func createFakeResultForLimitSplit(t *testing.T, mock sqlmock.Sqlmock, aValues []string, bValues []string, needEnd bool) {
	createFakeResultForCount(t, len(aValues))

	for i, a := range aValues {
		limitRows := sqlmock.NewRows([]string{"a", "b"})
		limitRows.AddRow(a, bValues[i])
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

func TestRandomSpliterHint(t *testing.T) {
	db, _, err := sqlmock.New()
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

			iter, err := NewRandomIteratorWithCheckpoint(ctx, "", tableDiff, db, nil)
			require.NoError(t, err)
			chunk, err := iter.Next()
			require.NoError(t, err)
			require.Equal(t, tc.expectColumns, chunk.IndexColumnNames)
		}
	}
}

func TestBucketSpliter(t *testing.T) {
	ctx := context.Background()
	db, tableInfo := createAnalyzedBucketTable(t)

	for _, chunkSize := range []int64{32, 50, 64, 127, 128, 129, 400} {
		t.Run(fmt.Sprintf("chunk_size_%d", chunkSize), func(t *testing.T) {
			tableDiff := &common.TableDiff{
				Schema:    "bucket_test",
				Table:     "test",
				Info:      tableInfo,
				Range:     "TRUE",
				ChunkSize: chunkSize,
			}

			iter, err := NewBucketIterator(ctx, "", tableDiff, db)
			require.NoError(t, err)
			defer iter.Close()

			chunkCount := requireBucketChunksCoverTable(t, ctx, db, iter, 320)
			if chunkSize < 320 {
				require.Greater(t, chunkCount, 1)
			} else {
				require.Equal(t, 1, chunkCount)
			}
		})
	}

	t.Run("checkpoint", func(t *testing.T) {
		tableDiff := &common.TableDiff{
			Schema:    "bucket_test",
			Table:     "test",
			Info:      tableInfo,
			Range:     "TRUE",
			ChunkSize: 64,
		}
		iter, err := NewBucketIterator(ctx, "", tableDiff, db)
		require.NoError(t, err)

		var lastChunk *chunk.Range
		for range 3 {
			lastChunk, err = iter.Next()
			require.NoError(t, err)
			require.NotNil(t, lastChunk)
		}
		rangeInfo := &RangeInfo{
			ChunkRange: lastChunk,
			IndexID:    iter.GetIndexID(),
		}
		for {
			remainingChunk, err := iter.Next()
			require.NoError(t, err)
			if remainingChunk == nil {
				break
			}
		}
		iter.Close()

		resumedIter, err := NewBucketIteratorWithCheckpoint(
			ctx,
			"",
			tableDiff,
			db,
			rangeInfo,
			utils.NewWorkerPool(1, "bucket-checkpoint"),
		)
		require.NoError(t, err)
		defer resumedIter.Close()

		nextChunk, err := resumedIter.Next()
		require.NoError(t, err)
		require.NotNil(t, nextChunk)
		for i, bound := range nextChunk.Bounds {
			require.Equal(t, lastChunk.Bounds[i].Upper, bound.Lower)
		}
	})

	t.Run("ignored_index_column", func(t *testing.T) {
		info, _ := utils.ResetColumns(tableInfo, []string{"a"})
		tableDiff := &common.TableDiff{
			Schema:    "bucket_test",
			Table:     "test",
			Info:      info,
			Range:     "TRUE",
			ChunkSize: 64,
		}
		_, err := NewBucketIterator(ctx, "", tableDiff, db)
		require.Error(t, err)
	})
}

func TestChunkSize(t *testing.T) {
	ctx := context.Background()
	bucketDB, tableInfo := createAnalyzedBucketTable(t)
	tableDiff := &common.TableDiff{
		Schema: "bucket_test",
		Table:  "test",
		Info:   tableInfo,
		Range:  "TRUE",
	}

	bucketIter, err := NewBucketIterator(ctx, "", tableDiff, bucketDB)
	require.NoError(t, err)
	require.Equal(t, int64(50000), bucketIter.chunkSize)
	bucketIter.Close()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Test random splitter chunk size. When chunkNum is 1, random values are not needed.
	createFakeResultForRandomSplit(t, mock, 1000, nil)
	randomIter, err := NewRandomIterator(ctx, "", tableDiff, db)
	require.NoError(t, err)
	require.Equal(t, int64(50000), randomIter.chunkSize)

	createFakeResultForRandomSplit(t, mock, 1000000000, [][]string{
		{"1", "2", "3", "4", "5"},
		{"a", "b", "c", "d", "e"},
	})
	randomIter, err = NewRandomIterator(ctx, "", tableDiff, db)
	require.NoError(t, err)
	require.Equal(t, int64(100000), randomIter.chunkSize)

	createTableSQL := "create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime)"
	tableInfo, err = utils.GetTableInfoBySQL(createTableSQL, parser.New())
	require.NoError(t, err)

	tableDiffNoIndex := &common.TableDiff{
		Schema: "test",
		Table:  "test",
		Info:   tableInfo,
	}
	createFakeResultForRandomSplit(t, mock, 1000, nil)
	randomIter, err = NewRandomIterator(ctx, "", tableDiffNoIndex, db)
	require.NoError(t, err)
	require.Equal(t, int64(1001), randomIter.chunkSize)

	// Test limit splitter chunk size.
	createFakeResultForCount(t, 1000)
	mock.ExpectQuery("SELECT `a`,.*limit 50000.*").
		WillReturnRows(sqlmock.NewRows([]string{"a", "b"}))
	_, err = NewLimitIterator(ctx, "", tableDiff, db)
	require.NoError(t, err)
}

func TestBucketSpliterHint(t *testing.T) {
	ctx := context.Background()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE DATABASE bucket_hint_test")
	tk.MustExec("USE bucket_hint_test")

	testCases := []struct {
		name            string
		createTableSQL  string
		expectedColumns []ast.CIStr
	}{
		{
			name:            "primary_idx",
			createTableSQL:  "CREATE TABLE primary_idx (a INT, b INT, c INT, PRIMARY KEY (a, b), UNIQUE KEY i1 (c))",
			expectedColumns: []ast.CIStr{ast.NewCIStr("a"), ast.NewCIStr("b")},
		},
		{
			name:            "unique_idx",
			createTableSQL:  "CREATE TABLE unique_idx (a INT, b INT, c INT, UNIQUE KEY i1 (c))",
			expectedColumns: []ast.CIStr{ast.NewCIStr("c")},
		},
		{
			name:            "normal_idx",
			createTableSQL:  "CREATE TABLE normal_idx (a INT, b INT, c INT, KEY i2 (b))",
			expectedColumns: []ast.CIStr{ast.NewCIStr("b")},
		},
	}

	for _, testCase := range testCases {
		tk.MustExec(testCase.createTableSQL)
		tk.MustExec(buildHintInsertSQL(testCase.name, 20))
		tk.MustExec("ANALYZE TABLE " + testCase.name + " WITH 5 BUCKETS, 0 TOPN")
	}

	db := testkit.CreateMockDB(tk)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			tbl, err := dom.InfoSchema().TableByName(
				ctx,
				ast.NewCIStr("bucket_hint_test"),
				ast.NewCIStr(testCase.name),
			)
			require.NoError(t, err)

			tableDiff := &common.TableDiff{
				Schema:    "bucket_hint_test",
				Table:     testCase.name,
				Info:      tbl.Meta(),
				ChunkSize: 1000,
			}
			iter, err := NewBucketIteratorWithCheckpoint(
				ctx,
				"",
				tableDiff,
				db,
				nil,
				utils.NewWorkerPool(1, "bucket-hint"),
			)
			require.NoError(t, err)

			chunkRange, err := iter.Next()
			require.NoError(t, err)
			require.NotNil(t, chunkRange)
			require.Equal(t, testCase.expectedColumns, chunkRange.IndexColumnNames)
			iter.Close()
		})
	}
}

func createAnalyzedBucketTable(t *testing.T) (*sql.DB, *model.TableInfo) {
	t.Helper()

	ctx := context.Background()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE DATABASE bucket_test")
	tk.MustExec("USE bucket_test")
	tk.MustExec("CREATE TABLE test (a INT, b VARCHAR(10), PRIMARY KEY (a, b))")
	tk.MustExec(buildBucketInsertSQL("test", 320))
	tk.MustExec("ANALYZE TABLE test WITH 5 BUCKETS, 0 TOPN")

	tbl, err := dom.InfoSchema().TableByName(
		ctx,
		ast.NewCIStr("bucket_test"),
		ast.NewCIStr("test"),
	)
	require.NoError(t, err)

	db := testkit.CreateMockDB(tk)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	return db, tbl.Meta()
}

func requireBucketChunksCoverTable(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	iter *BucketIterator,
	expectedRows int,
) int {
	t.Helper()

	seen := make(map[int]struct{}, expectedRows)
	chunkCount := 0
	for {
		chunkRange, err := iter.Next()
		require.NoError(t, err)
		if chunkRange == nil {
			break
		}
		chunkCount++

		where, args := chunkRange.ToString("")
		rows, err := db.QueryContext(ctx, "SELECT a FROM bucket_test.test WHERE "+where, args...)
		require.NoError(t, err)
		for rows.Next() {
			var value int
			require.NoError(t, rows.Scan(&value))
			_, exists := seen[value]
			require.False(t, exists, "row %d is covered by multiple chunks", value)
			seen[value] = struct{}{}
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}

	require.Positive(t, chunkCount)
	require.Len(t, seen, expectedRows)
	return chunkCount
}

func buildBucketInsertSQL(table string, rowCount int) string {
	values := make([]string, 0, rowCount)
	for i := range rowCount {
		values = append(values, fmt.Sprintf("(%d, '%d')", i, i%60))
	}
	return fmt.Sprintf("INSERT INTO %s VALUES %s", table, strings.Join(values, ","))
}

func buildHintInsertSQL(table string, rowCount int) string {
	values := make([]string, 0, rowCount)
	for i := range rowCount {
		values = append(values, fmt.Sprintf("(%d, %d, %d)", i, i%5, i))
	}
	return fmt.Sprintf("INSERT INTO %s VALUES %s", table, strings.Join(values, ","))
}
