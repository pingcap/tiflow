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
	"fmt"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/check"
	"github.com/pingcap/tidb/pkg/parser"
	ttypes "github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbutil/dbutiltest"
)

var _ = check.Suite(&testSpliterSuite{})

type testSpliterSuite struct{}

type chunkResult struct {
	chunkStr string
	args     []string
}

func (s *testSpliterSuite) TestSplitRangeByRandom(c *check.C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, check.IsNil)

	testCases := []struct {
		createTableSQL string
		splitCount     int
		originChunk    *ChunkRange
		randomValues   [][]interface{}
		expectResult   []chunkResult
	}{
		{
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))",
			3,
			NewChunkRange().copyAndUpdate("a", "0", "10").copyAndUpdate("b", "a", "z"),
			[][]interface{}{
				{5, 7},
				{"g", "n"},
			},
			[]chunkResult{
				{
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"0", "0", "a", "5", "5", "g"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"5", "5", "g", "7", "7", "n"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"7", "7", "n", "10", "10", "z"},
				},
			},
		}, {
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`b`))",
			3,
			NewChunkRange().copyAndUpdate("b", "a", "z"),
			[][]interface{}{
				{"g", "n"},
			},
			[]chunkResult{
				{
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"a", "g"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"g", "n"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"n", "z"},
				},
			},
		}, {
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`b`))",
			2,
			NewChunkRange().copyAndUpdate("b", "a", "z"),
			[][]interface{}{
				{"g"},
			},
			[]chunkResult{
				{
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"a", "g"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"g", "z"},
				},
			},
		}, {
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`b`))",
			3,
			NewChunkRange().copyAndUpdate("b", "a", "z"),
			[][]interface{}{
				{},
			},
			[]chunkResult{
				{
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"a", "z"},
				},
			},
		},
	}

	for i, testCase := range testCases {
		tableInfo, err := dbutiltest.GetTableInfoBySQL(testCase.createTableSQL, parser.New())
		c.Assert(err, check.IsNil)

		splitCols, err := getSplitFields(tableInfo, nil)
		c.Assert(err, check.IsNil)
		createFakeResultForRandomSplit(mock, 0, testCase.randomValues)

		chunks, err := splitRangeByRandom(db, testCase.originChunk, testCase.splitCount, "test", "test", splitCols, "", "")
		c.Assert(err, check.IsNil)
		for j, chunk := range chunks {
			chunkStr, args := chunk.toString("")
			c.Log(i, j, chunkStr, args)
			c.Assert(chunkStr, check.Equals, testCase.expectResult[j].chunkStr)
			c.Assert(args, check.DeepEquals, testCase.expectResult[j].args)
		}
	}
}

func (s *testSpliterSuite) TestRandomSpliter(c *check.C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, check.IsNil)

	testCases := []struct {
		createTableSQL string
		count          int
		randomValues   [][]interface{}
		expectResult   []chunkResult
	}{
		{
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))",
			10,
			[][]interface{}{
				{1, 2, 3, 4, 5},
				{"a", "b", "c", "d", "e"},
			},
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]string{"1", "1", "a"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"1", "1", "a", "2", "2", "b"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"2", "2", "b", "3", "3", "c"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"3", "3", "c", "4", "4", "d"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"4", "4", "d", "5", "5", "e"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]string{"5", "5", "e"},
				},
			},
		}, {
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`b`))",
			10,
			[][]interface{}{
				{"a", "b", "c", "d", "e"},
			},
			[]chunkResult{
				{
					"(`b` <= ?)",
					[]string{"a"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"a", "b"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"b", "c"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"c", "d"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"d", "e"},
				}, {
					"(`b` > ?)",
					[]string{"e"},
				},
			},
		},
	}

	for i, testCase := range testCases {
		tableInfo, err := dbutiltest.GetTableInfoBySQL(testCase.createTableSQL, parser.New())
		c.Assert(err, check.IsNil)

		tableInstance := &TableInstance{
			Conn:   db,
			Schema: "test",
			Table:  "test",
			info:   tableInfo,
		}

		splitCols, err := getSplitFields(tableInfo, nil)
		c.Assert(err, check.IsNil)

		createFakeResultForRandomSplit(mock, testCase.count, testCase.randomValues)

		rSpliter := new(randomSpliter)
		chunks, err := rSpliter.split(tableInstance, splitCols, 2, "TRUE", "")
		c.Assert(err, check.IsNil)

		for j, chunk := range chunks {
			chunkStr, args := chunk.toString("")
			c.Log(i, j, chunkStr, args)
			c.Assert(chunkStr, check.Equals, testCase.expectResult[j].chunkStr)
			c.Assert(args, check.DeepEquals, testCase.expectResult[j].args)
		}
	}
}

func createFakeResultForRandomSplit(mock sqlmock.Sqlmock, count int, randomValues [][]interface{}) {
	if count > 0 {
		// generate fake result for get the row count of this table
		countRows := sqlmock.NewRows([]string{"cnt"}).AddRow(count)
		mock.ExpectQuery("SELECT COUNT.*").WillReturnRows(countRows)
	}

	// generate fake result for get random value for column a
	for _, randomVs := range randomValues {
		randomRows := sqlmock.NewRows([]string{"a"})
		for _, value := range randomVs {
			randomRows.AddRow(value)
		}
		mock.ExpectQuery("ORDER BY rand_value").WillReturnRows(randomRows)
	}
}

func (s *testSpliterSuite) TestBucketSpliter(c *check.C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, check.IsNil)

	createTableSQL := "create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo, err := dbutiltest.GetTableInfoBySQL(createTableSQL, parser.New())
	c.Assert(err, check.IsNil)

	testCases := []struct {
		chunkSize     int
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
					[]string{"32", "32", "6"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"32", "32", "6", "63", "63", "11"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"63", "63", "11", "96", "96", "18"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"96", "96", "18", "127", "127", "23"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"127", "127", "23", "160", "160", "30"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"160", "160", "30", "191", "191", "35"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"191", "191", "35", "224", "224", "42"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"224", "224", "42", "255", "255", "47"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"255", "255", "47", "288", "288", "54"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"288", "288", "54", "319", "319", "59"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]string{"319", "319", "59"},
				},
			},
		}, {
			// chunk size less than the count of bucket 64, but 64 is  less than 2*40, so will not split every bucket
			40,
			nil,
			nil,
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]string{"63", "63", "11"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"63", "63", "11", "127", "127", "23"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"127", "127", "23", "191", "191", "35"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"191", "191", "35", "255", "255", "47"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"255", "255", "47", "319", "319", "59"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]string{"319", "319", "59"},
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
					[]string{"63", "63", "11"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"63", "63", "11", "127", "127", "23"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"127", "127", "23", "191", "191", "35"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"191", "191", "35", "255", "255", "47"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"255", "255", "47", "319", "319", "59"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]string{"319", "319", "59"},
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
					[]string{"127", "127", "23"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"127", "127", "23", "255", "255", "47"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]string{"255", "255", "47"},
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
					[]string{"127", "127", "23"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"127", "127", "23", "255", "255", "47"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]string{"255", "255", "47"},
				},
			},
		}, {
			// chunk size is greate than the double count of bucket 64, will combine three bucket into one chunk
			129,
			nil,
			nil,
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]string{"191", "191", "35"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]string{"191", "191", "35"},
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

	tableInstance := &TableInstance{
		Conn:   db,
		Schema: "test",
		Table:  "test",
		info:   tableInfo,
	}

	for i, testCase := range testCases {
		createFakeResultForBucketSplit(mock, testCase.aRandomValues, testCase.bRandomValues)
		bSpliter := new(bucketSpliter)
		chunks, err := bSpliter.split(tableInstance, testCase.chunkSize, "TRUE", "")
		c.Assert(err, check.IsNil)
		for j, chunk := range chunks {
			chunkStr, args := chunk.toString("")
			c.Log(i, j, chunkStr, args)
			c.Assert(chunkStr, check.Equals, testCase.expectResult[j].chunkStr)
			c.Assert(args, check.DeepEquals, testCase.expectResult[j].args)
		}
	}
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

	// Mock query with subquery to get all table_ids (main table + partitions) at once.
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
	mock.ExpectQuery("SELECT is_index, hist_id, bucket_id, count, lower_bound, upper_bound FROM mysql.stats_buckets WHERE table_id IN \\(\\s*SELECT tidb_table_id FROM information_schema.tables WHERE table_schema = \\? AND table_name = \\? UNION ALL SELECT tidb_partition_id FROM information_schema.partitions WHERE table_schema = \\? AND table_name = \\?\\s*\\) ORDER BY is_index, hist_id, bucket_id").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(statsRows)

	for i := 0; i < len(aRandomValues); i++ {
		aRandomRows := sqlmock.NewRows([]string{"a"})
		aRandomRows.AddRow(aRandomValues[i])
		mock.ExpectQuery("ORDER BY rand_value").WillReturnRows(aRandomRows)

		bRandomRows := sqlmock.NewRows([]string{"b"})
		bRandomRows.AddRow(bRandomValues[i])
		mock.ExpectQuery("ORDER BY rand_value").WillReturnRows(bRandomRows)
	}
}
