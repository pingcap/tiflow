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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dbutil

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestGetBucketsInfoWithMockStore(t *testing.T) {
	ctx := context.Background()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE DATABASE bucket_test")
	tk.MustExec("USE bucket_test")
	tk.MustExec("CREATE TABLE test (a INT, b VARCHAR(10), PRIMARY KEY (a, b), KEY idx_b (b))")
	tk.MustExec(buildInsertSQL("test", 320))
	tk.MustExec("ANALYZE TABLE test WITH 5 BUCKETS, 0 TOPN")

	tbl, err := dom.InfoSchema().TableByName(
		ctx,
		ast.NewCIStr("bucket_test"),
		ast.NewCIStr("test"),
	)
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	require.NotEmpty(t, tableInfo.Indices)

	db := testkit.CreateMockDB(tk)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	buckets, err := GetBucketsInfo(ctx, db, "bucket_test", "test", tableInfo)
	require.NoError(t, err)
	for _, indexName := range []string{"PRIMARY", "idx_b"} {
		indexInfo := tableInfo.FindIndexByName(strings.ToLower(indexName))
		require.NotNil(t, indexInfo)
		expectedCounts := getCumulativeCounts(t, ctx, db, tableInfo.ID, 1, indexInfo.ID)
		require.Len(t, expectedCounts, 5)
		require.Equal(t, int64(320), expectedCounts[len(expectedCounts)-1])

		indexBuckets := buckets[indexName]
		require.Len(t, indexBuckets, len(expectedCounts))
		for i, bucket := range indexBuckets {
			require.Equal(t, expectedCounts[i], bucket.Count)
		}
	}
}

func TestGetBucketsInfoForIntegerPrimaryKey(t *testing.T) {
	ctx := context.Background()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE DATABASE integer_pk_bucket_test")
	tk.MustExec("USE integer_pk_bucket_test")
	tk.MustExec("CREATE TABLE test (a INT PRIMARY KEY, b VARCHAR(10))")
	tk.MustExec(buildInsertSQL("test", 320))
	tk.MustExec("ANALYZE TABLE test WITH 5 BUCKETS, 0 TOPN")

	tbl, err := dom.InfoSchema().TableByName(
		ctx,
		ast.NewCIStr("integer_pk_bucket_test"),
		ast.NewCIStr("test"),
	)
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	primaryKeyColumn := tableInfo.GetPkColInfo()
	require.NotNil(t, primaryKeyColumn)
	// sync-diff adds a synthetic PRIMARY index to PKIsHandle metadata; mockstore InfoSchema does not.
	tableInfo.Indices = append(tableInfo.Indices, &model.IndexInfo{
		Name:    ast.NewCIStr("PRIMARY"),
		Primary: true,
		Unique:  true,
		State:   model.StatePublic,
		Tp:      ast.IndexTypeBtree,
		Columns: []*model.IndexColumn{{
			Name:   primaryKeyColumn.Name,
			Offset: primaryKeyColumn.Offset,
			Length: types.UnspecifiedLength,
		}},
	})

	db := testkit.CreateMockDB(tk)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	expectedCounts := getCumulativeCounts(t, ctx, db, tableInfo.ID, 0, primaryKeyColumn.ID)
	require.Len(t, expectedCounts, 5)
	require.Equal(t, int64(320), expectedCounts[len(expectedCounts)-1])

	buckets, err := GetBucketsInfo(ctx, db, "integer_pk_bucket_test", "test", tableInfo)
	require.NoError(t, err)
	primaryBuckets := buckets["PRIMARY"]
	require.Len(t, primaryBuckets, len(expectedCounts))
	for i, bucket := range primaryBuckets {
		require.Equal(t, expectedCounts[i], bucket.Count)
	}
	require.NotContains(t, buckets, primaryKeyColumn.Name.O)
}

func TestGetBucketsInfoForPartitionTable(t *testing.T) {
	testCases := []struct {
		pruneMode      string
		hasGlobalStats bool
	}{
		{pruneMode: "dynamic", hasGlobalStats: true},
		// Static pruning analyzes individual partitions without producing table-level global statistics.
		{pruneMode: "static", hasGlobalStats: false},
	}

	for _, testCase := range testCases {
		t.Run(testCase.pruneMode, func(t *testing.T) {
			ctx := context.Background()
			store, dom := testkit.CreateMockStoreAndDomain(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("SET @@tidb_partition_prune_mode = '" + testCase.pruneMode + "'")
			tk.MustExec("SET @@tidb_analyze_version = 2")
			tk.MustExec("CREATE DATABASE partition_bucket_test")
			tk.MustExec("USE partition_bucket_test")
			tk.MustExec(`CREATE TABLE test (a INT, b VARCHAR(10), PRIMARY KEY (a, b))
				PARTITION BY RANGE (a) (
					PARTITION p0 VALUES LESS THAN (160),
					PARTITION p1 VALUES LESS THAN MAXVALUE
				)`)
			tk.MustExec(buildInsertSQL("test", 320))
			tk.MustExec("ANALYZE TABLE test WITH 5 BUCKETS, 0 TOPN")

			tbl, err := dom.InfoSchema().TableByName(
				ctx,
				ast.NewCIStr("partition_bucket_test"),
				ast.NewCIStr("test"),
			)
			require.NoError(t, err)
			tableInfo := tbl.Meta()
			require.NotEmpty(t, tableInfo.Indices)

			db := testkit.CreateMockDB(tk)
			t.Cleanup(func() {
				require.NoError(t, db.Close())
			})

			var partitionBucketCount int
			err = db.QueryRowContext(
				ctx,
				`SELECT COUNT(*) FROM mysql.stats_buckets
				WHERE table_id IN (
					SELECT tidb_partition_id FROM information_schema.partitions
					WHERE table_schema = ? AND table_name = ?
				) AND is_index = 1 AND hist_id = ?`,
				"partition_bucket_test",
				"test",
				tableInfo.Indices[0].ID,
			).Scan(&partitionBucketCount)
			require.NoError(t, err)
			require.Positive(t, partitionBucketCount)

			expectedCounts := getCumulativeCounts(t, ctx, db, tableInfo.ID, 1, tableInfo.Indices[0].ID)
			buckets, err := GetBucketsInfo(ctx, db, "partition_bucket_test", "test", tableInfo)
			require.NoError(t, err)

			if !testCase.hasGlobalStats {
				require.Empty(t, expectedCounts)
				require.Empty(t, buckets)
				return
			}

			require.NotEmpty(t, expectedCounts)
			require.Equal(t, int64(320), expectedCounts[len(expectedCounts)-1])
			primaryBuckets := buckets["PRIMARY"]
			require.Len(t, primaryBuckets, len(expectedCounts))
			for i, bucket := range primaryBuckets {
				require.Equal(t, expectedCounts[i], bucket.Count)
			}
		})
	}
}

func getCumulativeCounts(
	t *testing.T,
	ctx context.Context,
	db QueryExecutor,
	tableID int64,
	isIndex int64,
	histID int64,
) []int64 {
	t.Helper()

	rows, err := db.QueryContext(
		ctx,
		`SELECT count FROM mysql.stats_buckets
		WHERE table_id = ? AND is_index = ? AND hist_id = ? ORDER BY bucket_id`,
		tableID,
		isIndex,
		histID,
	)
	require.NoError(t, err)
	defer rows.Close()

	counts := make([]int64, 0, 5)
	var cumulativeCount int64
	for rows.Next() {
		var count int64
		require.NoError(t, rows.Scan(&count))
		cumulativeCount += count
		counts = append(counts, cumulativeCount)
	}
	require.NoError(t, rows.Err())
	return counts
}

func buildInsertSQL(table string, rowCount int) string {
	values := make([]string, 0, rowCount)
	for i := range rowCount {
		values = append(values, fmt.Sprintf("(%d, '%d')", i, i%60))
	}
	return fmt.Sprintf("INSERT INTO %s VALUES %s", table, strings.Join(values, ","))
}
