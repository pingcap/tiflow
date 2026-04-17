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

package source

import (
	"context"
	"database/sql"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tiflow/sync_diff_inspector/config"
	"github.com/pingcap/tiflow/sync_diff_inspector/source/common"
	"github.com/pingcap/tiflow/sync_diff_inspector/splitter"
	"github.com/pingcap/tiflow/sync_diff_inspector/utils"
	"github.com/stretchr/testify/require"
)

// makeAnalyzer returns a TiDBTableAnalyzer and a *common.TableDiff whose
// SplitterStrategy is the provided value. The caller is responsible for
// setting up sqlmock expectations.
func makeAnalyzer(t *testing.T, db *sql.DB, strategy string) (*TiDBTableAnalyzer, *common.TableDiff) {
	t.Helper()
	tableInfo, err := utils.GetTableInfoBySQL(
		"CREATE TABLE `t` (`id` BIGINT PRIMARY KEY, `v` INT)",
		parser.New(),
	)
	require.NoError(t, err)

	td := &common.TableDiff{
		Schema:           "test",
		Table:            "t",
		Info:             tableInfo,
		SplitterStrategy: strategy,
		// ChunkSize > 0 so that NewLimitIteratorWithCheckpoint skips
		// getRowCount (which would otherwise issue a SELECT COUNT(1) query).
		ChunkSize: 1000,
	}
	a := &TiDBTableAnalyzer{
		dbConn:            db,
		bucketSpliterPool: utils.NewWorkerPool(1, "bucketIter"),
		sourceTableMap: map[string]*common.TableSource{
			dbutil.TableName("test", "t"): {
				OriginSchema: "test",
				OriginTable:  "t",
			},
		},
	}
	return a, td
}

// TestAnalyzeSplitterEnforcesLimitStrategy verifies that when SplitterStrategy
// is "limit", AnalyzeSplitter returns a *splitter.LimitIterator without ever
// touching the bucket-stats queries.
//
// Step-1 finding: GetBetterIndex short-circuits to the primary key without
// issuing any SQL; with ChunkSize=1000, getRowCount is also skipped. So no
// mock expectations are needed, and ExpectationsWereMet will catch any
// unexpected query (e.g., a SHOW STATS_BUCKETS from the bucket path).
func TestAnalyzeSplitterEnforcesLimitStrategy(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// No SQL expectations: if AnalyzeSplitter calls the bucket path it will
	// issue stats queries that sqlmock rejects, making ExpectationsWereMet fail.

	a, td := makeAnalyzer(t, db, config.SplitterStrategyLimit)

	iter, err := a.AnalyzeSplitter(context.Background(), td, nil)
	require.NoError(t, err)
	_, ok := iter.(*splitter.LimitIterator)
	require.True(t, ok, "expected *splitter.LimitIterator, got %T", iter)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestAnalyzeSplitterEnforcesRandomStrategy verifies that when SplitterStrategy
// is "random", AnalyzeSplitter returns a *splitter.RandomIterator without ever
// touching the bucket-stats queries.
//
// Step-1 finding: NewRandomIteratorWithCheckpoint always calls getRowCount
// (SELECT COUNT(1)) when startRange is nil. We mock it to return 0 so that
// splitRangeByRandom exits immediately with one chunk and no further SQL.
// The absence of any SHOW STATS_BUCKETS / information_schema.tidb_indexes
// expectation is what proves the bucket path was never taken.
func TestAnalyzeSplitterEnforcesRandomStrategy(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// NewRandomIteratorWithCheckpoint calls getRowCount → SELECT COUNT(1) cnt FROM `test`.`t`
	// We return 0 rows so splitRangeByRandom sees count=0 and returns early.
	countRows := sqlmock.NewRows([]string{"cnt"}).AddRow(0)
	mock.ExpectQuery("SELECT COUNT\\(1\\) cnt FROM").WillReturnRows(countRows)

	a, td := makeAnalyzer(t, db, config.SplitterStrategyRandom)

	iter, err := a.AnalyzeSplitter(context.Background(), td, nil)
	require.NoError(t, err)
	_, ok := iter.(*splitter.RandomIterator)
	require.True(t, ok, "expected *splitter.RandomIterator, got %T", iter)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestAnalyzeSplitterAutoFallsBackToRandomOnBucketError verifies that the
// "auto" strategy preserves the historical fallback behaviour: when the bucket
// iterator cannot be built (here because sqlmock rejects the stats queries),
// AnalyzeSplitter falls back to a *splitter.RandomIterator.
func TestAnalyzeSplitterAutoFallsBackToRandomOnBucketError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Bucket iterator will fail because we provide no stats-query expectations.
	// After the fallback, NewRandomIteratorWithCheckpoint calls getRowCount.
	countRows := sqlmock.NewRows([]string{"cnt"}).AddRow(0)
	mock.ExpectQuery("SELECT COUNT\\(1\\) cnt FROM").WillReturnRows(countRows)

	a, td := makeAnalyzer(t, db, config.SplitterStrategyAuto)

	iter, err := a.AnalyzeSplitter(context.Background(), td, nil)
	require.NoError(t, err)
	_, ok := iter.(*splitter.RandomIterator)
	require.True(t, ok, "expected *splitter.RandomIterator (auto fallback), got %T", iter)
	require.NoError(t, mock.ExpectationsWereMet())
}
