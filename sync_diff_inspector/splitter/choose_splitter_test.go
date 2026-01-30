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

package splitter

import (
	"context"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	ttypes "github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
	"github.com/pingcap/tiflow/sync_diff_inspector/source/common"
	"github.com/pingcap/tiflow/sync_diff_inspector/utils"
	"github.com/stretchr/testify/require"
)

const bucketQueryPattern = "SELECT is_index, hist_id, bucket_id, count, lower_bound, upper_bound FROM mysql.stats_buckets WHERE table_id IN \\(\\s*SELECT tidb_table_id FROM information_schema.tables WHERE table_schema = \\? AND table_name = \\? UNION ALL SELECT tidb_partition_id FROM information_schema.partitions WHERE table_schema = \\? AND table_name = \\?\\s*\\)[\\s\\S]*ORDER BY is_index, hist_id, bucket_id"

func expectDBVersion(mock sqlmock.Sqlmock, version string) {
	mock.ExpectQuery("SELECT version\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"version()"}).AddRow(version))
}

func mustFindPrimaryIndex(t *testing.T, tableInfo *model.TableInfo) *model.IndexInfo {
	t.Helper()
	for _, index := range tableInfo.Indices {
		if index.Primary || index.Name.O == "PRIMARY" {
			return index
		}
	}
	require.FailNow(t, "primary index not found")
	return nil
}

func mockBucketsForIndex(mock sqlmock.Sqlmock, index *model.IndexInfo) {
	lowerDatum := ttypes.NewIntDatum(0)
	upperDatum := ttypes.NewIntDatum(10)
	lowerEncoded, _ := codec.EncodeKey(time.UTC, nil, lowerDatum)
	upperEncoded, _ := codec.EncodeKey(time.UTC, nil, upperDatum)

	rows := sqlmock.NewRows([]string{"is_index", "hist_id", "bucket_id", "count", "lower_bound", "upper_bound"}).
		AddRow(1, index.ID, 0, 10, lowerEncoded, upperEncoded)

	mock.ExpectQuery(bucketQueryPattern).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(rows)
}

func TestChooseSplitTypeRandomWhenNotTiDB(t *testing.T) {
	ctx := context.Background()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	expectDBVersion(mock, "8.0.33")
	expectDBVersion(mock, "8.0.33")

	tableInfo, err := utils.GetTableInfoBySQL(
		"create table `test`.`t` (`a` int, primary key(`a`))",
		parser.New(),
	)
	require.NoError(t, err)

	tableDiff := &common.TableDiff{
		Schema: "test",
		Table:  "t",
		Info:   tableInfo,
	}

	tp, candidate, err := ChooseSplitType(ctx, db, tableDiff, nil)
	require.NoError(t, err)
	require.Equal(t, chunk.Random, tp)
	require.NotNil(t, candidate)

	primary := mustFindPrimaryIndex(t, tableInfo)
	require.Equal(t, primary.ID, candidate.Index.ID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestChooseSplitTypeLimitWhenRangeSet(t *testing.T) {
	ctx := context.Background()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	expectDBVersion(mock, "5.7.25-TiDB-v6.1.0")
	expectDBVersion(mock, "5.7.25-TiDB-v6.1.0")

	tableInfo, err := utils.GetTableInfoBySQL(
		"create table `test`.`t` (`a` int, primary key(`a`))",
		parser.New(),
	)
	require.NoError(t, err)

	tableDiff := &common.TableDiff{
		Schema: "test",
		Table:  "t",
		Info:   tableInfo,
		Range:  "a > 10",
	}

	tp, candidate, err := ChooseSplitType(ctx, db, tableDiff, nil)
	require.NoError(t, err)
	require.Equal(t, chunk.Limit, tp)
	require.NotNil(t, candidate)

	primary := mustFindPrimaryIndex(t, tableInfo)
	require.Equal(t, primary.ID, candidate.Index.ID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestChooseSplitTypeBucketWhenBucketsAvailable(t *testing.T) {
	ctx := context.Background()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	expectDBVersion(mock, "5.7.25-TiDB-v6.1.0")
	expectDBVersion(mock, "5.7.25-TiDB-v6.1.0")

	tableInfo, err := utils.GetTableInfoBySQL(
		"create table `test`.`t` (`a` int, primary key(`a`))",
		parser.New(),
	)
	require.NoError(t, err)

	primary := mustFindPrimaryIndex(t, tableInfo)
	mockBucketsForIndex(mock, primary)

	tableDiff := &common.TableDiff{
		Schema: "test",
		Table:  "t",
		Info:   tableInfo,
	}

	tp, candidate, err := ChooseSplitType(ctx, db, tableDiff, nil)
	require.NoError(t, err)
	require.Equal(t, chunk.Bucket, tp)
	require.NotNil(t, candidate)
	require.Equal(t, primary.ID, candidate.Index.ID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestChooseSplitTypeCheckpointRandomFallback(t *testing.T) {
	tableInfo, err := utils.GetTableInfoBySQL(
		"create table `test`.`t` (`a` int, `b` int)",
		parser.New(),
	)
	require.NoError(t, err)

	tableDiff := &common.TableDiff{
		Schema: "test",
		Table:  "t",
		Info:   tableInfo,
		Fields: "b",
	}

	startRange := &RangeInfo{
		ChunkRange: &chunk.Range{Type: chunk.Random},
		IndexID:    999,
	}

	tp, candidate, err := ChooseSplitType(context.Background(), nil, tableDiff, startRange)
	require.NoError(t, err)
	require.Equal(t, chunk.Random, tp)
	require.NotNil(t, candidate)
	require.Equal(t, int64(-1), candidate.Index.ID)
	require.Len(t, candidate.Columns, 1)
	require.Equal(t, "b", candidate.Columns[0].Name.O)
}
