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

package splitter

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser"
	dmlog "github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/sync_diff_inspector/source/common"
	"github.com/pingcap/tiflow/sync_diff_inspector/utils"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

func captureSplitterLogs(t *testing.T) *zaptest.Buffer {
	t.Helper()
	buffer := new(zaptest.Buffer)
	level := zap.NewAtomicLevelAt(zap.DebugLevel)
	core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), buffer, level)
	restore := log.ReplaceGlobals(zap.New(core), &log.ZapProperties{Core: core, Syncer: buffer, Level: level})
	redact := dmlog.IsRedactLogEnabled()
	t.Cleanup(func() {
		restore()
		dmlog.SetRedactLog(redact)
	})
	return buffer
}

func findSplitterLog(t *testing.T, buffer *zaptest.Buffer, message string) map[string]any {
	t.Helper()
	for _, line := range buffer.Lines() {
		var entry map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &entry))
		if entry["msg"] == message {
			return entry
		}
	}
	t.Fatalf("log message %q not found", message)
	return nil
}

func TestGetRowCountRedaction(t *testing.T) {
	buffer := captureSplitterLogs(t)
	testCases := []struct {
		name  string
		where string
		args  []any
	}{
		{name: "all rows"},
		{name: "configured range", where: "`id` > 'private-range'"},
		{name: "checkpoint bound", where: "`id` > ?", args: []any{"private-bound"}},
	}
	for _, redact := range []bool{false, true} {
		for _, tc := range testCases {
			t.Run(fmt.Sprintf("%s/redact=%t", tc.name, redact), func(t *testing.T) {
				dmlog.SetRedactLog(redact)
				buffer.Reset()
				db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
				require.NoError(t, err)
				defer db.Close()
				query := "SELECT COUNT(1) cnt FROM `test`.`test`"
				if tc.where != "" {
					query += " WHERE " + tc.where
				}
				args := make([]driver.Value, len(tc.args))
				for i, arg := range tc.args {
					args[i] = arg
				}
				mock.ExpectQuery(query).WithArgs(args...).WillReturnRows(sqlmock.NewRows([]string{"cnt"}).AddRow(17))
				count, err := getRowCount(context.Background(), db, "test", "test", tc.where, tc.args)
				require.NoError(t, err)
				require.Equal(t, int64(17), count)
				require.NoError(t, mock.ExpectationsWereMet())
				entry := findSplitterLog(t, buffer, "get row count")
				if redact {
					require.Equal(t, "?", entry["sql"])
					require.Equal(t, "?", entry["args"])
					require.NotContains(t, buffer.String(), "private-")
				} else {
					require.Equal(t, query, entry["sql"])
					if tc.args == nil {
						require.Nil(t, entry["args"])
					} else {
						require.Equal(t, tc.args, entry["args"])
					}
				}
			})
		}
	}
}

func TestGetRowCountErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	queryErr := errors.New("query failed")
	mock.ExpectQuery("SELECT COUNT").WillReturnError(queryErr)
	count, err := getRowCount(context.Background(), db, "test", "test", "", nil)
	require.Zero(t, count)
	require.Equal(t, queryErr, errors.Cause(err))

	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"cnt"}).AddRow(nil))
	count, err = getRowCount(context.Background(), db, "test", "test", "", nil)
	require.Zero(t, count)
	require.EqualError(t, err, "table `test`.`test` not found")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBucketIteratorRedaction(t *testing.T) {
	buffer := captureSplitterLogs(t)
	tableInfo, err := utils.GetTableInfoBySQL("CREATE TABLE test.test (id VARCHAR(64) PRIMARY KEY)", parser.New())
	require.NoError(t, err)
	for _, redact := range []bool{false, true} {
		t.Run(fmt.Sprintf("redact=%t", redact), func(t *testing.T) {
			dmlog.SetRedactLog(redact)
			buffer.Reset()
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			mock.ExpectQuery("SELECT is_index, hist_id, bucket_id, count, lower_bound, upper_bound").
				WithArgs("test", "test").
				WillReturnRows(sqlmock.NewRows([]string{"is_index", "hist_id", "bucket_id", "count", "lower_bound", "upper_bound"}).
					AddRow(0, tableInfo.Columns[0].ID, 0, 17, []byte("private-lower"), []byte("private-upper")))
			iter := &BucketIterator{
				table:     &common.TableDiff{Schema: "test", Table: "test", Info: tableInfo},
				chunkSize: 1,
				dbConn:    db,
			}
			require.NoError(t, iter.init(context.Background(), nil))
			require.Len(t, iter.buckets, 1)
			require.Equal(t, "private-lower", iter.buckets[0].LowerBound)
			require.Equal(t, "private-upper", iter.buckets[0].UpperBound)
			require.Equal(t, int64(17), iter.buckets[0].Count)
			require.NoError(t, mock.ExpectationsWereMet())
			entry := findSplitterLog(t, buffer, "buckets for index")
			require.Equal(t, "PRIMARY", entry["index"])
			if redact {
				require.Equal(t, "?", entry["buckets"])
				require.NotContains(t, buffer.String(), "private-")
			} else {
				require.Contains(t, buffer.String(), "private-lower")
				require.Contains(t, buffer.String(), "private-upper")
				require.IsType(t, map[string]any{}, entry["buckets"])
			}
		})
	}
}
