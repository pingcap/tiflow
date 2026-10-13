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
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	dmlog "github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
	"github.com/pingcap/tiflow/sync_diff_inspector/source/common"
	"github.com/pingcap/tiflow/sync_diff_inspector/utils"
	"github.com/stretchr/testify/require"
)

func TestBucketIteratorDecodeErrorRedaction(t *testing.T) {
	buffer := captureSplitterLogs(t)
	tableInfo, err := utils.GetTableInfoBySQL("CREATE TABLE test.test (id DATETIME PRIMARY KEY)", parser.New())
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
			require.Equal(t, []dbutil.Bucket{{
				Count:      17,
				LowerBound: fmt.Sprintf("0x%x", "private-lower"),
				UpperBound: fmt.Sprintf("0x%x", "private-upper"),
			}}, iter.buckets)
			require.NoError(t, mock.ExpectationsWereMet())
			for _, bound := range []string{"lower", "upper"} {
				entry := findSplitterLog(t, buffer, "Failed to decode "+bound+"_bound for column")
				require.Equal(t, float64(tableInfo.Columns[0].ID), entry["histID"])
				if redact {
					require.Equal(t, "?", entry["error"])
					require.NotContains(t, entry, "errorVerbose")
				} else {
					require.Contains(t, entry["error"], "private-"+bound)
				}
			}
			if redact {
				require.NotContains(t, buffer.String(), "private-")
			} else {
				require.Contains(t, buffer.String(), "private-lower")
				require.Contains(t, buffer.String(), "private-upper")
			}
		})
	}
}

func TestBucketIteratorTimestampErrorRedaction(t *testing.T) {
	buffer := captureSplitterLogs(t)
	tableInfo, err := utils.GetTableInfoBySQL("CREATE TABLE test.test (id TIMESTAMP PRIMARY KEY)", parser.New())
	require.NoError(t, err)
	_, expectedErr := dbutil.AnalyzeValuesFromBuckets("(private-bound)", tableInfo.Columns)
	require.ErrorContains(t, expectedErr, "private-bound")
	expectedLog := findSplitterLog(t, buffer, "analyze values from buckets")
	delete(expectedLog, "ts")
	for _, redact := range []bool{false, true} {
		for _, checkpoint := range []bool{false, true} {
			t.Run(fmt.Sprintf("redact=%t/checkpoint=%t", redact, checkpoint), func(t *testing.T) {
				dmlog.SetRedactLog(redact)
				buffer.Reset()
				iter := &BucketIterator{
					buckets:      []dbutil.Bucket{{Count: 17, UpperBound: "(private-bound)"}},
					table:        &common.TableDiff{Schema: "test", Table: "test", Info: tableInfo},
					indexColumns: tableInfo.Columns,
					chunkSize:    1,
					chunksCh:     make(chan []*chunk.Range, 1),
					errCh:        make(chan error, 1),
				}
				var startRange *RangeInfo
				if checkpoint {
					startRange = &RangeInfo{ChunkRange: &chunk.Range{
						Index:  &chunk.CID{BucketIndexRight: 0},
						Bounds: []*chunk.Bound{{Column: "id", Upper: "checkpoint-upper", HasUpper: true}},
					}}
				}
				iter.produceChunks(context.Background(), startRange)
				require.Len(t, iter.errCh, 1)
				actualErr := <-iter.errCh
				require.Equal(t, errors.Cause(expectedErr), errors.Cause(actualErr))
				require.EqualError(t, actualErr, expectedErr.Error())
				entry := findSplitterLog(t, buffer, "analyze values from buckets")
				require.Equal(t, "id", entry["column"])
				if redact {
					require.Equal(t, "?", entry["value"])
					require.Equal(t, "?", entry["error"])
					require.NotContains(t, buffer.String(), "private-bound")
				} else {
					delete(entry, "ts")
					require.Equal(t, expectedLog, entry)
				}
			})
		}
	}
}
