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
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

func TestGetBucketsInfoDecodeErrorField(t *testing.T) {
	buffer := new(zaptest.Buffer)
	level := zap.NewAtomicLevelAt(zap.DebugLevel)
	core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), buffer, level)
	restore := log.ReplaceGlobals(zap.New(core), &log.ZapProperties{Core: core, Syncer: buffer, Level: level})
	t.Cleanup(restore)
	tableInfo := &model.TableInfo{Columns: []*model.ColumnInfo{{
		ID: 1, Name: ast.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeDatetime),
	}}}
	for _, customize := range []bool{false, true} {
		t.Run(fmt.Sprintf("customize=%t", customize), func(t *testing.T) {
			buffer.Reset()
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			mock.ExpectQuery("SELECT is_index, hist_id, bucket_id, count, lower_bound, upper_bound").
				WithArgs("test", "test").
				WillReturnRows(sqlmock.NewRows([]string{"is_index", "hist_id", "bucket_id", "count", "lower_bound", "upper_bound"}).
					AddRow(0, 1, 0, 17, []byte("private-lower"), []byte("private-upper")))
			var opts []GetBucketsInfoOption
			var decodeErrors []error
			if customize {
				opts = append(opts, WithBucketDecodeErrorField(func(err error) zap.Field {
					decodeErrors = append(decodeErrors, err)
					return zap.String("error", "masked")
				}))
			}
			buckets, err := GetBucketsInfo(context.Background(), db, "test", "test", tableInfo, opts...)
			require.NoError(t, err)
			require.Equal(t, []Bucket{{
				Count:      17,
				LowerBound: fmt.Sprintf("0x%x", "private-lower"),
				UpperBound: fmt.Sprintf("0x%x", "private-upper"),
			}}, buckets["id"])
			require.NoError(t, mock.ExpectationsWereMet())
			if customize {
				require.Len(t, decodeErrors, 2)
				require.ErrorContains(t, decodeErrors[0], "private-lower")
				require.ErrorContains(t, decodeErrors[1], "private-upper")
				require.Contains(t, buffer.String(), `"error":"masked"`)
				require.NotContains(t, buffer.String(), "private-")
				require.NotContains(t, buffer.String(), "errorVerbose")
			} else {
				require.Contains(t, buffer.String(), "private-lower")
				require.Contains(t, buffer.String(), "private-upper")
				require.Contains(t, buffer.String(), "errorVerbose")
			}
		})
	}
}
