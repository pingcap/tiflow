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

package diff

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser"
	dmlog "github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
	"github.com/pingcap/tiflow/sync_diff_inspector/config"
	"github.com/pingcap/tiflow/sync_diff_inspector/report"
	"github.com/pingcap/tiflow/sync_diff_inspector/source"
	"github.com/pingcap/tiflow/sync_diff_inspector/source/common"
	"github.com/pingcap/tiflow/sync_diff_inspector/splitter"
	"github.com/pingcap/tiflow/sync_diff_inspector/utils"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

// checksumRedactionSource exercises the real checksum query for the upstream
// while returning the expected checksum directly for the downstream.
type checksumRedactionSource struct {
	source.Source
	db     *sql.DB
	tables []*common.TableDiff
}

func (s *checksumRedactionSource) GetTables() []*common.TableDiff {
	return s.tables
}

func (s *checksumRedactionSource) GetCountAndMD5(ctx context.Context, r *splitter.RangeInfo) *source.ChecksumInfo {
	if s.db == nil {
		return &source.ChecksumInfo{Count: 17, Checksum: 19}
	}
	table := s.tables[r.GetTableIndex()]
	c := r.GetChunk()
	count, checksum, err := utils.GetCountAndMD5Checksum(ctx, s.db, table.Schema, table.Table, table.Info, c.Where, "", c.Args)
	return &source.ChecksumInfo{Count: count, Checksum: checksum, Err: err}
}

func TestChecksumRedaction(t *testing.T) {
	previous := dmlog.IsRedactLogEnabled()
	t.Cleanup(func() { dmlog.SetRedactLog(previous) })
	tableInfo, err := utils.GetTableInfoBySQL("CREATE TABLE test.tbl (id varchar(100) DEFAULT 'private-default')", parser.New())
	require.NoError(t, err)
	queryErr := &mysql.MySQLError{Number: 1064, Message: "syntax error near 'private-range'"}
	for _, failQuery := range []bool{false, true} {
		for _, redact := range []bool{false, true} {
			t.Run(fmt.Sprintf("query-error=%t/redact=%t", failQuery, redact), func(t *testing.T) {
				dmlog.SetRedactLog(redact)
				logs := new(zaptest.Buffer)
				level := zap.NewAtomicLevelAt(zap.DebugLevel)
				core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), logs, level)
				t.Cleanup(log.ReplaceGlobals(zap.New(core), &log.ZapProperties{Core: core, Syncer: logs, Level: level}))

				db, mock, err := sqlmock.New()
				require.NoError(t, err)
				defer db.Close()
				query := mock.ExpectQuery("SELECT COUNT.*FROM `test`\\.`tbl` WHERE `id` != 'private-range' AND `id` > \\?;").WithArgs("private-bound")
				if failQuery {
					query.WillReturnError(queryErr)
				} else {
					query.WillReturnRows(sqlmock.NewRows([]string{"CNT", "CHECKSUM"}).AddRow(17, 19))
				}
				tables := []*common.TableDiff{{Schema: "test", Table: "tbl", Info: tableInfo}}
				df := &Diff{
					upstream:   &checksumRedactionSource{db: db, tables: tables},
					downstream: &checksumRedactionSource{tables: tables},
					report:     report.NewReport(&config.TaskConfig{OutputDir: t.TempDir()}),
					sqlCh:      make(chan *ChunkDML, 1),
				}
				df.report.Init(tables, nil, nil)
				rangeInfo := &splitter.RangeInfo{ChunkRange: &chunk.Range{
					Index: &chunk.CID{ChunkCnt: 1}, Type: chunk.Random,
					Where: "`id` != 'private-range' AND `id` > ?", Args: []any{"private-bound"},
				}}
				require.Equal(t, !failQuery, df.consume(context.Background(), rangeInfo))
				var output bytes.Buffer
				require.NoError(t, df.report.Print(&output))
				require.NoError(t, mock.ExpectationsWereMet())
				result := df.report.TableResults["test"]["tbl"]
				if failQuery {
					require.Same(t, queryErr, errors.Cause(result.MeetError))
				} else {
					require.Nil(t, result.MeetError)
					require.True(t, result.DataEqual)
					require.Equal(t, int64(17), result.UpCount)
					require.Equal(t, result.UpCount, result.DownCount)
				}
				wholeOutput := logs.String() + output.String()
				if redact {
					require.NotContains(t, wholeOutput, "private-")
				} else {
					for _, value := range []string{"private-default", "private-range", "private-bound"} {
						require.Contains(t, wholeOutput, value)
					}
				}
			})
		}
	}
}
